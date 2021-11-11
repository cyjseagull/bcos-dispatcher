#include "BlockExecutive.h"
#include "ChecksumAddress.h"
#include "SchedulerImpl.h"
#include "bcos-framework/libstorage/StateStorage.h"
#include "bcos-scheduler/Common.h"
#include "interfaces/executor/ExecutionMessage.h"
#include "interfaces/executor/ParallelTransactionExecutorInterface.h"
#include "libutilities/Error.h"
#include <tbb/parallel_for_each.h>
#include <boost/algorithm/hex.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/latch.hpp>
#include <atomic>
#include <chrono>
#include <iterator>
#include <thread>
#include <utility>

using namespace bcos::scheduler;
using namespace bcos::ledger;

void BlockExecutive::asyncExecute(
    std::function<void(Error::UniquePtr, protocol::BlockHeader::Ptr)> callback)
{
    if (m_result)
    {
        callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::InvalidStatus, "Invalid status"), nullptr);
        return;
    }

    m_currentTimePoint = std::chrono::system_clock::now();

    bool withDAG = false;
    if (m_block->transactionsMetaDataSize() > 0)
    {
        SCHEDULER_LOG(DEBUG) << LOG_KV("block number", m_block->blockHeaderConst()->number())
                             << LOG_KV("meta tx count", m_block->transactionsMetaDataSize());

        m_executiveResults.resize(m_block->transactionsMetaDataSize());
        for (size_t i = 0; i < m_block->transactionsMetaDataSize(); ++i)
        {
            auto metaData = m_block->transactionMetaData(i);

            auto message = m_scheduler->m_executionMessageFactory->createExecutionMessage();
            message->setContextID(i + m_startContextID);
            message->setType(protocol::ExecutionMessage::TXHASH);
            message->setTransactionHash(metaData->hash());

            if (metaData->to().empty())
            {
                message->setCreate(true);
            }
            else
            {
                message->setTo(preprocessAddress(metaData->to()));
            }

            message->setDepth(0);
            message->setGasAvailable(TRANSACTION_GAS);
            message->setStaticCall(false);

            auto& executive = m_executiveStates.emplace_back(i, std::move(message));

            if (metaData)
            {
                m_executiveResults[i].transactionHash = metaData->hash();
                m_executiveResults[i].source = metaData->source();
            }

            if (metaData->attribute() & bcos::protocol::Transaction::Attribute::DAG)
            {
                executive.enableDAG = true;
                withDAG = true;
            }
        }
    }
    else if (m_block->transactionsSize() > 0)
    {
        SCHEDULER_LOG(DEBUG) << LOG_KV("block number", m_block->blockHeaderConst()->number())
                             << LOG_KV("tx count", m_block->transactionsSize());

        m_executiveResults.resize(m_block->transactionsSize());
        for (size_t i = 0; i < m_block->transactionsSize(); ++i)
        {
            auto tx = m_block->transaction(i);
            m_executiveResults[i].transactionHash = tx->hash();
            m_executiveResults[i].source = tx->source();

            auto message = m_scheduler->m_executionMessageFactory->createExecutionMessage();
            message->setType(protocol::ExecutionMessage::MESSAGE);
            message->setContextID(i + m_startContextID);

            message->setOrigin(toHex(tx->sender()));
            message->setFrom(std::string(message->origin()));

            if (tx->to().empty())
            {
                message->setCreate(true);
            }
            else
            {
                message->setTo(preprocessAddress(tx->to()));
            }

            message->setDepth(0);
            message->setGasAvailable(TRANSACTION_GAS);
            message->setData(tx->input().toBytes());
            message->setStaticCall(m_staticCall);

            m_executiveStates.emplace_back(i, std::move(message));

            if (tx->attribute() & bcos::protocol::Transaction::Attribute::DAG)
            {
                // TODO: Executor must support dag execute
                SCHEDULER_LOG(ERROR) << "Execute transactions with dag!";
                callback(BCOS_ERROR_UNIQUE_PTR(
                             SchedulerError::UnknownError, "Execute transactions with dag!"),
                    nullptr);
                return;
            }
        }
    }

    if (!m_staticCall)
    {
        // Execute nextBlock
        batchNextBlock([this, withDAG, callback = std::move(callback)](Error::UniquePtr error) {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Next block with error!" << boost::diagnostic_information(*error);
                callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                             SchedulerError::NextBlockError, "Next block error!", *error),
                    nullptr);
                return;
            }

            if (withDAG)
            {
                DAGExecute([this, callback = std::move(callback)](Error::UniquePtr error) {
                    if (error)
                    {
                        SCHEDULER_LOG(ERROR) << "DAG execute block with error!"
                                             << boost::diagnostic_information(*error);
                        callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                                     SchedulerError::DAGError, "DAG execute error!", *error),
                            nullptr);
                        return;
                    }

                    DMTExecute(std::move(callback));
                });
            }
            else
            {
                DMTExecute(std::move(callback));
            }
        });
    }
    else
    {
        DMTExecute(std::move(callback));
    }
}

void BlockExecutive::asyncCommit(std::function<void(Error::UniquePtr)> callback)
{
    auto stateStorage = std::make_shared<storage::StateStorage>(m_scheduler->m_storage);

    m_currentTimePoint = std::chrono::system_clock::now();

    m_scheduler->m_ledger->asyncPrewriteBlock(stateStorage, m_block,
        [this, stateStorage, callback = std::move(callback)](Error::Ptr&& error) mutable {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Prewrite block error!" << boost::diagnostic_information(*error);
                callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                    SchedulerError::PrewriteBlockError, "Prewrite block error!", *error));
                return;
            }

            auto status = std::make_shared<CommitStatus>();
            status->total = 1 + m_scheduler->m_executorManager->size();  // self + all executors
            status->checkAndCommit = [this, callback = std::move(callback)](
                                         const CommitStatus& status) {
                if (status.success + status.failed < status.total)
                {
                    return;
                }

                if (status.failed > 0)
                {
                    SCHEDULER_LOG(WARNING) << "Prepare with errors! " +
                                                  boost::lexical_cast<std::string>(status.failed);
                    batchBlockRollback([this, callback](Error::UniquePtr&& error) {
                        if (error)
                        {
                            SCHEDULER_LOG(ERROR)
                                << "Rollback storage failed!" << LOG_KV("number", number()) << " "
                                << boost::diagnostic_information(*error);
                            // FATAL ERROR, NEED MANUAL FIX!

                            callback(std::move(error));
                            return;
                        }
                    });

                    return;
                }

                batchBlockCommit([this, callback](Error::UniquePtr&& error) {
                    if (error)
                    {
                        SCHEDULER_LOG(ERROR)
                            << "Commit block to storage failed!" << LOG_KV("number", number())
                            << boost::diagnostic_information(*error);

                        // FATAL ERROR, NEED MANUAL FIX!

                        callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(SchedulerError::UnknownError,
                            "Commit block to storage failed!", *error));
                        return;
                    }

                    m_commitElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now() - m_currentTimePoint);
                    SCHEDULER_LOG(INFO) << "CommitBlock: " << number()
                                        << " success, execute elapsed: " << m_executeElapsed.count()
                                        << "ms hash elapsed: " << m_hashElapsed.count()
                                        << "ms commit elapsed: " << m_commitElapsed.count() << "ms";

                    callback(nullptr);
                });
            };

            storage::TransactionalStorageInterface::TwoPCParams params;
            params.number = number();
            params.primaryTableName = SYS_CURRENT_STATE;
            params.primaryTableKey = SYS_KEY_CURRENT_NUMBER;
            m_scheduler->m_storage->asyncPrepare(
                params, stateStorage, [status, this](Error::Ptr&& error, uint64_t startTimeStamp) {
                    if (error)
                    {
                        ++status->failed;
                    }
                    else
                    {
                        ++status->success;
                    }

                    executor::ParallelTransactionExecutorInterface::TwoPCParams executorParams;
                    executorParams.number = number();
                    executorParams.primaryTableName = SYS_CURRENT_STATE;
                    executorParams.primaryTableKey = SYS_KEY_CURRENT_NUMBER;
                    executorParams.startTS = startTimeStamp;
                    for (auto& executorIt : *(m_scheduler->m_executorManager))
                    {
                        executorIt->prepare(executorParams, [status](Error::Ptr&& error) {
                            if (error)
                            {
                                ++status->failed;
                            }
                            else
                            {
                                ++status->success;
                                SCHEDULER_LOG(DEBUG)
                                    << "Prepare executor success, success: " << status->success;
                            }
                            status->checkAndCommit(*status);
                        });
                    }
                });
        });
}

void BlockExecutive::asyncNotify(
    std::function<void(bcos::protocol::BlockNumber, bcos::protocol::TransactionSubmitResultsPtr,
        std::function<void(Error::Ptr)>)>& notifier,
    std::function<void(Error::Ptr)> _callback)
{
    if (!notifier)
    {
        return;
    }
    auto results = std::make_shared<bcos::protocol::TransactionSubmitResults>();
    auto blockHeader = m_block->blockHeaderConst();
    auto blockHash = blockHeader->hash();
    size_t index = 0;
    for (auto& it : m_executiveResults)
    {
        auto submitResult = m_transactionSubmitResultFactory->createTxSubmitResult();
        submitResult->setTransactionIndex(index);
        submitResult->setBlockHash(blockHash);
        submitResult->setTxHash(it.transactionHash);
        submitResult->setStatus(it.receipt->status());
        submitResult->setTransactionReceipt(it.receipt);
        if (m_syncBlock)
        {
            auto tx = m_block->transaction(index);
            submitResult->setNonce(tx->nonce());
        }
        index++;
        results->emplace_back(submitResult);
    }
    auto txsSize = m_executiveResults.size();
    notifier(blockHeader->number(), results, [_callback, blockHeader, txsSize](Error::Ptr _error) {
        if (_callback)
        {
            _callback(_error);
        }
        if (_error == nullptr)
        {
            SCHEDULER_LOG(INFO) << LOG_DESC("notify block result success")
                                << LOG_KV("number", blockHeader->number())
                                << LOG_KV("hash", blockHeader->hash().abridged())
                                << LOG_KV("txsSize", txsSize);
            return;
        }
        SCHEDULER_LOG(INFO) << LOG_DESC("notify block result failed")
                            << LOG_KV("code", _error->errorCode())
                            << LOG_KV("msg", _error->errorMessage());
    });
}

void BlockExecutive::DAGExecute(std::function<void(Error::UniquePtr)> callback)
{
    std::multimap<std::string, decltype(m_executiveStates)::iterator> requests;

    for (auto it = m_executiveStates.begin(); it != m_executiveStates.end(); ++it)
    {
        if (it->enableDAG)
        {
            requests.emplace(it->message->to(), it);
        }
    }

    if (requests.empty())
    {
        callback(nullptr);
    }

    auto totalCount = std::make_shared<std::atomic_size_t>(requests.size());
    auto failed = std::make_shared<std::atomic_size_t>(0);
    auto callbackPtr = std::make_shared<decltype(callback)>(std::move(callback));

    for (auto it = requests.begin(); it != requests.end(); it = requests.upper_bound(it->first))
    {
        SCHEDULER_LOG(TRACE) << "DAG contract: " << it->first;

        auto executor = m_scheduler->m_executorManager->dispatchExecutor(it->first);
        auto count = requests.count(it->first);
        auto range = requests.equal_range(it->first);

        auto messages = std::make_shared<std::vector<protocol::ExecutionMessage::UniquePtr>>(count);
        auto iterators =
            std::make_shared<std::vector<decltype(m_executiveStates)::iterator>>(count);
        size_t i = 0;
        for (auto messageIt = range.first; messageIt != range.second; ++messageIt)
        {
            SCHEDULER_LOG(TRACE) << "message: " << messageIt->second->message.get()
                                 << " to: " << messageIt->first;
            messageIt->second->callStack.push(messageIt->second->currentSeq++);
            messages->at(i) = std::move(messageIt->second->message);
            iterators->at(i) = messageIt->second;

            ++i;
        }

        executor->dagExecuteTransactions(*messages,
            [messages, iterators, totalCount, failed, callbackPtr](bcos::Error::UniquePtr error,
                std::vector<bcos::protocol::ExecutionMessage::UniquePtr> responseMessages) {
                *totalCount -= responseMessages.size();

                if (error)
                {
                    ++(*failed);
                    SCHEDULER_LOG(ERROR)
                        << "DAG execute error: " << boost::diagnostic_information(*error);
                }
                else if (messages->size() != responseMessages.size())
                {
                    ++(*failed);
                    SCHEDULER_LOG(ERROR) << "DAG messages mismatch!";
                }
                else
                {
                    for (size_t i = 0; i < responseMessages.size(); ++i)
                    {
                        (*iterators)[i]->message = std::move(responseMessages[i]);
                    }
                }

                if (*totalCount == 0)
                {
                    if (*failed > 0)
                    {
                        (*callbackPtr)(BCOS_ERROR_UNIQUE_PTR(
                            SchedulerError::DAGError, "Execute dag with errors"));
                        return;
                    }

                    (*callbackPtr)(nullptr);
                }
            });
    }
}

void BlockExecutive::DMTExecute(
    std::function<void(Error::UniquePtr, protocol::BlockHeader::Ptr)> callback)
{
    startBatch([this, callback = std::move(callback)](Error::UniquePtr&& error) {
        auto recursionCallback = std::make_shared<std::function<void(Error::UniquePtr)>>();

        *recursionCallback = [this, recursionCallback, callback = std::move(callback)](
                                 Error::UniquePtr error) {
            if (error)
            {
                callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                             SchedulerError::DMTError, "Execute with errors", *error),
                    nullptr);
                return;
            }

            if (!m_executiveStates.empty())
            {
                SCHEDULER_LOG(TRACE) << "Non empty states, continue startBatch";
                m_calledContract.clear();

                startBatch(*recursionCallback);
            }
            else
            {
                SCHEDULER_LOG(TRACE) << "Empty states, end";
                auto now = std::chrono::system_clock::now();
                m_executeElapsed =
                    std::chrono::duration_cast<std::chrono::milliseconds>(now - m_currentTimePoint);
                m_currentTimePoint = now;

                if (m_staticCall)
                {
                    // Set result to m_block
                    for (auto& it : m_executiveResults)
                    {
                        m_block->appendReceipt(it.receipt);
                    }
                    callback(nullptr, nullptr);
                }
                else
                {
                    // All Transaction finished, get hash
                    batchGetHashes([this, callback = std::move(callback)](
                                       Error::UniquePtr error, crypto::HashType hash) {
                        if (error)
                        {
                            callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                                         SchedulerError::UnknownError, "Unknown error", *error),
                                nullptr);
                            return;
                        }

                        m_hashElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() - m_currentTimePoint);

                        // Set result to m_block
                        for (auto& it : m_executiveResults)
                        {
                            m_block->appendReceipt(it.receipt);
                        }
                        auto executedBlockHeader =
                            m_blockFactory->blockHeaderFactory()->populateBlockHeader(
                                m_block->blockHeader());
                        executedBlockHeader->setStateRoot(hash);
                        executedBlockHeader->setGasUsed(m_gasUsed);
                        executedBlockHeader->setReceiptsRoot(h256(0));  // TODO: calc
                                                                        // the receipt
                                                                        // root
                        m_result = executedBlockHeader;
                        callback(nullptr, m_result);
                    });
                }
            }
        };

        (*recursionCallback)(std::move(error));
    });
}

void BlockExecutive::batchNextBlock(std::function<void(Error::UniquePtr)> callback)
{
    auto status = std::make_shared<CommitStatus>();
    status->total = m_scheduler->m_executorManager->size();
    status->checkAndCommit = [this, callback = std::move(callback)](const CommitStatus& status) {
        if (status.success + status.failed < status.total)
        {
            return;
        }

        if (status.failed > 0)
        {
            auto message = "Next block:" + boost::lexical_cast<std::string>(number()) +
                           " with errors! " + boost::lexical_cast<std::string>(status.failed);
            SCHEDULER_LOG(ERROR) << message;

            callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::UnknownError, std::move(message)));
            return;
        }

        callback(nullptr);
    };

    for (auto& it : *(m_scheduler->m_executorManager))
    {
        SCHEDULER_LOG(TRACE) << "NextBlock for executor: " << it.get();
        auto blockHeader = m_block->blockHeaderConst();
        it->nextBlockHeader(blockHeader, [status](bcos::Error::Ptr&& error) {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Nextblock executor error!" << boost::diagnostic_information(*error);
                ++status->failed;
            }
            else
            {
                ++status->success;
            }

            status->checkAndCommit(*status);
        });
    }
}

void BlockExecutive::batchGetHashes(
    std::function<void(bcos::Error::UniquePtr, bcos::crypto::HashType)> callback)
{
    auto mutex = std::make_shared<std::mutex>();
    auto totalHash = std::make_shared<h256>();

    auto status = std::make_shared<CommitStatus>();
    status->total = m_scheduler->m_executorManager->size();  // all executors
    status->checkAndCommit = [this, totalHash, callback = std::move(callback)](
                                 const CommitStatus& status) {
        if (status.success + status.failed < status.total)
        {
            return;
        }

        if (status.failed > 0)
        {
            auto message = "Commit block:" + boost::lexical_cast<std::string>(number()) +
                           " with errors! " + boost::lexical_cast<std::string>(status.failed);
            SCHEDULER_LOG(WARNING) << message;

            callback(
                BCOS_ERROR_UNIQUE_PTR(SchedulerError::CommitError, std::move(message)), h256(0));
            return;
        }

        callback(nullptr, std::move(*totalHash));
    };

    for (auto& it : *(m_scheduler->m_executorManager))
    {
        it->getHash(number(), [status, mutex, totalHash](
                                  bcos::Error::Ptr&& error, crypto::HashType&& hash) {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Commit executor error!" << boost::diagnostic_information(*error);
                ++status->failed;
            }
            else
            {
                ++status->success;
                SCHEDULER_LOG(DEBUG) << "GetHash executor success, success: " << status->success;

                std::unique_lock<std::mutex> lock(*mutex);
                *totalHash ^= hash;
            }

            status->checkAndCommit(*status);
        });
    }
}

void BlockExecutive::batchBlockCommit(std::function<void(Error::UniquePtr)> callback)
{
    auto status = std::make_shared<CommitStatus>();
    status->total = 1 + m_scheduler->m_executorManager->size();  // self + all executors
    status->checkAndCommit = [this, callback = std::move(callback)](const CommitStatus& status) {
        if (status.success + status.failed < status.total)
        {
            return;
        }

        if (status.failed > 0)
        {
            auto message = "Commit block:" + boost::lexical_cast<std::string>(number()) +
                           " with errors! " + boost::lexical_cast<std::string>(status.failed);
            SCHEDULER_LOG(WARNING) << message;

            callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::CommitError, std::move(message)));
            return;
        }

        callback(nullptr);
    };

    storage::TransactionalStorageInterface::TwoPCParams params;
    params.number = number();
    m_scheduler->m_storage->asyncCommit(params, [status, this](Error::Ptr&& error) {
        if (error)
        {
            SCHEDULER_LOG(ERROR) << "Commit storage error!"
                                 << boost::diagnostic_information(*error);

            ++status->failed;
        }
        else
        {
            ++status->success;
        }

        executor::ParallelTransactionExecutorInterface::TwoPCParams executorParams;
        executorParams.number = number();
        tbb::parallel_for_each(m_scheduler->m_executorManager->begin(),
            m_scheduler->m_executorManager->end(), [&](auto const& executorIt) {
                executorIt->commit(executorParams, [status](bcos::Error::Ptr&& error) {
                    if (error)
                    {
                        SCHEDULER_LOG(ERROR)
                            << "Commit executor error!" << boost::diagnostic_information(*error);
                        ++status->failed;
                    }
                    else
                    {
                        ++status->success;
                        SCHEDULER_LOG(DEBUG)
                            << "Commit executor success, success: " << status->success;
                    }
                    status->checkAndCommit(*status);
                });
            });
    });
}

void BlockExecutive::batchBlockRollback(std::function<void(Error::UniquePtr)> callback)
{
    auto status = std::make_shared<CommitStatus>();
    status->total = 1 + m_scheduler->m_executorManager->size();  // self + all executors
    status->checkAndCommit = [this, callback = std::move(callback)](const CommitStatus& status) {
        if (status.success + status.failed < status.total)
        {
            return;
        }

        if (status.failed > 0)
        {
            auto message = "Rollback block:" + boost::lexical_cast<std::string>(number()) +
                           " with errors! " + boost::lexical_cast<std::string>(status.failed);
            SCHEDULER_LOG(WARNING) << message;

            callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::RollbackError, std::move(message)));
            return;
        }

        callback(nullptr);
    };

    storage::TransactionalStorageInterface::TwoPCParams params;
    params.number = number();
    m_scheduler->m_storage->asyncRollback(params, [status](Error::Ptr&& error) {
        if (error)
        {
            SCHEDULER_LOG(ERROR) << "Commit storage error!"
                                 << boost::diagnostic_information(*error);

            ++status->failed;
        }
        else
        {
            ++status->success;
        }
    });

    for (auto& it : *(m_scheduler->m_executorManager))
    {
        executor::ParallelTransactionExecutorInterface::TwoPCParams executorParams;
        executorParams.number = number();
        it->rollback(executorParams, [status](bcos::Error::Ptr&& error) {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Rollback executor error!" << boost::diagnostic_information(*error);
                ++status->failed;
            }
            else
            {
                ++status->success;
            }

            status->checkAndCommit(*status);
        });
    }
}

void BlockExecutive::startBatch(std::function<void(Error::UniquePtr)> callback)
{
    auto batchStatus = std::make_shared<BatchStatus>();
    batchStatus->callback = std::move(callback);

    bool deleted = true;
    for (auto it = m_executiveStates.begin();;)
    {
        if (!deleted)
        {
            ++it;
        }
        deleted = false;

        if (it == m_executiveStates.end())
        {
            batchStatus->allSended = true;
            break;
        }

        if (it->error)
        {
            batchStatus->allSended = true;
            ++batchStatus->error;

            SCHEDULER_LOG(TRACE) << "Detected error!";
            break;
        }

        // When to() is empty, create contract
        if (it->message->to().empty())
        {
            if (it->message->createSalt())
            {
                it->message->setTo(newEVMAddress(
                    it->message->from(), it->message->data(), *(it->message->createSalt())));
            }
            else
            {
                it->message->setTo(newEVMAddress(number(), it->contextID, it->message->seq()));
            }
        }

        // If call with key locks, acquire it
        if (!it->message->keyLocks().empty())
        {
            for (auto& keyLockIt : it->message->keyLocks())
            {
                if (!m_keyLocks.acquireKeyLock(
                        it->message->from(), keyLockIt, it->contextID, it->message->seq()))
                {
                    batchStatus->callback(BCOS_ERROR_UNIQUE_PTR(
                        UnexpectedKeyLockError, "Unexpected key lock error!"));
                    return;
                }
            }

            it->message->setKeyLocks({});
        }

        switch (it->message->type())
        {
        // Request type, push stack
        case protocol::ExecutionMessage::MESSAGE:
        case protocol::ExecutionMessage::TXHASH:
        {
            // Check if another context processing same contract
            auto contractIt = m_calledContract.lower_bound(it->message->to());
            if (contractIt != m_calledContract.end() && *contractIt == it->message->to())
            {
                continue;
            }
            m_calledContract.emplace_hint(contractIt, it->message->to());
            SCHEDULER_LOG(TRACE) << "Executing, "
                                 << "context id: " << it->message->contextID()
                                 << " seq: " << it->message->seq() << " txHash: " << std::hex
                                 << it->message->transactionHash() << " to: " << it->message->to();

            auto seq = it->currentSeq++;

            it->callStack.push(seq);

            it->message->setSeq(seq);

            break;
        }
        // Return type, pop stack
        case protocol::ExecutionMessage::FINISHED:
        case protocol::ExecutionMessage::REVERT:
        {
            // Clear the context keyLocks
            m_keyLocks.releaseKeyLocks(it->contextID, it->message->seq());

            it->callStack.pop();

            // Empty stack, execution is finished
            if (it->callStack.empty())
            {
                // Execution is finished, generate receipt
                m_executiveResults[it->contextID].receipt =
                    m_scheduler->m_blockFactory->receiptFactory()->createReceipt(
                        it->message->gasAvailable(), it->message->newEVMContractAddress(),
                        std::make_shared<std::vector<bcos::protocol::LogEntry>>(
                            it->message->takeLogEntries()),
                        it->message->status(), it->message->takeData(),
                        m_block->blockHeaderConst()->number());

                // Calc the gas
                m_gasUsed += (TRANSACTION_GAS - it->message->gasAvailable());

                // Remove executive state and continue
                SCHEDULER_LOG(TRACE) << "Eraseing: " << std::hex << it->message->transactionHash()
                                     << " " << it->message->to();

                it = m_executiveStates.erase(it);
                deleted = true;
                continue;
            }

            it->message->setSeq(it->callStack.top());
            it->message->setCreate(false);

            break;
        }
        // Retry type, send again
        case protocol::ExecutionMessage::WAIT_KEY:
        {
            // Try acquire key lock
            if (!m_keyLocks.acquireKeyLock(it->message->to(), it->message->keyLockAcquired(),
                    it->message->contextID(), it->message->seq()))
            {
                continue;
            }

            break;
        }
        // Retry type, send again
        case protocol::ExecutionMessage::SEND_BACK:
        {
            it->message->setType(protocol::ExecutionMessage::TXHASH);

            break;
        }
        }

        // Set current key lock into message
        auto keyLocks = m_keyLocks.getKeyLocksByContract(it->message->to(), it->contextID);
        it->message->setKeyLocks(std::move(keyLocks));

        ++batchStatus->total;
        auto executor = m_scheduler->m_executorManager->dispatchExecutor(it->message->to());

        auto executeCallback = [this, it, batchStatus](bcos::Error::UniquePtr&& error,
                                   bcos::protocol::ExecutionMessage::UniquePtr&& response) {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Execute transaction error: " << boost::diagnostic_information(*error);

                it->error = std::move(error);
                it->message.reset();
                // m_executiveStates.erase(it);  // Remove the error state

                // Set error to batch
                ++batchStatus->error;
            }
            else
            {
                it->message = std::move(response);
            }

            SCHEDULER_LOG(TRACE) << "Execute is finished!";

            ++batchStatus->received;
            checkBatch(*batchStatus);
        };

        if (it->message->staticCall())
        {
            executor->call(std::move(it->message), std::move(executeCallback));
        }
        else
        {
            executor->executeTransaction(std::move(it->message), std::move(executeCallback));
        }
    }

    checkBatch(*batchStatus);
}

void BlockExecutive::checkBatch(BatchStatus& status)
{
    SCHEDULER_LOG(TRACE) << "status: " << status.allSended << " " << status.received << " "
                         << status.total;
    if (status.allSended && status.received == status.total)
    {
        bool expect = false;
        if (status.callbackExecuted.compare_exchange_strong(expect, true))  // Run callback once
        {
            SCHEDULER_LOG(TRACE) << "Enter checkBatch callback: " << status.total << " "
                                 << status.received << " " << std::this_thread::get_id() << " "
                                 << status.callbackExecuted;

            SCHEDULER_LOG(TRACE) << "Batch run finished"
                                 << " total: " << status.allSended
                                 << " success: " << status.allSended - status.error
                                 << " error: " << status.error;

            if (status.error > 0)
            {
                status.callback(
                    BCOS_ERROR_UNIQUE_PTR(SchedulerError::BatchError, "Batch with errors"));
                return;
            }

            status.callback(nullptr);
        }
    }
}

std::string BlockExecutive::newEVMAddress(int64_t blockNumber, int64_t contextID, int64_t seq)
{
    auto hash = m_scheduler->m_hashImpl->hash(boost::lexical_cast<std::string>(blockNumber) + "_" +
                                              boost::lexical_cast<std::string>(contextID) + "_" +
                                              boost::lexical_cast<std::string>(seq));

    std::string hexAddress;
    hexAddress.reserve(40);
    boost::algorithm::hex(hash.data(), hash.data() + 20, std::back_inserter(hexAddress));

    toChecksumAddress(hexAddress, m_scheduler->m_hashImpl);

    return hexAddress;
}

std::string BlockExecutive::newEVMAddress(
    const std::string_view& _sender, bytesConstRef _init, u256 const& _salt)
{
    auto hash = m_scheduler->m_hashImpl->hash(
        bytes{0xff} + _sender + toBigEndian(_salt) + m_scheduler->m_hashImpl->hash(_init));

    std::string hexAddress;
    hexAddress.reserve(40);
    boost::algorithm::hex(hash.data(), hash.data() + 20, std::back_inserter(hexAddress));

    toChecksumAddress(hexAddress, m_scheduler->m_hashImpl);

    return hexAddress;
}

std::string BlockExecutive::preprocessAddress(const std::string_view& address)
{
    std::string out;
    if (address[0] == '0' && address[1] == 'x')
    {
        out = std::string(address.substr(2));
    }
    else
    {
        out = std::string(address);
    }

    boost::to_lower(out);
    return out;
}