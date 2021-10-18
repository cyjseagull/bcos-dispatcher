#include "BlockExecutive.h"
#include "ChecksumAddress.h"
#include "SchedulerImpl.h"
#include "bcos-framework/libstorage/StateStorage.h"
#include "bcos-scheduler/Common.h"
#include "interfaces/executor/ExecutionMessage.h"
#include "interfaces/executor/ParallelTransactionExecutorInterface.h"
#include "libutilities/Error.h"
#include <boost/algorithm/hex.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/lexical_cast.hpp>
#include <atomic>
#include <chrono>
#include <iterator>

using namespace bcos::scheduler;

void BlockExecutive::asyncExecute(
    std::function<void(Error::UniquePtr&&, protocol::BlockHeader::Ptr)> callback) noexcept
{
    if (m_result)
    {
        callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::InvalidStatus, "Invalid status"), nullptr);
        return;
    }

    m_currentTimePoint = std::chrono::system_clock::now();

    if (m_block->transactionsMetaDataSize() > 0)
    {
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
            message->setGasAvailable(3000000);  // TODO: add const var
            message->setStaticCall(false);

            m_executiveStates.emplace_back(i, std::move(message));
        }
    }
    else if (m_block->transactionsSize() > 0)
    {
        m_executiveResults.resize(m_block->transactionsSize());
        for (size_t i = 0; i < m_block->transactionsSize(); ++i)
        {
            auto tx = m_block->transaction(i);

            auto message = m_scheduler->m_executionMessageFactory->createExecutionMessage();
            message->setType(protocol::ExecutionMessage::MESSAGE);
            message->setContextID(i + m_startContextID);

            std::string sender;
            sender.reserve(tx->sender().size() * 2);
            boost::algorithm::hex_lower(tx->sender(), std::back_insert_iterator(sender));

            message->setOrigin(sender);
            message->setFrom(std::move(sender));

            if (tx->to().empty())
            {
                message->setCreate(true);
            }
            else
            {
                message->setTo(preprocessAddress(tx->to()));
            }

            message->setDepth(0);
            message->setGasAvailable(3000000);  // TODO: add const var
            message->setData(tx->input().toBytes());
            message->setStaticCall(m_staticCall);

            m_executiveStates.emplace_back(i, std::move(message));
        }
    }

    auto startExecute = [this, callback = std::move(callback)](Error::UniquePtr&& error) {
        if (error)
        {
            SCHEDULER_LOG(ERROR) << "Next block with error!"
                                 << boost::diagnostic_information(*error);
            callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                         SchedulerError::NextBlockError, "Next block error!", *error),
                nullptr);
            return;
        }

        class BatchCallback : public std::enable_shared_from_this<BatchCallback>
        {
        public:
            BatchCallback(BlockExecutive* self,
                std::function<void(Error::UniquePtr&&, protocol::BlockHeader::Ptr)> callback)
              : m_self(self), m_callback(std::move(callback))
            {}

            void operator()(Error::UniquePtr&& error) const
            {
                if (error)
                {
                    m_callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(-1, "Execute with errors", *error),
                        nullptr);
                    return;
                }

                if (!m_self->m_executiveStates.empty())
                {
                    m_self->m_calledContract.clear();

                    m_self->startBatch(std::bind(
                        &BatchCallback::operator(), shared_from_this(), std::placeholders::_1));
                }
                else
                {
                    auto now = std::chrono::system_clock::now();
                    m_self->m_executeElapsed =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() - m_self->m_currentTimePoint);
                    m_self->m_currentTimePoint = now;

                    if (m_self->m_staticCall)
                    {
                        // Set result to m_block
                        for (auto& it : m_self->m_executiveResults)
                        {
                            m_self->m_block->appendReceipt(std::move(it.receipt));
                        }

                        m_callback(nullptr, nullptr);
                    }
                    else
                    {
                        // All Transaction finished, get hash
                        m_self->batchGetHashes([self = m_self, callback = std::move(m_callback)](
                                                   Error::UniquePtr&& error,
                                                   crypto::HashType hash) {
                            if (error)
                            {
                                callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                                             SchedulerError::UnknownError, "Unknown error", *error),
                                    nullptr);
                                return;
                            }

                            self->m_hashElapsed =
                                std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now() - self->m_currentTimePoint);

                            // Set result to m_block
                            for (auto& it : self->m_executiveResults)
                            {
                                self->m_block->appendReceipt(std::move(it.receipt));
                            }

                            self->m_block->blockHeader()->setStateRoot(std::move(hash));
                            self->m_block->blockHeader()->setGasUsed(self->m_gasUsed);
                            self->m_block->blockHeader()->setReceiptsRoot(h256(0));  // TODO: calc
                                                                                     // the receipt
                                                                                     // root

                            self->m_result = self->m_block->blockHeader();
                            callback(nullptr, self->m_result);
                        });
                    }
                }
            }

        private:
            BlockExecutive* m_self;
            std::function<void(Error::UniquePtr&&, protocol::BlockHeader::Ptr)> m_callback;
        };

        auto batchCallback = std::make_shared<BatchCallback>(this, std::move(callback));
        startBatch(std::bind(&BatchCallback::operator(), batchCallback, std::placeholders::_1));
    };

    if (m_staticCall)
    {
        startExecute(nullptr);
    }
    else
    {
        // Execute nextBlock
        batchNextBlock(std::move(startExecute));
    }
}

void BlockExecutive::asyncCommit(std::function<void(Error::UniquePtr&&)> callback) noexcept
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
                    SCHEDULER_LOG(INFO) << "Commit block: " << number()
                                        << " success, execute elapsed: " << m_executeElapsed.count()
                                        << "ms hash elapsed: " << m_hashElapsed.count()
                                        << "ms commit elapsed: " << m_commitElapsed.count() << "ms";

                    callback(nullptr);
                });
            };

            storage::TransactionalStorageInterface::TwoPCParams params;
            m_scheduler->m_storage->asyncPrepare(
                params, stateStorage, [status](Error::Ptr&& error, uint64_t num) {
                    (void)num;  // TODO: how to use?

                    if (error)
                    {
                        ++status->failed;
                    }
                    else
                    {
                        ++status->success;
                    }

                    status->checkAndCommit(*status);
                });

            for (auto& it : *(m_scheduler->m_executorManager))
            {
                executor::ParallelTransactionExecutorInterface::TwoPCParams executorParams;
                executorParams.number = number();
                it->prepare(executorParams, [status](Error::Ptr&& error) {
                    if (error)
                    {
                        ++status->failed;
                    }
                    else
                    {
                        ++status->success;
                    }

                    status->checkAndCommit(*status);
                });
            }
        });
}

void BlockExecutive::batchNextBlock(std::function<void(Error::UniquePtr&&)> callback)
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
        it->nextBlockHeader(m_block->blockHeaderConst(), [status](bcos::Error::Ptr&& error) {
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
    std::function<void(bcos::Error::UniquePtr&&, bcos::crypto::HashType)> callback)
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
        it->getHash(number(),
            [status, mutex, totalHash](bcos::Error::Ptr&& error, crypto::HashType&& hash) {
                if (error)
                {
                    SCHEDULER_LOG(ERROR)
                        << "Commit executor error!" << boost::diagnostic_information(*error);
                    ++status->failed;
                }
                else
                {
                    ++status->success;

                    std::unique_lock<std::mutex> lock(*mutex);
                    *totalHash ^= hash;
                }

                status->checkAndCommit(*status);
            });
    }
}

void BlockExecutive::batchBlockCommit(std::function<void(Error::UniquePtr&&)> callback)
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
    m_scheduler->m_storage->asyncCommit(params, [status](Error::Ptr&& error) {
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

        status->checkAndCommit(*status);
    });

    for (auto& it : *(m_scheduler->m_executorManager))
    {
        executor::ParallelTransactionExecutorInterface::TwoPCParams executorParams;
        executorParams.number = number();
        it->commit(executorParams, [status](bcos::Error::Ptr&& error) {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Commit executor error!" << boost::diagnostic_information(*error);
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

void BlockExecutive::batchBlockRollback(std::function<void(Error::UniquePtr&&)> callback)
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

        status->checkAndCommit(*status);
    });

    for (auto& it : *(m_scheduler->m_executorManager))
    {
        executor::ParallelTransactionExecutorInterface::TwoPCParams executorParams;
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

void BlockExecutive::startBatch(std::function<void(Error::UniquePtr&&)> callback)
{
    auto batchStatus = std::make_shared<BatchStatus>();
    batchStatus->callback = std::move(callback);

    for (auto it = m_executiveStates.begin();; ++it)
    {
        if (it == m_executiveStates.end())
        {
            batchStatus->allSended = true;
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

        if (m_calledContract.find(it->message->to()) != m_calledContract.end())
        {
            continue;  // Another context processing
        }

        m_calledContract.emplace(it->message->to());
        switch (it->message->type())
        {
        // Request type, push stack
        case protocol::ExecutionMessage::TXHASH:
        case protocol::ExecutionMessage::MESSAGE:
        {
            auto seq = it->currentSeq++;

            it->callStack.push(seq);

            it->message->setSeq(seq);

            break;
        }
        // Return type, pop stack
        case protocol::ExecutionMessage::FINISHED:
        case protocol::ExecutionMessage::REVERT:
        {
            it->callStack.pop();

            // Empty stack, execution is finished
            if (it->callStack.empty())
            {
                // Execution is finished, generate receipt
                m_executiveResults[it->contextID].receipt =
                    m_scheduler->m_blockFactory->receiptFactory()->createReceipt(
                        it->message->gasAvailable(), it->message->newEVMContractAddress(),
                        std::make_shared<std::vector<bcos::protocol::LogEntry>>(
                            std::move(it->message->takeLogEntries())),
                        it->message->status(), std::move(it->message->takeData()),
                        m_block->blockHeaderConst()->number());

                // Calc the gas
                ++m_gasUsed;  // TODO: calc the total gas used

                // Remove executive state and continue
                it = m_executiveStates.erase(it);
                continue;
            }

            it->message->setSeq(it->callStack.top());
            it->message->setCreate(false);

            break;
        }
        // Retry type, send again
        case protocol::ExecutionMessage::WAIT_KEY:
        {
            BOOST_THROW_EXCEPTION(BCOS_ERROR(SchedulerError::UnknownError, "Unsupported method"));
            break;
        }
        // Retry type, send again
        case protocol::ExecutionMessage::SEND_BACK:
        {
            it->message->setType(protocol::ExecutionMessage::TXHASH);

            break;
        }
        }

        ++batchStatus->total;
        auto executor = m_scheduler->m_executorManager->dispatchExecutor(it->message->to());

        auto executeCallback = [this, it, batchStatus](bcos::Error::UniquePtr&& error,
                                   bcos::protocol::ExecutionMessage::UniquePtr&& response) {
            ++batchStatus->received;

            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Execute transaction error: " << boost::diagnostic_information(*error);

                it->error = std::move(error);
                m_executiveStates.erase(it);
            }
            else
            {
                it->message = std::move(response);
            }

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
    if (status.allSended && status.received == status.total)
    {
        bool expect = false;
        if (status.callbackExecuted.compare_exchange_strong(expect, true))  // Run callback once
        {
            size_t errorCount = 0;
            size_t successCount = 0;

            for (auto& it : m_executiveStates)
            {
                if (it.error)
                {
                    // with errors
                    ++errorCount;
                    SCHEDULER_LOG(ERROR)
                        << "Batch with error: " << boost::diagnostic_information(*it.error);
                }
                else
                {
                    ++successCount;
                }
            }

            SCHEDULER_LOG(INFO) << "Batch run success"
                                << " total: " << errorCount + successCount
                                << " success: " << successCount << " error: " << errorCount;

            if (errorCount > 0)
            {
                status.callback(BCOS_ERROR_UNIQUE_PTR(-1, "Batch with errors"));
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