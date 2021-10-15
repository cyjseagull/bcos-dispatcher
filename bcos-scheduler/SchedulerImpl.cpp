#include "SchedulerImpl.h"
#include "Common.h"
#include "interfaces/ledger/LedgerConfig.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include "libutilities/Error.h"
#include <boost/exception/diagnostic_information.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/throw_exception.hpp>
#include <mutex>
#include <variant>

using namespace bcos::scheduler;

void SchedulerImpl::executeBlock(bcos::protocol::Block::Ptr block, bool verify,
    std::function<void(bcos::Error::Ptr&&, bcos::protocol::BlockHeader::Ptr&&)> callback) noexcept
{
    SCHEDULER_LOG(INFO) << "ExecuteBlock request"
                        << LOG_KV("block number", block->blockHeaderConst()->number())
                        << LOG_KV("verify", verify);

    std::unique_lock<std::mutex> executeLock(m_executeMutex, std::try_to_lock);
    if (!executeLock.owns_lock())
    {
        auto message = "Another block is executing!";
        SCHEDULER_LOG(ERROR) << "ExecuteBlock error, " << message;
        callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::InvalidStatus, message), nullptr);
        return;
    }

    std::unique_lock<std::mutex> blocksLock(m_blocksMutex);

    if (!m_blocks.empty())
    {
        auto requestNumber = block->blockHeaderConst()->number();
        auto& frontBlock = m_blocks.front();
        auto& backBlock = m_blocks.back();

        // Block already executed
        if (requestNumber >= frontBlock.number() && requestNumber < backBlock.number())
        {
            SCHEDULER_LOG(INFO) << "ExecuteBlock success, return executed block"
                                << LOG_KV("block number", block->blockHeaderConst()->number())
                                << LOG_KV("verify", verify);

            auto it = m_blocks.begin();
            while (it->number() != requestNumber)
            {
                ++it;
            }

            callback(nullptr, bcos::protocol::BlockHeader::Ptr(it->result()));
            return;
        }

        if (requestNumber - backBlock.number() != 1)
        {
            auto message =
                "Invalid block number: " +
                boost::lexical_cast<std::string>(block->blockHeaderConst()->number()) +
                " current last number: " + boost::lexical_cast<std::string>(backBlock.number());
            SCHEDULER_LOG(ERROR) << "ExecuteBlock error, " << message;

            callback(
                BCOS_ERROR_PTR(SchedulerError::InvalidBlockNumber, std::move(message)), nullptr);

            return;
        }
    }

    m_blocks.emplace_back(std::move(block), this);

    auto executeLockPtr = std::make_shared<decltype(executeLock)>(std::move(executeLock));
    m_blocks.back().asyncExecute([callback = std::move(callback), executeLock =
                                                                      std::move(executeLockPtr)](
                                     Error::UniquePtr&& error, protocol::BlockHeader::Ptr header) {
        if (error)
        {
            SCHEDULER_LOG(ERROR) << "Unknown error, " << boost::diagnostic_information(*error);
            callback(
                BCOS_ERROR_WITH_PREV_PTR(SchedulerError::UnknownError, "Unknown error", *error),
                nullptr);
            return;
        }
        SCHEDULER_LOG(INFO) << "ExecuteBlock success" << LOG_KV("block number", header->number())
                            << LOG_KV("state root", header->stateRoot().hex());
        callback(std::move(error), std::move(header));
    });
}

// by pbft & sync
void SchedulerImpl::commitBlock(bcos::protocol::BlockHeader::Ptr header,
    std::function<void(bcos::Error::Ptr&&, bcos::ledger::LedgerConfig::Ptr&&)> callback) noexcept
{
    SCHEDULER_LOG(INFO) << "CommitBlock request" << LOG_KV("block number", header->number());

    std::unique_lock<std::mutex> commitLock(m_commitMutex, std::try_to_lock);
    if (!commitLock.owns_lock())
    {
        auto message = "Another block is commiting!";
        SCHEDULER_LOG(ERROR) << "CommitBlock error, " << message;
        callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::InvalidStatus, message), nullptr);
        return;
    }

    std::unique_lock<std::mutex> blocksLock(m_blocksMutex);

    if (m_blocks.empty())
    {
        auto message = "No uncommitted block";
        SCHEDULER_LOG(ERROR) << "CommitBlock error, " << message;
        callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::InvalidBlocks, message), nullptr);
        return;
    }

    auto& frontBlock = m_blocks.front();
    if (!frontBlock.result())
    {
        auto message = "Block is executing";
        SCHEDULER_LOG(ERROR) << "CommitBlock error, " << message;
        callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::InvalidStatus, message), nullptr);
        return;
    }

    if (header->number() != frontBlock.number())
    {
        auto message = "Invalid block number, available block number: " +
                       boost::lexical_cast<std::string>(frontBlock.number());
        SCHEDULER_LOG(ERROR) << "CommitBlock error, " << message;
        callback(BCOS_ERROR_UNIQUE_PTR(SchedulerError::InvalidBlockNumber, message), nullptr);
        return;
    }

    auto commitLockPtr = std::make_shared<decltype(commitLock)>(
        std::move(commitLock));  // std::function need copyable

    blocksLock.unlock();
    frontBlock.asyncCommit([this, callback = std::move(callback), block = frontBlock.block(),
                               commitLock = std::move(commitLockPtr)](Error::UniquePtr&& error) {
        if (error)
        {
            SCHEDULER_LOG(ERROR) << "CommitBlock error, " << boost::diagnostic_information(*error);
            callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                         SchedulerError::UnknownError, "Unknown error", *error),
                nullptr);
            return;
        }

        asyncGetLedgerConfig([this, callback = std::move(callback)](
                                 Error::Ptr&& error, ledger::LedgerConfig::Ptr ledgerConfig) {
            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Get system config error, " << boost::diagnostic_information(*error);
                callback(BCOS_ERROR_WITH_PREV_UNIQUE_PTR(
                             SchedulerError::UnknownError, "Get system config error", *error),
                    nullptr);
                return;
            }

            std::unique_lock<std::mutex> blocksLock(m_blocksMutex);
            m_blocks.pop_front();

            callback(nullptr, std::move(ledgerConfig));
        });
    });
}

// by console, query committed committing executing
void SchedulerImpl::status(
    std::function<void(Error::Ptr&&, bcos::protocol::Session::ConstPtr&&)> callback) noexcept
{
    (void)callback;
}

// by rpc
void SchedulerImpl::call(protocol::Transaction::Ptr tx,
    std::function<void(Error::Ptr&&, protocol::TransactionReceipt::Ptr&&)> callback) noexcept
{
    (void)tx;
    (void)callback;
}

void SchedulerImpl::registerExecutor(std::string name,
    bcos::executor::ParallelTransactionExecutorInterface::Ptr executor,
    std::function<void(Error::Ptr&&)> callback) noexcept
{
    try
    {
        SCHEDULER_LOG(INFO) << "registerExecutor request: " << LOG_KV("name", name);
        m_executorManager->addExecutor(name, executor);
    }
    catch (std::exception& e)
    {
        SCHEDULER_LOG(ERROR) << "registerExecutor error: " << boost::diagnostic_information(e);
        callback(BCOS_ERROR_WITH_PREV_PTR(-1, "addExecutor error", e));
        return;
    }

    SCHEDULER_LOG(INFO) << "registerExecutor success";
    callback(nullptr);
}

void SchedulerImpl::unregisterExecutor(
    const std::string& name, std::function<void(Error::Ptr&&)> callback) noexcept
{
    (void)name;
    (void)callback;
}

void SchedulerImpl::reset(std::function<void(Error::Ptr&&)> callback) noexcept
{
    (void)callback;
}

template <class... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};
// explicit deduction guide (not needed as of C++20)
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

void SchedulerImpl::asyncGetLedgerConfig(
    std::function<void(Error::Ptr&&, ledger::LedgerConfig::Ptr ledgerConfig)> callback)
{
    auto collecter =
        [summary = std::make_shared<std::tuple<size_t, std::atomic_size_t, std::atomic_size_t>>(
             7, 0, 0),
            ledgerConfig = std::make_shared<ledger::LedgerConfig>(),
            callback = std::make_shared<decltype(callback)>(std::move(callback))](Error::Ptr error,
            std::variant<std::tuple<bool, consensus::ConsensusNodeListPtr>,
                std::tuple<int, std::string>, bcos::protocol::BlockNumber, bcos::crypto::HashType>&&
                result) mutable {
            auto& [total, success, failed] = *summary;

            if (error)
            {
                SCHEDULER_LOG(ERROR)
                    << "Get ledger config with errors: " << boost::diagnostic_information(*error);
                ++failed;
            }
            else
            {
                std::visit(
                    overloaded{[&ledgerConfig](
                                   std::tuple<bool, consensus::ConsensusNodeListPtr>& nodeList) {
                                   auto& [isSealer, list] = nodeList;

                                   if (isSealer)
                                   {
                                       ledgerConfig->setConsensusNodeList(*list);
                                   }
                                   else
                                   {
                                       ledgerConfig->setObserverNodeList(*list);
                                   }
                               },
                        [&ledgerConfig](std::tuple<int, std::string> config) {
                            auto& [type, value] = config;
                            switch (type)
                            {
                            case 0:
                                ledgerConfig->setBlockTxCountLimit(
                                    boost::lexical_cast<uint64_t>(value));
                                break;
                            case 1:
                                ledgerConfig->setConsensusTimeout(
                                    boost::lexical_cast<uint64_t>(value));
                                break;
                            case 2:
                                ledgerConfig->setLeaderSwitchPeriod(
                                    boost::lexical_cast<uint64_t>(value));
                                break;
                            default:
                                BOOST_THROW_EXCEPTION(BCOS_ERROR(SchedulerError::UnknownError,
                                    "Unknown type: " + boost::lexical_cast<std::string>(type)));
                                break;
                            }
                        },
                        [&ledgerConfig](bcos::protocol::BlockNumber number) {
                            ledgerConfig->setBlockNumber(number);
                        },
                        [&ledgerConfig](
                            bcos::crypto::HashType hash) { ledgerConfig->setHash(hash); }},
                    result);

                ++success;
            }

            // Collect done
            if (success + failed == total)
            {
                if (failed > 0)
                {
                    SCHEDULER_LOG(ERROR) << "Get ledger config with error: " << failed;
                    (*callback)(BCOS_ERROR_PTR(
                                    SchedulerError::UnknownError, "Get ledger config with error"),
                        nullptr);

                    return;
                }

                (*callback)(nullptr, std::move(ledgerConfig));
            }
        };

    m_ledger->asyncGetNodeListByType(ledger::CONSENSUS_SEALER,
        [collecter](Error::Ptr error, consensus::ConsensusNodeListPtr list) mutable {
            collecter(std::move(error), std::tuple{true, std::move(list)});
        });
    m_ledger->asyncGetNodeListByType(ledger::CONSENSUS_OBSERVER,
        [collecter](Error::Ptr error, consensus::ConsensusNodeListPtr list) mutable {
            collecter(std::move(error), std::tuple{false, std::move(list)});
        });
    m_ledger->asyncGetSystemConfigByKey(ledger::SYSTEM_KEY_TX_COUNT_LIMIT,
        [collecter](Error::Ptr error, std::string config, protocol::BlockNumber) mutable {
            collecter(std::move(error), std::tuple{0, std::move(config)});
        });
    m_ledger->asyncGetSystemConfigByKey(ledger::SYSTEM_KEY_CONSENSUS_TIMEOUT,
        [collecter](Error::Ptr error, std::string config, protocol::BlockNumber) mutable {
            collecter(std::move(error), std::tuple{1, std::move(config)});
        });
    m_ledger->asyncGetSystemConfigByKey(ledger::SYSTEM_KEY_CONSENSUS_LEADER_PERIOD,
        [collecter](Error::Ptr error, std::string config, protocol::BlockNumber) mutable {
            collecter(std::move(error), std::tuple{2, std::move(config)});
        });
    m_ledger->asyncGetBlockNumber(
        [collecter, ledger = m_ledger](Error::Ptr error, protocol::BlockNumber number) mutable {
            ledger->asyncGetBlockHashByNumber(
                number, [collecter](Error::Ptr error, const crypto::HashType& hash) mutable {
                    collecter(std::move(error), std::move(hash));
                });
            collecter(std::move(error), std::move(number));
        });
}