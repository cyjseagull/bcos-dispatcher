#pragma once

#include "BlockExecutive.h"
#include "ExecutorManager.h"
#include "bcos-framework/interfaces/dispatcher/SchedulerInterface.h"
#include "bcos-framework/interfaces/ledger/LedgerInterface.h"
#include "interfaces/crypto/CommonType.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include "libprotocol/TransactionSubmitResultFactoryImpl.h"
#include <bcos-framework/interfaces/executor/ParallelTransactionExecutorInterface.h>
#include <bcos-framework/interfaces/protocol/BlockFactory.h>
#include <bcos-framework/interfaces/rpc/RPCInterface.h>
#include <tbb/concurrent_hash_map.h>
#include <list>

namespace bcos::scheduler
{
class SchedulerImpl : public SchedulerInterface, public std::enable_shared_from_this<SchedulerImpl>
{
public:
    friend class BlockExecutive;

    SchedulerImpl(ExecutorManager::Ptr executorManager, bcos::ledger::LedgerInterface::Ptr ledger,
        bcos::storage::TransactionalStorageInterface::Ptr storage,
        bcos::protocol::ExecutionMessageFactory::Ptr executionMessageFactory,
        bcos::protocol::BlockFactory::Ptr blockFactory,
        bcos::protocol::TransactionSubmitResultFactory::Ptr transactionSubmitResultFactory,
        bcos::crypto::Hash::Ptr hashImpl)
      : m_executorManager(std::move(executorManager)),
        m_ledger(std::move(ledger)),
        m_storage(std::move(storage)),
        m_executionMessageFactory(std::move(executionMessageFactory)),
        m_transactionSubmitResultFactory(std::move(transactionSubmitResultFactory)),
        m_blockFactory(std::move(blockFactory)),
        m_hashImpl(std::move(hashImpl))
    {}

    SchedulerImpl(const SchedulerImpl&) = delete;
    SchedulerImpl(SchedulerImpl&&) = delete;
    SchedulerImpl& operator=(const SchedulerImpl&) = delete;
    SchedulerImpl& operator=(SchedulerImpl&&) = delete;

    void executeBlock(bcos::protocol::Block::Ptr block, bool verify,
        std::function<void(bcos::Error::Ptr&&, bcos::protocol::BlockHeader::Ptr&&)> callback)
        override;

    void commitBlock(bcos::protocol::BlockHeader::Ptr header,
        std::function<void(bcos::Error::Ptr&&, bcos::ledger::LedgerConfig::Ptr&&)> callback)
        override;

    void status(
        std::function<void(Error::Ptr&&, bcos::protocol::Session::ConstPtr&&)> callback) override;

    void call(protocol::Transaction::Ptr tx,
        std::function<void(Error::Ptr&&, protocol::TransactionReceipt::Ptr&&)>) override;

    void registerExecutor(std::string name,
        bcos::executor::ParallelTransactionExecutorInterface::Ptr executor,
        std::function<void(Error::Ptr&&)> callback) override;

    void unregisterExecutor(
        const std::string& name, std::function<void(Error::Ptr&&)> callback) override;

    void reset(std::function<void(Error::Ptr&&)> callback) override;

    void registerBlockNumberReceiver(
        std::function<void(protocol::BlockNumber blockNumber)> callback) override;

    void registerTransactionNotifier(std::function<void(bcos::protocol::BlockNumber,
            bcos::protocol::TransactionSubmitResultsPtr, std::function<void(Error::Ptr)>)>
            txNotifier);

private:
    void asyncGetLedgerConfig(
        std::function<void(Error::Ptr&&, ledger::LedgerConfig::Ptr ledgerConfig)> callback);

    std::list<BlockExecutive> m_blocks;
    std::mutex m_blocksMutex;

    std::mutex m_executeMutex;
    std::mutex m_commitMutex;

    std::atomic_int64_t m_calledContextID = 0;

    auto getLastCommitedBlockNumber()
    {
        std::tuple<bcos::protocol::BlockNumber, std::unique_lock<std::mutex>> result(
            m_lastCommitedBlockNumber, m_lastCommitedBlockNumberMutex);
        return result;
    }
    void setLastCommitedBlockNumber(bcos::protocol::BlockNumber lastCommitedBlockNumber)
    {
        std::unique_lock<std::mutex> lock(m_lastCommitedBlockNumberMutex);
        m_lastCommitedBlockNumber = lastCommitedBlockNumber;
    }

    bcos::protocol::BlockNumber m_lastCommitedBlockNumber = 0;
    std::mutex m_lastCommitedBlockNumberMutex;

    ExecutorManager::Ptr m_executorManager;
    bcos::ledger::LedgerInterface::Ptr m_ledger;
    bcos::storage::TransactionalStorageInterface::Ptr m_storage;
    bcos::protocol::ExecutionMessageFactory::Ptr m_executionMessageFactory;
    bcos::protocol::TransactionSubmitResultFactory::Ptr m_transactionSubmitResultFactory;
    bcos::protocol::BlockFactory::Ptr m_blockFactory;
    bcos::crypto::Hash::Ptr m_hashImpl;

    std::function<void(protocol::BlockNumber blockNumber)> m_blockNumberReceiver;
    std::function<void(bcos::protocol::BlockNumber, bcos::protocol::TransactionSubmitResultsPtr,
        std::function<void(Error::Ptr)>)>
        m_txNotifier;
};
}  // namespace bcos::scheduler