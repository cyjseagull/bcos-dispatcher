#pragma once

#include "BlockExecutive.h"
#include "ExecutorManager.h"
#include "bcos-framework/interfaces/dispatcher/SchedulerInterface.h"
#include "bcos-framework/interfaces/ledger/LedgerInterface.h"
#include <bcos-framework/interfaces/executor/ParallelTransactionExecutorInterface.h>
#include <tbb/concurrent_queue.h>
#include <list>

namespace bcos::scheduler
{
class SchedulerImpl : public SchedulerInterface
{
public:
    friend class BlockExecutive;

    SchedulerImpl(ExecutorManager::Ptr executorManager, bcos::ledger::LedgerInterface::Ptr ledger,
        bcos::storage::TransactionalStorageInterface::Ptr storage,
        bcos::protocol::ExecutionMessageFactory::Ptr executionMessageFactory,
        bcos::protocol::TransactionReceiptFactory::Ptr transactionReceiptFactory,
        bcos::crypto::Hash::Ptr hashImpl)
      : m_executorManager(std::move(executorManager)),
        m_ledger(std::move(ledger)),
        m_storage(std::move(storage)),
        m_executionMessageFactory(std::move(executionMessageFactory)),
        m_transactionReceiptFactory(std::move(transactionReceiptFactory)),
        m_hashImpl(std::move(hashImpl))
    {}

    SchedulerImpl(const SchedulerImpl&) = delete;
    SchedulerImpl(SchedulerImpl&&) = delete;
    SchedulerImpl& operator=(const SchedulerImpl&) = delete;
    SchedulerImpl& operator=(SchedulerImpl&&) = delete;

    // by pbft & sync
    void executeBlock(bcos::protocol::Block::Ptr block, bool verify,
        std::function<void(bcos::Error::Ptr&&, bcos::protocol::BlockHeader::Ptr&&)>
            callback) noexcept override;

    // by pbft & sync
    void commitBlock(bcos::protocol::BlockHeader::Ptr header,
        std::function<void(bcos::Error::Ptr&&, bcos::ledger::LedgerConfig::Ptr&&)>
            callback) noexcept override;

    // by console, query committed committing executing
    void status(std::function<void(Error::Ptr&&, bcos::protocol::Session::ConstPtr&&)>
            callback) noexcept override;

    // by rpc
    void call(protocol::Transaction::Ptr tx,
        std::function<void(Error::Ptr&&, protocol::TransactionReceipt::Ptr&&)>) noexcept override;

    // by executor
    void registerExecutor(std::string name,
        bcos::executor::ParallelTransactionExecutorInterface::Ptr executor,
        std::function<void(Error::Ptr&&)> callback) noexcept override;

    void unregisterExecutor(
        const std::string& name, std::function<void(Error::Ptr&&)> callback) noexcept override;

    void reset(std::function<void(Error::Ptr&&)> callback) noexcept override;

private:
    void asyncGetLedgerConfig(
        std::function<void(Error::Ptr&&, ledger::LedgerConfig::Ptr ledgerConfig)> callback);

    std::list<BlockExecutive> m_blocks;
    std::mutex m_blocksMutex;

    std::mutex m_executeMutex;
    std::mutex m_commitMutex;

    ExecutorManager::Ptr m_executorManager;
    bcos::ledger::LedgerInterface::Ptr m_ledger;
    bcos::storage::TransactionalStorageInterface::Ptr m_storage;
    bcos::protocol::ExecutionMessageFactory::Ptr m_executionMessageFactory;
    bcos::protocol::TransactionReceiptFactory::Ptr m_transactionReceiptFactory;
    bcos::crypto::Hash::Ptr m_hashImpl;
};
}  // namespace bcos::scheduler