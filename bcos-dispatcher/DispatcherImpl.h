#pragma once

#include "bcos-framework/interfaces/dispatcher/DispatcherInterface.h"
#include "bcos-framework/interfaces/protocol/ProtocolTypeDef.h"
#include <bcos-framework/interfaces/txpool/TxPoolInterface.h>
#include <tbb/mutex.h>
#include <unordered_map>

#define DISPATCHER_LOG(LEVEL) BCOS_LOG(LEVEL) << LOG_BADGE("Dispatcher")

namespace bcos
{
namespace dispatcher
{
class DispatcherImpl : public DispatcherInterface
{
public:
    using Ptr = std::shared_ptr<DispatcherImpl>;
    using ExecutionResultCallback =
        std::function<void(const Error::Ptr&, const protocol::BlockHeader::Ptr&)>;
    DispatcherImpl() = default;
    ~DispatcherImpl() override {}
    void asyncExecuteBlock(const protocol::Block::Ptr& _block, bool _verify,
        ExecutionResultCallback _callback) override;
    void asyncGetLatestBlock(
        std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)> _callback) override;

    void asyncNotifyExecutionResult(const Error::Ptr& _error,
        bcos::crypto::HashType const& _orgHash, const protocol::BlockHeader::Ptr& _header,
        std::function<void(const Error::Ptr&)> _callback) override;

    void init(bcos::txpool::TxPoolInterface::Ptr _txpool) { m_txpool = _txpool; }
    void start() override;
    void stop() override;

    virtual void asyncExecuteCompletedBlock(
        const protocol::Block::Ptr& _block, bool _verify, ExecutionResultCallback _callback);

    virtual void updateExecResultCache(const bcos::Error::Ptr& _error,
        bcos::crypto::HashType const& _orgHash, const bcos::protocol::BlockHeader::Ptr& _header);
    virtual bcos::protocol::BlockHeader::Ptr getExecResultCache(
        bcos::crypto::HashType const& _hash);

private:
    bcos::txpool::TxPoolInterface::Ptr m_txpool;

    // increase order
    struct BlockCmp
    {
        bool operator()(
            protocol::Block::Ptr const& _first, protocol::Block::Ptr const& _second) const
        {
            // increase order
            return _first->blockHeader()->number() > _second->blockHeader()->number();
        }
    };
    // store the block to be executed
    std::priority_queue<protocol::Block::Ptr, std::vector<protocol::Block::Ptr>, BlockCmp>
        m_blockQueue;
    mutable SharedMutex x_blockQueue;

    // map between the block to be executed and the callbackList
    std::map<bcos::crypto::HashType, std::queue<ExecutionResultCallback>> m_callbackMap;
    // cache of the block execution result
    std::map<bcos::crypto::HashType, bcos::protocol::BlockHeader::Ptr> m_execResultCache;
    mutable SharedMutex x_execResultCache;
    unsigned m_execResultCacheSize = 20;

    // cache the callback of the executor
    std::queue<std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)>> m_waitingQueue;
};
}  // namespace dispatcher
}  // namespace bcos