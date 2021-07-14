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
    DispatcherImpl() = default;
    ~DispatcherImpl() override {}
    void asyncExecuteBlock(const protocol::Block::Ptr& _block, bool _verify,
        std::function<void(const Error::Ptr&, const protocol::BlockHeader::Ptr&)> _callback)
        override;
    void asyncGetLatestBlock(
        std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)> _callback) override;

    void asyncNotifyExecutionResult(const Error::Ptr& _error,
        const protocol::BlockHeader::Ptr& _header,
        std::function<void(const Error::Ptr&)> _callback) override;

    void init(bcos::txpool::TxPoolInterface::Ptr _txpool) { m_txpool = _txpool; }
    void start() override;
    void stop() override;

    virtual void asyncExecuteCompletedBlock(const protocol::Block::Ptr& _block, bool _verify,
        std::function<void(const Error::Ptr&, const protocol::BlockHeader::Ptr&)> _callback);

private:
    struct BlockWithCallback
    {
        protocol::Block::Ptr block;
        bool verify;
        std::function<void(const Error::Ptr&, const protocol::BlockHeader::Ptr&)> callback;
    };

    bcos::txpool::TxPoolInterface::Ptr m_txpool;

    // increase order
    struct BlockCmp
    {
        bool operator()(BlockWithCallback const& _first, BlockWithCallback const& _second) const
        {
            // increase order
            return _first.block->blockHeader()->number() > _second.block->blockHeader()->number();
        }
    };
    std::priority_queue<BlockWithCallback, std::vector<BlockWithCallback>, BlockCmp> m_blockQueue;
    std::queue<std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)>> m_waitingQueue;
    mutable SharedMutex x_blockQueue;
};
}  // namespace dispatcher
}  // namespace bcos