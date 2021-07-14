#include "DispatcherImpl.h"
#include <future>
#include <thread>

using namespace bcos::dispatcher;
using namespace bcos::crypto;

void DispatcherImpl::asyncExecuteBlock(const protocol::Block::Ptr& _block, bool _verify,
    std::function<void(const Error::Ptr&, const protocol::BlockHeader::Ptr&)> _callback)
{
    // with completed block
    if (_verify)
    {
        asyncExecuteCompletedBlock(_block, _verify, _callback);
        return;
    }
    // with only txsHash
    auto txsHashList = std::make_shared<HashList>();
    for (size_t i = 0; i < _block->transactionsHashSize(); i++)
    {
        txsHashList->emplace_back(_block->transactionHash(i));
    }
    auto self = std::weak_ptr<DispatcherInterface>(shared_from_this());

    std::promise<bool> shouldExecCompletedBlock;
    m_txpool->asyncFillBlock(
        txsHashList, [&shouldExecCompletedBlock, _block, _callback](
                         Error::Ptr _error, bcos::protocol::TransactionsPtr _txs) {
            if (_error)
            {
                DISPATCHER_LOG(ERROR)
                    << LOG_DESC("asyncExecuteBlock: asyncFillBlock failed")
                    << LOG_KV("consNum", _block->blockHeader()->number())
                    << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                    << LOG_KV("code", _error->errorCode()) << LOG_KV("msg", _error->errorMessage());
                _callback(_error, nullptr);
                shouldExecCompletedBlock.set_value(false);
                return;
            }
            // fill the block
            for (auto tx : *_txs)
            {
                _block->appendTransaction(tx);
            }
            // calculate the txsRoot(TODO: async here to optimize the performance)
            _block->calculateTransactionRoot(true);
            shouldExecCompletedBlock.set_value(true);
        });
    if (!shouldExecCompletedBlock.get_future().get())
    {
        return;
    }
    asyncExecuteCompletedBlock(_block, _verify, _callback);
}

void DispatcherImpl::asyncExecuteCompletedBlock(const protocol::Block::Ptr& _block, bool _verify,
    std::function<void(const Error::Ptr&, const protocol::BlockHeader::Ptr&)> _callback)
{
    // Note: the waiting queue must be exist to accelerate the blocks-fetching speed
    std::list<std::function<void()>> callbacks;
    {
        WriteGuard l(x_blockQueue);
        m_blockQueue.push(BlockWithCallback({_block, _verify, _callback}));
        DISPATCHER_LOG(INFO) << LOG_DESC("asyncExecuteCompletedBlock")
                             << LOG_KV("consNum", _block->blockHeader()->number())
                             << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                             << LOG_KV("queueSize", m_blockQueue.size());
        while (!m_waitingQueue.empty())
        {
            auto callback = m_waitingQueue.front();
            m_waitingQueue.pop();

            auto frontItem = m_blockQueue.top();
            callbacks.push_back([callback, frontItem]() { callback(nullptr, frontItem.block); });
        }
    }
    for (auto callback : callbacks)
    {
        callback();
    }
}

void DispatcherImpl::asyncGetLatestBlock(
    std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)> _callback)
{
    protocol::Block::Ptr _obtainedBlock = nullptr;
    {
        ReadGuard l(x_blockQueue);
        // get pending block to execute
        if (!m_blockQueue.empty())
        {
            auto item = m_blockQueue.top();
            // m_blockQueue.pop();
            DISPATCHER_LOG(INFO) << LOG_DESC("asyncGetLatestBlock")
                                 << LOG_KV("consNum", item.block->blockHeader()->number())
                                 << LOG_KV("hash", item.block->blockHeader()->hash().abridged());
            _obtainedBlock = item.block;
        }
        else
        {
            m_waitingQueue.emplace(_callback);
        }
    }
    if (_obtainedBlock)
    {
        _callback(nullptr, _obtainedBlock);
    }
}

void DispatcherImpl::asyncNotifyExecutionResult(const Error::Ptr& _error,
    const protocol::BlockHeader::Ptr& _header, std::function<void(const Error::Ptr&)> _callback)
{
    WriteGuard l(x_blockQueue);
    if (!m_blockQueue.empty())
    {
        auto item = m_blockQueue.top();

        if (item.block->blockHeader()->number() != _header->number())
        {
            DISPATCHER_LOG(ERROR) << LOG_DESC("asyncNotifyExecutionResult error")
                                  << LOG_KV("notify number", item.block->blockHeader()->number())
                                  << LOG_KV("front number",
                                         m_blockQueue.top().block->blockHeader()->number());

            auto error = std::make_shared<bcos::Error>(
                -2, "asyncNotifyExecutionResult error" +
                        boost::lexical_cast<std::string>(_header->number()));
            _callback(error);
            return;
        }

        m_blockQueue.pop();
        DISPATCHER_LOG(INFO) << LOG_DESC("asyncNotifyExecutionResult")
                             << LOG_KV("consNum", _header->number())
                             << LOG_KV("hashAfterExec", _header->hash().abridged());
        item.callback(_error, _header);
    }
    else
    {
        auto error = std::make_shared<bcos::Error>(
            -1, "No such block: " + boost::lexical_cast<std::string>(_header->number()));
        _callback(error);
        return;
    }
    _callback(nullptr);
}

void DispatcherImpl::start() {}

void DispatcherImpl::stop() {}