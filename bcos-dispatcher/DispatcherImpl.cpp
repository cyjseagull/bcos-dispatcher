#include "DispatcherImpl.h"
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
    m_txpool->asyncFillBlock(txsHashList, [self, _block, _verify, _callback](Error::Ptr _error,
                                              bcos::protocol::TransactionsPtr _txs) {
        if (_error)
        {
            DISPATCHER_LOG(ERROR) << LOG_DESC("asyncExecuteBlock: asyncFillBlock failed")
                                  << LOG_KV("consNum", _block->blockHeader()->number())
                                  << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                                  << LOG_KV("code", _error->errorCode())
                                  << LOG_KV("msg", _error->errorMessage());
            _callback(_error, nullptr);
            return;
        }
        // fill the block
        for (auto tx : *_txs)
        {
            _block->appendTransaction(tx);
        }
        try
        {
            auto dispatcher = self.lock();
            if (!dispatcher)
            {
                _callback(std::make_shared<Error>(-1, "asyncExecuteBlock exception"), nullptr);
                return;
            }
            // calculate the txsRoot(TODO: async here to optimize the performance)
            _block->calculateTransactionRoot(true);
            auto dispatcherImpl = std::dynamic_pointer_cast<DispatcherImpl>(dispatcher);
            dispatcherImpl->asyncExecuteCompletedBlock(_block, _verify, _callback);
        }
        catch (std::exception const& e)
        {
            DISPATCHER_LOG(ERROR) << LOG_DESC("asyncExecuteBlock exception")
                                  << LOG_KV("error", boost::diagnostic_information(e));
        }
    });
}

void DispatcherImpl::asyncExecuteCompletedBlock(const protocol::Block::Ptr& _block, bool _verify,
    std::function<void(const Error::Ptr&, const protocol::BlockHeader::Ptr&)> _callback)
{
    DISPATCHER_LOG(INFO) << LOG_DESC("asyncExecuteCompletedBlock")
                         << LOG_KV("consNum", _block->blockHeader()->number())
                         << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                         << LOG_KV("verify", _verify);
    std::list<std::function<void()>> callbacks;
    
    {
        tbb::mutex::scoped_lock scoped(m_mutex);

        m_blockQueue.emplace(BlockWithCallback({_block, _verify, _callback}));

        // clear the waiting queue
        while (!m_waitingQueue.empty())
        {
            auto callback = m_waitingQueue.front();
            m_waitingQueue.pop();

            auto frontItem = m_blockQueue.front();
            callbacks.push_back([callback, frontItem]() { callback(nullptr, frontItem.block); });
        }
    }

    for(auto callback: callbacks) {
        callback();
    }
}

void DispatcherImpl::asyncGetLatestBlock(
    std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)> _callback)
{
    tbb::mutex::scoped_lock scoped(m_mutex);
    if (!m_blockQueue.empty())
    {
        auto item = m_blockQueue.front();
        // m_blockQueue.pop();
        DISPATCHER_LOG(INFO) << LOG_DESC("asyncGetLatestBlock")
                             << LOG_KV("consNum", item.block->blockHeader()->number())
                             << LOG_KV("hash", item.block->blockHeader()->hash().abridged());
        scoped.release();
        _callback(nullptr, item.block);
    }
    else
    {
        m_waitingQueue.emplace(_callback);
    }
}

void DispatcherImpl::asyncNotifyExecutionResult(const Error::Ptr& _error,
    const protocol::BlockHeader::Ptr& _header, std::function<void(const Error::Ptr&)> _callback)
{
    {
        tbb::mutex::scoped_lock scoped(m_mutex);

        if (!m_blockQueue.empty())
        {
            auto item = m_blockQueue.front();

            if (item.block->blockHeader()->number() != _header->number())
            {
                DISPATCHER_LOG(ERROR)
                    << LOG_DESC("asyncNotifyExecutionResult error")
                    << LOG_KV("notify number", item.block->blockHeader()->number())
                    << LOG_KV("front number", m_blockQueue.front().block->blockHeader()->number());

                auto error = std::make_shared<bcos::Error>(
                    -2, "asyncNotifyExecutionResult error" +
                            boost::lexical_cast<std::string>(_header->number()));

                scoped.release();
                _callback(error);
                return;
            }

            m_blockQueue.pop();

            scoped.release();
            DISPATCHER_LOG(INFO) << LOG_DESC("asyncNotifyExecutionResult")
                                 << LOG_KV("consNum", _header->number())
                                 << LOG_KV("hashAfterExec", _header->hash().abridged());
            item.callback(_error, _header);
        }
        else
        {
            auto error = std::make_shared<bcos::Error>(
                -1, "No such block: " + boost::lexical_cast<std::string>(_header->number()));

            scoped.release();
            _callback(error);

            return;
        }
    }

    _callback(nullptr);
}

void DispatcherImpl::start() {}

void DispatcherImpl::stop() {}