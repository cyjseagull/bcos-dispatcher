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
    auto item = BlockWithCallback({_block, _verify, _callback});
    std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)> callback;

    DISPATCHER_LOG(INFO) << LOG_DESC("asyncExecuteCompletedBlock")
                         << LOG_KV("consNum", _block->blockHeader()->number())
                         << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                         << LOG_KV("verify", _verify);
    tbb::mutex::scoped_lock scoped(m_mutex);
    auto result = m_waitingQueue.try_pop(callback);
    if (result)
    {
        m_number2Callback.emplace(item.block->blockHeader()->number(), item);
        scoped.release();
        callback(nullptr, item.block);
    }
    else
    {
        m_blockQueue.emplace(BlockWithCallback({_block, _verify, _callback}));
    }
}

void DispatcherImpl::asyncGetLatestBlock(
    std::function<void(const Error::Ptr&, const protocol::Block::Ptr&)> _callback)
{
    BlockWithCallback item;

    tbb::mutex::scoped_lock scoped(m_mutex);
    auto result = m_blockQueue.try_pop(item);
    if (result)
    {
        DISPATCHER_LOG(INFO) << LOG_DESC("asyncGetLatestBlock")
                             << LOG_KV("consNum", item.block->blockHeader()->number())
                             << LOG_KV("hash", item.block->blockHeader()->hash().abridged());
        m_number2Callback.emplace(item.block->blockHeader()->number(), item);
        scoped.release();
        _callback(nullptr, item.block);
    }
    else
    {
        m_waitingQueue.push(_callback);
    }
}

void DispatcherImpl::asyncNotifyExecutionResult(const Error::Ptr& _error,
    const protocol::BlockHeader::Ptr& _header, std::function<void(const Error::Ptr&)> _callback)
{
    {
        tbb::mutex::scoped_lock scoped(m_mutex);
        auto it = m_number2Callback.find(_header->number());
        if (it != m_number2Callback.end())
        {
            auto item = it->second;
            m_number2Callback.erase(it);

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
            _callback(error);

            return;
        }
    }

    _callback(nullptr);
}

void DispatcherImpl::start() {}

void DispatcherImpl::stop() {}