#include "DispatcherImpl.h"
#include <thread>

using namespace bcos::dispatcher;
using namespace bcos::crypto;
using namespace bcos::protocol;

void DispatcherImpl::asyncExecuteBlock(
    const Block::Ptr& _block, bool _verify, ExecutionResultCallback _callback)
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
    m_txpool->asyncFillBlock(
        txsHashList, [self, _block, _verify, _callback](Error::Ptr _error, TransactionsPtr _txs) {
            if (_error)
            {
                DISPATCHER_LOG(ERROR)
                    << LOG_DESC("asyncExecuteBlock: asyncFillBlock failed")
                    << LOG_KV("consNum", _block->blockHeader()->number())
                    << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                    << LOG_KV("code", _error->errorCode()) << LOG_KV("msg", _error->errorMessage());
                _callback(_error, nullptr);
                return;
            }
            try
            {
                auto dispatcher = self.lock();
                if (!dispatcher)
                {
                    _callback(std::make_shared<Error>(-1, "internal error"), nullptr);
                    return;
                }
                // fill the block
                for (auto tx : *_txs)
                {
                    _block->appendTransaction(tx);
                }
                // calculate the txsRoot(TODO: async here to optimize the performance)
                _block->calculateTransactionRoot(true);
                auto dispatcherImpl = std::dynamic_pointer_cast<DispatcherImpl>(dispatcher);
                dispatcherImpl->asyncExecuteCompletedBlock(_block, _verify, _callback);
            }
            catch (std::exception const& e)
            {
                DISPATCHER_LOG(WARNING) << LOG_DESC("asyncExecuteBlock exception")
                                        << LOG_KV("error", boost::diagnostic_information(e))
                                        << LOG_KV("consNum", _block->blockHeader()->number())
                                        << LOG_KV("hash", _block->blockHeader()->hash().abridged());
                _callback(std::make_shared<Error>(-1, "internal error"), nullptr);
            }
        });
}

void DispatcherImpl::asyncExecuteCompletedBlock(
    const Block::Ptr& _block, bool _verify, ExecutionResultCallback _callback)
{
    auto cacheHeader = getExecResultCache(_block->blockHeader()->hash());
    if (cacheHeader)
    {
        _callback(nullptr, cacheHeader);
        DISPATCHER_LOG(INFO) << LOG_DESC("asyncExecuteCompletedBlock: hit the cache")
                             << LOG_KV("consNum", _block->blockHeader()->number())
                             << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                             << LOG_KV("hashAfterExec", cacheHeader->hash().abridged())
                             << LOG_KV("verify", _verify);
        return;
    }
    // Note: the waiting queue must be exist to accelerate the blocks-fetching speed
    std::list<std::function<void()>> callbacks;
    {
        WriteGuard l(x_blockQueue);
        auto hash = _block->blockHeader()->hash();
        m_blockQueue.push(_block);
        if (m_callbackMap.count(hash))
        {
            m_callbackMap[hash].push(_callback);
        }
        else
        {
            std::queue<ExecutionResultCallback> callbackList;
            m_callbackMap[hash] = callbackList;
            m_callbackMap[hash].push(_callback);
        }
        DISPATCHER_LOG(INFO) << LOG_DESC("asyncExecuteCompletedBlock")
                             << LOG_KV("consNum", _block->blockHeader()->number())
                             << LOG_KV("hash", _block->blockHeader()->hash().abridged())
                             << LOG_KV("queueSize", m_blockQueue.size())
                             << LOG_KV("verify", _verify);
        std::vector<Block::Ptr> blockQueueCache;
        while (!m_waitingQueue.empty() && !m_blockQueue.empty())
        {
            auto callback = m_waitingQueue.front();
            m_waitingQueue.pop();
            // Note: since the callback maybe uncalled for executor timeout, here can't pop the
            // block
            auto block = m_blockQueue.top();
            blockQueueCache.push_back(block);
            m_blockQueue.pop();
            DISPATCHER_LOG(INFO) << LOG_DESC(
                                        "asyncGetLatestBlock: dispatch block to the waiting queue")
                                 << LOG_KV("consNum", block->blockHeader()->number())
                                 << LOG_KV("hash", block->blockHeader()->hash().abridged());
            callbacks.push_back([callback, block]() { callback(nullptr, block); });
        }
        // repush the block into the blockQueue
        for (size_t i = 0; i < blockQueueCache.size(); i++)
        {
            m_blockQueue.push(blockQueueCache[i]);
        }
    }
    for (auto callback : callbacks)
    {
        callback();
    }
}

void DispatcherImpl::asyncGetLatestBlock(
    std::function<void(const Error::Ptr&, const Block::Ptr&)> _callback)
{
    Block::Ptr _obtainedBlock = nullptr;
    bool existUnExecutedBlock = false;
    {
        WriteGuard l(x_blockQueue);
        // clear the expired waiting queue
        if (!m_waitingQueue.empty())
        {
            m_waitingQueue.pop();
        }
        // get pending block to execute
        while (!m_blockQueue.empty())
        {
            _obtainedBlock = m_blockQueue.top();
            auto blockHash = _obtainedBlock->blockHeader()->hash();
            m_blockQueue.pop();
            if (m_callbackMap.count(blockHash))
            {
                existUnExecutedBlock = true;
                break;
            }
            // the block has already been executed
            DISPATCHER_LOG(INFO) << LOG_DESC("asyncGetLatestBlock: block has already been executed")
                                 << LOG_KV("consNum", _obtainedBlock->blockHeader()->number())
                                 << LOG_KV("hash", blockHash.abridged());
        }
        // push back the callback to the waiting queue for the new block
        if (!existUnExecutedBlock)
        {
            m_waitingQueue.emplace(_callback);
            return;
        }
    }
    if (existUnExecutedBlock)
    {
        _callback(nullptr, _obtainedBlock);
        DISPATCHER_LOG(INFO) << LOG_DESC("asyncGetLatestBlock: dispatch block")
                             << LOG_KV("consNum", _obtainedBlock->blockHeader()->number())
                             << LOG_KV("hash", _obtainedBlock->blockHeader()->hash().abridged());
    }
}

void DispatcherImpl::updateExecResultCache(const Error::Ptr& _error,
    bcos::crypto::HashType const& _orgHash, const BlockHeader::Ptr& _header)
{
    if (_error)
    {
        return;
    }
    UpgradableGuard l(x_execResultCache);
    if (m_execResultCache.count(_orgHash))
    {
        return;
    }
    UpgradeGuard ul(l);
    if (m_execResultCache.size() >= m_execResultCacheSize)
    {
        m_execResultCache.clear();
    }
    // TODO: save a populated blockHeader in case of the header will be change by the caller
    m_execResultCache[_orgHash] = _header;
    m_execResultCache[_header->hash()] = _header;
}

BlockHeader::Ptr DispatcherImpl::getExecResultCache(bcos::crypto::HashType const& _hash)
{
    ReadGuard l(x_execResultCache);
    if (!m_execResultCache.count(_hash))
    {
        return nullptr;
    }
    // TODO: save a populated blockHeader in case of the header will be change by the caller
    return m_execResultCache[_hash];
}

void DispatcherImpl::asyncNotifyExecutionResult(const Error::Ptr& _error,
    bcos::crypto::HashType const& _orgHash, const BlockHeader::Ptr& _header,
    std::function<void(const Error::Ptr&)> _callback)
{
    ExecutionResultCallback callback = nullptr;
    {
        WriteGuard l(x_blockQueue);
        if (!m_callbackMap.count(_orgHash))
        {
            auto error = std::make_shared<bcos::Error>(
                -1, "No such block: " + boost::lexical_cast<std::string>(_header->number()));
            _callback(error);
            return;
        }
        callback = m_callbackMap[_orgHash].front();
        m_callbackMap[_orgHash].pop();
        if ((m_callbackMap[_orgHash]).empty())
        {
            m_callbackMap.erase(_orgHash);
        }
        // clear the blockQueue
        if (m_callbackMap.size() == 0)
        {
            std::priority_queue<protocol::Block::Ptr, std::vector<protocol::Block::Ptr>, BlockCmp>
                emptyQueue;
            m_blockQueue = emptyQueue;
        }
    }
    // Note: must call the callback after the lock, in case of the callback retry to call
    // asyncExecuteBlock
    auto cachedHeader = getExecResultCache(_orgHash);
    if (_error && cachedHeader)
    {
        // the block has already been executed
        callback(nullptr, cachedHeader);
        DISPATCHER_LOG(WARNING)
            << LOG_DESC("asyncNotifyExecutionResult: block execute failed, but hit the cache")
            << LOG_KV("code", _error->errorCode()) << LOG_KV("msg", _error->errorMessage())
            << LOG_KV("consNum", cachedHeader->number()) << LOG_KV("orgHash", _orgHash.abridged())
            << LOG_KV("hashAfterExec", cachedHeader->hash().abridged());
    }
    else
    {
        callback(_error, _header);
    }
    updateExecResultCache(_error, _orgHash, _header);

    DISPATCHER_LOG(INFO) << LOG_DESC("asyncNotifyExecutionResult")
                         << LOG_KV("consNum", _header->number())
                         << LOG_KV("orgHash", _orgHash.abridged())
                         << LOG_KV("hashAfterExec", _header->hash().abridged());
    // notify success to the executor
    _callback(nullptr);
}

void DispatcherImpl::start() {}

void DispatcherImpl::stop()
{
    DISPATCHER_LOG(INFO) << LOG_DESC("stop the dispatcher");
    // clear all the callbacks
    // Note: here must call all the callback in case of the executor stucked at waiting the callback
    std::list<std::function<void()>> callbacks;
    {
        WriteGuard l(x_blockQueue);
        while (!m_waitingQueue.empty())
        {
            auto callback = m_waitingQueue.front();
            m_waitingQueue.pop();
            callbacks.push_back([callback]() {
                callback(std::make_shared<Error>(-1, "the blockQueue is empty"), nullptr);
            });
        }
    }
    for (auto callback : callbacks)
    {
        callback();
    }
}