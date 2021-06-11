#include "DispatcherImpl.h"
#include <thread>

using namespace bcos::dispatcher;

void DispatcherImpl::asyncExecuteBlock(const protocol::Block::Ptr &_block, bool _verify,
                                       std::function<void(const Error::Ptr &, const protocol::BlockHeader::Ptr &)> _callback) {
  auto item = BlockWithCallback({_block, _verify, _callback});
  std::function<void(const Error::Ptr &, const protocol::Block::Ptr &)> callback;

  tbb::mutex::scoped_lock scoped(m_mutex);
  auto result = m_waitingQueue.try_pop(callback);
  if (result) {
    m_number2Callback.emplace(item.block->blockHeader()->number(), item);
    scoped.release();
    callback(nullptr, item.block);
  } else {
    m_blockQueue.emplace(BlockWithCallback({_block, _verify, _callback}));
  }
}

void DispatcherImpl::asyncGetLatestBlock(std::function<void(const Error::Ptr &, const protocol::Block::Ptr &)> _callback) {
  BlockWithCallback item;

  tbb::mutex::scoped_lock scoped(m_mutex);
  auto result = m_blockQueue.try_pop(item);
  if (result) {
    m_number2Callback.emplace(item.block->blockHeader()->number(), item);
    scoped.release();
    _callback(nullptr, item.block);
  } else {
    m_waitingQueue.push(_callback);
  }
}

void DispatcherImpl::asyncNotifyExecutionResult(const Error::Ptr &_error, const protocol::BlockHeader::Ptr &_header,
                                                std::function<void(const Error::Ptr &)> _callback) {
  {
    tbb::mutex::scoped_lock scoped(m_mutex);
    auto it = m_number2Callback.find(_header->number());
    if (it != m_number2Callback.end()) {
      auto &item = it->second;

      item.callback(_error, _header);
    }
  }

  _callback(nullptr);
}

void DispatcherImpl::start() {}

void DispatcherImpl::stop() {}