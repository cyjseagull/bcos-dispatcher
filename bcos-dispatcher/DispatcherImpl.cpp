#include "DispatcherImpl.h"
#include <thread>

using namespace bcos::dispatcher;

void DispatcherImpl::asyncExecuteBlock(const protocol::Block::Ptr &_block, bool _verify,
                                       std::function<void(const Error::Ptr &, const protocol::BlockHeader::Ptr &)> _callback) {
  m_queue.emplace(_block->blockHeader()->number(), BlockWithCallback({_block, _verify, _callback}));
}
void DispatcherImpl::asyncGetLatestBlock(std::function<void(const Error::Ptr &, const protocol::Block::Ptr &)> _callback) {
    auto item = m_queue.begin();
    if(item == m_queue.end()) {
        // std::thread::hardware_concurrency()
    }
}

void DispatcherImpl::asyncNotifyExecutionResult(const Error::Ptr &_error, const protocol::BlockHeader::Ptr &_header,
                                                std::function<void(const Error::Ptr &)> _callback) {}
void DispatcherImpl::start() {}
void DispatcherImpl::stop() {}