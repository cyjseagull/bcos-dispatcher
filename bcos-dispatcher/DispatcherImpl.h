#pragma once

#include <gsl/span>
#include <tbb/concurrent_queue.h>
#include "bcos-framework/interfaces/dispatcher/DispatcherInterface.h"
#include "bcos-framework/interfaces/executor/ExecutorInterface.h"

namespace bcos {
namespace dispatcher {
class DispatcherImpl : public DispatcherInterface {
public:
  ~DispatcherImpl() override {}
  void asyncExecuteBlock(const protocol::Block::Ptr &_block, bool _verify,
                         std::function<void(const Error::Ptr &, const protocol::BlockHeader::Ptr &)> _callback) override;
  void asyncGetLatestBlock(std::function<void(const Error::Ptr &, const protocol::Block::Ptr &)> _callback) override;

  void asyncNotifyExecutionResult(const Error::Ptr &_error, const protocol::BlockHeader::Ptr &_header,
                                  std::function<void(const Error::Ptr &)> _callback) override;
  void start() override;
  void stop() override;

private:
  struct BlockWithCallback {
    protocol::Block::Ptr block;
    bool verify;
    std::function<void(const Error::Ptr &, const protocol::BlockHeader::Ptr &)> callback;
  };

  std::shared_ptr<bcos::executor::ExecutorInterface> m_executor;
  std::map<bcos::protocol::BlockNumber, BlockWithCallback> m_queue;
};
} // namespace dispatcher
} // namespace bcos