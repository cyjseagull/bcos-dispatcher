#pragma once
#include "interfaces/executor/ParallelTransactionExecutorInterface.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include <boost/test/unit_test.hpp>

namespace bcos::test
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
class MockParallelExecutor : public bcos::executor::ParallelTransactionExecutorInterface
{
public:
    MockParallelExecutor(const std::string& name) : m_name(name) {}

    virtual ~MockParallelExecutor() {}

    const std::string& name() const { return m_name; }

    void nextBlockHeader(const bcos::protocol::BlockHeader::ConstPtr& blockHeader,
        std::function<void(bcos::Error::UniquePtr&&)> callback) noexcept override
    {
        m_blockNumber = blockHeader->number();
        callback(nullptr);  // always success
    }

    void executeTransaction(bcos::protocol::ExecutionMessage::UniquePtr input,
        std::function<void(bcos::Error::UniquePtr&&, bcos::protocol::ExecutionMessage::UniquePtr&&)>
            callback) noexcept override
    {
        // Always success
        BOOST_CHECK(input);
        input->setStatus(0);
        input->setMessage("");

        std::string data = "Hello world!";
        input->setData(bcos::bytes(data.begin(), data.end()));
        input->setType(bcos::protocol::ExecutionMessage::FINISHED);

        callback(nullptr, std::move(input));
    }

    void dagExecuteTransactions(
        const gsl::span<bcos::protocol::ExecutionMessage::UniquePtr>& inputs,
        std::function<void(
            bcos::Error::UniquePtr&&, std::vector<bcos::protocol::ExecutionMessage::UniquePtr>&&)>
            callback) noexcept override
    {}

    void call(bcos::protocol::ExecutionMessage::UniquePtr input,
        std::function<void(bcos::Error::UniquePtr&&, bcos::protocol::ExecutionMessage::UniquePtr&&)>
            callback) noexcept override
    {}

    void getHash(bcos::protocol::BlockNumber number,
        std::function<void(bcos::Error::UniquePtr&&, crypto::HashType&&)> callback) noexcept
        override
    {}

    void prepare(const TwoPCParams& params,
        std::function<void(bcos::Error::Ptr&&)> callback) noexcept override
    {}

    void commit(const TwoPCParams& params,
        std::function<void(bcos::Error::Ptr&&)> callback) noexcept override
    {}

    void rollback(const TwoPCParams& params,
        std::function<void(bcos::Error::Ptr&&)> callback) noexcept override
    {}

    void reset(std::function<void(bcos::Error::Ptr&&)> callback) noexcept override {}

    std::string m_name;
    bcos::protocol::BlockNumber m_blockNumber = 0;
};
#pragma GCC diagnostic pop
}  // namespace bcos::test
