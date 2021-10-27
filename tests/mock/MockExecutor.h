#pragma once
#include "bcos-scheduler/Common.h"
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

    ~MockParallelExecutor() override {}

    const std::string& name() const { return m_name; }

    void nextBlockHeader(const bcos::protocol::BlockHeader::ConstPtr& blockHeader,
        std::function<void(bcos::Error::UniquePtr)> callback) override
    {
        SCHEDULER_LOG(TRACE) << "Receiving nextBlock: " << blockHeader->number();
        m_blockNumber = blockHeader->number();
        callback(nullptr);  // always success
    }

    void executeTransaction(bcos::protocol::ExecutionMessage::UniquePtr input,
        std::function<void(bcos::Error::UniquePtr, bcos::protocol::ExecutionMessage::UniquePtr)>
            callback) override
    {
        // Always success
        BOOST_CHECK(input);
        if (input->type() == bcos::protocol::ExecutionMessage::TXHASH)
        {
            BOOST_CHECK_NE(input->transactionHash(), bcos::crypto::HashType());
        }

        input->setStatus(0);
        input->setMessage("");

        std::string data = "Hello world!";
        input->setData(bcos::bytes(data.begin(), data.end()));
        input->setType(bcos::protocol::ExecutionMessage::FINISHED);

        callback(nullptr, std::move(input));
    }

    void dagExecuteTransactions(gsl::span<bcos::protocol::ExecutionMessage::UniquePtr> inputs,
        std::function<void(
            bcos::Error::UniquePtr, std::vector<bcos::protocol::ExecutionMessage::UniquePtr>)>
            callback) override
    {}

    void call(bcos::protocol::ExecutionMessage::UniquePtr input,
        std::function<void(bcos::Error::UniquePtr, bcos::protocol::ExecutionMessage::UniquePtr)>
            callback) override
    {}

    void getHash(bcos::protocol::BlockNumber number,
        std::function<void(bcos::Error::UniquePtr, crypto::HashType)> callback) override
    {
        callback(nullptr, h256(12345));
    }

    void prepare(const TwoPCParams& params, std::function<void(bcos::Error::Ptr)> callback) override
    {
        callback(nullptr);
    }

    void commit(const TwoPCParams& params, std::function<void(bcos::Error::Ptr)> callback) override
    {
        callback(nullptr);
    }

    void rollback(
        const TwoPCParams& params, std::function<void(bcos::Error::Ptr)> callback) override
    {
        callback(nullptr);
    }

    void reset(std::function<void(bcos::Error::Ptr)> callback) override {}

    std::string m_name;
    bcos::protocol::BlockNumber m_blockNumber = 0;
};
#pragma GCC diagnostic pop
}  // namespace bcos::test
