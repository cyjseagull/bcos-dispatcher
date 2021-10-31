#pragma once

#include "../../bcos-scheduler/Common.h"
#include <bcos-framework/interfaces/rpc/RPCInterface.h>
#include <boost/test/unit_test.hpp>
#include <boost/thread/latch.hpp>

namespace bcos::test
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

class MockRPC : public bcos::rpc::RPCInterface
{
public:
    void start() override {}
    void stop() override {}

    void asyncNotifyBlockNumber(std::string const& _groupID, std::string const& _nodeName,
        bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr)> _callback) override
    {}

    void asyncNotifyTransactionResult(std::string const&, const std::string_view& groupID,
        bcos::crypto::HashType txHash, bcos::protocol::TransactionSubmitResult::Ptr result) override
    {
        SCHEDULER_LOG(TRACE) << "Submit callback execute";

        BOOST_CHECK_EQUAL(groupID.size(), 0);
        BOOST_CHECK_EQUAL(result->status(), 0);
        BOOST_CHECK_NE(result->blockHash(), h256(0));
        BOOST_CHECK(result->transactionReceipt());
        BOOST_CHECK_LT(result->transactionIndex(), 1000 * 8);

        auto receipt = result->transactionReceipt();
        auto output = receipt->output();
        std::string_view outputStr((char*)output.data(), output.size());
        BOOST_CHECK_EQUAL(outputStr, "Hello world!");

        if (latch)
        {
            latch->count_down();
        }
    }

    void asyncNotifyGroupInfo(
        bcos::group::GroupInfo::Ptr _groupInfo, std::function<void(Error::Ptr&&)>) override
    {}

    boost::latch* latch = nullptr;
};

#pragma GCC diagnostic pop

}  // namespace bcos::test