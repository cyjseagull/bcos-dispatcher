#include "bcos-dispatcher/DispatcherImpl.h"
#include "bcos-framework/interfaces/protocol/Block.h"
#include "bcos-framework/interfaces/protocol/BlockHeader.h"
#include "bcos-framework/interfaces/protocol/ProtocolTypeDef.h"
#include "bcos-framework/testutils/protocol/FakeBlock.h"
#include "bcos-framework/testutils/protocol/FakeBlockHeader.h"
#include <bcos-framework/interfaces/crypto/CryptoSuite.h>
#include <bcos-framework/testutils/crypto/HashImpl.h>
#include <bcos-framework/testutils/crypto/SignatureImpl.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

namespace bcos
{
namespace test
{
struct DispatcherFixture
{
    DispatcherFixture() { dispatcher = std::make_shared<dispatcher::DispatcherImpl>(); }

    dispatcher::DispatcherImpl::Ptr dispatcher;
};

BOOST_FIXTURE_TEST_SUITE(TestDispatcher, DispatcherFixture)

BOOST_AUTO_TEST_CASE(queue)
{
    tbb::atomic<size_t> receiveCount = 0;
    tbb::atomic<size_t> sendCount = 0;

    auto hashImpl = std::make_shared<Keccak256Hash>();
    auto signatureImpl = std::make_shared<Secp256k1SignatureImpl>();
    auto cryptoSuite = std::make_shared<CryptoSuite>(hashImpl, signatureImpl, nullptr);
    auto blockFactory = createBlockFactory(cryptoSuite);

    std::random_device rng;
    std::uniform_int_distribution<> during(0, 100);

    tbb::parallel_invoke(
        [this, &sendCount, &during, &rng, cryptoSuite, blockFactory]() {
            for (size_t i = 100; i < 2000; ++i)
            {
                usleep(during(rng));

                auto testBlock = fakeAndCheckBlock(cryptoSuite, blockFactory, true, 0, 0);
                testBlock->blockHeader()->setNumber(i);
                // sim push tx
                dispatcher->asyncExecuteCompletedBlock(testBlock, false,
                    [testBlock, &sendCount](
                        const Error::Ptr& error, const protocol::BlockHeader::Ptr& block) {
                        (void)error;
                        BOOST_CHECK_EQUAL(testBlock->blockHeader()->number(), block->number());
                        ++sendCount;
                    });
            }
        },
        [this, &receiveCount, &during, &rng]() {
            tbb::atomic<int64_t> i = 100;
            while (true)
            {
                usleep(during(rng));

                dispatcher->asyncGetLatestBlock([this, &receiveCount, &i](const Error::Ptr&,
                                                    const protocol::Block::Ptr& block) {
                    // BOOST_CHECK(block->blockHeader()->number() == i ||
                    //             block->blockHeader()->number() - i == 1);
                    BOOST_CHECK(block->blockHeader()->number() < 2000);
                    ++receiveCount;

                    if (block->blockHeader()->number() - i == 1)
                    {
                        i = block->blockHeader()->number();
                    }

                    // sim get and run tx
                    dispatcher->asyncNotifyExecutionResult(nullptr, block->blockHeader()->hash(),
                        block->blockHeader(), [](const Error::Ptr& error) {
                            if (error)
                            {
                                BOOST_CHECK(error->errorCode() != -2);
                            }
                        });
                });

                if (i == 1999)
                {
                    break;
                }
            }
        });

    dispatcher->asyncGetLatestBlock(
        [](const Error::Ptr&, const protocol::Block::Ptr&) { BOOST_FAIL("Expect to be empty"); });

    auto testBlockHeader = testPBBlockHeader(cryptoSuite);
    testBlockHeader->setNumber(500);
    dispatcher->asyncNotifyExecutionResult(nullptr, testBlockHeader->hash(), testBlockHeader,
        [](const Error::Ptr& error) { BOOST_CHECK_EQUAL(error->errorCode(), -1); });

    BOOST_CHECK_GE(receiveCount, sendCount);
}

BOOST_AUTO_TEST_SUITE_END()

}  // namespace test
}  // namespace bcos
