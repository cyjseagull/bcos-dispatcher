#include "bcos-dispatcher/DispatcherImpl.h"
#include "bcos-framework/interfaces/protocol/Block.h"
#include "bcos-framework/interfaces/protocol/BlockHeader.h"
#include "bcos-framework/interfaces/protocol/ProtocolTypeDef.h"
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>

namespace bcos {
namespace test {

class MockBlockHeader : public bcos::protocol::BlockHeader {
public:
  MockBlockHeader(bcos::protocol::BlockNumber number) : bcos::protocol::BlockHeader(nullptr), m_number(number) {}

  void decode(bytesConstRef) override {}
  void encode(bytes &) const override {}
  bytesConstRef encode(bool) const override {}
  void clear() override {}
  int32_t version() const override {}
  gsl::span<const bcos::protocol::ParentInfo> parentInfo() const override {}
  bcos::crypto::HashType const &txsRoot() const override {}
  bcos::crypto::HashType const &receiptsRoot() const override {}
  bcos::crypto::HashType const &stateRoot() const override {}
  bcos::protocol::BlockNumber number() const override { return m_number; }
  u256 const &gasUsed() override {}
  int64_t timestamp() override {}
  int64_t sealer() override {}
  gsl::span<const bytes> sealerList() const override {}
  bytesConstRef extraData() const override {}
  gsl::span<const bcos::protocol::Signature> signatureList() const override {}
  gsl::span<const uint64_t> consensusWeights() const override {}
  void setVersion(int32_t) override {}
  void setParentInfo(gsl::span<const bcos::protocol::ParentInfo> const &) override {}
  void setParentInfo(bcos::protocol::ParentInfoList &&) override {}
  void setTxsRoot(bcos::crypto::HashType const &) override {}
  void setReceiptsRoot(bcos::crypto::HashType const &) override {}
  void setStateRoot(bcos::crypto::HashType const &) override {}
  void setNumber(bcos::protocol::BlockNumber) override {}
  void setGasUsed(u256 const &) override {}
  void setTimestamp(int64_t) override {}
  void setSealer(int64_t) override {}
  void setSealerList(gsl::span<const bytes> const &) override {}
  void setSealerList(std::vector<bytes> &&) override {}
  void setConsensusWeights(gsl::span<const uint64_t> const &) override {}
  void setConsensusWeights(std::vector<uint64_t> &&) override {}
  void setExtraData(bytes const &) override {}
  void setExtraData(bytes &&) override {}
  void setSignatureList(gsl::span<const bcos::protocol::Signature> const &) override {}
  void setSignatureList(bcos::protocol::SignatureList &&) override {}

private:
  bcos::protocol::BlockNumber m_number;
};

class MockBlock : public bcos::protocol::Block {
public:
  MockBlock(protocol::BlockHeader::Ptr header) : bcos::protocol::Block(nullptr, nullptr), m_header(header) {}

  void decode(bytesConstRef, bool, bool) override {}
  void encode(bytes &) const override {}
  int32_t version() const override {}
  void setVersion(int32_t) override {}
  bcos::protocol::BlockType blockType() const override {}
  bcos::protocol::BlockHeader::Ptr blockHeader() override { return m_header; }
  bcos::protocol::Transaction::ConstPtr transaction(size_t) const override {}
  bcos::protocol::TransactionReceipt::ConstPtr receipt(size_t) const override {}
  bcos::crypto::HashType const &transactionHash(size_t) const override {}
  void setBlockType(bcos::protocol::BlockType) override {}
  void setBlockHeader(bcos::protocol::BlockHeader::Ptr) override {}
  void setTransaction(size_t, bcos::protocol::Transaction::Ptr) override {}
  void appendTransaction(bcos::protocol::Transaction::Ptr) override {}
  void setReceipt(size_t, bcos::protocol::TransactionReceipt::Ptr) override {}
  void appendReceipt(bcos::protocol::TransactionReceipt::Ptr) override {}
  void appendTransactionHash(bcos::crypto::HashType const &) override {}
  size_t transactionsSize() const override {}
  size_t transactionsHashSize() const override {}
  size_t receiptsSize() const override {}
  void setNonceList(bcos::protocol::NonceList const &) override {}
  void setNonceList(bcos::protocol::NonceList &&) override {}
  bcos::protocol::NonceList const &nonceList() const override {}

private:
  protocol::BlockHeader::Ptr m_header;
};

struct DispatcherFixture {
  DispatcherFixture() { dispatcher = std::make_shared<dispatcher::DispatcherImpl>(); }

  dispatcher::DispatcherImpl::Ptr dispatcher;
};

BOOST_FIXTURE_TEST_SUITE(TestDispatcher, DispatcherFixture)

BOOST_AUTO_TEST_CASE(queue) {
  tbb::atomic<size_t> receiveCount = 0;
  tbb::atomic<size_t> sendCount = 0;

  std::random_device rng;
  std::uniform_int_distribution<> during(0, 1000);

  tbb::parallel_invoke(
      [this, &sendCount, &during, &rng]() {
        for (size_t i = 100; i < 2000; ++i) {
          usleep(during(rng));
          auto testBlockHeader = std::make_shared<MockBlockHeader>(i);
          auto testBlock = std::make_shared<MockBlock>(testBlockHeader);

          // sim push tx
          dispatcher->asyncExecuteBlock(testBlock, false, [this, testBlock, &sendCount](const Error::Ptr &error, const protocol::BlockHeader::Ptr &block) {
            BOOST_CHECK_EQUAL(testBlock->blockHeader()->number(), block->number());
            ++sendCount;
          });
        }
      },
      [this, &receiveCount, &during, &rng]() {
        for (size_t i = 100; i < 2000; ++i) {
          usleep(during(rng));
          dispatcher->asyncGetLatestBlock([this, &receiveCount, i, &during, &rng](const Error::Ptr &error, const protocol::Block::Ptr &block) {
            BOOST_CHECK_EQUAL(block->blockHeader()->number(), i);
            ++receiveCount;

            // sim get and run tx
            usleep(during(rng));
            dispatcher->asyncNotifyExecutionResult(nullptr, block->blockHeader(), [](const Error::Ptr &error) { BOOST_CHECK(!error); });
          });
        }
      });

  dispatcher->asyncGetLatestBlock([](const Error::Ptr &error, const protocol::Block::Ptr &block) { BOOST_FAIL("Expect to be empty"); });

  auto testBlockHeader = std::make_shared<MockBlockHeader>(500);
  dispatcher->asyncNotifyExecutionResult(nullptr, testBlockHeader, [](const Error::Ptr &error) { BOOST_CHECK_EQUAL(error->errorCode(), -1); });

  BOOST_CHECK_EQUAL(receiveCount, sendCount);
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace test
} // namespace bcos
