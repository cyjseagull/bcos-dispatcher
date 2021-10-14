#include "bcos-scheduler/ExecutorManager.h"
#include "bcos-scheduler/SchedulerImpl.h"
#include "interfaces/crypto/CryptoSuite.h"
#include "interfaces/executor/ExecutionMessage.h"
#include "interfaces/ledger/LedgerInterface.h"
#include "interfaces/protocol/BlockHeaderFactory.h"
#include "interfaces/protocol/TransactionReceiptFactory.h"
#include "interfaces/storage/StorageInterface.h"
#include "mock/MockExecutor.h"
#include "mock/MockExecutor3.h"
#include "mock/MockLedger.h"
#include "mock/MockTransactionalStorage.h"
#include <bcos-framework/libexecutor/NativeExecutionMessage.h>
#include <bcos-framework/testutils/crypto/HashImpl.h>
#include <bcos-framework/testutils/crypto/SignatureImpl.h>
#include <bcos-tars-protocol/BlockFactoryImpl.h>
#include <bcos-tars-protocol/BlockHeaderFactoryImpl.h>
#include <bcos-tars-protocol/TransactionFactoryImpl.h>
#include <bcos-tars-protocol/TransactionMetaDataImpl.h>
#include <bcos-tars-protocol/TransactionReceiptFactoryImpl.h>
#include <boost/test/unit_test.hpp>

namespace bcos::test
{
struct SchedulerFixture
{
    SchedulerFixture()
    {
        hashImpl = std::make_shared<Keccak256Hash>();
        signature = std::make_shared<Secp256k1SignatureImpl>();
        suite = std::make_shared<bcos::crypto::CryptoSuite>(hashImpl, signature, nullptr);

        ledger = std::make_shared<MockLedger>();
        executorManager = std::make_shared<scheduler::ExecutorManager>();
        storage = std::make_shared<MockTransactionalStorage>();
        transactionFactory = std::make_shared<bcostars::protocol::TransactionFactoryImpl>(suite);
        transactionReceiptFactory =
            std::make_shared<bcostars::protocol::TransactionReceiptFactoryImpl>(suite);
        blockHeaderFactory = std::make_shared<bcostars::protocol::BlockHeaderFactoryImpl>(suite);
        executionMessageFactory = std::make_shared<bcos::executor::NativeExecutionMessageFactory>();

        scheduler = std::make_shared<scheduler::SchedulerImpl>(executorManager, ledger, storage,
            executionMessageFactory, transactionReceiptFactory, blockHeaderFactory, hashImpl);

        blockFactory = std::make_shared<bcostars::protocol::BlockFactoryImpl>(
            suite, blockHeaderFactory, transactionFactory, transactionReceiptFactory);
    }

    ledger::LedgerInterface::Ptr ledger;
    scheduler::ExecutorManager::Ptr executorManager;
    storage::TransactionalStorageInterface::Ptr storage;
    protocol::ExecutionMessageFactory::Ptr executionMessageFactory;
    protocol::TransactionReceiptFactory::Ptr transactionReceiptFactory;
    protocol::BlockHeaderFactory::Ptr blockHeaderFactory;
    bcos::crypto::Hash::Ptr hashImpl;
    scheduler::SchedulerImpl::Ptr scheduler;

    bcostars::protocol::TransactionFactoryImpl::Ptr transactionFactory;
    bcos::crypto::SignatureCrypto::Ptr signature;
    bcos::crypto::CryptoSuite::Ptr suite;
    bcostars::protocol::BlockFactoryImpl::Ptr blockFactory;
};

BOOST_FIXTURE_TEST_SUITE(Scheduler, SchedulerFixture)

BOOST_AUTO_TEST_CASE(executeBlock)
{
    // Add executor
    executorManager->addExecutor("executor1", std::make_shared<MockParallelExecutor3>("executor1"));

    // Generate a test block
    auto block = blockFactory->createBlock();
    block->blockHeader()->setNumber(100);

    for (size_t i = 0; i < 10; ++i)
    {
        auto metaTx =
            std::make_shared<bcostars::protocol::TransactionMetaDataImpl>(h256(i), "contract1");
        block->appendTransactionMetaData(std::move(metaTx));
    }

    for (size_t i = 10; i < 20; ++i)
    {
        auto metaTx =
            std::make_shared<bcostars::protocol::TransactionMetaDataImpl>(h256(i), "contract2");
        block->appendTransactionMetaData(std::move(metaTx));
    }

    for (size_t i = 20; i < 30; ++i)
    {
        auto metaTx =
            std::make_shared<bcostars::protocol::TransactionMetaDataImpl>(h256(i), "contract3");
        block->appendTransactionMetaData(std::move(metaTx));
    }

    bcos::protocol::BlockHeader::Ptr executedHeader;

    scheduler->executeBlock(
        block, false, [&](bcos::Error::Ptr&& error, bcos::protocol::BlockHeader::Ptr&& header) {
            BOOST_CHECK(!error);
            BOOST_CHECK(header);

            executedHeader = std::move(header);
        });

    BOOST_CHECK(executedHeader);
    BOOST_CHECK_NE(executedHeader->stateRoot(), h256());

    scheduler->commitBlock(
        executedHeader, [&](bcos::Error::Ptr&& error, bcos::ledger::LedgerConfig::Ptr&& config) {
            BOOST_CHECK(!error);
            // BOOST_CHECK(config);
            (void)config;
        });
}

BOOST_AUTO_TEST_CASE(registerExecutor)
{
    auto executor = std::make_shared<MockParallelExecutor>("executor1");
    auto executor2 = std::make_shared<MockParallelExecutor>("executor2");

    scheduler->registerExecutor(
        "executor1", executor, [&](Error::Ptr&& error) { BOOST_CHECK(!error); });
    scheduler->registerExecutor(
        "executor2", executor2, [&](Error::Ptr&& error) { BOOST_CHECK(!error); });
}

BOOST_AUTO_TEST_SUITE_END()
}  // namespace bcos::test