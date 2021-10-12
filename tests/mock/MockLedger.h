#pragma once

#include "bcos-framework/interfaces/ledger/LedgerInterface.h"

namespace bcos::test
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
class MockLedger : public bcos::ledger::LedgerInterface
{
    void asyncPrewriteBlock(bcos::storage::StorageInterface::Ptr storage,
        bcos::protocol::Block::ConstPtr block, std::function<void(Error::Ptr&&)> callback)
    {}

    void asyncStoreTransactions(std::shared_ptr<std::vector<bytesConstPtr>> _txToStore,
        crypto::HashListPtr _txHashList, std::function<void(Error::Ptr)> _onTxStored)
    {}

    void asyncGetBlockDataByNumber(protocol::BlockNumber _blockNumber, int32_t _blockFlag,
        std::function<void(Error::Ptr, protocol::Block::Ptr)> _onGetBlock)
    {}

    void asyncGetBlockNumber(std::function<void(Error::Ptr, protocol::BlockNumber)> _onGetBlock) {}

    void asyncGetBlockHashByNumber(protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, crypto::HashType const&)> _onGetBlock)
    {}

    void asyncGetBlockNumberByHash(crypto::HashType const& _blockHash,
        std::function<void(Error::Ptr, protocol::BlockNumber)> _onGetBlock)
    {}

    void asyncGetBatchTxsByHashList(crypto::HashListPtr _txHashList, bool _withProof,
        std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr,
            std::shared_ptr<std::map<std::string, bcos::ledger::MerkleProofPtr>>)>
            _onGetTx)
    {}

    void asyncGetTransactionReceiptByHash(crypto::HashType const& _txHash, bool _withProof,
        std::function<void(
            Error::Ptr, protocol::TransactionReceipt::ConstPtr, bcos::ledger::MerkleProofPtr)>
            _onGetTx)
    {}

    void asyncGetTotalTransactionCount(std::function<void(Error::Ptr, int64_t _totalTxCount,
            int64_t _failedTxCount, protocol::BlockNumber _latestBlockNumber)>
            _callback)
    {}

    void asyncGetSystemConfigByKey(std::string const& _key,
        std::function<void(Error::Ptr, std::string, protocol::BlockNumber)> _onGetConfig)
    {}

    void asyncGetNodeListByType(std::string const& _type,
        std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
    {}

    void asyncGetNonceList(protocol::BlockNumber _startNumber, int64_t _offset,
        std::function<void(
            Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
            _onGetList)
    {}
};
#pragma GCC diagnostic pop
}  // namespace bcos::test