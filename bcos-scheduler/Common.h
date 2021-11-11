#pragma once

#include <cstdint>

namespace bcos::scheduler
{
#define SCHEDULER_LOG(LEVEL) BCOS_LOG(LEVEL) << LOG_BADGE("SCHEDULER")

enum SchedulerError
{
    UnknownError = -70000,
    InvalidStatus,
    InvalidBlockNumber,
    InvalidBlocks,
    NextBlockError,
    PrewriteBlockError,
    CommitError,
    RollbackError,
    UnexpectedKeyLockError,
    BatchError,
    DMTError,
    DAGError,
};

inline const uint64_t TRANSACTION_GAS = 30000000;

}  // namespace bcos::scheduler