#pragma once

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
};

}  // namespace bcos::scheduler