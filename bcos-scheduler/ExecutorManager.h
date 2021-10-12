#pragma once

#include <bcos-framework/interfaces/executor/ParallelTransactionExecutorInterface.h>
#include <tbb/blocked_range.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_for.h>
#include <boost/iterator/iterator_categories.hpp>
#include <boost/range/any_range.hpp>
#include <iterator>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace bcos::scheduler
{
class ExecutorManager
{
public:
    using Ptr = std::shared_ptr<ExecutorManager>;

    void addExecutor(
        std::string name, bcos::executor::ParallelTransactionExecutorInterface::Ptr executor);

    bcos::executor::ParallelTransactionExecutorInterface::Ptr dispatchExecutor(
        const std::string_view& contract);

    void removeExecutor(const std::string_view& name);

private:
    struct ExecutorInfo
    {
        using Ptr = std::shared_ptr<ExecutorInfo>;

        std::string name;
        bcos::executor::ParallelTransactionExecutorInterface::Ptr executor;
        std::set<std::string> contracts;
    };

    struct ExecutorInfoComp
    {
        bool operator()(const ExecutorInfo::Ptr& lhs, const ExecutorInfo::Ptr& rhs) const
        {
            return lhs->contracts.size() > rhs->contracts.size();
        }
    };

    tbb::concurrent_unordered_map<std::string_view, ExecutorInfo::Ptr, std::hash<std::string_view>>
        m_contract2ExecutorInfo;
    std::unordered_map<std::string_view, ExecutorInfo::Ptr, std::hash<std::string_view>>
        m_name2Executors;
    std::priority_queue<ExecutorInfo::Ptr, std::vector<ExecutorInfo::Ptr>, ExecutorInfoComp>
        m_executorPriorityQueue;
    std::shared_mutex m_mutex;
};
}  // namespace bcos::scheduler