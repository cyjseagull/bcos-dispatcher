#include "GraphKeyLocks.h"
#include "Common.h"
#include <bcos-framework/libutilities/DataConvertUtility.h>
#include <bcos-framework/libutilities/Error.h>
#include <boost/core/ignore_unused.hpp>
#include <boost/format.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/detail/adjacency_list.hpp>
#include <boost/graph/edge_list.hpp>
#include <boost/graph/graph_selectors.hpp>
#include <boost/graph/properties.hpp>
#include <boost/graph/visitors.hpp>
#include <boost/throw_exception.hpp>

using namespace bcos::scheduler;

bool GraphKeyLocks::batchAcquireKeyLock(
    std::string_view contract, gsl::span<std::string const> keys, ContextID contextID, Seq seq)
{
    if (!keys.empty())
    {
        for (auto& it : keys)
        {
            if (!acquireKeyLock(contract, it, contextID, seq))
            {
                auto message = (boost::format("Batch acquire lock failed, contract: %s"
                                              ", key: %s, contextID: %ld, seq: %ld") %
                                contract % toHex(it) % contextID % seq)
                                   .str();
                SCHEDULER_LOG(ERROR) << message;
                BOOST_THROW_EXCEPTION(BCOS_ERROR(UnexpectedKeyLockError, message));
                return false;
            }
        }
    }

    return true;
}

bool GraphKeyLocks::acquireKeyLock(
    std::string_view contract, std::string_view key, int64_t contextID, int64_t seq)
{
    auto keyVertex = touchKeyLock(std::make_tuple(contract, key));
    auto contextVertex = touchContext(contextID);

    auto range = boost::out_edges(keyVertex, m_graph);
    for (auto it = range.first; it != range.second; ++it)
    {
        auto vertex = boost::get(VertexPropertyTag(), boost::target(*it, m_graph));
        if (std::get<0>(*vertex) != contextID)
        {
            SCHEDULER_LOG(TRACE) << boost::format(
                                        "Acquire key lock failed, request: [%s, %s, %ld, %ld] "
                                        "exists: [%ld]") %
                                        contract % key % contextID % seq % std::get<0>(*vertex);

            // Key lock holding by another context
            addEdge(contextVertex, keyVertex, seq);
            return false;
        }
    }

    addEdge(keyVertex, contextVertex, seq);

    SCHEDULER_LOG(TRACE) << "Acquire key lock success, contract: " << contract << " key: " << key
                         << " contextID: " << contextID << " seq: " << seq;

    return true;
}


std::vector<std::string> GraphKeyLocks::getKeyLocksNotHoldingByContext(
    std::string_view contract, int64_t excludeContextID) const
{
    std::set<std::string> uniqueKeyLocks;

    auto range = boost::edges(m_graph);
    for (auto it = range.first; it != range.second; ++it)
    {
        auto sourceVertex = boost::get(VertexPropertyTag(), boost::source(*it, m_graph));
        auto targetVertex = boost::get(VertexPropertyTag(), boost::target(*it, m_graph));

        if (targetVertex->index() == 0 && std::get<0>(*targetVertex) != excludeContextID &&
            sourceVertex->index() == 1 && std::get<0>(std::get<1>(*sourceVertex)) == contract)
        {
            uniqueKeyLocks.emplace(std::get<1>(std::get<1>(*sourceVertex)));
        }
    }

    std::vector<std::string> keyLocks;
    keyLocks.reserve(uniqueKeyLocks.size());
    for (auto& it : uniqueKeyLocks)
    {
        keyLocks.emplace_back(std::move(it));
    }

    return keyLocks;
}

void GraphKeyLocks::releaseKeyLocks(int64_t contextID, int64_t seq)
{
    SCHEDULER_LOG(TRACE) << "Release key lock, contextID: " << contextID << " seq: " << seq;
    auto vertex = touchContext(contextID);

    auto range = boost::in_edges(vertex, m_graph);
    for (auto next = range.first; range.first != range.second; range.first = next)
    {
        ++next;
        auto edgeSeq = boost::get(EdgePropertyTag(), *range.first);
        if (edgeSeq == seq)
        {
            if (bcos::LogLevel::TRACE >= bcos::c_fileLogLevel)
            {
                auto source = boost::get(VertexPropertyTag(), boost::source(*range.first, m_graph));
                const auto& [contract, key] = std::get<1>(*source);
                SCHEDULER_LOG(TRACE)
                    << "Releasing key lock, contract: " << contract << " key: " << key;
            }
            boost::remove_edge(*range.first, m_graph);
        }
    }
}

std::forward_list<std::tuple<ContextID, Seq, GraphKeyLocks::ContractView, GraphKeyLocks::KeyView>>
GraphKeyLocks::detectDeadLock()
{
    struct GraphVisitor
    {
        GraphVisitor(std::forward_list<std::tuple<ContextID, Seq, GraphKeyLocks::ContractView,
                GraphKeyLocks::KeyView>>& contextIDList)
          : m_contextIDList(contextIDList)
        {}

        void initialize_vertex(VertexID, const Graph&) {}
        void start_vertex(VertexID, const Graph&) {}
        void discover_vertex(VertexID, const Graph&) {}
        void examine_edge(EdgeID, const Graph&) {}
        void tree_edge(EdgeID, const Graph&) {}
        void forward_or_cross_edge(EdgeID, const Graph&) {}
        void finish_edge(EdgeID, const Graph&) {}
        void finish_vertex(VertexID, const Graph&) {}

        void back_edge(EdgeID e, const Graph& g) const
        {
            auto seq = boost::get(EdgePropertyTag(), e);
            auto sourceVertex = boost::get(VertexPropertyTag(), boost::source(e, g));
            auto targetVertex = boost::get(VertexPropertyTag(), boost::target(e, g));

            ContextID contextID = 0;
            ContractView contractView;
            KeyView keyView;
            if (targetVertex->index() == 0)
            {
                contextID = std::get<0>(*targetVertex);
                contractView = std::get<0>(std::get<1>(*sourceVertex));
                keyView = std::get<1>(std::get<1>(*sourceVertex));
            }
            else
            {
                contextID = std::get<0>(*sourceVertex);
                contractView = std::get<0>(std::get<1>(*targetVertex));
                keyView = std::get<1>(std::get<1>(*targetVertex));
            }

            m_contextIDList.emplace_front(contextID, seq, contractView, keyView);
        }

        std::forward_list<std::tuple<ContextID, Seq, GraphKeyLocks::ContractView,
            GraphKeyLocks::KeyView>>& m_contextIDList;
    };

    std::forward_list<
        std::tuple<ContextID, Seq, GraphKeyLocks::ContractView, GraphKeyLocks::KeyView>>
        contextIDList;
    std::map<VertexID, boost::default_color_type> vertexColors;

    boost::depth_first_search(
        m_graph, GraphVisitor(contextIDList), boost::make_assoc_property_map(vertexColors));

    return contextIDList;
}

GraphKeyLocks::VertexID GraphKeyLocks::touchContext(int64_t contextID)
{
    auto [it, inserted] = m_vertexes.emplace(Vertex(contextID), VertexID());
    if (inserted)
    {
        it->second = boost::add_vertex(&(it->first), m_graph);
    }
    auto contextVertexID = it->second;

    return contextVertexID;
}

GraphKeyLocks::VertexID GraphKeyLocks::touchKeyLock(KeyLockView keyLockView)
{
    auto contractKeyView = keyLockView;
    auto it = m_vertexes.lower_bound(contractKeyView);
    if (it != m_vertexes.end() && it->first == contractKeyView)
    {
        return it->second;
    }

    auto inserted = m_vertexes.emplace_hint(it,
        Vertex(std::make_tuple(
            std::string(std::get<0>(contractKeyView)), std::string(std::get<1>(contractKeyView)))),
        VertexID());
    inserted->second = boost::add_vertex(&(inserted->first), m_graph);
    return inserted->second;
}

void GraphKeyLocks::addEdge(VertexID source, VertexID target, int64_t seq)
{
    bool exists = false;

    auto range = boost::edge_range(source, target, m_graph);
    for (auto it = range.first; it != range.second; ++it)
    {
        auto edgeSeq = boost::get(EdgePropertyTag(), *it);
        if (edgeSeq == seq)
        {
            exists = true;
        }
    }

    if (!exists)
    {
        boost::add_edge(source, target, seq, m_graph);
    }
}

void GraphKeyLocks::removeEdge(VertexID source, VertexID target, int64_t seq)
{
    auto range = boost::edge_range(source, target, m_graph);
    for (auto it = range.first; it != range.second; ++it)
    {
        auto edgeSeq = boost::get(EdgePropertyTag(), *it);
        if (edgeSeq == seq)
        {
            boost::remove_edge(*it, m_graph);
            break;
        }
    }
}