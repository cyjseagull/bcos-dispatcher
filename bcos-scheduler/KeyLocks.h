#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/indexed_by.hpp>
#include <boost/multi_index/key.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index_container_fwd.hpp>
#include <any>
#include <forward_list>
#include <functional>
#include <string_view>
#include <variant>

namespace bcos::scheduler
{
class KeyLocks
{
public:
    using Ptr = std::shared_ptr<KeyLocks>;

    KeyLocks() = default;
    KeyLocks(const KeyLocks&) = delete;
    KeyLocks(KeyLocks&&) = delete;
    KeyLocks& operator=(const KeyLocks&) = delete;
    KeyLocks& operator=(KeyLocks&&) = delete;

    struct KeyLockItem
    {
        std::string contract;
        std::string key;
        int64_t contextID;
        int64_t seq;

        std::tuple<std::string_view, std::string_view, int64_t, int64_t> uniqueView() const
        {
            return {contract, key, contextID, seq};
        }
        std::tuple<std::string_view, std::string_view> contractKeyView() const
        {
            return {contract, key};
        }
        std::string_view contractView() const { return contract; }
    };

    bool acquireKeyLock(const std::string_view& contract, const std::string_view& key,
        int64_t contextID, int64_t seq);

    std::vector<std::string> getKeyLocksByContract(
        const std::string_view& contract, int64_t excludeContextID) const;

    void releaseKeyLocks(int64_t contextID, int64_t seq);

private:
    boost::multi_index_container<KeyLockItem,
        boost::multi_index::indexed_by<
            boost::multi_index::hashed_unique<boost::multi_index::const_mem_fun<KeyLockItem,
                std::tuple<std::string_view, std::string_view, int64_t, int64_t>,
                &KeyLockItem::uniqueView>>,
            boost::multi_index::ordered_non_unique<boost::multi_index::const_mem_fun<KeyLockItem,
                std::tuple<std::string_view, std::string_view>, &KeyLockItem::contractKeyView>>,
            boost::multi_index::ordered_non_unique<boost::multi_index::const_mem_fun<KeyLockItem,
                std::string_view, &KeyLockItem::contractView>>,
            boost::multi_index::ordered_non_unique<
                boost::multi_index::key<&KeyLockItem::contextID, &KeyLockItem::seq>>>>
        m_keyLocks;
};
}  // namespace bcos::scheduler