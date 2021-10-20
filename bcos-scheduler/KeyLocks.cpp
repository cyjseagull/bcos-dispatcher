#include "KeyLocks.h"
#include "Common.h"
#include <assert.h>
#include <bcos-framework/libutilities/Error.h>
#include <boost/throw_exception.hpp>

using namespace bcos::scheduler;

bool KeyLocks::acquireKeyLock(
    const std::string_view& contract, const std::string_view& key, int64_t contextID, int64_t seq)
{
    auto it = m_keyLocks.get<1>().find(std::tuple{contract, key});
    if (it != m_keyLocks.get<1>().end())
    {
        if (it->contextID != contextID)
        {
            // Another context is owing the key
            return false;
        }
    }

    // Current context owing the key, accquire it
    auto [insertedIt, inserted] = m_keyLocks.get<1>().emplace(
        KeyLockItem{std::string(contract), std::string(key), contextID, seq});

    if (!inserted)
    {
        BOOST_THROW_EXCEPTION(BCOS_ERROR(scheduler::SchedulerError::UnexpectedKeyLockError,
            "Unexpected insert key lock failed!"));
    }

    return true;
}

std::vector<std::string> KeyLocks::getKeyLocksByContract(
    const std::string_view& contract, int64_t excludeContextID) const
{
    std::vector<std::string> results;
    auto count = m_keyLocks.get<2>().count(contract);

    if (count > 0)
    {
        auto range = m_keyLocks.get<2>().equal_range(contract);

        for (auto it = range.first; it != range.second; ++it)
        {
            if (it->contextID != excludeContextID)
            {
                results.emplace_back(it->key);
            }
        }
    }

    return results;
}

void KeyLocks::releaseKeyLocks(int64_t contextID, int64_t seq)
{
    auto range = m_keyLocks.get<3>().equal_range(std::tuple{contextID, seq});
    m_keyLocks.get<3>().erase(range.first, range.second);
}