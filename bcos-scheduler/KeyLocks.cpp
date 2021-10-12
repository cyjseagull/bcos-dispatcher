#include "KeyLocks.h"
#include <assert.h>

using namespace bcos::executor;

bool KeyLocks::acquireKeyLock(
    const std::string_view& table, const std::string_view& key, int contextID)
{
    assert(contextID >= 0);

    auto it = m_key2ContextID.find(std::tuple{table, key});
    if (it != m_key2ContextID.end())
    {
        if (it->second == contextID || it->second < 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    auto contextIt = m_contextID2Key.find(contextID);
    if (contextIt != m_contextID2Key.end())
    {
        auto inserted =
            contextIt->second.emplace_front(std::tuple{std::string(table), std::string(key)});
        m_key2ContextID.emplace(
            std::tuple{std::get<0>(inserted), std::get<1>(inserted)}, contextID);
    }
    return true;
}

void KeyLocks::releaseKeyLocks(int contextID)
{
    assert(contextID > 0);

    auto it = m_contextID2Key.find(contextID);
    if (it != m_contextID2Key.end())
    {
        for (auto& key : it->second)
        {
            m_key2ContextID[key] = -1;
        }
    }
}