#include "KeyLocks.h"
#include "Common.h"
#include <assert.h>
#include <bcos-framework/libutilities/DataConvertUtility.h>
#include <bcos-framework/libutilities/Error.h>
#include <boost/format.hpp>
#include <boost/throw_exception.hpp>

using namespace bcos::scheduler;

bool KeyLocks::batchAcquireKeyLock(std::string_view contract, gsl::span<std::string const> keyLocks,
    int64_t contextID, int64_t seq)
{
    if (!keyLocks.empty())
    {
        for (auto& it : keyLocks)
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

bool KeyLocks::acquireKeyLock(
    std::string_view contract, std::string_view key, int64_t contextID, int64_t seq)
{
    auto it = m_keyLocks.get<1>().find(std::tuple{contract, key});
    if (it != m_keyLocks.get<1>().end())
    {
        if (it->contextID != contextID)
        {
            SCHEDULER_LOG(TRACE) << boost::format(
                                        "Acquire key lock failed, request: [%s, %s, %ld, %ld] "
                                        "exists: [%ld, %ld]") %
                                        contract % key % contextID % seq % it->contextID % it->seq;

            // Another context is owing the key
            return false;
        }
    }

    SCHEDULER_LOG(TRACE) << "Acquire key lock success, contract: " << contract << " key: " << key
                         << " contextID: " << contextID << " seq: " << seq;

    // Current context owing the key, accquire it
    m_keyLocks.emplace(KeyLockItem{std::string(contract), std::string(key), contextID, seq});

    return true;
}


std::vector<std::string> KeyLocks::getKeyLocksByContract(
    std::string_view contract, int64_t excludeContextID) const
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
    SCHEDULER_LOG(TRACE) << "Release key lock, contextID: " << contextID << " seq: " << seq;

    auto range = m_keyLocks.get<3>().equal_range(std::tuple{contextID, seq});

    for (auto it = range.first; it != range.second; ++it)
    {
        SCHEDULER_LOG(TRACE) << "Releasing key lock, contract: " << it->contract
                             << " key: " << toHex(it->key) << " contextID: " << it->contextID
                             << " seq: " << it->seq;
    }

    m_keyLocks.get<3>().erase(range.first, range.second);
}