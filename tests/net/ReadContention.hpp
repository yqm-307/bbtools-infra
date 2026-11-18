#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

#include <bbt/infra/net/detail/TestAccess.hpp>

namespace bbt::infra::net {

/**
 * 跨测试步骤保持真实读保护；锁的获取、释放及通知仍由 ReceiveRing 完整执行。
 * native 辅助线程仅等待测试释放屏障，业务协程和 owner 不进入此等待。
 */
class ReadContention {
public:
    template <class Target>
    explicit ReadContention(const Target& target)
        : m_thread([this, target] {
            const bool acquired = ConnectionTestAccess::ExerciseReadContention(target, [this] {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_held = true;
                m_entered = true;
                m_cv.notify_all();
                m_cv.wait(lock, [this] { return m_release; });
            });
            if (!acquired) {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_entered = true;
                m_cv.notify_all();
            }
        })
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [this] { return m_entered; });
    }

    ~ReadContention() { Reset(); }

    explicit operator bool() const noexcept { return m_held; }

    void Reset()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_release = true;
        }
        m_cv.notify_all();
        if (m_thread.joinable())
            m_thread.join();
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_held{false};
    bool m_entered{false};
    bool m_release{false};
    std::thread m_thread;
};

} // namespace bbt::infra::net
