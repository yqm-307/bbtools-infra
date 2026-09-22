// Issue #20 最小能力探针（probe only，非生产代码，不实现 IoWait/CoTCP/CoUDP）：
// 实测 bbtools-coroutine detail::CoPollEvent 是否支持在【同一事件】上组合
//   FD readable（真实 socketpair）+ custom wakeup + timeout
// 四个确定性场景：
//   1. fd-first            FD 首胜（custom/timeout 已武装但未触发）
//   2. custom-first        custom 首胜（FD 静默、timeout 兜底未到）
//   3. custom-before-park  park 前 custom 触发不丢（onyield 回调内先 Trigger 后 Regist）
//   4. timeout-first       timeout 首胜（FD 静默、无 custom）
// 每场景由 GetLastResumeEvent() 唤醒掩码直接裁决唯一首胜原因。
//
// 稳定性声明：本测试使用 bbt::coroutine::detail 私有头，以及
// Coroutine::RegistCustom + CoPollEvent::InitFdEvent 的非公开组合路径，
// 均为非稳定 API；结论仅用于 IoWait 组合等待能力裁决，
// 不得作为生产 IoWait 实现依据。该路径不经 Coroutine::_RegistAwaitEvent
//（无 parked 登记），因此本探针不能用于 Stop 安全断言（明确不在本切片）。
//
// 无第三方测试框架依赖，失败输出到 stderr 并以非零退出。
// 同步全部使用有界条件屏障（10s 上限），无猜测时序的 sleep。

#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <mutex>

#include <bbt/coroutine/detail/CoPollEvent.hpp>
#include <bbt/coroutine/detail/CoPoller.hpp>
#include <bbt/coroutine/detail/Coroutine.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

namespace {

int g_checks   = 0;
int g_failures = 0;

void Check(bool cond, const char* expr, const char* file, int line) {
    ++g_checks;
    if (!cond) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d: %s\n", file, line, expr);
    }
}

#define CHECK(cond) Check(static_cast<bool>(cond), #cond, __FILE__, __LINE__)

using bbt::coroutine::detail::CoPollEvent;

// 唤醒掩码数值空间（来自上游触发路径，实测口径）：
//   FD readable → bbt::pollevent::EventOpt::READABLE(0x02，libevent 数值空间)
//   timeout     → bbt::pollevent::EventOpt::TIMEOUT(0x01)
//   custom      → detail::POLL_EVENT_CUSTOM(0x08，PollEventType 空间)
// 注意 FD/timer 与 custom 分属两个数值空间，这是掩码仲裁实现必须显式处理的差异。
using bbt::coroutine::detail::POLL_EVENT_CUSTOM;
using bbt::coroutine::detail::POLL_EVENT_READABLE;
using bbt::coroutine::detail::POLL_EVENT_TIMEOUT;

constexpr int     kProbeCustomKey = 100;  // 不与 CoPollEventCustom 既有语义重合
constexpr int     kLongTimeoutMs  = 8000; // 组合等待中的兜底 timeout（不应成为胜者）
constexpr int     kShortTimeoutMs = 150;  // timeout 首胜场景
constexpr int     kHandoffCapSec  = 10;   // 主/协程有界屏障上限

// park 前 custom 触发位置
enum class PreParkTrigger {
    None,
    InOnYieldBeforeRegist,  // onyield 回调内：先 Trigger 再 Regist
};

// 单场景观测记录：协程线程写、控制线程读，全部经同一 mutex 保护
struct ProbeRecord {
    std::mutex              mtx;
    std::condition_variable cv_created;
    std::condition_variable cv_done;
    bool                    created = false;
    bool                    done    = false;

    std::shared_ptr<CoPollEvent> ev;          // 组合事件本体（完成后仍可查询/拒绝触发）
    int     combined_getevent  = 0;           // GetEvent()：应同时含 READABLE|TIMEOUT|CUSTOM 位
    int     fd_seen            = -1;
    int64_t timeout_seen       = -1;
    int     trigger_pre_regist = -1000;       // 场景3：park 前 custom Trigger 返回值
    int     resume_mask        = -1;          // GetLastResumeEvent()：唯一首胜原因
    bool    wait_failed        = false;       // YieldWithCallback 报告登记失败
    bool    setup_failed       = false;       // RegistCustom/InitFdEvent 失败

    void NotifyCreated() {
        std::lock_guard<std::mutex> lk(mtx);
        created = true;
        cv_created.notify_all();
    }

    bool WaitForCreated() {
        std::unique_lock<std::mutex> lk(mtx);
        return cv_created.wait_for(lk, std::chrono::seconds(kHandoffCapSec),
                                   [this] { return created; });
    }

    void NotifyDone() {
        std::lock_guard<std::mutex> lk(mtx);
        done = true;
        cv_done.notify_all();
    }

    bool WaitDone() {
        std::unique_lock<std::mutex> lk(mtx);
        return cv_done.wait_for(lk, std::chrono::seconds(kHandoffCapSec),
                                [this] { return done; });
    }
};

// 协程体：RegistCustom 建立当前 await event，再以 InitFdEvent 在同一事件上
// 追加 FD+TIMEOUT，经 YieldWithCallback 回调内 Regist() 登记并 park。
void CoRunCombinedWait(int wait_fd, int timeout_ms, PreParkTrigger mode,
                       ProbeRecord& out)
{
    auto* co = g_bbt_tls_coroutine_co;
    if (co == nullptr) {
        std::lock_guard<std::mutex> lk(out.mtx);
        out.setup_failed = true;
        out.done         = true;
        out.cv_done.notify_all();
        return;
    }

    auto ev = co->RegistCustom(kProbeCustomKey);
    bool ok = (ev != nullptr) &&
              (ev->InitFdEvent(wait_fd,
                               static_cast<short>(bbt::pollevent::EventOpt::READABLE |
                                                  bbt::pollevent::EventOpt::TIMEOUT |
                                                  bbt::pollevent::EventOpt::FINALIZE),
                               timeout_ms) == 0);

    {
        std::lock_guard<std::mutex> lk(out.mtx);
        out.ev = ev;
        out.setup_failed = !ok;
        if (ok) {
            out.combined_getevent = ev->GetEvent();
            out.fd_seen           = ev->GetFd();
            out.timeout_seen      = ev->GetTimeout();
        }
        out.created = true;
        out.cv_created.notify_all();
    }

    if (!ok) {
        std::lock_guard<std::mutex> lk(out.mtx);
        out.done = true;
        out.cv_done.notify_all();
        return;
    }

    const int yrc = co->YieldWithCallback([&]() -> bool {
        if (mode == PreParkTrigger::InOnYieldBeforeRegist) {
            // park 前触发 custom：先于 Regist（phase INITED → PENDING），
            // 验证 PENDING 提交路径不丢唤醒
            std::lock_guard<std::mutex> lk(out.mtx);
            out.trigger_pre_regist = ev->Trigger(POLL_EVENT_CUSTOM);
        }
        return ev->Regist() == 0;
    });

    {
        std::lock_guard<std::mutex> lk(out.mtx);
        out.wait_failed = (yrc != 0);
        if (yrc == 0)
            out.resume_mask = co->GetLastResumeEvent();
        out.done = true;
        out.cv_done.notify_all();
    }
}

struct ProbeSnapshot {
    bool    setup_failed = true;
    bool    wait_failed  = true;
    bool    has_ev       = false;
    int     combined_getevent = 0;
    int     fd_seen = -1;
    int64_t timeout_seen = -1;
    int     trigger_pre_regist = -1000;
    int     resume_mask = -1;
};

ProbeSnapshot Snapshot(ProbeRecord& rec) {
    std::lock_guard<std::mutex> lk(rec.mtx);
    ProbeSnapshot s;
    s.setup_failed       = rec.setup_failed;
    s.wait_failed        = rec.wait_failed;
    s.has_ev             = (rec.ev != nullptr);
    s.combined_getevent  = rec.combined_getevent;
    s.fd_seen            = rec.fd_seen;
    s.timeout_seen       = rec.timeout_seen;
    s.trigger_pre_regist = rec.trigger_pre_regist;
    s.resume_mask        = rec.resume_mask;
    return s;
}

// 通用断言：组合登记确实落在同一事件上（FD+TIMEOUT+CUSTOM 三位齐备）
void CheckCombinedArmed(const ProbeSnapshot& s, int expect_fd, int expect_timeout_ms) {
    CHECK(!s.setup_failed);
    CHECK(!s.wait_failed);
    CHECK(s.has_ev);
    CHECK(s.combined_getevent ==
          (POLL_EVENT_READABLE | POLL_EVENT_TIMEOUT | POLL_EVENT_CUSTOM));
    CHECK(s.fd_seen == expect_fd);
    CHECK(s.timeout_seen == expect_timeout_ms);
}

// 场景1：FD 首胜。控制线程在协程调度前先写对端（FD 先于事件武装即就绪），
// custom 只在唤醒后才尝试触发、timeout 为 8s 兜底，故唯一可能的首胜是 READABLE。
void ScenarioFdFirst() {
    std::printf("[probe] === scenario 1: fd-first ===\n");
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        CHECK(false && "socketpair failed");
        return;
    }
    CHECK(write(fds[1], "x", 1) == 1);  // FD 先行就绪（数据留在 fds[0] 待武装后上报）

    ProbeRecord rec;
    g_scheduler->RegistCoroutineTask(
        [&] { CoRunCombinedWait(fds[0], kLongTimeoutMs, PreParkTrigger::None, rec); },
        "probe.fd-first");

    CHECK(rec.WaitDone());
    const auto s = Snapshot(rec);
    CheckCombinedArmed(s, fds[0], kLongTimeoutMs);
    CHECK(s.resume_mask == static_cast<int>(bbt::pollevent::EventOpt::READABLE));

    // 单次完成语义：事件完成后，同一事件上的 custom 触发必须被拒绝
    int late_trigger = -1000;
    {
        std::lock_guard<std::mutex> lk(rec.mtx);
        if (rec.ev)
            late_trigger = rec.ev->Trigger(POLL_EVENT_CUSTOM);
    }
    CHECK(late_trigger == -1);

    close(fds[0]);
    close(fds[1]);
}

// 场景2：custom 首胜。FD 全程静默、timeout 8s 兜底，事件武装后由控制线程
// 经 CoPoller::NotifyCustomEvent 触发 custom。无论触发落在 park 前后
//（INITED/ARMING/ARMED→PENDING 或 PARKED→直接完成），首胜都必须是 CUSTOM。
void ScenarioCustomFirst() {
    std::printf("[probe] === scenario 2: custom-first ===\n");
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        CHECK(false && "socketpair failed");
        return;
    }

    ProbeRecord rec;
    g_scheduler->RegistCoroutineTask(
        [&] { CoRunCombinedWait(fds[0], kLongTimeoutMs, PreParkTrigger::None, rec); },
        "probe.custom-first");

    CHECK(rec.WaitForCreated());
    int notify_ret = -1000;
    {
        std::lock_guard<std::mutex> lk(rec.mtx);
        if (rec.ev)
            notify_ret = g_bbt_poller->NotifyCustomEvent(rec.ev);
    }
    CHECK(notify_ret == 0);

    CHECK(rec.WaitDone());
    const auto s = Snapshot(rec);
    CheckCombinedArmed(s, fds[0], kLongTimeoutMs);
    CHECK(s.resume_mask == static_cast<int>(POLL_EVENT_CUSTOM));

    int late_trigger = -1000;
    {
        std::lock_guard<std::mutex> lk(rec.mtx);
        if (rec.ev)
            late_trigger = rec.ev->Trigger(POLL_EVENT_CUSTOM);
    }
    CHECK(late_trigger == -1);

    close(fds[0]);
    close(fds[1]);
}

// 场景3：park 前 custom 不丢。onyield 回调内先 Trigger custom 再 Regist：
// Trigger 在 phase INITED 即发布 PENDING，Regist 见 PENDING 早退成功，
// CommitPark 在 Processer 线程消费 PENDING 并以 CUSTOM 掩码完成唤醒。
void ScenarioCustomBeforePark() {
    std::printf("[probe] === scenario 3: custom-before-park ===\n");
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        CHECK(false && "socketpair failed");
        return;
    }

    ProbeRecord rec;
    g_scheduler->RegistCoroutineTask(
        [&] { CoRunCombinedWait(fds[0], kLongTimeoutMs,
                                PreParkTrigger::InOnYieldBeforeRegist, rec); },
        "probe.custom-before-park");

    CHECK(rec.WaitDone());
    const auto s = Snapshot(rec);
    CheckCombinedArmed(s, fds[0], kLongTimeoutMs);
    CHECK(s.trigger_pre_regist == 0);
    CHECK(s.resume_mask == static_cast<int>(POLL_EVENT_CUSTOM));

    close(fds[0]);
    close(fds[1]);
}

// 场景4：timeout 首胜。FD 静默、无 custom，短超时（150ms）成为唯一触发源。
void ScenarioTimeoutFirst() {
    std::printf("[probe] === scenario 4: timeout-first ===\n");
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        CHECK(false && "socketpair failed");
        return;
    }

    ProbeRecord rec;
    g_scheduler->RegistCoroutineTask(
        [&] { CoRunCombinedWait(fds[0], kShortTimeoutMs, PreParkTrigger::None, rec); },
        "probe.timeout-first");

    CHECK(rec.WaitDone());
    const auto s = Snapshot(rec);
    CheckCombinedArmed(s, fds[0], kShortTimeoutMs);
    CHECK(s.resume_mask == static_cast<int>(bbt::pollevent::EventOpt::TIMEOUT));

    close(fds[0]);
    close(fds[1]);
}

} // namespace

int main() {
    g_scheduler->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
    if (!g_scheduler->IsRunning()) {
        std::fprintf(stderr, "FAIL: scheduler start failed\n");
        return 1;
    }

    ScenarioFdFirst();
    ScenarioCustomFirst();
    ScenarioCustomBeforePark();
    ScenarioTimeoutFirst();

    g_scheduler->Stop();

    std::printf("iowait.probe: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
