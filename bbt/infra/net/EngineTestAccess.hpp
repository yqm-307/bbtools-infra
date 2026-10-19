#pragma once

// 测试/故障注入私有接缝（不列入公共头自检，不承诺产品 API）。
// 通过 Engine 的 friend 访问其 Impl；只在测试可执行文件中使用。

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <system_error>
#include <vector>

namespace bbt::coroutine::sync { class CoWaiter; }

namespace bbt::infra::net {

class Engine;

class EngineTestAccess {
public:
    static std::error_code InjectDriverFailure(Engine& engine);
    static void SetConnectLimit(Engine& engine, std::size_t limit);
    static std::size_t ConnectTableSize(Engine& engine);
    static std::size_t PendingConnectCount(Engine& engine);
    static void SetConnectDeliveryDelay(Engine& engine, std::chrono::milliseconds delay);
    // 第一条已接纳连接的 fd（仅观察/getsockopt）；无连接返回 -1。
    static int FirstConnectionFd(Engine& engine);
    static std::size_t DirtyCount(Engine& engine);
    static std::size_t RecvHoldBytes(Engine& engine);
    static void SetConnectStartDelay(Engine& engine, std::chrono::milliseconds delay);
    // 一次性屏障：WaitConnect 初始检查后、Create/Add 前。
    static void SetConnectOpBarrierOnce(Engine& engine, std::uint64_t id,
                                        std::function<void()> barrier);
    // 把 op hub 填满到 kMaxWaiters；返回实际加入数（测试持有 keep 保活）。
    static bool ConnectReceiverClaimed(Engine& engine, std::uint64_t id);
    static std::size_t FillConnectHub(
        Engine& engine, std::uint64_t id,
        std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep);
    static void SetCancelPostFailureOnce(Engine& engine);
    static void SetSealDelay(Engine& engine, std::chrono::milliseconds delay);
    // F6：普通回调开始授权点前/后屏障（正常为空）。
    static void SetAuthBarriers(Engine& engine, std::function<void()> pre_auth,
                                std::function<void()> post_auth);
    static std::size_t ConnectionCount(Engine& engine);
    // 测试屏障：owner recv 后、ring append 前调用（正常为空）。
    static void SetReadAppendHook(Engine& engine, std::function<void()> hook);
    // 单次 seal post 失败注入（验证 Run 保持 Closing、不伪 Stopped）。
    static void SetSealPostFailureOnce(Engine& engine);
    // 私有 test：seal handler 抛一次（验证 Run 走真实错误出口、不永久挂）。
    static void SetSealThrowOnce(Engine& engine);
    // 私有 test：让 install/seal 等待的 driver_failed 谓词命中一次（模拟真实
    // snapshot Failed 的等待出口；真实谓词仍核 token/status/state）。
    static void SetWaitDriverFailedOnce(Engine& engine);
    // 私有 test：下一次 connect op 的 CoWaiter 建立失败一次（验证 claim 回滚）。
    static void FailNextConnectWaiterCreateOnce(Engine& engine);
    // 私有 test：owner install 延迟（验证无隐藏 deadline/不提前返回）。
    static void SetInstallDelay(Engine& engine, std::chrono::milliseconds delay);
    // 一次性 dirty 屏障：MarkDirty weak-lock 之后、登记/入链之前。
    static void SetDirtyBarrierOnce(Engine& engine, std::function<void()> barrier);
    // 一次性屏障：RemoveConn 撤链后、registry 擦除前（仍持 dirty+registry）。
    static void SetRemoveBarrierOnce(Engine& engine, std::function<void()> barrier);
    // 一次性屏障：ShutdownOwner 在 stage hold / 关 backend 之前。
    static void SetShutdownBarrierOnce(Engine& engine, std::function<void()> barrier);
    // 一次性屏障：owner write readiness 已到达、写泵持锁之前。
    static void SetWriteReadyBarrierOnce(Engine& engine, std::function<void()> barrier);
    // 测试控制：强制各 owner 推进一次 PumpOwner（独立于被暂停的 dirty Wake）。
    static void KickOwner(Engine& engine);
    // 一次性提交屏障：已取得提交资格、尚未 post（仍持 driver 租约）。
    static void SetSubmitBarrierOnce(Engine& engine, std::function<void()> barrier);
};

} // namespace bbt::infra::net
