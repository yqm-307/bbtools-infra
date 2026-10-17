#pragma once

namespace bbt::infra::net {

// 事件回调返回的显式动作。
// - None：不附加动作，连接继续按既有契约推进。
// - Close：由 Engine 在事件协程内对该连接发起真实封口 + 物理关闭
//   （Close(Unblock)，干净关闭原因），后续事件按既有契约收尾。
// - Shutdown：**向上层请求关闭**，本动作自身不含任何自行动作。返回它的连接不封口、
//   不物理关闭；Engine 不因此封门、不停 ticker、不改 stopping_/phase_、不调用 Stop、
//   不自动拒绝业务。是否停止、何时停止、是否完全忽略，由使用者在自己的控制域决定。
//   每次 OnOpen/OnTraffic 真实返回本动作都上报一次 EventHandler::OnShutdownRequest
//   （可重复，无「关闭首胜/不可复活」）；它也不是「已关闭」通知——引擎仍在运行。
// 未交付的动作不出现在本枚举里（后续包按 0010 目标补充，见
// docs/architecture/gnet-network-execution.md 的 Engine 小节）。
enum class Action : int {
    None = 0,
    Close = 1,
    Shutdown = 2,
};

} // namespace bbt::infra::net
