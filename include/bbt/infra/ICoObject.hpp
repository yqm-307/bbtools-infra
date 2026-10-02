#pragma once
// co-network/v1 N0：协程受管对象的身份与等待参数。
//
// 上游 coroutine 已按进程寿命运行时收敛（候选 HEAD 03430a5），本头聚合其
// 公开头，不复制上游类型定义：
//   - bbt::coroutine::ICoObject / CoObjectInfo / CoObjectId（对象身份不再
//     携带运行时代际）
//   - bbt::coroutine::Deadline / WaitStatus / WaitOptions，以及
//     bbt::coroutine::sync::CoWaiter / CombinedWaitOptions / CoEventValue
//     （请求等待与带载荷唤醒）
// 已删除的上游类型不再被 infra 引用：CompletionSignal、CancellationToken /
// CancellationSource、RuntimeGeneration / CurrentRuntimeGeneration。
// 契约公共签名一律写 bbt::coroutine:: 限定名，infra 不保留同名别名
// （见 docs/decisions/0002-co-network-contract-v1.md）。

#include <bbt/coroutine/object/interface/ICoObject.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/CoEventValue.hpp>
