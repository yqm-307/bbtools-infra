/**
 * @file ICoObject.hpp
 * @brief 协程受管对象的身份与等待参数：聚合 coroutine 的公开头，不复制上游类型定义。
 *
 * 上游 coroutine 已收敛到进程寿命运行时，本头只做聚合：
 *   - bbt::coroutine::ICoObject、CoObjectInfo、CoObjectId：对象身份不再携带运行时代际；
 *   - bbt::coroutine::Deadline、WaitStatus、WaitOptions，以及 bbt::coroutine::sync::CoWaiter、
 *     CombinedWaitOptions、CoEventValue：请求等待与带载荷唤醒。
 * 上游不再提供 CompletionSignal、CancellationToken/CancellationSource 与 RuntimeGeneration 系列类型，
 * infra 也不引用它们。公共签名统一写 bbt::coroutine:: 限定名，infra 不提供同名别名。
 */
#pragma once

#include <bbt/coroutine/object/interface/ICoObject.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/CoEventValue.hpp>
