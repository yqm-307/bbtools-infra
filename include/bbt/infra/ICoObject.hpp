#pragma once
// co-network/v1 N0：协程受管对象的身份与等待参数。
//
// coroutine#347（上游 service-runtime/v1 C0/C1）已交付，本头聚合其公开头，
// 不复制上游类型定义：
//   - bbt::coroutine::ICoObject / CoObjectInfo / CoObjectId / RuntimeGeneration
//   - bbt::coroutine::Deadline / CancellationToken / CancellationSource
// 契约公共签名一律写 bbt::coroutine:: 限定名，infra 不保留同名别名
// （见 docs/decisions/0002-co-network-contract-v1.md）。

#include <bbt/coroutine/object/interface/ICoObject.hpp>
#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>
