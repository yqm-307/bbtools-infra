# bbtools-coroutine 消费出处（net 候选期固定 commit）

本说明记录 `bbt::infra_net` 候选期验证所绑定的上游来源。候选实现已合入 main（位于 `bbt/infra/net/`），因此本文是**历史出处记录**，不是当前构建 pin。

## 当前 pin

- 当前构建 pin 为 `cmake/coroutine.sha`，由 `scripts/ci/verify_coroutine_pin.sh` 校验为 40 位小写 hex。
- CI 以该 pin 检出 `yqm-307/bbtools-coroutine`，并要求 `prepare_deps.sh` 输出的 `coro_ref` 与 pin 相等。
- 本文的 `8ae6a889…` 不是当前 pin，只用于复现候选期结论。

## 候选期固定 commit

- 仓库：<https://github.com/yqm-307/bbtools-coroutine>
- 固定 commit（消费标识）：`8ae6a88997c5acaa4fe03430c35e1820ed5b29b0`
- 源码树（tree）：`89c5012ca22d06a226727943a89dd7f08548b8cc`
- 父提交：`6dfda8158d76f5030cde50a9fbc3c97a8335e932`
- 落点：该 commit 是上游一个已合入 PR 的 merge commit（base 为 `main`）。

### 消费语义（固定关系，不得替换）

- 消费标识是**落地 commit `8ae6a889…` + 其 tree `89c5012…`**。
- PR head `a20a6d7bacfd09876f64a19d13d46ece8d21c64d` 不用于消费：head 是 PR 分支上的中间态，不是 base 上的可复现落点。也不以分支名或可变 `main` 代替消费版本。
- 父提交 `6dfda815…` 同样是上游已合入变更的 merge commit。因此该固定 commit 同时包含两项已落地修复：查询不再惰性创建执行器，以及 Hook/单次分配失败时的 close 收口。
- 消费方式：`BBT_COROUTINE_SOURCE_DIR` 指向该固定 commit 的**干净检出**；CMake 的来源路径检查不代替 SHA/clean 与实际链接核验。

## 旧 dirty 候选的证据边界

- 候选期更早曾绑定过一份未提交差异的工作树。它与固定 commit 不是祖先关系（两条基线不同），从固定 commit 重建的 DSO 必然不同于旧 DSO。
- 旧 DSO 证据不得迁移到固定 commit。文件内容相同不等于 DSO 相同，一切以实测 DSO、`readelf`、`ldd` 为准。

## 候选期验证边界

- 候选期在该固定 commit 上完成了干净重建与网络注册集验证。当前测试面由 `tests/net/` 注册集决定，并由 `scripts/ci/check_network_tests.py` 核对；本文不复述候选期的通过数，当前证据见合入后的 CI run。
- 未完成：Reactor 级 Release 构建、sanitizer、长时压力，见 [0010](../decisions/0010-gnet-network-foundation.md) 状态行。

## 限制

- 本说明不含原始日志、实现补丁正文或私有绝对路径。
