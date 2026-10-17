# bbtools-coroutine 候选绑定出处（本轮新 net 候选 · 固定 commit 重绑定）

本说明记录本仓新 net 候选（`bbt::infra_net`）本轮集中验证所绑定的上游来源。
**本轮已把消费出处从「未提交 dirty 差异」重绑定到已合并固定 commit**：不再依赖未发布补丁或未跟踪文件即可重建源码。
本说明仍**不是已发布版本声明**——本仓该候选自身尚未合入 main。

## 固定上游 commit

- 仓库：<https://github.com/yqm-307/bbtools-coroutine>
- 固定 commit（消费标识）：`8ae6a88997c5acaa4fe03430c35e1820ed5b29b0`
- 源码树（tree）：`89c5012ca22d06a226727943a89dd7f08548b8cc`
- 父提交：`6dfda8158d76f5030cde50a9fbc3c97a8335e932`
- 落点：该 commit 是 [PR #392](https://github.com/yqm-307/bbtools-coroutine/pull/392) 的 `mergeCommit`
  （`state=MERGED`、`mergedAt=2026-10-17T11:10:36Z`、`baseRefName=main`）。

### 消费语义（固定关系，不得替换）

- 消费标识是 **落地 commit `8ae6a889…` + 其 tree `89c5012…`**。
- **PR head `a20a6d7bacfd09876f64a19d13d46ece8d21c64d` 不用于消费**：head 是 PR 分支上的中间态，
  不是 base 上的可复现落点。也不以分支名或可变 `main` 代替消费版本。
- `8ae6a889…` 的父提交 `6dfda815…` 即 [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388)
  （协程当前查询不再惰性创建执行器，`state=MERGED`、`mergedAt=2026-10-15T16:20:54Z`）的 `mergeCommit`。
  因此该固定 commit **同时包含** #388 的查询无创建修复与 #392 的 Hook/单次分配失败 close 收口，两项均已落地。
- 消费方式不变：`BBT_COROUTINE_SOURCE_DIR` 指向该固定 commit 的**干净检出**；
  CMake 的来源路径检查不代替 SHA/clean 与实际链接核验。

## 旧 dirty 候选与固定 commit 的关系（实测）

- 旧候选绑定的是 `6f650eee6d382ed747d5a59c04c682312661485f` + 一份未提交本地差异
  （该 tracked diff sha256 `263436432b9f00d691deecc90f1fcfea9b49f9bbfd3db4577df55faaca2e6549`）。
- `6f650eee…` **不是** `8ae6a889…` 的祖先；二者 `merge-base` = `39fdfff71a8a66d98a874b2e71408f9936b1166b`。
  即两条基线不同，从固定 commit 重新构建得到的 DSO **必然不同于**旧 DSO（已实测，见下），旧 DSO 证据不得迁移。
- 产品/测试闭包逐文件比对（旧 dirty 工作树 vs `git show 8ae6a889…:<path>`，sha256 全等）：
  `bbt/coroutine/detail/CoPollEvent.cc`、`bbt/coroutine/detail/CoPollEvent.hpp`、
  `bbt/coroutine/detail/CoPoller.cc`、`bbt/coroutine/detail/Hook.cc`、
  `unit_test/CMakeLists.txt`、`unit_test/Test_runtime_oom.cc`。
  仅 `agent-docs/api-reference.md` 与 `agent-docs/2026-09-07-core-runtime-contract.md` 存在文档措辞差异。
- **旧说明的闭包 HOLD 前提已消失**：`unit_test/Test_runtime_oom.cc` 现已在固定 tree 内被跟踪
  （blob sha256 `ab92bebde62d8bed96cbf4fdd57a44f2f522e001ad32101af51b47ab6e59d6e2`，与旧未跟踪文件同内容），
  并由 `unit_test/CMakeLists.txt` 引用；不再存在「diff 未覆盖的未跟踪文件」这一不可重建缺口。
- **不得据此推断二进制相同**：文件内容相同 ≠ DSO 相同，一切以实测 DSO、`readelf`、`ldd` 为准。

## 本轮实际链接运行库（从固定 commit 干净重建）

- `libbbt_coroutine.so` sha256 `0f8e03796304319ab110f3d1d07403c71fe8901490639b7c09fc28ff2cdee5b0`
  （SONAME `libbbt_coroutine.so`；GNU build-id `f28d22a77311202fe7d796844c612c90fe1ebcdc`）。
- 产出位置：`infra-refresh-160/build/_deps/bbtools-coroutine/lib/`；源码输入是只读检出
  `infra-refresh-160/co-fixed`（detached @ `8ae6a889…`，`git status --porcelain --untracked-files=all` 为空），
  且不含指向旧候选树的 git alternates。
- 被测二进制与该 DSO 的 `ldd`/`RUNPATH` 实解析一致：`net_foundation_consumer`、公开示例
  `Example_co_net_service`、`Test_net_engine_server`、`Test_net_foundation`、`Test_net_client_connect` 等。
- 旧 DSO `2a3b084b4ba42f528968458e367247979b7761a2e574faab06c27e5915e63ebe` 属于旧 dirty 候选，
  **不得**与固定 commit 的证据混用或互相替代。

## 本轮验证边界（固定 commit）

- **已完成**：从该固定 commit 干净重建（Debug / ccache / `BBT_INFRA_NETWORK_ONLY=ON`），
  固定版本影响面 **107/107 通过、0 失败、0 skip**（= 152 的 98 项影响面 + 9 项 socket primitives / `net.context_no_create` 门禁），
  含 `net.engine_shutdown_request_*`、`net.engine_tick_*`、`net.client_*`、150 家族守卫、
  151 多 loop/LB/预算、负编译 `net.udp_high_level_compile_reject`，以及公开示例 4 模式真实重链运行。
- **未完成（不得据本轮升级为已交付）**：Release 构建、ASAN、完整 P6 压测、infra CI。
  本仓该候选自身仍未独立审查、未提交、未合入 main。
- **旧证据边界**：旧候选的 98 项与有界 P6 结果绑定旧 dirty DSO，**不能**作为固定 commit 的 PASS 依据；
  本轮不引用其绿灯。

## 限制

- 本说明不含原始日志、实现补丁正文或私有绝对路径。
