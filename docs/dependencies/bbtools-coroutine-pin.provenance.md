# bbtools-coroutine 指定版本来源与本地消费

版本选择规则以[开发指南](../guides/development.md#依赖版本选择)为真源：设计指定版本，
开发/构建未指定版本时默认使用所选本地 Git 检出的当前源码，允许未提交改动。
本文记录现有 CI/发布 pin 的来源及显式锁版核验，不把它作为本地默认构建要求。

## 固定上游 commit（本 pin）

- 仓库：<https://github.com/yqm-307/bbtools-coroutine>
- 固定 commit（消费标识）：`b60a924e401c3f40d684988e4b1977c5aadeb92c`
  （runtime 候选合入版本，包含原生宿主 hook、配置快照、栈所有权及 CoPool 路由）
- 语义：显式选择此 pin 时消费不可变 commit，不随 `main` 更新；本地最新模式不声明命中此 pin。
- 该 commit 来自上游已合入的 [PR #434](https://github.com/yqm-307/bbtools-coroutine/pull/434)。
  消费落点必须是该 commit 的干净 Git 检出才可称为精确版本消费；上游合入不代表本仓集成验证已完成。

## 单一 pin 真源

CI/发布现有 pin 的真源文件为 **`cmake/coroutine.sha`**（单行、恰好 40 个小写
hex 字符，不做规范化/截断）。本地默认不会读取它：

1. `cmake/CoroutinePin.cmake`：接收可选 `BBT_COROUTINE_PINNED_SHA`；留空使用当前源码，
   非空校验格式并在 configure 期严格核验。本地复现 CI pin 时，由调用者读取 `coroutine.sha` 显式传入；
   `GITHUB_ACTIONS=true` 时公共入口直接读取此文件，保留已有 CI 严格核验，拒绝冲突的显式值，不修改本地 cache。
2. `.github/workflows/ci.yml`：prepare-deps 的「读取 coroutine pin 真源」
   step 读同一文件，格式校验后产出 `coroutine_pin_sha`——coroutine
   checkout ref、deps 记录复核、network job 双钉复核全用这个 job output。
3. 本文「固定上游 commit」段：文档口径引用（改 pin 时同步本段）。

## CMake 核验口径

`bbt_coroutine_pin_verify()`（普通 `CMakeLists.txt` 与 `cmake/NetworkOnly.cmake`
经公共接入模块调用）核验 `BBT_COROUTINE_SOURCE_DIR`（已 REALPATH）。本地未指定 SHA 时只确认 Git 来源，
允许当前修改并输出基线 HEAD；显式指定 SHA 或 GitHub Actions 从 `coroutine.sha` 选定 SHA 时执行下列版本与洁净性检查：

- **必须是 git 检出**：`git -C <dir> rev-parse HEAD` 等于 pin，
  `git diff --quiet HEAD --`（tracked）与 `git diff --cached --quiet`
  （暂存）都必须为零——dirty/staged 检出不是固定版本；另用
  `git status --porcelain -- CMakeLists.txt cmake bbt` 拒绝消费面内的
  untracked/未提交源码输入（干净检出之外混入的源码同样不是固定版本；
  其它 untracked 残留如构建产物不阻塞）。
- **非 git 树一律拒绝**：来历不明的源码不能证明自己是固定版本；
  没有「非 git 放行通道」。原 `.bbt-coroutine-pin` tree-manifest 模式已删除
  ——它只核验自我声明的 SHA 文本、不锚定内容，不证明来源（其 SHA 段正则
  在 CMake `REGEX MATCH` 下还恒不匹配，属死代码）；配套的
  `scripts/bench/coroutine_source_archive.py` 一并移除。
- 复用路径同样 fail-closed：普通 `CMakeLists.txt` 与 `NetworkOnly.cmake`
  在 `bbt_coroutine` target 已存在时，先经
  `bbt_coroutine_verify_existing_target()` 核已建 target 的真实
  `SOURCE_DIR`（解 alias、realpath）与声明目录一致——声明目录 pin 过了
  不代表实际链接的是同一棵树；不一致/无 SOURCE_DIR（imported/同名
  target）即拒绝，通过后才对声明目录 pin（等价对实际来源 pin）。
- coroutine 源码入口不提供已安装包回退；最新模式仍要求 Git 来源，固定 SHA 模式还要求干净检出。

## 当前消费/验证边界（本包范围）

- **实现范围**：默认本地最新源码，显式 SHA 时 configure 核验（git HEAD + tracked/staged 干净 + 消费面无
  untracked 源码）；普通/NetworkOnly 两路径始终对已建 target 的 SOURCE_DIR
  复核；CI checkout ref 固定到 `coroutine.sha` 真源、prepare-deps 输出
  复核、network job 双钉复核（本轮未改 CI）；pin 负向/正向 ctest 覆盖默认消费、拒绝与命中路径
  （`net.pin_positive`/`net.pin_wrong_sha`/`net.pin_dirty`/
  `net.pin_untracked_src`/`net.pin_no_git`/`net.pin_manifest_rejected`/
  `net.pin_reuse_mismatch`/`net.pin_reuse_ok`，由
  `tests/net/consumers/pin_expect.cmake` + `pin_reuse/` 驱动）。
- **未做/边界**：非 git 来源（source archive、已安装包）一律不在锁版证据
  范围——要锁版就提供 `BBT_COROUTINE_SOURCE_DIR` 指向的 git 干净检出。
- **不属于本包**：bbtools-infra 其它既有依赖（hiredis/mongo/protobuf/boost）
  的 pin 语义不变；本 pin 只覆盖 coroutine。

## 与历史 provenance 的关系

当前 pin 已从本地候选 `4837cec9e803ebd4250345feb40a748f0afe8c87` 更新为上文的合入版本；下述旧版本仅作历史出处。

- `bbtools-coroutine-candidate.provenance.md` 固定的是 `8ae6a889…`（PR #392
  mergeCommit，候选期证明「可脱离未提交差异重建」）；该记录已随 PR #84 合入
  main，是**历史出处**。
- `b0ba3e02…`（PR #425 head）以及旧 pin `{{SECRET_2k7nsbz}}`（PR #414 merge，轻量 async/await 语法糖）都是历史版本；当前 pin 以上文「固定上游 commit（本 pin）」为准。
- 上述历史段落中的 `4837cec9e803ebd4250345feb40a748f0afe8c87` 已被替代，不能作为当前消费标识。
- 历史版本不混用证据；旧 DSO/旧 dirty 候选的实测与当前 pin 无关。
