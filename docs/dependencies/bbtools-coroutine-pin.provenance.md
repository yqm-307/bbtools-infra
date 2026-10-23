# bbtools-coroutine 消费 pin（infra-perf 优化闭环，T1）

本文记录 bbtools-infra 对上游 bbtools-coroutine 的**固定 commit 消费**。
这是本轮 infra-perf 优化闭环的锁版说明，不是历史出处回顾（那一份见
`bbtools-coroutine-candidate.provenance.md`，其 `8ae6a889…` 记录属于 PR #84
候选期，已随合入成为历史）。

## 固定上游 commit（本 pin）

- 仓库：<https://github.com/yqm-307/bbtools-coroutine>
- 固定 commit（消费标识）：`a4d52c230c436fe54d946ded8e94c9ddbb75d85e`
- 语义：不可变 commit——整个优化序列只读消费此版本，不随 `main` 更新。
  任何「main 现在恰好是某 SHA」的浮动消费都违反本 pin 语义。

## 单一 pin 真源

pin 值只有一个真源文件：**`cmake/coroutine.sha`**（单行、恰好 40 个小写
hex 字符，不做规范化/截断）。下游消费点都从它读，不维护第二份字面量：

1. `cmake/CoroutinePin.cmake`：include 时读取 `coroutine.sha` 并校验格式；
   `bbt_coroutine_pin_verify()` 在 configure 期 fail-closed 核验。
2. `.github/workflows/ci.yml`：prepare-deps 的「读取 coroutine pin 真源」
   step 读同一文件，格式校验后产出 `coroutine_pin_sha`——coroutine
   checkout ref、deps 记录复核、network job 双钉复核全用这个 job output。
3. 本文「固定上游 commit」段：文档口径引用（改 pin 时同步本段）。

## CMake 核验口径（git-only，诚实边界）

`bbt_coroutine_pin_verify()`（普通 `CMakeLists.txt` 与 `cmake/NetworkOnly.cmake`
都调用）按以下口径核验 `BBT_COROUTINE_SOURCE_DIR`（已 REALPATH）：

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
- 已安装 `find_package` 包路径无法证明消费了 pin：configure 明确拒绝，要求提供固定 SHA 的干净源码检出。

## 当前消费/验证边界（本包范围）

- **已实现**：configure 核验（git HEAD + tracked/staged 干净 + 消费面无
  untracked 源码）；普通/NetworkOnly 两路径对已建 target 的 SOURCE_DIR
  复核；CI checkout ref 固定到 `coroutine.sha` 真源、prepare-deps 输出
  复核、network job 双钉复核；pin 负向/正向 ctest 覆盖拒绝与命中路径
  （`net.pin_positive`/`net.pin_wrong_sha`/`net.pin_dirty`/
  `net.pin_untracked_src`/`net.pin_no_git`/`net.pin_manifest_rejected`/
  `net.pin_reuse_mismatch`/`net.pin_reuse_ok`，由
  `tests/net/consumers/pin_expect.cmake` + `pin_reuse/` 驱动）。
- **未做/边界**：非 git 来源（source archive、已安装包）一律不在锁版证据
  范围——要锁版就提供 `BBT_COROUTINE_SOURCE_DIR` 指向的 git 干净检出。
- **不属于本包**：bbtools-infra 其它既有依赖（hiredis/mongo/protobuf/boost）
  的 pin 语义不变；本 pin 只覆盖 coroutine。

## 与历史 provenance 的关系

- `bbtools-coroutine-candidate.provenance.md` 固定的是 `8ae6a889…`（PR #392
  mergeCommit，候选期证明「可脱离未提交差异重建」）；该记录已随 PR #84 合入
  main，是**历史出处**。本文件 `a4d52c23…` 是优化闭环的**当前 pin**，两者
  不互相替代、不混用证据。旧 DSO/旧 dirty 候选的实测与本 pin 无关。
