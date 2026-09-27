# protobuf 3.21.12 依赖 provenance（Issue #8）

本文件固定 RPC wire 模块所用 protoc/C++ runtime 的来源、版本与校验材料。
CMake 接入只允许经 `-DBBT_PROTOBUF_PREFIX=<前缀>` 显式消费，且前缀内
protoc/runtime 版本必须精确等于本文件锁定值、runtime 库文件 SHA256 必须
命中锁定清单，不满足即 configure FATAL_ERROR。禁止把本机私有绝对路径写进
默认值或提交内容。

## 锁定版本

- 上游版本：`google/protobuf v3.21.12`（git tag `v3.21.12`；
  tag object/源码包 digest 以 GitHub 发布物为准，见「上游源码」节）
- Ubuntu 二进制包版本：`3.21.12-15ubuntu1`（resolute 发行版，`amd64`，
  含 CVE-2026-0994 安全补丁，见下）
- 版本号编码：`3021012`（x*1_000_000 + y*1_000 + z；
  `GOOGLE_PROTOBUF_VERSION` 宏与 `protoc --version` 输出须一致）

## 上游源码与许可证

- 仓库：`https://github.com/protocolbuffers/protobuf`
- tag：`v3.21.12`；轻量 tag 指向 commit `f0dc78d7e6e331b8c6bb2d5283e06aa26883ca7c`
  （可经 `git ls-remote https://github.com/protocolbuffers/protobuf v3.21.12`
  回读验证；本行已按该命令实测输出记录）
- 许可证：`BSD-3-Clause`（上游 LICENSE；
  本前缀 `usr/share/doc/<pkg>/copyright` 记为 `BSD-3-Clause~Google`）
- 维护状态：上游活跃，3.21.x 为 LTS 分支；跨版本兼容性由
  https://protobuf.dev/support/cross-version-runtime-guarantee/ 约束

## Ubuntu 二进制包（apt 仓库元数据，SHA256 可回读）

以下来自 Ubuntu resolute `apt-cache show <pkg>`（`/var/lib/apt/lists` 中
archive.ubuntu.com resolute 的 Packages 索引快照；同一索引同时给出
`Filename`/`SHA256`/`SHA512`）。「包 SHA256」是 `.deb` 文件本身的哈希，
可用 `apt-get download <pkg>` 取回后 `sha256sum` 复核；「内容 SHA256」是
本前缀实际解包文件的哈希，已同时钉入 `CMakeLists.txt` 的
`BBT_PROTOBUF_KNOWN_LIB_SHA256` 作为 configure 期运行时库门禁。

| 包 | .deb SHA256（apt 索引） | 关键内容 SHA256（解包后实测） |
|---|---|---|
| `libprotobuf32t64` 3.21.12-15ubuntu1 | `adab0eb28161aea2101e33d8b4c8a336ad62c34c70e01bd3d7c1026279ca5757` | `usr/lib/x86_64-linux-gnu/libprotobuf.so.32.0.12` = `09a7b17e8f42aac853a4833b37191439442db92fce9da28f690bfa7a34c7ef9f` |
| `libprotobuf-dev` 3.21.12-15ubuntu1 | `259c9f383dee92f18da256ba31f6709a2eca835f50f7b5befdcbc1ea02fb05b1` | `usr/lib/x86_64-linux-gnu/libprotobuf.a` = `3e3e4ae1f24b2dd52e8104e7af229918a2d504274a08f7d660aaff59bae7a2de`；`usr/include/google/protobuf/stubs/common.h` = `0d67264d1da8cf32ba1a8fa2b86bb1d483d88388c0599766ab338add777b24f8` |
| `libprotoc32t64` 3.21.12-15ubuntu1 | `878419d40eaae00845382d79896d20b446aa12b0550a9e12db381895b05d86c0` | `usr/lib/x86_64-linux-gnu/libprotoc.so.32.0.12`（protoc 的运行时库） |
| `protobuf-compiler` 3.21.12-15ubuntu1 | `2501bf552ae297d415eb31d40da8768c53d9810fc082fe5e7c93271e24d37f7a` | `usr/bin/protoc`（Debian 打包二进制；本前缀内副本经 patchelf 修正 RUNPATH，文件 SHA256 会漂移，见下） |
| `libprotobuf-lite32t64` 3.21.12-15ubuntu1 | `7894b4fbb505bda24b1537494327ac0642f0f0cd36d77165f625456205cee395` | `usr/lib/x86_64-linux-gnu/libprotobuf-lite.so.32.0.12` |
| `libprotoc-dev` 3.21.12-15ubuntu1 | `53bdb086bd40239d9356ab03465c26e9e78abd95ad6f99c2fe650317a41da6f7` | `usr/lib/x86_64-linux-gnu/libprotoc.a` |

其他前缀文件（`usr/lib/x86_64-linux-gnu/pkgconfig/protobuf.pc` 等）以
`protobuf.pc` 的 `Version: 3.21.12` 为交叉校验点：CMake configure 读取该
文件并要求 Version 与锁定版本完全一致（见 `CMakeLists.txt` RPC 接入段）。

## 本机接入前缀（候选私有前缀，不入库）

- 前缀：`<repo>/.deps/protobuf-3.21.12/`（`.deps/` 已加入 .gitignore）
- 来源：Ubuntu resolute `apt-get download` 解包的官方 `.deb`，未做重新
  编译；`.deb` 包名与版本由 `usr/share/doc/*/changelog.Debian.gz` 与
  `copyright` 锚定，目录结构与包内 `md5sums`/`control` 一致
- 预期布局：
  - `usr/bin/protoc`（`--version` 输出 `libprotoc 3.21.12`）
  - `usr/include/google/protobuf/stubs/common.h`
    （`#define GOOGLE_PROTOBUF_VERSION 3021012`）
  - `usr/lib/x86_64-linux-gnu/libprotobuf.{a,so.32.0.12}`
  - `usr/lib/x86_64-linux-gnu/pkgconfig/protobuf.pc`
- RUNPATH 修正：`usr/bin/protoc` 内置 RUNPATH 为 Debian 打包路径，
  不能在本机前缀外可靠解析 `libprotoc.so.32`；候选前缀内的 `protoc`
  经 `patchelf --set-rpath <prefix>/usr/lib/x86_64-linux-gnu` 修正，故其
  文件 SHA256 与上游 `.deb` 内原始二进制不同——这是有意差异，真实校验
  点是版本输出与 `NEEDED libprotoc.so.32` 解析到同前缀内（`readelf -d`
  可复核）。`.deb` 包 SHA256 钉的是包来源，前缀内 `protoc` 的哈希不作为
  锁定键。
- CMake 调用 protoc 时经
  `cmake -E env LD_LIBRARY_PATH=<prefix>/usr/lib/<triplet>` 限定到同一前缀，
  禁止系统 libprotobuf/libprotoc 混入。
- 链接策略：bbt_infra_rpc_proto 与测试目标静态链接 `libprotobuf.a`，
  产物不引入 protobuf 运行时 rpath 依赖。

## 版本钉死校验（CMake configure 期）

- `protoc --version` 解析得到 `x.y.z`，与 runtime 头文件
  `GOOGLE_PROTOBUF_VERSION`（数字）必须完全相等；
- 进一步与 `BBT_PROTOBUF_PINNED_VERSION_NUM`（`3021012`）一致；
- 实际选中的 `libprotobuf.{a,so}` 文件 SHA256 必须命中
  `BBT_PROTOBUF_KNOWN_LIB_SHA256` 清单（本表「内容 SHA256」列）；
- 同前缀 `pkgconfig/protobuf.pc` 的 `Version:` 必须等于锁定版本；
- 任一不满足即 FATAL_ERROR，不允许「同源但非锁定版本」或
  「版本对但二进制漂移」。

## 更换/升级路径

- 需要更换 protobuf 版本时，必须同时更新：
  1. 本文件的版本号、上游 tag/commit、包/内容 SHA256、许可证摘要；
  2. `CMakeLists.txt` 中 `BBT_PROTOBUF_PINNED_VERSION(_NUM)` 与
     `BBT_PROTOBUF_KNOWN_LIB_SHA256`；
  3. 前缀 `.deps/protobuf-<新版>/` 内容；
  4. Issue #8 与 framework #4 消费边界复核记录。
- 未经审查不能放宽「精确版本匹配」语义为「兼容范围」，也不能把
  「版本字符串相等」替代「库文件哈希命中」。

## 复现入口（本机）

```bash
cmake -B build -S . \
  -DBBT_COROUTINE_SOURCE_DIR=<coroutine 源码树> \
  -DBBT_CORE_SOURCE_DIR=<core 源码树> \
  -DBBT_PROTOBUF_PREFIX=<本仓>/.deps/protobuf-3.21.12 \
  -DBUILD_TESTING=ON
cmake --build build --parallel <n>
ctest --test-dir build -R 'rpc.wire' --output-on-failure
```

`cmake --version` ≥ 3.16；无 protoc 全局安装要求。
