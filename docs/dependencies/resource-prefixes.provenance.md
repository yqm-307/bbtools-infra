# 资源依赖私有前缀（hiredis / mongoc / mongocxx）provenance 与消费契约

本文件固定 Redis/Mongo 资源验收所需第三方依赖的来源、锁定值与消费方式，是本仓
「资源依赖准备入口」的唯一版本真源。实现只有一个：
`scripts/prepare_resource_deps.sh`（固定 tag 源码 → 私有前缀 → manifest）；
消费侧自检为 `scripts/resource_prefix_verify/`（消费者 CMake 校验 imported target
与运行版本）。framework 资源 CI 只消费前缀路径，不在自身仓内复制依赖构建逻辑。

决策依据：[0003 Redis/hiredis](decisions/0003-redis-client-hiredis-dependency.md)、
[0004 Mongo/mongocxx](decisions/0004-mongo-client-mongocxx-dependency.md)。
不发布 bundle/镜像、不新增 runner、不引入新包管理器：脚本在调用方给定的私有前缀内
从公开源码构建，不写 `/usr/local`、不 `sudo`、不改 runner 镜像。

## 锁定来源

tag 一律解析到完整 commit（`git ls-remote <repo> <tag>^{}`）后固定；脚本 clone 固定
tag 后再次 `git rev-parse HEAD` 必须等于下表 commit，否则 FATAL。recipe 走 git clone，
因此本 manifest 只记录可实际校验的 commit，不把未下载、未校验的 release archive
摘要写成已验证证据。

| 组件 | 上游 repository | tag | commit（tag 解析后） | 库内版本 | 许可证 | 前缀子目录 | CMake targets |
|---|---|---|---|---|---|---|---|
| hiredis | `https://github.com/redis/hiredis` | `v1.4.1` | `616f2286ba5503f74ae96e720623fa11dbc690af` | `1.4.0` | BSD-3-Clause | `hiredis/` | `hiredis::hiredis` |
| mongo-c-driver | `https://github.com/mongodb/mongo-c-driver` | `2.5.4` | `ad87ab88907a0105823469fb5d393ed717bed9ba` | `2.5.4` | Apache-2.0 | `mongoc/` | `mongoc::shared`、`bson::shared` |
| mongo-cxx-driver | `https://github.com/mongodb/mongo-cxx-driver` | `r4.6.0` | `5cfce9d754a91d47e996394e2cdc9f2e60403542` | `4.6.0` | Apache-2.0 | `mongocxx/` | `mongo::mongocxx_shared`、`mongo::bsoncxx_shared` |

注意 hiredis 的 tag（`v1.4.1`）与库内版本（`1.4.0`）不一致，两者都记录且都校验。

## 准备入口

```bash
# 1) 构建（<empty-prefix-root> 必须不存在；已存在一律拒绝）
scripts/prepare_resource_deps.sh <empty-prefix-root> [--jobs N]

# 2) 缓存键（无副作用，供 actions/cache key 使用）
scripts/prepare_resource_deps.sh --print-cache-key
# -> bbt-resource-deps-hiredis-616f2286-mongoc-ad87ab88-mongocxx-5cfce9d7
```

- 只安装进 `<empty-prefix-root>/{hiredis,mongoc,mongocxx}`，manifest 落在
  `<empty-prefix-root>/resource-deps.manifest.json`。
- 并行度默认 `nproc/4`（最小 2）；存在 `ccache` 时自动
  `-DCMAKE_{C,CXX}_COMPILER_LAUNCHER=ccache`（只影响速度，不改变锁定版本）。
- 构建全程在 `TMPDIR` 下的临时工作目录进行，完成后原子的把 staging 改名为最终前缀；
  脚本不污染调用者工作树（mongo-cxx-driver 的 install 清单会写到当前目录，故脚本
  先切到 scratch 工作目录）。

### manifest 字段

`resource-deps.manifest.json`（`schema: bbt-resource-deps/v1`）记录：`cache_key`、
`install_root`、`cmake_prefix_paths`、`toolchain`、`abi_requirements`，以及每个组件的
`upstream`/`tag`/`commit`/`installed_version`/
`cmake_package_targets`，和实际安装产物的每个 `lib/*.so*`（相对名、realpath、sha256）。

- `commit` 与 `installed_version` 是**可复现锁定值**：同一上游状态重复构建必然一致。
- 每个 `.so` 的 `sha256` 绑定**该次构建的实际产物**（编译期嵌入了构建/安装路径），
  跨机器/跨前缀不保证字节相同，用于溯源与「缓存里的库是否就是这份产物」的核对，
  不作为跨构建相等断言。
- `cache_key` 只由三个锁定 commit 派生，与主机/时间无关。
- framework 消费者会按同一三个 commit 的前 8 位自行重算并比对该 `cache_key`；
  因此更新任一 recipe 锁定 commit 时，必须同步更新消费者期望值与缓存键。

## 缓存语义（Actions/cache 建议流程）

缓存对象是整个 `<prefix-root>`（含 manifest）。命中缓存不等于版本正确，必须复核：

```text
key = <prepare_resource_deps.sh --print-cache-key>
actions/cache 命中  -> 运行消费者自检 scripts/resource_prefix_verify（见下），失败即视为 miss
actions/cache 未命中 -> 运行 scripts/prepare_resource_deps.sh <prefix-root>，再自检
```

`prepare_resource_deps.sh` 自身在构建后即读取已安装头文件的版本宏与锁定值精确比对
（hiredis `1.4.0` / mongoc `2.5.4` / mongocxx `4.6.0`），版本不符即 FATAL，不存在
「缓存里躺着同名异版库却能冒充锁定版本」。

## 消费契约

与 infra 根 `CMakeLists.txt`、`src/CMakeLists.txt` 一致，只允许经显式前缀接入：

```text
-DBBT_HIREDIS_PREFIX=<prefix-root>/hiredis
-DBBT_MONGOC_PREFIX=<prefix-root>/mongoc
-DBBT_MONGOCXX_PREFIX=<prefix-root>/mongocxx
```

- `find_package` 一律 `NO_DEFAULT_PATH`，杜绝误链 `/usr/local` 等同名库。
- `BBT_MONGOCXX_PREFIX` 与 `BBT_MONGOC_PREFIX` 必须成对给出（mongocxxConfig 的
  `find_dependency(mongoc/bsoncxx)` 需经 `<pkg>_DIR` 钉到私有前缀）。
- 未给前缀时对应模块整体跳过，不产出半成品 target。

### 消费者自检

```bash
cmake -S scripts/resource_prefix_verify -B <build> -G Ninja \
  -DRESOURCE_PREFIX_ROOT=<prefix-root>
cmake --build <build>
```

该工程按消费契约 `find_package` 三份依赖，断言
`hiredis::hiredis`/`mongo::mongocxx_shared`/`mongo::bsoncxx_shared`/`mongoc::shared`
存在，编译链接一个真实程序并运行，运行期版本（hiredis 头宏、`mongoc_get_version()`、
`MONGOCXX_VERSION_STRING`）必须等于锁定值，否则构建失败。前缀缺失/非法一律
configure FATAL_ERROR。

验证据还必须真实调用 hiredis 导出符号（`redisReaderCreate`/`redisReaderFree`，纯内存、
不发起网络），并断言 `ldd` 解析到的 hiredis/mongoc/mongocxx 全部落在本次前缀内：只取
函数地址会被优化消掉，不构成「libhiredis 被实际链接」的证据。

最小回归入口：`tests/resource-prefix/run.sh [--prefix <prefix-root>]`（fail-closed 路径 +
正向 / 异版负向，不重复三方依赖构建）。

## 运行时与重定位限制

- 脚本以 `-DCMAKE_INSTALL_RPATH=<prefix-root>/{hiredis,mongoc,mongocxx}/lib` 构建，
  安装产物的 `RUNPATH` 指向**最终前缀**（不是构建期 staging 路径），因此前缀在
  `install_root` 处自定位：消费者自检可直接运行，无需额外设置搜索路径。
- 前缀若被移动到 `install_root` 之外（例如缓存恢复到别的路径），自带的绝对 RUNPATH
  失效，此时需回退注入：

```text
LD_LIBRARY_PATH=<prefix-root>/hiredis/lib:<prefix-root>/mongoc/lib:<prefix-root>/mongocxx/lib
```

- 链接器默认写 `RUNPATH`（非传递）：可执行文件自身 RUNPATH 不解析间接依赖；
  `libmongocxx1.so.1` 的 `NEEDED`（`libbsoncxx1`/`libbson2`）靠库自身的 RUNPATH 解析。
  移动前缀后 LD_LIBRARY_PATH 是最稳妥的补齐手段。
- 已知残留：`libmongocxx1.so.1` 的 RUNPATH 在最终前缀项之后还带一条上游写入的
  构建期 staging 路径（死路径）；最终前缀项在前，解析确定且正确，仅属上游烘路径的
  残留记录。
- `ldd` 应解析到私有前缀，不得命中 `/usr/local` 或系统同名库。

## 镜像层集成（增量层，供咨询；本仓不发布镜像）

本入口可直接放进读取同一 recipe 的 **builder 阶段**，向固定前缀安装后作为增量层
拷贝进最终 runner，避免每 job 从浮动上游重编驱动。要点：

1. **builder 与最终 runner 必须同 OS/ABI**：二进制把构建期 glibc/libstdc++ 的符号版本
   烘进产物，换 ABI 的 runner 会 `version 'GLIBC_2.xx' not found`。builder 必须使用
   framework 仓库当前 runner Dockerfile 对应的同一基础镜像/锁定输入（当前 Dockerfile
   标识为 v2；不要根据旧的 v1 标签猜测 ABI），无需另造工具链。
2. **固定安装到 `/opt` 前缀**（示例 `/opt/bbt-resource-deps`）：builder 内
   `prepare_resource_deps.sh /opt/bbt-resource-deps` 后，只把该目录拷进最终层；
   RUNPATH 已指向 `/opt` 前缀，runner 内自定位。
3. **最终 runner 需补的 runtime 系统依赖**（实测 `NEEDED`/符号版本）：

   | 依赖 | 来源 | 实测要求 |
   |---|---|---|
   | `libc6`（glibc） | 基础镜像 | prefix4 实测三份前缀产物最高要求 `GLIBC_2.38`（`manifest.abi_requirements.glibc`，`objdump -T` 汇总）。实际镜像必须以同基座构建并重新核对 |
   | `libstdc++6` | g++ 运行时 | prefix4 实测需 `GLIBCXX_3.4.32`（随 builder gcc 版本） |
   | `libgcc-s1` | gcc 运行时 | `libgcc_s.so.1` |
   | `libm`/`libresolv` | libc6 提供 | mongoc `NEEDED`（现代 glibc 已并入 libc6） |

   - 本 recipe 以 `-DENABLE_SSL=OFF` 构建，**不需要** `libssl3`/`libcrypto3`，也无需
     zstd/snappy/zlib（`NEEDED` 未出现）。若将来需要 Redis/Mongo TLS，需改用带 SSL 的
     构建并在镜像补 `libssl3`，同时更新本文件。
   - runner 若已含 g++/libstdc++（当前 runner 有），通常已满足上表；无需额外安装。
4. **不扩大范围**：不在本仓发布 bundle/镜像、不部署 runner、不改 `ci.yml` job 结构；
   镜像方案落地前仍需授权，本文件只说明 recipe 如何复用到镜像层。

## fail-closed 规则汇总

1. `<empty-prefix-root>` 已存在（不论是否为空，含悬浮符号链接）→ 拒绝，不覆盖；
   改名瞬间再核一次，准备期间出现同名目录也拒绝。
2. tag 解析出的 commit 与锁定 commit 不符 → FATAL。
3. 安装后头文件版本与锁定值不符 → FATAL。
4. 消费者自检缺目录、缺 imported target、运行版本不符、链接来源不在前缀内 → configure/build 失败。
5. `objdump` 缺失、前缀内无可读 `*.so*`、或读符号表失败 → FATAL（ABI 字段不写
   `unknown` 冒充已记录证据）。

## 更新/升级路径

更换任一依赖版本时必须同时：

1. 更新本文件的 tag/commit/库内版本/归档 SHA256 与 decision 0003/0004；
2. 更新 `scripts/prepare_resource_deps.sh` 顶部锁定常量与 `CACHE_KEY` 派生；
3. 更新 `scripts/resource_prefix_verify/CMakeLists.txt` 中生成的版本期望；
4. 在全新空前缀重跑准备 + 消费者自检，并复核 `ldd`；
5. 走独立审查（依赖来源变更）。

不得把「tag 名相同」替代「commit 相同」，也不得把「版本字符串相等」替代
「消费者实际链接/运行版本核对」。

## 非目标

- 不在 framework 复制三依赖构建逻辑；不在本仓发布 bundle/镜像或部署 runner。
- 不引入包管理器/依赖编排平台。
- 本入口只服务 CI 资源验收，不代表生产部署形态。
