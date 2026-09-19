# Redis 客户端切片：hiredis 依赖选型与执行域决策

## 状态与范围

日期：2026-09-19。关联 Issue：#6（拆分 #3：CoRedisCli 协程客户端与真实 Redis 验收）、父任务 #3、契约文档 `0002-co-network-contract-v1.md`。本决策只覆盖 `CoRedisCli` 首版切片：固定最小命令集、hiredis asynchronous API、共享 coroutine executor。不引入万能命令 DSL。

## 固定依赖

| 项 | 值 |
|---|---|
| 库 | hiredis（Redis 官方 C 客户端） |
| 固定 tag | `v1.4.1` |
| 固定 commit | `616f2286ba5503f74ae96e720623fa11dbc690af` |
| archive SHA-256 | `1a6b3e5cd8d127bf1c6b7a9a5d42d7acd89cdd31ea35e316e0595b008204c8d2` |
| 库内版本 | `1.4.0`（源码宏与 SONAME 均报告 1.4.0；tag 与库内版本不一致，核查时两者都必须记录） |
| SONAME | `libhiredis.so.1`（实文件 `libhiredis.so.1.4.0`） |
| 许可证 | BSD-3-Clause |
| 消费方式 | 私有前缀安装 + CMake package config（`hiredis::hiredis` imported target） |

## 链接来源与接入

- 构建经 `BBT_HIREDIS_PREFIX`（cache PATH，默认空）定位前缀：`find_package(hiredis CONFIG REQUIRED PATHS "${BBT_HIREDIS_PREFIX}" NO_DEFAULT_PATH)`。
- `NO_DEFAULT_PATH` 保证不可能误链 `/usr/local` 等系统位置的同名库；未给前缀时 `bbt_infra_redis` 整体跳过，不产出半成品 target。
- 绝对前缀路径只允许经 `-DBBT_HIREDIS_PREFIX=` 配置期传入，不写入任何提交内文件。
- 实测链接解析：`libhiredis.so.1 => <prefix>/lib/libhiredis.so.1`，可经 `ldd` 核查。

## 选型理由

- hiredis 是 Redis 官方维护的 C 客户端，asynchronous API 提供完整 event hooks（`addRead/delRead/addWrite/delWrite/cleanup/scheduleTimer`），可接入任意既有 event loop——与「共享 executor、不新建 io_context/线程」的硬约束天然匹配。
- 同步 API + Hook 被明确禁止：同步 `redisCommand` 在协程内走 Hook 只能覆盖「等待」形态，连接/读写生命周期不可控，且与 async 路径混用会破坏 hiredis context 的单执行域不变量。
- 自写 RESP 协议栈被否决：首版只需 5 个命令，但协议解析、连接生命周期、错误分类都是长期资产，自写无法提供等价成熟度。

## 执行域与生命周期不变量

- 所有 hiredis context 操作只在 conn 所属 io strand 上进行；`RedisEvBridge` 以 `shared_ptr` 保活，`ev.data` 指向 bridge。
- `redisAsyncSetConnectCallback` 内部会立即触发 `_EL_ADD_WRITE`：event 钩子与 `stream_descriptor` 必须在安装回调之前装好。
- `redisAsyncFree` 同步催出 pending 命令的 NULL reply 回调，随后 cleanup、再触发（已连接 context 的）disconnect 回调；cleanup 后不得再访问 context。
- `cleanup` 中 `desc.release()` 摘除 reactor 监视但不 close fd——fd 最终由 hiredis `redisFree` 关闭，提前 close 会让 hiredis 二次 close 撞复用 fd。
- **连接对象是单生命周期**：断开后 `m_conn` 立即脱离（新命令走新建连接），对象本体移入 `m_retired_conns`，销毁延迟到下一次 `EnsureConnOnIoDomain`——通知发生在 hiredis 回调栈内，栈内销毁会让 bridge 先于 hiredis cleanup 析构（desc 误关 fd / cleanup 访问已死对象）。`EnsureConnOnIoDomain` 只由 admission 触发，必然脱离回调栈。
- 断开语义：已发送命令由 hiredis NULL 回调逐个落定（connect 失败阶段无在途）；pending 队列以连接错误统一落定；不跨连接迁移、不重发。

## 测试架构

- 单测不依赖 Docker：内置 FakeRedis（真实 RESP multibulk 应答/吞包）+ 静默服务端覆盖 PING/GET/SET/EXISTS/DEL、RemoteError、确定性 Overloaded、deadline/cancel/close、连接拒绝、线程数不变。
- 真实验收 `tests/redis-live/`：`redis:7-alpine` 容器 `cpus<=0.50`、`mem_limit<=256m`、动态项目名、固定宿主机端口（实测 `compose stop`+`start` 会重分随机映射端口，固定端口才能保证重连语义可验收）、`trap down -v` 清理容器/网络/数据。
- coroutine Hook 全进程拦截 `nanosleep/usleep/...` 并对非协程线程断言：测试线程的定长睡眠一律 `SYS_nanosleep` 直达 syscall；`Scheduler::Stop()` 路径含同类睡眠，套件收尾以 `Scheduler::GetInstance().release()` 走漏单例所有权，绕过该基线竞态（上游缺陷，测试侧不可修复，已注释在案）。

## 升级路径

1. 选定新 tag/commit 与 archive，更新固定版本表（tag、commit、SHA-256、库内版本四者同步记录）。
2. 重新产出私有前缀，`-DBBT_HIREDIS_PREFIX=<新前缀>` 配置构建。
3. 复跑 `redis.unit` + `redis.live` 全套真实容器验收；关注 `async.h` 的 ev 钩子签名与 connect/disconnect 时序变化。

## 退出/替换路径

- 公共头 `CoRedisCli.hpp` 不暴露 hiredis 类型；替换后端只需重写 `src/redis/` 内部（RedisDetail 解码、RedisConnection adapter、CoRedisCliImpl 编排），API 与语义不变。
- `RedisDetail` 的 reply 解码与错误分类已隔离为独立翻译层，换后端时错误映射（`RemoteError/TransportError/ProtocolError` + `backend_category="hiredis"`）需要在新后端的等价错误域上重新实现并复核。
- 若未来需要万能命令接口，在 `CoRedisCli` 之外新增通用 Command API，不回改固定命令集语义。

## 未覆盖与已知差异

- hiredis `scheduleTimer` 未接：本切片不设 connect/command 级 hiredis timeout，deadline 由调用侧 `CompletionSignal` 承担。
- 主机名解析在 io 域内同步进行（数值 IP 为即时路径）；DNS 协程化留待后续切片。
- 基线差异：`Scheduler::m_is_running`/`m_run_generation` 默认即真，「Scheduler 未 Start」不可经 `IsRunning()` 区分；RuntimeUnavailable 的可达路径是 client 未 `Start`。
