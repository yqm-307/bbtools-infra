# Redis/Mongo 遗留验收与真实后端 CI

## 范围

本次仅补 Mongo pool 资源寿命回归、接入 hosted CI 的真实 Redis/Mongo 流程及对齐 #12/#34 的过时记录。不调整客户端架构、公共 API、上游运行时、依赖版本、runner、缓存或安装/导出机制。

## Mongo pool 回归

`tests/Test_mongo_unit.cc` 的三个新增用例检查真实 `MongoPoolLease` 共享资源，不依赖新测试开关：

| 用例 | 验收行为 |
|---|---|
| `t_spawn_failure_returns_pool_lease` | reserve / 第 0、1、3 个 worker 启动失败后，失败 owner 的 lease 引用已归还；最后持有者释放后 weak 引用失效，之后可重新建立 pool |
| `t_spawn_failure_lease_isolated_from_sibling` | 同 URI 的失败 owner 不泄漏自身引用、不释放健康兄弟的引用；健康 owner 仍运行且可派发错误输入校验；关闭后归还自身 lease |
| `t_pool_acquire_failure_gates_start` | 真实驱动拒绝非法 URI、进程 pool 数达到上限时，Start 返回 InvalidArgument、不起 worker、保留可重试状态；释放一个槽位后同一 owner 可成功重试 |

instance 由进程级注册表强持；pool 表持 weak 引用。释放一个 owner 的 lease 不等于销毁其他 owner 仍共享的 pool。兄弟用例的错误输入校验不冒充成功数据库读写；真实 CRUD 由下述 live 流程承担。未覆盖任意内存分配点的失败、合法 URI 下 pool 构造失败，以及 sanitizer/跨平台。

## CI 与复现

Release job 复用已构建二进制。以下命令从仓库根目录复现（CI 中仓库检出在 `$GITHUB_WORKSPACE/infra`，脚本使用该绝对路径）：

```bash
bash tests/redis-live/run.sh "$BUILD_DIR/tests/Test_redis_live" "$BUILD_DIR/tests/Test_redis_cotcp_binding"
bash tests/mongo-live/run.sh "$BUILD_DIR/tests/Test_mongo_live"
```

- Redis A：PING、二进制 KV、服务端错误、32 并发冒烟、断连恢复、关闭；额外 CoTCP binding live：真实 RESP2 往返与 2 MiB 大值回归。
- Mongo A：CRUD、not-found、重复键、驱动错误映射、16 并发冒烟、关闭与多集合 owner。
- 两模块 B：停止容器后命令失败；C：恢复健康后新 client 恢复。
- 每阶段只对该阶段的有效用例计验收；非该阶段的用例可能打印 phase skip，不得合并成每阶段全用例通过。无服务的前段 CTest live Skipped 不替代后段 live。
- PR（含 fork）、push→main、workflow_dispatch 均在一次性 hosted ubuntu-24.04 执行；docs-only 可跳过 Release，code/unknown 必须执行。live 失败使 Release 失败，并进入唯一 result 汇聚，不产生另一个同名 check。
- 顶层无权限，checkout job 仅 contents:read、persist-credentials:false；无 secrets/OIDC、生产地址或共享后端，不使用 pull_request_target。fork 平台审批机制未改变，不声称已执行真实 fork 或手动 run。
- 仓库当前无 branch-protection required contexts；本任务不修改保护配置。合入验收必须人为核对精确 PR head 的 Release/Debug 与汇聚检查通过，不能把名字带 required 的注释当作平台强制保护。

复用 compose 的 redis:7-alpine / mongo:8.0；Redis 0.5 CPU/256 MiB，Mongo 0.75 CPU/512 MiB、WiredTiger cache 0.25 GiB。使用独占 project、临时 loopback 端口、有限健康等待和测试时间。Release job 上限 45 分钟，Redis/Mongo 两个 live step 上限分别为 19/16 分钟（计入 timeout 的 kill-after 宽限）；这些是故障预算，不是正常耗时。只清理自有 project 的容器/卷/网络，不 prune 他人资源；一次性 hosted VM 回收是硬杀后的隔离边界，不推广到常驻 runner。镜像 tag 可变，日志记录实际镜像身份，不宣称 digest pin。

## #12 旧条款重判

| 旧条款 | 当前处理 |
|---|---|
| M1 worker 启动异常 | 既有 finalizer 和 reserve/worker 故障注入，新增 pool lease 寿命、共享 owner 与 pool 获取失败回归；不恢复 RequestClose/WaitClosed |
| M2 EffectiveUri | PR #23 已修，完整 mongo.unit 已进入 CI；不重做修复 |
| M3 prefix 0/1/多命中 | 现 CMake 已区分未找到、多版本和非目录；已有真实 prefix 正向及拒绝路径证据 |
| M4 URI/pool 错误映射 | 已有真实 driver InvalidArgument 单测，以及 live Unavailable/RemoteError；新 pool 获取失败用例补状态边界 |
| M5 core/installed 消费 | core 冻结、当前 coroutine 自闭环，旧 core 必须依赖前置已废止；按 README 现行源码消费契约验收。infra 没有安装/导出包，旧 installed-package 要求不再作为本票完成条件；CMake 保留的上游已安装 coroutine 查找分支未验证，不宣称支持或移除该分支 |
| R-M3 / R-I1 | hiredis async 的 off-domain 清理及 OpenOnIoDomain 旧路径已删除；不为已删除路径补注入或单独重构 |
| R-M1 / R-M2 | 当前无对应 SleepUnhooked 注释及 RequestClose 8×4ms 路径；核销为旧路径不适用，不称旧机制实测完成 |
| R-M4 | 以本文件可复现命令、对应 PR/main 日志及依赖 SHA 补公开证据；CI 部分由 #34 同一门禁验收，避免重复实施 |

Redis RemoteError 误拆连接已由 PR #75 修复；仅完整服务端 ERROR 帧保留连接，其他读错误仍拆断。旧 runner Create/Connect 前置失败根因未定位，无证据与该读帧缺陷同根；保留历史不确定性，不作为当前已复现缺陷，也不称根因已修。

本地聚焦测试及真实容器实跑通过不等于 hosted 通过。最终在线证据发布在本候选 PR 和 #12/#34 验收评论，必须绑定精确 head/merge SHA、测试结果和未覆盖项；不得只凭退出码、绿色摘要或历史 docs-only run 关闭票。
