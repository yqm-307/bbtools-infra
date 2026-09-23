# infra 基础层与动态配置边界决策

## 状态与范围

状态：已确认方向，待按此实现

日期：2026-09-23。

本文定义 `bbtools-infra` 作为 bbt-framework 分布式框架底层工具层时的职责边界，以及基础动态配置模块的最小能力。本文不定义 framework 的服务发现、配置治理、灰度发布或业务配置模型。

## 定位

`bbtools-infra` 是工具层和基础接入层：

- 封装第三方客户端、协议适配、执行域、错误映射、连接与资源生命周期；
- 提供可被 framework、独立程序和其他上层组件消费的稳定 C++ 公共契约；
- 提供分布式环境所需的基础配置读取、版本观察和动态更新能力；
- 不承载 App、Service、业务领域对象、服务发现编排、配置治理策略或业务级热更新流程。

上层封装由 `bbt-framework` 负责，例如：

- 服务启动时的配置组合与依赖注入；
- 服务级配置 schema、默认值和校验策略；
- 配置中心选择、租约、权限、灰度和发布审批；
- 配置更新对 Service、Resource Registry 和业务状态的影响编排；
- 动态更新失败后的业务回滚与降级策略。

## 命名空间与目录

新增 Redis/Mongo 公共 API 不再使用旧的扁平模块命名空间，目标形态为：

```cpp
namespace bbt::infra::redis {}
namespace bbt::infra::mongo {}
```

目标目录：

```text
include/bbt/infra/
├── config/
│   ├── Source.hpp
│   ├── Snapshot.hpp
│   ├── Value.hpp
│   └── Watch.hpp
├── redis/
│   ├── Client.hpp
│   ├── Config.hpp
│   ├── Options.hpp
│   └── ...
└── mongo/
    ├── Client.hpp
    ├── Config.hpp
    ├── Options.hpp
    └── ...

src/config/
src/redis/
src/mongo/
```

配置模块使用 `bbt::infra::config`。Redis 和 Mongo 的客户端公共契约分别使用 `bbt::infra::redis` 与 `bbt::infra::mongo`。不在公共头中泄漏 hiredis、mongocxx、bsoncxx 或底层线程/连接池类型。

现有旧命名空间和旧客户端切片在迁移前保持事实记录；新公共接口不得继续扩大旧扁平命名空间的使用范围。兼容别名是否保留，由具体迁移 PR 和消费者证据决定。

## 基础动态配置模块

infra 只提供基础机制，不实现 framework 级配置平台。最小公共能力包括：

### 配置源

配置源抽象只负责读取带版本的快照和监听变更：

- 本地文件/内存快照源；
- 远程 KV/配置中心适配入口；
- 启动读取与运行期 watch 使用同一版本化快照语义；
- 远程源不可用时可由上层选择本地快照、旧版本或 fail-fast。

配置源不得强制绑定某一个配置中心、注册中心或传输协议。

### 版本化快照

每次配置读取或变更都携带最小元数据：

```text
namespace / key
version
revision 或 etag
observed_at
source
```

快照应当不可变或表现为不可变值。消费者以快照版本判断是否已观察到更新，不能只依赖本地时间戳。

### 动态监听

watch 机制提供：

- 初始快照；
- 变更通知；
- 版本去重；
- 断线后的重连与重新拉取；
- 监听关闭和资源收口；
- 明确的错误、断线和恢复状态。

watch 回调只负责把变更投递到项目协程运行时，不在第三方回调线程直接执行业务逻辑。具体如何应用变更由 framework 编排。

### 配置校验与应用边界

infra 可以提供结构化值、路径读取、类型转换和基础格式校验，但不拥有业务 schema。建议由 framework 负责：

```text
读取快照
→ framework schema 校验
→ 生成新的资源/服务配置
→ 原子替换或拒绝
→ 记录生效版本
```

Redis/Mongo 客户端只接受已经解析并校验的客户端配置。配置更新不得隐式重建客户端；是否原地更新、双实例切换或延迟到下一次启动，由 framework 根据资源生命周期决定。

## 协程与执行域

所有异步等待和变更通知必须基于 `bbtools-coroutine` 的现有模型：

- 不使用 C++20 `co_await`、`Task<T>` 作为公共示例或公共契约；
- 使用项目已有的 CompletionSignal、executor、协程等待/恢复和取消语义；
- Redis 的 hiredis async 回调只负责推进 adapter 状态并唤醒等待者；
- Mongo 的同步 driver 调用运行在有界 worker bridge，完成后回投共享 coroutine executor；
- 配置 watch 的第三方回调不得直接切入业务协程。

## Redis/Mongo 与配置模块的关系

配置模块只提供基础配置读取和变更机制：

```text
bbt::infra::config::Snapshot
        ↓ framework 解析/校验
bbt::infra::redis::Config 或 bbt::infra::mongo::Config
        ↓ framework Resource Registry
bbt::infra::redis::Client / bbt::infra::mongo::Client
```

Redis/Mongo 模块负责：

- driver 适配；
- 命令/查询接口；
- 错误映射；
- deadline/cancel 的底层语义；
- 连接、队列、worker、关闭和资源寿命。

Redis/Mongo 模块不负责：

- 配置中心租约；
- 服务发现；
- 多实例灰度；
- 业务配置 schema；
- framework Service 的重载策略。

## 非目标

- 不在 infra 建设完整配置中心；
- 不实现服务发现、注册、租约和治理控制面；
- 不定义业务配置 schema 或 framework Service 生命周期；
- 不把所有 Redis/Mongo 命令简单暴露为字符串命令 DSL；
- 不以 C++20 协程模型重写现有 coroutine runtime；
- 不因动态配置而增加隐藏 io context、隐藏线程或绕过既有关闭顺序。

## 验收方向

基础配置模块后续至少需要验证：

- 本地快照读取和版本元数据；
- 远程源适配边界不泄漏具体 provider；
- watch 变更、重复版本去重、断线恢复和关闭；
- 回调只回投项目协程执行域；
- framework 可在上层完成 schema 校验并决定拒绝/替换；
- Redis/Mongo 资源配置可以由 framework Resource Registry 消费，但 infra 不承担资源编排。
