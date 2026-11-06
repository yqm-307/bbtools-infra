# 0017：从请求创建关联完整的 RPC 响应

## 决定

在 `bbt::infra::rpc::Response` 增加公共便捷入口：

```cpp
static Response FromRequest(const Request& request);
```

返回独立的 Response 值，仅复制 `RequestId`、`Service`、`Method`、`RequestSchema`、`ResponseSchema` 五个字符串。初始 payload 与 metadata 为空，不复制请求的 payload、metadata、context、traffic、ordering 或 message_id，也不持有 Request、context、session 或 socket 引用。字符串复制可能分配，因此不声明 `noexcept`。

## 原因与责任

回复方逐一复制五个关联字段容易漏填；此入口集中正常回复的初始化规则。保持值层 header-only，不引入会话状态。业务显式设置回复 payload 与 metadata；示例和会话测试的 Echo 使用同一入口。

`FromRequest` 只初始化值，不授权回复、不提交写入，也不代替 `ReplyCompletion`。现有五字段 setter 和默认/payload 构造均保留；创建后误改关联字段仍由 `ReplyCompletion` 比较全部五字段并返回 `ProtocolError`。不新增构造重载，避免改变现有大括号构造的重载选择。

## 兼容与验证

不改变 Response 数据布局、wire 字段编号、编码或错误语义，不调整 Inbound/Outbound 和旧 transport 入口。[RPC 指南](../guides/rpc-net.md#会话回复构造)说明业务与提交边界。

`rpc.net.response_from_request` 验证五字段复制、初始空 payload/metadata、源请求修改及销毁后的独立性；会话回程、双向调用及 `rpc.net.v2s_reply_corr_mismatch` 验证实际使用与误改拒绝。
