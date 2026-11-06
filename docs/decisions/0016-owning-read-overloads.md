# 0016：以 Buffer 和错误码统一读取结果

## 决定

Connection 提供非等待 owning 读取；ICoConnection/CoTcpConnection 在相同移交语义上增加协程等待，并暴露同语义非等待入口：

```cpp
std::error_code TryRead(Buffer& out);
std::tuple<Buffer, std::error_code> TryRead();
std::error_code Read(Buffer& out, const WaitOptions& options = {});
std::tuple<Buffer, std::error_code> Read(const WaitOptions& options = {});
```

非等待移交和协程等待各只有一个核心实现；tuple 重载只创建空 Buffer、调用出参重载并组装返回值。派生实现使用 using 保留基类便捷重载，避免 C++ 名称隐藏。ReadOutcome/ReadState 退出现役公共接口，协议消费者直接按 error_code 判断。

成功必须交付非空 owning Buffer。失败保持 out 原值、零消费；tuple 失败交付空 Buffer。无数据且尚未终结为 NetErrc::WouldBlock，锁竞争为 NetErrc::Busy，尾部耗尽后的读终态为 NetErrc::Eof 或持久错误。先交付全部已发布尾部再报告终态；EOF 只描述读方向。超时、取消、执行域和内存错误沿用原错误。Read 保留 Busy 尝试前释放代次采样、等待登记及谓词复查，不改并发或 wire 模型。

Buffer 独占已交付载荷，与连接状态和 socket 寿命解耦。持有 Buffer 不阻止物理回收，不计入库内未交付缓冲额度；不承诺上层总内存有界或 Buffer 可并发修改。

## 兼容边界

本决定取代 0010、0012 中 ReadOutcome/ReadState 的读取结果承诺。旧 ADR 正文保留，仅追加此取代关系。现有 HTTP/RPC/Redis 读取、直接测试和示例迁移到四重载。

Connection::TryRead(std::string&)、TryAccess 和 ReadAccess 暂时保留：它们仍有 raw 回调、守卫竞争测试及冻结消费门禁。四重载签名授权不等于所有既有 raw 消费者及仓外消费者退役授权。因此本轮达到 owning 读取结果统一，尚未达到全部读取面简化。仓外仅列出只读影响，不在本仓迁移中代写；新增重载使无显式目标类型的方法取址需要消歧。
