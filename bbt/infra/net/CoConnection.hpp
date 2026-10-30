#pragma once

// Co 薄封装：协程消费面已收口为成员接口（docs/decisions/0012-icoconn-member-interface.md）——
//   - 建连/收连接：Engine::CoDial / Engine::CoAccept（ConnectionResult.hpp）；
//   - 单连接读写：ICoConnection / CoTcpConnection（Read→Buffer、
//     Write→WriteReport、AsyncWrite→WriteOperation、WaitClosed、Status）。
//
// 执行域（D4）：所有协程成员入口在消费、接纳、提交、创建等待责任之前统一
// 校验当前执行域；非协程上下文（普通线程、runtime idle、raw owner 回调）
// 一律 operation_not_permitted，包括带现成数据/零长度写/已就绪 accept 的
// 快路径。gate 不构造 CoObject、不创建 Processer、不产生网络副作用。
//
// 输出（D4）：成员 Write 先置 Rejected/0/本次错误与无效 operation，接纳后
// 才赋本次 op；成员 Read 失败不交付 Buffer 且不消费；Engine::CoAccept/CoDial
// 用临时结果，成功才交付 connection，失败不覆盖调用者原句柄。
//
// 不自动为连接建业务协程、不等业务回包、不自动关闭/重连/重发。

#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

// 本头保留仅为兼容：CoConnection.cc 里的 WriteOperation::Wait 实现与
// detail/CoInternal.hpp 内部装配仍归本编译单元；公开 Co* 自由函数已删除。
