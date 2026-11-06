#pragma once

/**
 * @file CoConnection.hpp
 * @brief 协程消费面的内部装配头；不声明公共接口。
 *
 * 协程入口已收口为成员：建连/收连接见 Engine::CoDial 与 Engine::CoAccept（ConnectionResult.hpp）；
 * 单连接读写见 ICoConnection / CoTcpConnection（Read→Buffer、Write→WriteReport、
 * AsyncWrite→WriteOperation、WaitClosed、Status）。
 *
 * 执行域：所有协程入口在消费、接纳、提交或创建等待责任之前统一校验当前执行域。非协程上下文
 * （普通线程、runtime idle、raw owner 回调）一律返回 operation_not_permitted，包括带现成数据、
 * 零长度写或已就绪 accept 的快路径。gate 不构造 CoObject、不创建 Processer，也不产生网络副作用。
 *
 * 输出：成员 Write 先置 Rejected/0/本次错误与无效 operation，接纳后才写入本次 op；成员 Read
 * 失败不交付 Buffer 且不消费；Engine::CoAccept 与 CoDial 先写入局部结果，成功才交付 connection，失败不覆盖
 * 调用者原句柄。
 *
 * 不自动为连接建业务协程，不等待业务回包，不自动关闭、重连或重发。
 *
 * 本头只做装配：WriteOperation::Wait 与 detail/CoInternal.hpp 的内部实现归本编译单元；
 * 公开的 Co* 自由函数不存在，协程入口只通过 Engine 与 CoTcpConnection 成员提供。
 */

#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/WriteOperation.hpp>
