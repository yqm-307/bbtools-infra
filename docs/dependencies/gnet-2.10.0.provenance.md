# gnet v2.10.0 移植出处与适用许可

参考仓库：https://github.com/panjf2000/gnet ，固定 tag v2.10.0 对应 commit `58b59a11350707153f8c48369693ab9beba7b2fa`。本包没有运行 Go gnet，也没有用其 benchmark 数值描述 C++ 性能。

## 本包来源 / 修改性质

- `gnet.go`、`options.go`、`client_unix.go`、`connection_unix.go`、`eventloop_unix.go`、`acceptor_unix.go`、`load_balancer.go`、`pkg/socket/`：API、owner、回调、socket 行为研究来源。
- `pkg/buffer/elastic/`、`pkg/buffer/ring/ring_buffer.go`：后续缓冲语义参考，本包不复制/直译其算法。
- 新 C++ 地址解析复用已有 Boost 1.90；fd 基础是独立 POSIX/Linux RAII 实现，不含复制的 Go 源代码。修改差异包括显式 fd 所有权、std::error_code、C++17、Linux-only 支持边界，以及拒绝正数 linger；完整行为差异在决策 0010 列出。
- 本声明不改变仓库自有代码许可。将来若复制/实质直译参考代码，应在对应文件保留原版权、适用许可证、固定 commit/文件出处和 C++ 修改记录；不能用本包“仅设计参考”的声明替代届时义务。

## Apache-2.0 主体

gnet 主体 LICENSE 为 Apache License, Version 2.0；文件头包含：

> Copyright (c) 2019 The Gnet Authors. All rights reserved.
>
> Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
>
> Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

部分文件年份不同（例如 client_unix.go 为 2021），移植时按**源文件实际头**保留，不统一改成 2019。完整固定 LICENSE：[来源](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/LICENSE)。若存在适用 NOTICE，随移植同时核验并保留，不凭本摘要豁免。

## ring 文件 MIT 例外

固定 `pkg/buffer/ring/ring_buffer.go` 不是主体 Apache 文件头；以下实际适用声明必须随该算法的未来移植保留：

```text
Copyright (c) 2019 The Gnet Authors. All rights reserved.
Copyright (c) 2019 Chao yuepan, Allen Xu

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

固定 [ring 来源](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/pkg/buffer/ring/ring_buffer.go)。不能将 ring 笼统重新许可为 Apache-2.0，也不能把 Apache 主体笼统改为 MIT。

## 纯逻辑包（buffer / Options / LB）移植出处

以下 C++ 实现依据上述 gnet 语义编写，全部为自有实现（未复制、未直译 Go 源码），此处只保留语义依据：

- `load_balancer.go`（Apache-2.0，gnet.go:128-140 hash 语义）：RoundRobin 顺序（`nextIndex % size`）、LeastConnections 严格小于（同值优先第一个）、SourceAddrHash 对 `net.Addr.String()` 完整字节做 IEEE CRC32 后按 size 取模。未移植其 owner 注册表与并发警告相关结构；C++ 侧单 owner 选择器显式非线程安全，跨域串行化留给 Engine 层（见 [0010](../decisions/0010-gnet-network-foundation.md)）。32 位 Go int 取负再 abs 的路径不复现（0010：不承诺 Go 二进制兼容）。
- `gnet.go:584-608`（Apache-2.0）：read/write cap、ET chunk 与 LockOSThread 的默认值与取整语义。这些字段已按 [0010](../decisions/0010-gnet-network-foundation.md) 的删除表从公共面去掉，C++ 侧不再有对应实现，本文件不描述其差异。
- `pkg/buffer/ring/ring_buffer.go`（MIT）：未移植该算法。C++ 侧 `EventRingBuffer` 的存储为 Boost 1.90 `boost::beast::multi_buffer`（Boost 软件许可）；它只增长、不覆盖最旧数据，追加超总量时在修改前拒绝。上方 MIT 声明仅在将来实质移植该算法时适用。
- `options.go`（Apache-2.0）：只映射到类型化 `Options` 的值层字段（现行字段见 [0010](../decisions/0010-gnet-network-foundation.md) 的「值层与并发接缝的现行口径」），不移植 `With*` 回调语法。
- CRC32 用 Boost `crc_32_type`（Boost 软件许可，仓内既有依赖）：与 Go `hash/crc32.ChecksumIEEE` 同为 IEEE CRC32；理由是复用成熟实现、不自造 hash、不用 std::hash（跨实现不稳定）、不新增依赖。
