# 第四轮用户试用：对抗性模糊/压力 + RecvSource buffer_size 修复（2026-09-29）

第四轮继续以用户身份用 `find_package(Alyrn)` 写对抗性程序，在 ASan/UBSan/LSan 下做差分模糊与
压力测试。**没有再发现内存安全或 DoS 漏洞**——除下面的 H1 外，库在重压下都稳。本文记录覆盖到的
攻击面与唯一的修复项。

## 覆盖到的攻击面（均在 ASan/UBSan/LSan 下，结论：稳）

| 探测 | 方法 | 结果 |
|---|---|---|
| `net::Buffer` Find/Linearize/Append/Drain | 80 万次差分模糊 vs 平坦参考模型，1–8 字节小块强制跨块 | 稳 |
| 拆连接竞态 | RST / 半关 / 只发不收 / 在途时 SIGTERM 强停，两后端 | 干净，无泄漏/UAF |
| 数据完整性 | 120 并发、1B–1MB 随机负载跨块，Read + Recv 两条路径 | 字节级一致 |
| `Endpoint`/`ParseIpAddress` | 150 万次对抗字符串（内嵌 null、超长、畸形 IP） | 无崩溃/UB |
| `connect_timeout` churn | 160 次黑洞连接超时 | 全部超时，无 fd/内存泄漏 |
| uring `RecvSource` 溢出/暂停-恢复 | 极小容量 + 洪泛 + 拆连接 | 字节级一致，干净拆卸 |

## H1 RecvSource 的 `buffer_size` 陷阱（易用性，低）

现象：一个 Loop 上所有 `RecvSource` 共享同一个 provided-buffer ring，其缓冲区大小在 `Loop::Init`
时由 `uring::Options::shared_buffer_size`（默认 16 KiB）固定。但 `RecvSourceOptions::buffer_size`
看着像每个 source 可配，实则**必须等于该 loop 的大小**，否则 `RecvSource::Create` 返回一个**没有
任何解释的裸 `EINVAL`**（`loop.cc` 的 `buffer_size != shared_buffer_size_`）。在 Runtime 下更糟：
没有入口改 `shared_buffer_size`，且 recv-source 测试的 skip 判据把 `EINVAL` 当成"环境不可用"直接
跳过（`test_luring_recv_source_smoke.cc` 的 `IsEnvironmentSkip`），把这个陷阱一直藏着。

试用时我把 `buffer_size` 设成 512 去压溢出路径，结果每条连接都 `Create` 失败→handler 关流→因
有未读数据触发 RST，排查许久才定位到是 size 不匹配。

修复：

- [x] `RecvSourceOptions::buffer_size` 默认改为 `0`，含义"采用 loop 的 shared buffer size"。
      `Create` 在 0 时用 loop 的大小、并从选中的 pool 读回权威大小；非 0 时仍要求与 ring 一致
      （作为显式断言）。向后兼容：0 继承，默认 loop 仍是 16 KiB，与旧默认同值。
- [x] 顺带修正容量溢出检查里对零 `buffer_size` 的除零隐患。
- [x] 新测试：对**非默认** ring 大小验证继承成功，并验证不匹配的显式大小被拒。用了更严格的 skip
      判据（不把 `EINVAL` 当环境跳过），以免继承回归被掩盖。做了正反验证：还原旧默认，新测试即以
      "inherited-size RecvSource creation failed: Invalid argument" 失败。

未改（记录）：`buffer_size` 非 0 且与 ring 不一致时仍返回 `EINVAL`（errno 表达力有限）；已在字段
注释写明其为 loop 级属性、0 表示继承，推荐留空。

## 验收

- [x] 单元测试覆盖继承与不匹配；正反验证通过。
- [x] 全量测试（epoll+uring）通过；gcc-14 严格+io_uring 0 错 0 警；ASan/UBSan、TSan 干净。
- [x] 端到端：修复后用**默认** `RecvSourceOptions` 的 RecvSource 服务器在 Runtime 下字节级一致
      （修复前默认值会以裸 `EINVAL` 失败）。

## 结果

分支 `agent/user-trial-round4-fixes`，在 `main`（847a887）之上：

| 提交 | 内容 |
|---|---|
| 760342b | `RecvSource` 采用 loop 的 shared buffer size（H1） |
