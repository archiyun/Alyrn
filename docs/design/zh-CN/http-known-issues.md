# HTTP 层已知问题

> 记录 HTTP 层已识别、暂未修复的设计缺陷与协议偏差，方便后续按优先级处理。

## Set-Cookie 仅支持单值（待修复）

### 现象

`HttpResponse::headers_` 是 `std::unordered_map<std::string, std::string>`，每个 header 名只能映射到一个值。

按 RFC 6265 §3，`Set-Cookie` 是 HTTP/1.x 唯一一个标准里**明确允许在同一响应中重复出现**的 header：

```http
HTTP/1.1 200 OK
Set-Cookie: session=abc; Path=/; HttpOnly
Set-Cookie: theme=dark;  Path=/; Max-Age=31536000
```

当 handler 连续调用：

```cpp
resp.AddHeader("Set-Cookie", "session=abc; Path=/; HttpOnly");
resp.AddHeader("Set-Cookie", "theme=dark;  Path=/; Max-Age=31536000");
```

当前实现会让第二次写入**覆盖第一次**，浏览器只能收到一个 cookie。

### 触发条件

任何需要在单个响应里下发**多个独立 cookie** 的场景，例如登录后同时设置会话凭证 + 用户偏好。

### 暂不修复的原因

项目当前不涉及会话存储/认证流程（网关角色为反向代理 + 负载均衡 + 健康检查），实际负载不会触发该场景。

### 建议方案（届时再选）

| 方案 | 说明 | 取舍 |
|---|---|---|
| A. 改 `unordered_multimap` | 所有 header 都支持多值 | 通用，但需要审视 `find` 语义（取第一个？合并？），现有调用点都要看一遍 |
| B. `headers_` 不变，新增 `std::vector<std::string> set_cookies_` | 单独的字段承载多值 cookie，`ToString()` 单独 emit | 改动最小，语义清晰；HTTP/2 路径里 `Http2Session::SendResponse` 也得读这个字段 |
| C. 容器换成 `std::unordered_map<std::string, std::vector<std::string>>` | 任意 header 都支持多值 | 内存开销略大，绝大多数 header 只有一个值，列表头是浪费 |

倾向方案 **B**——`Set-Cookie` 是当前唯一的多值需求，单独建模符合"为已发生的复杂性服务"的原则；其他 header 真要支持多值时再考虑 A 或 C。

### 改动涉及面

- `include/runtime/http/http_response.h` — 新增 `set_cookies_` 字段和 `AddSetCookie(std::string_view)` 方法
- `src/http/http_response.cpp` — `ToString()` 在 header 段循环里之后 emit
- `src/http/http2_session.cpp` — `SendResponse` 在头部组装循环里追加
- `AddHeader("Set-Cookie", ...)` 路径建议直接转发到 `AddSetCookie`，保持兼容
