# C++ 书写规范

机械空白遵循 `.clang-format`（Google C++，100 列）。本页是语义拼写规则。

## 右侧已经写出类型时用 `auto`

局部变量左侧不要再重复右侧已经写出的长限定名。典型右侧：

- 构造或 `Type{...}` / `Type(...)`
- 显式转换（`static_cast`、`reinterpret_cast`、`const_cast`）
- 枚举值（`backend::LoopState::kCreated`）
- `std::make_unique<T>(...)` / `std::make_shared<T>(...)`

值用 `auto`。指针用 `auto*`（或 `auto**`），不要写会推成指针的裸 `auto`。

```cpp
// Yes
auto expected = backend::LoopState::kCreated;
auto* op = static_cast<Op*>(io_uring_cqe_get_data(cqe));
auto timer = std::make_unique<Timer>(std::move(cb), deadline, time::Duration::zero());

// No: the type is already on the right
backend::LoopState expected = backend::LoopState::kCreated;
::alyrn::uring::detail::Op* op = static_cast<::alyrn::uring::detail::Op*>(...);
std::unique_ptr<::alyrn::detail::Timer> timer = std::make_unique<::alyrn::detail::Timer>(...);
```

右侧看不出类型时保持显式：成员、函数参数、返回类型、`load()`、`State()`，以及调用点没有写出结果类型的调用。

```cpp
backend::LoopState observed = state_.load(std::memory_order_acquire);
const backend::LoopState state = State();
```

短名字（`int`、`bool`、当前命名空间里的 `Loop*`）即使旁边有转换也可以保持显式。这条规则是为了去掉 `namespace::...::Type` 前缀，不是隐藏每一个局部类型。

## `detail` 与 friend

在 `alyrn::epoll` / `uring` 里，`detail` 是嵌套命名空间，引用共享内部时要写 `::alyrn::detail::...`。文件内 `using Timer = ::alyrn::detail::Timer;` 优于每个局部或 lambda 参数重复前缀。

不要写混搭 friend，例如 `friend void detail::Fn(::alyrn::uring::detail::Op*)`。

- 在 `namespace alyrn::uring` 里写 `detail::Fn(detail::Op*)`
- 在匿名命名空间里写 `::alyrn::uring::detail::Fn(::alyrn::uring::detail::Op*)`

`scripts/check-namespace-layout.py` 会拒绝混写。编译仍可能通过，因为 `detail::` 可能绑到另一个函数。
