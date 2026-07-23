# examples —— 按 runtime 各层拆分到子目录
#
#   foundation/  → coropact_foundation （log / base 等基础设施）
#   net/         → coropact_net        （Reactor / timer / io_uring）
#   gateway/     → coropact_gateway    （反向代理 / 基准）
#
# 各子目录自带 CMakeLists.txt，只声明本层的 demo 目标。

add_subdirectory(foundation)
add_subdirectory(net)
add_subdirectory(gateway)
