# http 层 examples —— 依赖 vexo_http
#   demo_http_server : HttpServer + Router，与 demo_echo_server 基准对比
#   www/             : 静态资源占位（index.html 描述可用路由）

add_executable(demo_http_server
    demo_http_server.cc
)
target_link_libraries(demo_http_server
    PRIVATE
        vexo_http
        vexo_foundation
)
