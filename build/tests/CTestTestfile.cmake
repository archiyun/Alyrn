# CMake generated Testfile for 
# Source directory: /home/yun/design/high-concurrency-runtime/tests
# Build directory: /home/yun/design/high-concurrency-runtime/build/tests
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
include("/home/yun/design/high-concurrency-runtime/build/tests/runtime_unit_tests[1]_include.cmake")
include("/home/yun/design/high-concurrency-runtime/build/tests/runtime_integration_tests[1]_include.cmake")
add_test([=[memory_pool_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/memory_pool_smoke_test")
set_tests_properties([=[memory_pool_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;12;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
add_test([=[object_pool_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/object_pool_smoke_test")
set_tests_properties([=[object_pool_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;23;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
add_test([=[logger_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/logger_smoke_test")
set_tests_properties([=[logger_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;39;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
add_test([=[buffer_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/buffer_smoke_test")
set_tests_properties([=[buffer_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;55;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
add_test([=[http_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/http_smoke_test")
set_tests_properties([=[http_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;71;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
add_test([=[epoll_poller_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/epoll_poller_smoke_test")
set_tests_properties([=[epoll_poller_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;89;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
add_test([=[event_loop_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/event_loop_smoke_test")
set_tests_properties([=[event_loop_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;105;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
add_test([=[tcp_server_smoke_test]=] "/home/yun/design/high-concurrency-runtime/build/tests/tcp_server_smoke_test")
set_tests_properties([=[tcp_server_smoke_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;121;add_test;/home/yun/design/high-concurrency-runtime/tests/CMakeLists.txt;0;")
