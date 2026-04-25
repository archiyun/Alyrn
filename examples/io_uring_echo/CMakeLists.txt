if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    return()
endif()

add_executable(io_uring_echo main.cpp)

target_compile_features(io_uring_echo PRIVATE cxx_std_20)

# liburing headers vendored in third_party（.so 已随系统安装）
target_include_directories(io_uring_echo PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/liburing/include
)

target_link_libraries(io_uring_echo PRIVATE
    /usr/lib/x86_64-linux-gnu/liburing.so.2
)
