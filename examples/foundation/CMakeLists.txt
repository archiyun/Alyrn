# foundation 层 examples —— 依赖 runtime_foundation
#   demo_logger  : AsyncLogger / LOG_* 流式宏
#   demo_rbtree  : base/rbtree.h 红黑树

add_executable(demo_logger_example
    demo_logger.cc
)
target_link_libraries(demo_logger_example
    PRIVATE
        runtime_foundation
)

add_executable(demo_rbtree
    demo_rbtree.cc
)
target_link_libraries(demo_rbtree
    PRIVATE
        runtime_foundation
)
