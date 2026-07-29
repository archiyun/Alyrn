# foundation 层 examples —— 依赖 coropact_foundation
#   demo_rbtree  : base/rbtree.h 红黑树

add_executable(demo_rbtree
    demo_rbtree.cc
)
target_link_libraries(demo_rbtree
    PRIVATE
        coropact_foundation
)
