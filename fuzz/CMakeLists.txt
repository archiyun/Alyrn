add_executable(recv_source_state_fuzzer
    recv_source_state_fuzzer.cc
)

target_include_directories(recv_source_state_fuzzer
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(recv_source_state_fuzzer
    PRIVATE
        alyrn_io_contract
        alyrn_foundation
)

target_compile_features(recv_source_state_fuzzer PRIVATE cxx_std_23)
target_compile_options(recv_source_state_fuzzer PRIVATE -fsanitize=fuzzer,address,undefined)
target_link_options(recv_source_state_fuzzer PRIVATE -fsanitize=fuzzer,address,undefined)
