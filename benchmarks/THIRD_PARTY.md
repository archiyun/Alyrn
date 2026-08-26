# Third-party comparison benchmarks

Third-party comparison programs (libuv, libevent, libev, libaio, and
standalone Asio) are intentionally kept out of the source distribution. Put
local copies under `benchmarks/third_party/`; that directory is ignored by
Git and is not built by the main CMake project.
