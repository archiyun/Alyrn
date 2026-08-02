# syntax=docker/dockerfile:1

# This image is an artifact builder, not a runtime image: CoroPact is a
# library, so the useful Docker output is the installable package.
FROM ubuntu:24.04 AS liburing-build

ARG LIBURING_VERSION=2.9

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /tmp

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth=1 --branch "liburing-${LIBURING_VERSION}" \
        https://github.com/axboe/liburing.git \
        /tmp/liburing

WORKDIR /tmp/liburing
RUN ./configure --prefix=/opt/liburing \
    && make -j"$(nproc)" \
    && make install

FROM ubuntu:24.04 AS build

ARG COROPACT_ENABLE_URING=ON

ENV DEBIAN_FRONTEND=noninteractive
ENV PKG_CONFIG_PATH=/opt/liburing/lib/pkgconfig
ENV LD_LIBRARY_PATH=/opt/liburing/lib
WORKDIR /src

COPY --from=liburing-build /opt/liburing /opt/liburing

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        dpkg-dev \
        g++ \
        libgtest-dev \
        ninja-build \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTS=ON \
        -DBUILD_EXAMPLES=OFF \
        -DBUILD_BENCHMARKS=OFF \
        -DCOROPACT_ENABLE_URING=${COROPACT_ENABLE_URING} \
        -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build build -j"$(nproc)" \
    && ctest --test-dir build --output-on-failure \
    && cpack --config build/CPackConfig.cmake

# `docker buildx build --target artifacts --output=type=local,dest=dist .`
# copies these files to the host without creating a runtime container image.
FROM scratch AS artifacts

COPY --from=build /src/build/*.deb /
COPY --from=build /src/build/*.tar.gz /
