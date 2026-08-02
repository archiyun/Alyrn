# Packaging and installation on Linux

CoroPact supports two installation layers:

1. **Portable CMake installation** — the recommended path for every Linux
   distribution. It installs headers, static libraries, and relocatable
   `CoroPactConfig.cmake` metadata into any prefix.
2. **Native distribution packages** — optional convenience packages such as
   Debian `.deb` and Arch `PKGBUILD` packages. These are not interchangeable:
   a `.deb` is for Debian-family systems, not Arch or Fedora.

The Reactor backend does not require liburing. The io_uring backend requires
the liburing development package and a kernel with the capabilities used by
the selected runtime path.

## 1. Install build dependencies

The package names below are common names for current releases. If a
distribution has renamed one of them, install the equivalent C++23 compiler,
CMake, Ninja, pkg-config/pkgconf, and liburing development package.

### Arch Linux

```bash
sudo pacman -Syu --needed base-devel cmake ninja pkgconf liburing
```

`base-devel` supplies the compiler and standard build tools. Add `gtest` if
you want to build the optional test suite from the distribution package.

### Debian and Ubuntu

```bash
sudo apt update
sudo apt install build-essential cmake ninja-build pkg-config liburing-dev
```

### Fedora, RHEL-compatible systems, and CentOS Stream

```bash
sudo dnf install gcc-c++ cmake ninja-build pkgconf-pkg-config liburing-devel
```

### openSUSE

```bash
sudo zypper install gcc-c++ cmake ninja pkg-config liburing-devel
```

### Alpine Linux

```bash
sudo apk add build-base cmake ninja pkgconf liburing-dev
```

For a Reactor-only build, omit the liburing package and pass
`-DCOROPACT_ENABLE_URING=OFF` in the configure command below.

## 2. Portable installation from a release source archive

This path works on Arch, Fedora, openSUSE, Alpine, Debian, Ubuntu, and other
Linux distributions with a supported C++23 compiler. It does not depend on
`apt`, `dnf`, `pacman`, or a particular system library layout.

Download the source archive for the release:

```bash
VERSION=0.1.0
curl -fL \
  "https://github.com/archiyun/CoroPact/archive/refs/tags/v${VERSION}.tar.gz" \
  -o "CoroPact-${VERSION}.tar.gz"
tar -xzf "CoroPact-${VERSION}.tar.gz"
```

Build and install the io_uring-enabled package into `/usr/local`:

```bash
cmake -S "CoroPact-${VERSION}" -B "build-coropact-${VERSION}" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DBUILD_BENCHMARKS=OFF \
  -DCOROPACT_ENABLE_URING=ON \
  -DCMAKE_INSTALL_PREFIX=/usr/local

cmake --build "build-coropact-${VERSION}"
sudo cmake --install "build-coropact-${VERSION}"
```

For a Reactor-only install, use the same command with
`-DCOROPACT_ENABLE_URING=OFF` and without the liburing dependency.

To install without root privileges, use a user prefix:

```bash
cmake -S "CoroPact-${VERSION}" -B build-coropact-user -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DCMAKE_INSTALL_PREFIX=/home/your-user/.local
cmake --build build-coropact-user
cmake --install build-coropact-user
```

When consuming a user-prefix installation, provide the prefix to CMake:

```bash
cmake -S my-app -B build-my-app \
  -DCMAKE_PREFIX_PATH=/home/your-user/.local
```

## 3. Verify the installation

The install tree contains files similar to:

```text
/usr/local/include/coropact/...
/usr/local/lib/libcoropact_net.a
/usr/local/lib/libcoropact_reactor.a
/usr/local/lib/cmake/CoroPact/CoroPactConfig.cmake
```

A consuming application's `CMakeLists.txt` can use the exported targets:

```cmake
find_package(CoroPact CONFIG REQUIRED)

add_executable(my_app main.cc)
target_link_libraries(my_app PRIVATE CoroPact::coropact_reactor)
```

For io_uring:

```cmake
find_package(CoroPact CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE CoroPact::coropact_luring)
```

The installed package recreates external dependencies through CMake; it does
not embed absolute paths from the developer's source tree.

## 4. Native package installation

### Debian and Ubuntu: `.deb`

Download the Debian asset from the GitHub release and install it with `apt`:

```bash
gh release download v0.1.0 \
  --repo archiyun/CoroPact \
  --pattern '*.deb'
sudo apt install ./*.deb
```

This package is intended for Debian-family systems. Do not pass it to
`pacman`, `dnf`, or `zypper`.

### Arch Linux: `PKGBUILD`

Arch users can build a native package with `makepkg`:

```bash
git clone --depth=1 --branch v0.1.0 \
  https://github.com/archiyun/CoroPact.git
cd CoroPact/packaging/arch
makepkg -si
```

The package installs under `/usr`, records the files in pacman's database, and
declares `liburing` as a dependency for the io_uring-enabled build. The
maintainer should run `updpkgsums` and review the checksum before publishing a
stable Arch package.

### Fedora, openSUSE, Alpine, and other distributions

Use the portable CMake installation in section 2. It is the supported
cross-distribution path for this release. A future release may add native RPM
or APK packages, but their absence does not prevent installation or CMake
consumption on those systems.

## 5. Prebuilt tarball

The release `.tar.gz` is a system-layout artifact. It can be extracted at the
filesystem root on a compatible Linux host:

```bash
gh release download v0.1.0 \
  --repo archiyun/CoroPact \
  --pattern '*.tar.gz'
sudo tar -xzf coropact-*.tar.gz -C /
```

This does not register files with `pacman`, `rpm`, or another package manager.
Use the source-install path or a native package when package-manager tracking
and clean removal matter.

## 6. Docker release artifacts

The root `Dockerfile` is a multi-stage artifact builder. It runs the test suite
and emits `.deb` and `.tar.gz` files; it is not a runtime image because
CoroPact is a library rather than a daemon:

```bash
docker buildx build \
  --target artifacts \
  --output=type=local,dest=dist \
  .
```

To build a Reactor-only artifact:

```bash
docker buildx build \
  --build-arg COROPACT_ENABLE_URING=OFF \
  --target artifacts \
  --output=type=local,dest=dist-reactor \
  .
```

The Docker builder currently uses Ubuntu as a reproducible packaging
environment. That choice does not restrict the host distribution: Arch and
other systems should use the source install or their native package path.

## 7. Creating a release

Keep the project version and Git tag aligned:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The tag workflow builds the Debian/tarball artifacts and uploads them to the
GitHub Release. The Arch `PKGBUILD` consumes the same tag and source archive.
