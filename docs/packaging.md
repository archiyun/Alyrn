# Packaging and installation

CoroPact is distributed as a CMake package. A release contains a Debian
package and a tarball, both containing headers, static libraries, and the
`CoroPactConfig.cmake` package metadata.

## Build release artifacts with Docker

The root `Dockerfile` is a multi-stage artifact builder. It runs the tests in
the builder stage and copies only the generated packages to the host:

```bash
docker buildx build \
  --target artifacts \
  --output=type=local,dest=dist \
  .
```

The default artifact includes the io_uring backend. To build the Reactor-only
package:

```bash
docker buildx build \
  --build-arg COROPACT_ENABLE_URING=OFF \
  --target artifacts \
  --output=type=local,dest=dist-reactor \
  .
```

The Docker build is intentionally an artifact build. CoroPact has no daemon
or executable runtime image to launch; applications install the package on
their own build hosts or inside their own application image.

## Download a release

For a tagged GitHub release, download either asset from the release page or
with the GitHub CLI:

```bash
gh release download v0.1.0 \
  --repo archiyun/CoroPact \
  --pattern '*.deb'
```

The tarball can be downloaded with `--pattern '*.tar.gz'` instead.

## Install into system paths

The Debian package is the recommended Linux installation method:

```bash
sudo apt install ./*.deb
```

The package installs headers under `/usr/include`, libraries under `/usr/lib`,
and CMake package files under `/usr/lib/cmake/CoroPact`. The io_uring package
also declares `liburing-dev` as a Debian dependency.

The tarball has the same `/usr`-relative layout and can be installed without a
package manager:

```bash
sudo tar -xzf coropact-*.tar.gz -C /
```

For a non-root or custom-prefix installation, build from source with an
explicit prefix instead of unpacking the system-layout tarball:

```bash
cmake -S . -B build-install \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DCMAKE_INSTALL_PREFIX="$HOME/.local"
cmake --build build-install
cmake --install build-install
```

## Consume the installed package

An application can use the exported targets from any standard CMake prefix:

```cmake
find_package(CoroPact CONFIG REQUIRED)

target_link_libraries(my_app PRIVATE CoroPact::coropact_reactor)
```

For a custom prefix, pass it during configuration:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH="$HOME/.local"
```

The io_uring targets are available when the package was built with
`COROPACT_ENABLE_URING=ON`:

```cmake
find_package(CoroPact CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE CoroPact::coropact_luring)
```

The installed package remains relocatable within its prefix: consumers use
the exported `CoroPact::` targets rather than hard-coded include or library
paths.

## Creating a GitHub release

Update the `project(... VERSION ...)` value in the root `CMakeLists.txt`, commit
it, and push a matching tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The tag workflow builds the Docker artifacts, runs the test suite as part of
the image build, and uploads the `.deb` and `.tar.gz` files to the GitHub
release.
