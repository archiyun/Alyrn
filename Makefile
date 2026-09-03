JOBS ?= $(shell nproc)
TYPE ?= Debug
BUILD_DIR ?= build/$(shell echo $(TYPE) | tr A-Z a-z)
URING_BUILD_DIR ?= build/uring/$(shell echo $(TYPE) | tr A-Z a-z)
EXAMPLE ?= simple_echo
URING_EXAMPLE ?= simple_echo_luring

.PHONY: configure build debug release test clean run configure-uring \
	compile-commands-uring uring test-uring run-uring

configure:
	cmake -S . -B $(BUILD_DIR) \
		-G Ninja \
		-DCMAKE_BUILD_TYPE=$(TYPE)

compile_commands.json: configure
	ln -sfn $(BUILD_DIR)/compile_commands.json $@

build: compile_commands.json
	cmake --build $(BUILD_DIR) --parallel $(JOBS)

configure-uring:
	cmake -S . -B $(URING_BUILD_DIR) \
		-G Ninja \
		-DCMAKE_BUILD_TYPE=$(TYPE) \
		-DALYRN_ENABLE_URING=ON

compile-commands-uring: configure-uring
	ln -sfn $(URING_BUILD_DIR)/compile_commands.json compile_commands.json

uring: compile-commands-uring
	cmake --build $(URING_BUILD_DIR) --parallel $(JOBS)

test-uring: uring
	ctest --test-dir $(URING_BUILD_DIR) \
		--output-on-failure

run-uring: uring
	./$(URING_BUILD_DIR)/examples/$(URING_EXAMPLE)

debug:
	$(MAKE) build TYPE=Debug BUILD_DIR=build/debug

release:
	$(MAKE) build TYPE=Release BUILD_DIR=build/release

test: build
	ctest --test-dir $(BUILD_DIR) \
		--output-on-failure

run: build
	./$(BUILD_DIR)/examples/$(EXAMPLE)

clean:
	rm -rf build
