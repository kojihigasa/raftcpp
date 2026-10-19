.PHONY: all configure build test clean

BUILD_DIR ?= build
CMAKE ?= cmake

all: test

configure:
	$(CMAKE) -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug

build: configure
	$(CMAKE) --build $(BUILD_DIR)

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

clean:
	$(CMAKE) -E rm -rf $(BUILD_DIR)
