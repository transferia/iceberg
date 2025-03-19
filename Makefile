
# removes static build artifacts
.PHONY: clean
clean:
	@echo "--------------> Running 'make clean'"
	@rm -rf binaries tmp
	rm -f *.tgz

# Define the `build` target
API ?= trcli

.PHONY: build
build:
	go build -o  binaries/$(API) ./cmd/trcli/*.go

docker: build
	cp binaries/$(API) . && docker build -t transfer

.PHONY: test
test:
	USE_TESTCONTAINERS=1 gotestsum --rerun-fails --format github-actions --packages="..." -- -timeout=30m


# Define the `run-tests` target
.PHONY: run-tests
run-tests:
	export USE_TESTCONTAINERS=1; \
	sanitized_dir=$$(echo "$$dir" | sed 's|/|_|g'); \
	gotestsum \
	  --junitfile="reports/all_$$sanitized_dir.xml" \
	  --junitfile-project-name="all" \
	  --junitfile-testsuite-name="short" \
	  --rerun-fails \
	  --format github-actions \
	  --packages="." \
	  -- -timeout=15m; \
