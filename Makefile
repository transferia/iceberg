
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
	export RECIPE_CLICKHOUSE_BIN=clickhouse; \
	export USE_TESTCONTAINERS=1; \
	sanitized_dir=$$(echo "$$dir" | sed 's|/|_|g'); \
	gotestsum \
	  --junitfile="reports/all_$$sanitized_dir.xml" \
	  --junitfile-project-name="all" \
	  --junitfile-testsuite-name="short" \
	  --format github-actions \
	  --packages="./..." \
	  -- -timeout=15m; \

.PHONY: recipe
recipe:
	docker compose -f recipe/docker-compose.yml up -d
	sleep 5
	docker compose -f recipe/docker-compose.yml exec -T spark-iceberg ipython ./provision.py
	sleep 5
	echo "AWS_S3_ENDPOINT=http://$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' minio):9000"
	echo "CATALOG_ENDPOINT=http://$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' iceberg-rest):8181"
