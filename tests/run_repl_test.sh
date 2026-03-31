#!/bin/bash
set -euo pipefail

export USE_TESTCONTAINERS=1
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/var/run/docker.sock"
export AWS_S3_ENDPOINT="${AWS_S3_ENDPOINT:-http://localhost:9000}"
export CATALOG_ENDPOINT="${CATALOG_ENDPOINT:-http://localhost:8181}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-admin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-password}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export LOG_LEVEL="${LOG_LEVEL:-info}"

TEST_PATTERN="${1:-Test}"
TEST_PKG="${2:-./tests/...}"
TIMEOUT="${3:-15m}"

echo "=== Running: $TEST_PATTERN in $TEST_PKG (timeout: $TIMEOUT) ==="
go test -run "$TEST_PATTERN" -timeout "$TIMEOUT" -count=1 -v "$TEST_PKG" 2>&1 | \
    grep -v "DEBUG" | \
    grep -v "pgx\|logger.go:46\|select t.oid\|pg_type\|pg_class\|pg_namespace\|pg_attribute\|pg_replication\|pg_constraint\|pg_inherits\|information_schema\|pg_settings\|pg_catalog\|SELECT version\|select now\|SET TRANSACTION\|SELECT EXISTS\|SELECT.*FROM.*pg_\|Exec.*SET LOCAL\|select setting\|Exec.*commit\|Exec.*begin"
echo ""
echo "=== Done ==="
