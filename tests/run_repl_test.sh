#!/bin/bash
set -euo pipefail

export USE_TESTCONTAINERS=1
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/var/run/docker.sock"
export AWS_S3_ENDPOINT="http://localhost:9000"
export CATALOG_ENDPOINT="http://localhost:8181"
export AWS_ACCESS_KEY_ID="admin"
export AWS_SECRET_ACCESS_KEY="password"
export AWS_REGION="us-east-1"
export GONOSUMCHECK=*
export GONOSUMDB=*
export GOPROXY=off
export LOG_LEVEL="${LOG_LEVEL:-info}"

TEST_PATTERN="${1:-TestSnapshotAndReplication}"
TEST_PKG="${2:-./tests/pg2iceberg/replication/}"
TIMEOUT="${3:-5m}"

echo "=== Running: $TEST_PATTERN in $TEST_PKG (timeout: $TIMEOUT) ==="
go test -run "$TEST_PATTERN" -timeout "$TIMEOUT" -v "$TEST_PKG" 2>&1 | \
    grep -v "DEBUG" | \
    grep -v "pgx\|logger.go:46\|select t.oid\|pg_type\|pg_class\|pg_namespace\|pg_attribute\|pg_replication\|pg_constraint\|pg_inherits\|information_schema\|pg_settings\|pg_catalog\|SELECT version\|select now\|SET TRANSACTION\|begin isolation\|SELECT EXISTS\|SELECT.*FROM.*pg_\|commit.*commandTag\|Exec.*commit\|Exec.*begin\|Exec.*SET LOCAL"
echo ""
echo "=== Done ==="
