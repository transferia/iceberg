#!/usr/bin/env bash
#
# Simple CDC load generator for the demo.
# Produces a mix of INSERT/UPDATE/DELETE at a configurable rate.
#
# Usage:
#   ./loadgen.sh              # default: 10 rows/sec, 60/30/10 mix
#   ./loadgen.sh --rate 50    # 50 rows/sec
#   ./loadgen.sh --duration 120 --rate 20
#
set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
PGDATABASE="${PGDATABASE:-demo}"
export PGPASSWORD

RATE=10          # ops per second
DURATION=60      # seconds
INSERT_PCT=60    # % inserts
UPDATE_PCT=30    # % updates (rest = deletes)

while [[ $# -gt 0 ]]; do
    case $1 in
        --rate)     RATE="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --insert)   INSERT_PCT="$2"; shift 2 ;;
        --update)   UPDATE_PCT="$2"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

DELETE_PCT=$((100 - INSERT_PCT - UPDATE_PCT))
SLEEP=$(awk "BEGIN {printf \"%.4f\", 1/$RATE}")
TOTAL=$((RATE * DURATION))

PSQL="psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -qtAX"

# Ensure table exists
$PSQL -c "
CREATE TABLE IF NOT EXISTS orders (
    id         BIGSERIAL PRIMARY KEY,
    customer   VARCHAR(100) NOT NULL,
    product    VARCHAR(100) NOT NULL,
    quantity   INT NOT NULL,
    price      NUMERIC(10,2) NOT NULL,
    status     VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
" 2>/dev/null

CUSTOMERS=("alice" "bob" "charlie" "diana" "eve" "frank" "grace" "heidi" "ivan" "judy")
PRODUCTS=("widget-a" "widget-b" "gadget-x" "gadget-y" "gadget-z" "thing-1" "thing-2")
STATUSES=("pending" "shipped" "delivered" "returned")

echo "=== CDC Load Generator ==="
echo "Rate:     $RATE ops/sec"
echo "Duration: ${DURATION}s (~$TOTAL ops)"
echo "Mix:      ${INSERT_PCT}% insert / ${UPDATE_PCT}% update / ${DELETE_PCT}% delete"
echo "=========================="

inserts=0; updates=0; deletes=0; errors=0
start_time=$(date +%s)

for ((i=1; i<=TOTAL; i++)); do
    roll=$((RANDOM % 100))
    cust=${CUSTOMERS[$((RANDOM % ${#CUSTOMERS[@]}))]}
    prod=${PRODUCTS[$((RANDOM % ${#PRODUCTS[@]}))]}
    stat=${STATUSES[$((RANDOM % ${#STATUSES[@]}))]}
    qty=$((RANDOM % 20 + 1))
    price=$(awk "BEGIN {printf \"%.2f\", ($RANDOM % 500 + 1) / 10.0}")

    if (( roll < INSERT_PCT )); then
        # INSERT
        $PSQL -c "INSERT INTO orders (customer, product, quantity, price, status) VALUES ('${cust}_${i}', '$prod', $qty, $price, '$stat');" 2>/dev/null && ((inserts++)) || ((errors++))
    elif (( roll < INSERT_PCT + UPDATE_PCT )); then
        # UPDATE a random recent row
        $PSQL -c "UPDATE orders SET status = '$stat', quantity = $qty, price = $price WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1);" 2>/dev/null && ((updates++)) || ((errors++))
    else
        # DELETE a random row (table stays non-empty due to INSERT majority)
        $PSQL -c "DELETE FROM orders WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1);" 2>/dev/null && ((deletes++)) || ((errors++))
    fi

    # Progress every 10 seconds worth of ops
    if (( i % (RATE * 10) == 0 )); then
        elapsed=$(( $(date +%s) - start_time ))
        row_count=$($PSQL -c "SELECT count(*) FROM orders;" 2>/dev/null || echo "?")
        echo "[${elapsed}s] ops: $i/$TOTAL (I:$inserts U:$updates D:$deletes E:$errors) PG rows: $row_count"
    fi

    sleep "$SLEEP"
done

elapsed=$(( $(date +%s) - start_time ))
row_count=$($PSQL -c "SELECT count(*) FROM orders;" 2>/dev/null || echo "?")

echo ""
echo "=== Done ==="
echo "Duration:  ${elapsed}s"
echo "Total ops: $TOTAL (I:$inserts U:$updates D:$deletes E:$errors)"
echo "PG rows:   $row_count"
echo "============"
