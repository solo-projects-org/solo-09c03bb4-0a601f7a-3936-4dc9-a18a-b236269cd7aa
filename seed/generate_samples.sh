#!/usr/bin/env bash
# Generates three sample pg_dump custom-format backups into /samples:
#   normal.dump  - small, valid (3 plain tables with PK/FK/unique/check/indexes)
#   slow.dump    - one large table + index; restore takes a while (cancel demo)
#   corrupt.dump - normal.dump with a zeroed chunk in the middle (restore fails)
set -euo pipefail

PGHOST="${PGHOST:-db}"
PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
SLOW_ROWS="${SLOW_ROWS:-2000000}"
OUT=/samples

psql() { command psql -h "$PGHOST" -U "$PGUSER" -v ON_ERROR_STOP=1 "$@"; }

echo "waiting for postgres..."
until pg_isready -h "$PGHOST" -U "$PGUSER" -q; do sleep 1; done

echo "building sample source databases..."
psql -d postgres -c "DROP DATABASE IF EXISTS sample_normal"
psql -d postgres -c "DROP DATABASE IF EXISTS sample_slow"
psql -d postgres -c "CREATE DATABASE sample_normal"
psql -d postgres -c "CREATE DATABASE sample_slow"

psql -d sample_normal <<'SQL'
CREATE TABLE customers (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email      text NOT NULL UNIQUE,
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE orders (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    amount      numeric(12,2) NOT NULL CHECK (amount >= 0),
    status      text NOT NULL DEFAULT 'new',
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_created  ON orders(created_at);
CREATE TABLE events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint REFERENCES customers(id),
    kind        text NOT NULL,
    payload     text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_events_kind ON events(kind);

INSERT INTO customers(email, name)
SELECT 'user' || g || '@example.com', 'User ' || g FROM generate_series(1, 1000) g;
INSERT INTO orders(customer_id, amount, status)
SELECT 1 + (g * 7) % 1000, (random() * 500)::numeric(12,2),
       CASE WHEN g % 3 = 0 THEN 'paid' ELSE 'new' END
FROM generate_series(1, 5000) g;
INSERT INTO events(customer_id, kind, payload)
SELECT 1 + (g * 13) % 1000,
       (ARRAY['click','view','purchase','logout'])[1 + g % 4],
       md5(g::text)
FROM generate_series(1, 100000) g;
SQL

psql -d sample_slow <<SQL
CREATE TABLE big_events (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload    text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO big_events(payload)
SELECT substr(md5(random()::text), 1, 24) FROM generate_series(1, ${SLOW_ROWS});
CREATE INDEX idx_big_events_payload ON big_events(payload);
SQL

mkdir -p "$OUT"
echo "dumping normal.dump..."
pg_dump -h "$PGHOST" -U "$PGUSER" -Fc -d sample_normal -f "$OUT/normal.dump"
echo "dumping slow.dump (${SLOW_ROWS} rows)..."
pg_dump -h "$PGHOST" -U "$PGUSER" -Fc -d sample_slow -f "$OUT/slow.dump"

echo "creating corrupt.dump..."
cp "$OUT/normal.dump" "$OUT/corrupt.dump"
size=$(stat -c%s "$OUT/corrupt.dump")
dd if=/dev/zero of="$OUT/corrupt.dump" bs=1 seek=$((size / 2)) count=4096 conv=notrunc status=none

psql -d postgres -c "DROP DATABASE IF EXISTS sample_normal"
psql -d postgres -c "DROP DATABASE IF EXISTS sample_slow"

ls -lh "$OUT"
echo "samples ready."
