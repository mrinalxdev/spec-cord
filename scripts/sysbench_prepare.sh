#!/usr/bin/env bash

set -euo pipefail

echo "==> Seeding shard-a (accounts 1–10000)"
docker exec mysql-shard-a mysql \
    -ucoordinator -pcoord_pass shard_a \
    -e "CALL seed_accounts(1, 10000);" 2>/dev/null || true

echo "==> Seeding shard-b (accounts 10001–20000)"
docker exec mysql-shard-b mysql \
    -ucoordinator -pcoord_pass shard_b \
    -e "CALL seed_accounts(10001, 20000);" 2>/dev/null || true

echo "==> Seeding shard-c (accounts 20001–30000)"
docker exec mysql-shard-c mysql \
    -ucoordinator -pcoord_pass shard_c \
    -e "CALL seed_accounts(20001, 30000);" 2>/dev/null || true

echo "==> Done. Verifying counts:"
docker exec mysql-shard-a mysql -ucoordinator -pcoord_pass shard_a \
    -e "SELECT COUNT(*) as shard_a_accounts FROM accounts;" 2>/dev/null
docker exec mysql-shard-b mysql -ucoordinator -pcoord_pass shard_b \
    -e "SELECT COUNT(*) as shard_b_accounts FROM accounts;" 2>/dev/null
docker exec mysql-shard-c mysql -ucoordinator -pcoord_pass shard_c \
    -e "SELECT COUNT(*) as shard_c_accounts FROM accounts;" 2>/dev/null
