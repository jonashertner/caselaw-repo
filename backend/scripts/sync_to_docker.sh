#!/bin/bash
# Sync local PostgreSQL to Docker PostgreSQL

echo "=== Syncing local database to Docker ==="
echo "Time: $(date)"

# Get counts before sync
LOCAL_COUNT=$(psql -d swisslaw -t -c "SELECT COUNT(*) FROM decisions;" | tr -d ' ')
DOCKER_COUNT=$(docker exec swiss-caselaw-db-1 psql -U postgres -d swisslaw -t -c "SELECT COUNT(*) FROM decisions;" | tr -d ' ')

echo "Before sync:"
echo "  Local DB:  $LOCAL_COUNT decisions"
echo "  Docker DB: $DOCKER_COUNT decisions"

# Export from local
echo ""
echo "Exporting from local database..."
pg_dump -d swisslaw -t decisions -t chunks -t ingestion_runs --data-only --inserts > /tmp/swisslaw_data.sql

# Clear Docker tables and import
echo "Clearing Docker database tables..."
docker exec swiss-caselaw-db-1 psql -U postgres -d swisslaw -c "TRUNCATE decisions, chunks CASCADE;" 2>/dev/null || \
docker exec swiss-caselaw-db-1 psql -U postgres -d swisslaw -c "TRUNCATE decisions CASCADE;"

echo "Importing to Docker database..."
docker exec -i swiss-caselaw-db-1 psql -U postgres -d swisslaw < /tmp/swisslaw_data.sql

# Verify
DOCKER_COUNT_AFTER=$(docker exec swiss-caselaw-db-1 psql -U postgres -d swisslaw -t -c "SELECT COUNT(*) FROM decisions;" | tr -d ' ')

echo ""
echo "After sync:"
echo "  Docker DB: $DOCKER_COUNT_AFTER decisions"
echo ""
echo "=== Sync complete! ==="
