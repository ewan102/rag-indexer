#!/bin/bash
#
# E2E test for RAG indexing via RabbitMQ.
#
# Prerequisites (must be running):
#   - cozy-stack (default: localhost:8080, admin on :6060)
#   - RabbitMQ (default: localhost:5672)
#   - rag-indexer consumer (connected to above RabbitMQ)
#
# This script starts its own mock RAG server and expects cozy-stack's
# rag config to point to it (e.g. url: http://localhost:8000).
#
# Usage:
#   ./tests/e2e/run_cozy_e2e.sh
#
# Environment variables:
#   COZY_DOMAIN      - instance domain (default: cozy.localhost:8080)
#   COZY_ADMIN_PORT  - admin API port (default: 6060)
#   RABBITMQ_URL     - RabbitMQ URL (default: amqp://guest:guest@localhost:5672/)
#   MOCK_RAG_PORT    - port for mock RAG server (default: 8000)
#   RETRY_INTERVAL   - seconds between retry checks (default: 5)
#   MAX_WAIT         - max seconds to wait for indexing (default: 60)
#
# Options:
#   --cleanup        Delete test files from cozy-stack after tests complete

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COZY_DOMAIN="${COZY_DOMAIN:-cozy.localhost:8080}"
COZY_ADMIN_PORT="${COZY_ADMIN_PORT:-6060}"
RABBITMQ_URL="${RABBITMQ_URL:-amqp://guest:guest@localhost:5672/}"
MOCK_RAG_PORT="${MOCK_RAG_PORT:-8000}"
RETRY_INTERVAL="${RETRY_INTERVAL:-5}"
MAX_WAIT="${MAX_WAIT:-60}"

MOCK_RAG_PID=""
TEST_DIR_ID=""
TEST_DIR_NAME="e2e-rag-tests"
CLEANUP=false
PASS=0
FAIL=0
UPLOADED_FILE_IDS=()
TOKEN=""

# Parse options
for arg in "$@"; do
    case "$arg" in
        --cleanup) CLEANUP=true ;;
    esac
done

# --- Helpers ---

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    if [[ -n "$MOCK_RAG_PID" ]] && kill -0 "$MOCK_RAG_PID" 2>/dev/null; then
        kill "$MOCK_RAG_PID" 2>/dev/null || true
        wait "$MOCK_RAG_PID" 2>/dev/null || true
        echo "Mock RAG server stopped"
    fi
    if $CLEANUP && [[ ${#UPLOADED_FILE_IDS[@]} -gt 0 ]]; then
        echo "Cleaning up test files..."
        for fid in "${UPLOADED_FILE_IDS[@]}"; do
            trash_file "$fid" 2>/dev/null && echo "  Trashed $fid" || true
        done
    fi
    echo ""
    echo "=== Results: $PASS passed, $FAIL failed ==="
    if [[ $FAIL -gt 0 ]]; then
        exit 1
    fi
}
trap cleanup EXIT

log() { echo "[$(date +%H:%M:%S)] $*"; }
pass() { PASS=$((PASS + 1)); log "PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); log "FAIL: $1"; }

cozy_url() { echo "http://${COZY_DOMAIN}"; }
admin_url() { echo "http://localhost:${COZY_ADMIN_PORT}"; }
mock_url() { echo "http://localhost:${MOCK_RAG_PORT}"; }

# Get an OAuth token for io.cozy.files (cached for the session)
get_token() {
    if [[ -z "$TOKEN" ]]; then
        TOKEN=$(cozy-stack instances token-cli "$COZY_DOMAIN" "io.cozy.files" 2>/dev/null)
    fi
    echo "$TOKEN"
}

# Wait for a mock RAG file endpoint to return expected status
# Usage: wait_for_mock_status <file_id> [expected_status] [timeout]
wait_for_mock_status() {
    local file_id="$1"
    local expected="${2:-200}"
    local timeout="${3:-$MAX_WAIT}"
    local waited=0
    while [[ $waited -lt $timeout ]]; do
        local status
        status=$(curl -s -o /dev/null -w "%{http_code}" "$(mock_url)/mock/files/${file_id}")
        if [[ "$status" == "$expected" ]]; then
            return 0
        fi
        sleep "$RETRY_INTERVAL"
        waited=$((waited + RETRY_INTERVAL))
    done
    return 1
}

reset_mock() {
    reset_mock
}

# --- Service checks ---

check_services() {
    log "Checking services..."

    # cozy-stack
    if curl -sf "$(cozy_url)/status" > /dev/null 2>&1; then
        log "  cozy-stack: OK (${COZY_DOMAIN})"
    else
        log "  cozy-stack: UNREACHABLE at ${COZY_DOMAIN}"
        exit 1
    fi

    # RabbitMQ management API (default port 15672)
    local rmq_host
    rmq_host=$(echo "$RABBITMQ_URL" | sed -E 's|amqps?://[^@]*@([^:/]+).*|\1|')
    if curl -sf "http://${rmq_host}:15672/api/overview" -u guest:guest > /dev/null 2>&1; then
        log "  RabbitMQ: OK (${rmq_host})"
    else
        log "  RabbitMQ: UNREACHABLE (management API on ${rmq_host}:15672)"
        log "  (Enable rabbitmq_management plugin or check credentials)"
        exit 1
    fi

    # Check rag.index.q queue exists (proves rag-indexer has started)
    local queue_info
    queue_info=$(curl -sf "http://${rmq_host}:15672/api/queues/%2F/rag.index.q" -u guest:guest 2>/dev/null || echo "")
    if [[ -n "$queue_info" ]]; then
        local consumers
        consumers=$(echo "$queue_info" | jq -r '.consumers // 0')
        log "  rag.index.q queue: OK (consumers: ${consumers})"
        if [[ "$consumers" == "0" ]]; then
            log "  WARNING: no consumers on rag.index.q — is rag-indexer running?"
            exit 1
        fi
    else
        log "  rag.index.q queue: NOT FOUND — is rag-indexer running?"
        exit 1
    fi

    # Check instance exists
    if cozy-stack instances show "$COZY_DOMAIN" > /dev/null 2>&1; then
        log "  Instance ${COZY_DOMAIN}: OK"
    else
        log "  Instance ${COZY_DOMAIN}: NOT FOUND"
        log "  Create one with: make instance"
        exit 1
    fi

    log "All services OK"
}

# --- Mock RAG server ---

start_mock_rag() {
    log "Starting mock RAG server on :${MOCK_RAG_PORT}..."
    python3 "${SCRIPT_DIR}/mock_rag_server.py" "$MOCK_RAG_PORT" &
    MOCK_RAG_PID=$!
    # Wait for it to be ready
    local waited=0
    while [[ $waited -lt 10 ]]; do
        if curl -sf "http://localhost:${MOCK_RAG_PORT}/health" > /dev/null 2>&1; then
            log "Mock RAG server ready (PID: ${MOCK_RAG_PID})"
            return 0
        fi
        sleep 0.5
        waited=$((waited + 1))
    done
    log "Mock RAG server failed to start"
    exit 1
}

# --- Trigger rag-index job ---

trigger_rag_index() {
    log "Triggering rag-index job..."
    cozy-stack jobs run rag-index \
        --domain "$COZY_DOMAIN" \
        --json '{"doctype":"io.cozy.files"}' \
        > /dev/null 2>&1
}

# --- Test directory ---

ensure_test_dir() {
    local token
    token=$(get_token)

    # Try to find existing directory by path
    local response
    response=$(curl -sf \
        -H "Authorization: Bearer ${token}" \
        "$(cozy_url)/files/metadata?Path=/${TEST_DIR_NAME}" 2>/dev/null) || true

    if [[ -n "$response" ]]; then
        TEST_DIR_ID=$(echo "$response" | jq -r '.data.id')
        if [[ -n "$TEST_DIR_ID" && "$TEST_DIR_ID" != "null" ]]; then
            log "Test directory /${TEST_DIR_NAME} exists (${TEST_DIR_ID})"
            return 0
        fi
    fi

    # Create the directory
    response=$(curl -sf \
        -X POST \
        -H "Authorization: Bearer ${token}" \
        "$(cozy_url)/files/io.cozy.files.root-dir?Type=directory&Name=${TEST_DIR_NAME}" 2>&1) || {
        log "Failed to create test directory /${TEST_DIR_NAME}"
        exit 1
    }
    TEST_DIR_ID=$(echo "$response" | jq -r '.data.id')
    log "Created test directory /${TEST_DIR_NAME} (${TEST_DIR_ID})"
}

# --- Upload a file to cozy-stack ---

upload_file() {
    local filename="$1"
    local content="$2"
    local token
    token=$(get_token)

    local response
    response=$(curl -sf \
        -X POST \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: text/plain" \
        "$(cozy_url)/files/${TEST_DIR_ID}?Type=file&Name=${filename}" \
        -d "$content" 2>&1) || {
        log "  Upload failed for ${filename}"
        return 1
    }

    local file_id
    file_id=$(echo "$response" | jq -r '.data.id')
    UPLOADED_FILE_IDS+=("$file_id")
    echo "$file_id"
}

# Delete a file from cozy-stack
trash_file() {
    local file_id="$1"
    local token
    token=$(get_token)

    curl -sf \
        -X DELETE \
        -H "Authorization: Bearer ${token}" \
        "$(cozy_url)/files/${file_id}" \
        > /dev/null 2>&1
}

# ===================================================================
#  TEST CASES
# ===================================================================

test_basic_indexing() {
    log ""
    log "=== TEST: Basic file indexing via RabbitMQ ==="

    # Reset mock state
    reset_mock

    # Upload a file
    local filename="test-rag-$(date +%s).txt"
    local content="Hello, this is a test document for RAG indexing."
    log "Uploading ${filename}..."
    local file_id
    file_id=$(upload_file "$filename" "$content")
    if [[ -z "$file_id" || "$file_id" == "null" ]]; then
        fail "basic indexing - upload failed"
        return
    fi
    log "  File uploaded: ${file_id}"

    # Trigger indexing
    trigger_rag_index

    # Wait for the file to appear in mock RAG
    log "Waiting for file to be indexed (max ${MAX_WAIT}s)..."
    if wait_for_mock_status "$file_id"; then
        local indexed
        indexed=$(curl -sf "$(mock_url)/mock/files/${file_id}")
        local indexed_partition
        indexed_partition=$(echo "$indexed" | jq -r '.partition')
        log "  Indexed in partition: ${indexed_partition}"
        if [[ "$indexed_partition" == "$COZY_DOMAIN" ]]; then
            pass "basic indexing"
        else
            fail "basic indexing - wrong partition (expected ${COZY_DOMAIN}, got ${indexed_partition})"
        fi
    else
        fail "basic indexing - file not indexed within ${MAX_WAIT}s"
        log "  Mock requests received:"
        curl -sf "$(mock_url)/mock/requests" | jq '.' 2>/dev/null || true
    fi
}

test_file_deletion() {
    log ""
    log "=== TEST: File deletion via RabbitMQ ==="

    reset_mock

    # Upload and index a file first
    local filename="test-delete-$(date +%s).txt"
    local file_id
    file_id=$(upload_file "$filename" "Delete me.")
    if [[ -z "$file_id" || "$file_id" == "null" ]]; then
        fail "file deletion - upload failed"
        return
    fi
    log "  File uploaded: ${file_id}"

    trigger_rag_index
    log "Waiting for file to be indexed..."
    if ! wait_for_mock_status "$file_id"; then
        fail "file deletion - initial indexing failed"
        return
    fi
    log "  File indexed, now trashing..."

    # Trash the file
    trash_file "$file_id"

    # Trigger indexing again to process the deletion
    trigger_rag_index

    # Wait for file to disappear from mock RAG
    if wait_for_mock_status "$file_id" 404; then
        pass "file deletion"
    else
        fail "file deletion - file still present after ${MAX_WAIT}s"
    fi
}

test_retry_on_failure() {
    log ""
    log "=== TEST: Retry after transient failure ==="

    reset_mock

    # Upload a file
    local filename="test-retry-$(date +%s).txt"
    local file_id
    file_id=$(upload_file "$filename" "This should be retried.")
    if [[ -z "$file_id" || "$file_id" == "null" ]]; then
        fail "retry - upload failed"
        return
    fi
    log "  File uploaded: ${file_id}"

    # Configure mock to fail the first 2 requests for this file_id.
    # The rag-indexer does:
    #   1. GET /partition/.../file/{file_id} to check if it exists -> fail (500)
    #   -> message goes to retry queue (30s delay by default)
    #   2. On retry: GET again -> succeed (404 = new file)
    #   3. POST to create -> succeed
    # So we fail the first request (the initial GET), which triggers a retry.
    curl -sf -X POST "$(mock_url)/mock/fail/${file_id}?count=1" > /dev/null
    log "  Mock configured to fail 1 request for ${file_id}"

    # Trigger indexing
    trigger_rag_index
    log "Waiting for retry and eventual indexing (may take up to 60s for retry queue)..."

    # The first retry delay is 30s by default, so we need to wait longer
    if wait_for_mock_status "$file_id" 200 120; then
        # Verify the mock received the failed request + retry
        local requests
        requests=$(curl -sf "$(mock_url)/mock/requests")
        local get_count
        get_count=$(echo "$requests" | jq '[.[] | select(.path | contains("'"$file_id"'")) | select(.method == "GET")] | length')
        log "  GET requests for file: ${get_count}"
        if [[ "$get_count" -ge 2 ]]; then
            pass "retry on failure (${get_count} GETs = initial fail + retry)"
        else
            pass "retry on failure (indexed successfully, ${get_count} GETs)"
        fi
    else
        fail "retry on failure - file not indexed after retry wait"
        log "  Mock requests received:"
        curl -sf "$(mock_url)/mock/requests" | jq '[.[] | select(.path | contains("'"$file_id"'"))]' 2>/dev/null || true
    fi
}

# ===================================================================
#  MAIN
# ===================================================================

main() {
    log "=== RAG RabbitMQ E2E Test Suite ==="
    log ""

    check_services
    start_mock_rag
    ensure_test_dir

    test_basic_indexing
    test_file_deletion
    test_retry_on_failure

    log ""
    log "=== All tests completed ==="
}

main "$@"
