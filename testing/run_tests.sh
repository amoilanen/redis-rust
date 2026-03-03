#!/usr/bin/env bash
#
# Run the full automated test suite for redis-rust.
#
# Usage:
#   ./testing/run_tests.sh              # Run all tests
#   ./testing/run_tests.sh -v           # Verbose output
#   ./testing/run_tests.sh -k "ping"    # Run only tests matching "ping"
#   ./testing/run_tests.sh --setup      # Install Python dependencies first
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Redis-Rust Automated Test Suite ===${NC}"
echo ""

# Check for --setup flag
if [[ "${1:-}" == "--setup" ]]; then
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip3 install -r "$SCRIPT_DIR/requirements.txt"
    shift
    echo ""
fi

# Verify dependencies
if ! python3 -c "import redis" 2>/dev/null; then
    echo -e "${RED}Error: Python 'redis' package not installed.${NC}"
    echo "Run: pip3 install -r $SCRIPT_DIR/requirements.txt"
    echo "Or:  $0 --setup"
    exit 1
fi

if ! python3 -c "import pytest" 2>/dev/null; then
    echo -e "${RED}Error: pytest not installed.${NC}"
    echo "Run: pip3 install pytest"
    exit 1
fi

# Build first
echo -e "${YELLOW}Building the project...${NC}"
cd "$PROJECT_ROOT"
cargo build 2>&1 | tail -3
echo ""

# Run tests
echo -e "${GREEN}Running tests...${NC}"
echo ""
python3 -m pytest "$SCRIPT_DIR" \
    --tb=short \
    -x \
    "$@"
