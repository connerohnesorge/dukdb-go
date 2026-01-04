#!/bin/bash
# build.sh - Build script for dukdb-go WASM example
#
# This script builds the WASM binary and copies the necessary files
# for browser-based testing of the dukdb-go cloud filesystem.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Building dukdb-go WASM module..."
echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"

# Change to script directory
cd "$SCRIPT_DIR"

# Build the WASM binary
echo ""
echo "Compiling Go to WASM..."
GOOS=js GOARCH=wasm go build -o dukdb.wasm ./main.go

if [ -f "dukdb.wasm" ]; then
    SIZE=$(ls -lh dukdb.wasm | awk '{print $5}')
    echo "Built dukdb.wasm ($SIZE)"
else
    echo "ERROR: Failed to build dukdb.wasm"
    exit 1
fi

# Copy wasm_exec.js from Go installation
echo ""
echo "Copying wasm_exec.js..."
GOROOT=$(go env GOROOT)

# Try different locations where wasm_exec.js might be found
WASM_EXEC_LOCATIONS=(
    "$GOROOT/misc/wasm/wasm_exec.js"
    "$GOROOT/lib/wasm/wasm_exec.js"
    "$(dirname "$GOROOT")/share/go/misc/wasm/wasm_exec.js"
    "$(dirname "$GOROOT")/share/go/lib/wasm/wasm_exec.js"
)

WASM_EXEC_FOUND=""
for loc in "${WASM_EXEC_LOCATIONS[@]}"; do
    if [ -f "$loc" ]; then
        WASM_EXEC_FOUND="$loc"
        break
    fi
done

if [ -n "$WASM_EXEC_FOUND" ]; then
    cp "$WASM_EXEC_FOUND" ./wasm_exec.js
    echo "Copied wasm_exec.js from $WASM_EXEC_FOUND"
else
    echo "ERROR: wasm_exec.js not found"
    echo "Searched in:"
    for loc in "${WASM_EXEC_LOCATIONS[@]}"; do
        echo "  - $loc"
    done
    echo "Please ensure Go is properly installed."
    exit 1
fi

# List built files
echo ""
echo "Build complete! Files in $SCRIPT_DIR:"
ls -la *.wasm *.js *.html 2>/dev/null || true

# Print usage instructions
echo ""
echo "=============================================="
echo "  dukdb-go WASM Build Complete!"
echo "=============================================="
echo ""
echo "To run the test page:"
echo ""
echo "  Option 1: Using Python (recommended)"
echo "    cd $SCRIPT_DIR"
echo "    python3 -m http.server 8080"
echo "    Open http://localhost:8080 in your browser"
echo ""
echo "  Option 2: Using Node.js"
echo "    npx serve $SCRIPT_DIR"
echo "    Open the URL shown in your terminal"
echo ""
echo "  Option 3: Using Go"
echo "    go run serve.go"
echo ""
echo "Note: You must serve the files via HTTP - opening index.html"
echo "directly will not work due to CORS restrictions."
echo ""
echo "For testing with HTTPS (required for some features):"
echo "  Use a tool like mkcert to generate local certificates,"
echo "  or deploy to a server with HTTPS enabled."
echo ""
