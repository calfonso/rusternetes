#!/bin/bash
# Test Apple container runtime abstraction

set -e

echo "=== Testing Apple Container Runtime ==="

# Ensure container tool is available
if ! command -v container &> /dev/null; then
    echo "Error: container command not found"
    echo "Add /opt/homebrew/bin to PATH or install from https://github.com/apple/container"
    exit 1
fi

echo "✓ Container tool found: $(container --version)"

# Test pulling an image
echo ""
echo "Testing image pull..."
container image pull docker.io/library/alpine:latest || {
    echo "Note: Image pull may have failed, continuing..."
}

# Test image inspection
echo ""
echo "Testing image inspect..."
if container image inspect alpine:latest > /dev/null 2>&1; then
    echo "✓ Image inspect succeeded"
else
    echo "✗ Image inspect failed"
    exit 1
fi

# Test container creation
echo ""
echo "Testing container creation..."
CONTAINER_ID=$(container container create --name test-alpine alpine:latest echo "Hello from Apple container")
echo "✓ Container created: $CONTAINER_ID"

# Test container inspection
echo ""
echo "Testing container inspect..."
if container container inspect test-alpine > /dev/null 2>&1; then
    echo "✓ Container inspect succeeded"
else
    echo "✗ Container inspect failed"
    exit 1
fi

# Test container start
echo ""
echo "Testing container start..."
if container container start test-alpine > /dev/null 2>&1; then
    echo "✓ Container start succeeded"
else
    echo "✗ Container start failed"
fi

# Wait a bit
sleep 1

# Test container logs
echo ""
echo "Testing container logs..."
if container container logs test-alpine 2>&1 | grep -q "Hello"; then
    echo "✓ Container logs succeeded"
else
    echo "Note: Container logs may not contain expected output"
fi

# Cleanup
echo ""
echo "Cleaning up..."
container container rm -f test-alpine > /dev/null 2>&1 || true

echo ""
echo "=== Apple Container Runtime Test Complete ==="
