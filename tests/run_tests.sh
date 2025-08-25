#!/bin/bash

# ClickHouse Rust Client Test Runner
# This script runs various test suites for the ClickHouse client library

set -e

echo "🚀 Starting ClickHouse Rust Client Tests"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if ClickHouse is running
check_clickhouse() {
    print_status "Checking ClickHouse server availability..."
    
    # Try to connect to ClickHouse on default ports
    if nc -z localhost 9000 2>/dev/null; then
        print_success "ClickHouse native protocol (port 9000) is accessible"
        NATIVE_AVAILABLE=true
    else
        print_warning "ClickHouse native protocol (port 9000) is not accessible"
        NATIVE_AVAILABLE=false
    fi
    
    if nc -z localhost 8123 2>/dev/null; then
        print_success "ClickHouse HTTP interface (port 8123) is accessible"
        HTTP_AVAILABLE=true
    else
        print_warning "ClickHouse HTTP interface (port 8123) is not accessible"
        HTTP_AVAILABLE=false
    fi
    
    if [ "$NATIVE_AVAILABLE" = false ] && [ "$HTTP_AVAILABLE" = false ]; then
        print_warning "No ClickHouse interfaces are accessible. Some tests will be skipped."
        print_status "To start ClickHouse, you can use:"
        echo "  - Docker: docker run -d --name clickhouse-server -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server:latest"
        echo "  - System service: sudo systemctl start clickhouse-server"
        echo "  - Manual: clickhouse-server --config-file=/etc/clickhouse-server/config.xml"
    fi
}

# Run unit tests (don't require ClickHouse)
run_unit_tests() {
    print_status "Running unit tests..."
    
    if cargo test --test unit_tests --no-default-features; then
        print_success "Unit tests passed!"
    else
        print_error "Unit tests failed!"
        exit 1
    fi
}

# Run integration tests
run_integration_tests() {
    print_status "Running integration tests..."
    
    if cargo test --test integration_tests --no-default-features; then
        print_success "Integration tests passed!"
    else
        print_error "Integration tests failed!"
        exit 1
    fi
}

# Run all tests
run_all_tests() {
    print_status "Running all tests..."
    
    if cargo test --tests --no-default-features; then
        print_success "All tests passed!"
    else
        print_error "Some tests failed!"
        exit 1
    fi
}

# Run tests with specific features
run_feature_tests() {
    local feature=$1
    print_status "Running tests with feature: $feature"
    
    if cargo test --tests --features "$feature" --no-default-features; then
        print_success "Tests with feature '$feature' passed!"
    else
        print_error "Tests with feature '$feature' failed!"
        exit 1
    fi
}

# Run tests with verbose output
run_verbose_tests() {
    print_status "Running tests with verbose output..."
    
    if cargo test --tests --no-default-features -- --nocapture; then
        print_success "Verbose tests passed!"
    else
        print_error "Verbose tests failed!"
        exit 1
    fi
}

# Run tests with specific test pattern
run_pattern_tests() {
    local pattern=$1
    print_status "Running tests matching pattern: $pattern"
    
    if cargo test --tests --no-default-features "$pattern"; then
        print_success "Tests matching '$pattern' passed!"
    else
        print_error "Tests matching '$pattern' failed!"
        exit 1
    fi
}

# Show test coverage (if grcov is available)
show_coverage() {
    print_status "Checking test coverage..."
    
    if command -v grcov &> /dev/null; then
        print_status "Generating coverage report..."
        
        # Clean previous coverage data
        rm -rf target/debug/coverage
        
        # Run tests with coverage
        CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='target/debug/coverage/cargo-test-%p-%m.profraw' cargo test --tests --no-default-features
        
        # Generate coverage report
        grcov . --binary-path ./target/debug/ -s . -t html --branch --ignore-not-existing -o ./target/debug/coverage/
        
        print_success "Coverage report generated at target/debug/coverage/index.html"
    else
        print_warning "grcov not found. Install it with: cargo install grcov"
        print_status "Alternative: Install cargo-tarpaulin for coverage: cargo install cargo-tarpaulin"
    fi
}

# Show help
show_help() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  unit              Run only unit tests"
    echo "  integration       Run only integration tests"
    echo "  all               Run all tests (default)"
    echo "  verbose           Run tests with verbose output"
    echo "  coverage          Show test coverage"
    echo "  feature <name>    Run tests with specific feature"
    echo "  pattern <regex>   Run tests matching pattern"
    echo "  check             Check if ClickHouse is available"
    echo "  help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run all tests"
    echo "  $0 unit              # Run only unit tests"
    echo "  $0 integration       # Run only integration tests"
    echo "  $0 verbose           # Run tests with verbose output"
    echo "  $0 feature tls       # Run tests with TLS feature"
    echo "  $0 pattern 'block'   # Run tests containing 'block'"
    echo "  $0 coverage          # Generate coverage report"
}

# Main execution
main() {
    case "${1:-all}" in
        "unit")
            run_unit_tests
            ;;
        "integration")
            run_integration_tests
            ;;
        "all")
            check_clickhouse
            run_all_tests
            ;;
        "verbose")
            run_verbose_tests
            ;;
        "coverage")
            show_coverage
            ;;
        "feature")
            if [ -z "$2" ]; then
                print_error "Feature name is required"
                echo "Usage: $0 feature <feature_name>"
                exit 1
            fi
            run_feature_tests "$2"
            ;;
        "pattern")
            if [ -z "$2" ]; then
                print_error "Pattern is required"
                echo "Usage: $0 pattern <regex_pattern>"
                exit 1
            fi
            run_pattern_tests "$2"
            ;;
        "check")
            check_clickhouse
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
