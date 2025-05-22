#!/usr/bin/env bash
set -euo pipefail

BIN_DIR=bin
LOG_DIR=${LOG_DIR:-logs}
CONFIG=config.yaml

mkdir -p "$LOG_DIR"

enable_cleanup=true
cleanup() {
  if [[ "$enable_cleanup" = true ]]; then
    rm -f input_*.dat sorted_*.dat input_all.dat sorted_all.dat ref.txt sorted_all.txt
    rm -rf "$LOG_DIR"/*
  fi
}
trap cleanup EXIT

generate_config() {
  local num_nodes=$1
  local config_file=$2
  cat > "$config_file" <<EOF
nodes:
EOF
  for i in $(seq 0 $((num_nodes-1))); do
    cat >> "$config_file" <<EOF
  - nodeID: $i
    host: "localhost"
    port: $((8000 + i))
EOF
  done
}

test_single() {
  local desc="$1"
  local gen_arg="$2"
  local case_dir="$LOG_DIR/single_${desc// /_}"
  local config_file="$case_dir/config.yaml"
  mkdir -p "$case_dir"

  echo "===== Single Node Test: $desc ====="
  rm -f input_0.dat sorted_0.dat

  generate_config 1 "$config_file"

  "$BIN_DIR/gensort" "$gen_arg" input_0.dat

  "$BIN_DIR/globesort" 0 input_0.dat sorted_0.dat "$config_file" &>"$case_dir/node_0.log";

  if [[ -f sorted_0.dat ]]; then
    echo "Node 0: + output exists"
    "$BIN_DIR/showsort" sorted_0.dat | sort > sorted_all.txt
    "$BIN_DIR/showsort" input_0.dat  | sort > ref.txt
    if diff -u ref.txt sorted_all.txt; then
      echo "Node 0: + result is correct"
    else
      echo "Node 0: - result is incorrect"
    fi
  else
    echo "Node 0: - output does not exist"
  fi
  echo
}

test_multinode() {
  local num_nodes=$1
  local gen_args=("${@:2:${#}-2}")
  local desc="${@:$(($#))}"
  local case_dir="$LOG_DIR/multi_${desc// /_}"
  local config_file="$case_dir/config.yaml"
  mkdir -p "$case_dir"

  echo "===== Multi Node Test: $desc ====="

  generate_config $num_nodes "$config_file"
  rm -f input_*.dat sorted_*.dat

  for ((i=0; i<num_nodes; i++)); do
    "$BIN_DIR/gensort" "${gen_args[i]}" "input_${i}.dat"
  done

  enable_cleanup=false
  for ((i=0; i<num_nodes; i++)); do
    nohup "$BIN_DIR/globesort" "$i" "input_${i}.dat" "sorted_${i}.dat" "$config_file" \
      >"$case_dir/node_${i}.log" 2>&1 &
  done
  wait
  enable_cleanup=true

  cat input_*.dat > input_all.dat
  "$BIN_DIR/showsort" input_all.dat | sort > ref.txt
  cat sorted_*.dat > sorted_all.dat
  "$BIN_DIR/showsort" sorted_all.dat | sort > sorted_all.txt

  for ((i=0; i<num_nodes; i++)); do
    echo "Node $i:"
    if [[ -f sorted_${i}.dat ]]; then
      echo "- output exists"
      if grep -qE "^${i}" sorted_${i}.dat; then
        echo "- contains correct node IDs"
      else
        echo "- contains wrong records"
      fi
    else
      echo "- output does not exist"
    fi
  done

  echo "Global compare:"
  if diff -u ref.txt sorted_all.txt; then
    echo "+ all nodes result is correct"
  else
    echo "- mismatch with reference"
  fi
  echo
}

# Test cases
test_single "single-node single-record" 1
test_single "single-node 10-record" 10
test_single "single-node 10MiB" 10mb
test_multinode 2 1mb 0 "2-node single 1MiB input"
# test_multinode 2 1mb 1mb "2-node single 1MiB all to node0"
test_multinode 2 10mb 0 "2-node single 10MiB input"
test_multinode 2 10mb 10mb "2-node two 10MiB inputs"
test_multinode 8 1mb 0 0 0 0 0 0 0 "8-node single 1MiB input"
test_multinode 8 10mb 0 0 0 0 0 0 0 "8-node single 10MiB input"
test_multinode 8 10mb 10mb 10mb 10mb 10mb 10mb 10mb 10mb "8-node eight 10MiB inputs"