#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

mkdir -p $script_dir/../test_input

$script_dir/../gensort/64/gensort -a 500000 $script_dir/../test_input/test2.txt