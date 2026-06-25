#!/usr/bin/env bash
# Build the WASM bindings into web/pkg. Run inside `nix develop`.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build -p interstellar-wasm --target wasm32-unknown-unknown --release

wasm-bindgen \
  --target web \
  --out-dir web/pkg \
  --no-typescript \
  target/wasm32-unknown-unknown/release/interstellar_wasm.wasm

echo "built -> web/pkg/"
