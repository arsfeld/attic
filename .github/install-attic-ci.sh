#!/usr/bin/env bash
set -euo pipefail

# Install attic-client from nixpkgs
nix profile install nixpkgs#attic-client

echo "Attic installed successfully"
