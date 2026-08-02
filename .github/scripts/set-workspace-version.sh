#!/usr/bin/env bash
# Rewrites the workspace version and refreshes Cargo.lock, then asserts the
# result. Called twice by the publish workflow: once on the tagged checkout
# before publishing, once on main to record the released version.
#
# Usage: set-workspace-version.sh <version>
set -euo pipefail

VERSION="${1:?usage: set-workspace-version.sh <version>}"

# Version is centralized in [workspace.package]; both crates inherit it via
# `version.workspace = true`. Written as `sed > tmp && mv` rather than `sed -i`,
# whose syntax differs between GNU and BSD, so the command exercised locally on
# macOS is the one that runs on the Ubuntu runner.
sed -E '/^\[workspace\.package\]/,/^\[workspace\.dependencies\]/ s/^version = "[^"]*"/version = "'"$VERSION"'"/' \
  Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml

# The internal pin must move in lockstep: sqlx-gen depends on sqlx-gen-macros by
# version and both crates are always released together, so leaving it behind
# would publish a sqlx-gen requiring a macros version that never shipped.
sed -E 's|^(sqlx-gen-macros = \{ path = "crates/sqlx_gen_macros", version = )"[^"]*"|\1"'"$VERSION"'"|' \
  Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml

cargo update --workspace

# Neither sed fails loudly if its pattern stops matching, so assert on the
# resolved metadata: without this, a drifted manifest layout would silently
# republish the previous version.
failed=0
for pkg in sqlx-gen sqlx-gen-macros; do
  got=$(cargo metadata --no-deps --format-version 1 \
    | jq -r --arg p "$pkg" '.packages[] | select(.name==$p) | .version')
  echo "$pkg: $got"
  if [ "$got" != "$VERSION" ]; then
    echo "::error::$pkg is at $got, expected $VERSION"
    failed=1
  fi
done

# Both package versions above are inherited from [workspace.package], so they
# would still read correct if only the internal pin stopped being rewritten.
# That drift is invisible to the loop and must be asserted separately.
req=$(cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.name=="sqlx-gen") | .dependencies[]
           | select(.name=="sqlx-gen-macros") | .req')
echo "sqlx-gen -> sqlx-gen-macros: $req"
if [ "$req" != "^$VERSION" ]; then
  echo "::error::sqlx-gen requires sqlx-gen-macros $req, expected ^$VERSION"
  failed=1
fi

exit $failed
