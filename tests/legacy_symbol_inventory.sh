#!/usr/bin/env bash
set -euo pipefail

repo_root="${1:-.}"

# Define patterns using token concatenation to avoid literal self-matches during final sweeps.
token_db="DB"
token_open="Open"
token_sync="Sync"
token_get="Get"
token_release="Release"
token_snapshot="Snapshot"
token_read_options="ReadOptions"
token_field="snapshot"
token_const="const"

pat_db_open="${token_db}::${token_open}"
pat_sync_db="${token_sync}${token_db}\\s*\\("
pat_get_snapshot="${token_get}${token_snapshot}\\s*\\("
pat_release_snapshot="${token_release}${token_snapshot}\\s*\\("
pat_read_options_field="${token_read_options}::${token_field}"
pat_const_snapshot="${token_const} ${token_snapshot}\\*"

regex="\\b${pat_db_open}\\b|${pat_sync_db}|${pat_get_snapshot}|${pat_release_snapshot}|${pat_read_options_field}|${pat_const_snapshot}"
git -C "$repo_root" grep -nE "$regex" include src tests benchmark docs
