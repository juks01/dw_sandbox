#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
    cat <<'EOF'
Usage: ./mock_source/dev.sh <up|mutate|down> [options]

  up       Build/start the mock services and initialize data if empty.
           Optional arguments are passed to `init` (for example --seed 7).
  mutate   Update/add rows. Optional generator arguments follow `mutate`.
  down     Remove mock containers, data volume, and local API image.
EOF
}

action="${1:-}"
if [[ -z "$action" || "$action" == "help" || "$action" == "--help" ]]; then
    usage
    exit 0
fi
shift

case "$action" in
    up|mutate|down) ;;
    *)
        usage >&2
        exit 2
        ;;
esac

if [[ "$action" != "down" && ! -f ../.env ]]; then
    printf 'Missing .env. From the repository root, run: cp .env-template .env\n' >&2
    exit 1
fi

compose=(podman compose)
if [[ -f ../.env ]]; then
    compose+=(--env-file ../.env)
fi

case "$action" in
    up)
        network_name="${DW_NETWORK_NAME:-}"
        if [[ -z "$network_name" ]]; then
            network_name="$(sed -n 's/^DW_NETWORK_NAME=//p' ../.env | tail -n 1)"
        fi
        network_name="${network_name:-dw-dew_dw}"
        if ! podman network exists "$network_name"; then
            printf 'Creating shared Compose network %s...\n' "$network_name"
            podman network create "$network_name" >/dev/null
        fi

        printf 'Building and starting mock-source services...\n'
        "${compose[@]}" up -d --build --wait --wait-timeout 120
        printf 'Initializing mock data (the large datasets may take a while)...\n'
        "${compose[@]}" run --rm --no-deps mock-source-api \
            python -m app.generate init --if-empty "$@"
        ;;
    mutate)
        "${compose[@]}" run --rm --no-deps mock-source-api \
            python -m app.generate mutate "$@"
        ;;
    down)
        "${compose[@]}" down --volumes --rmi local
        ;;
esac
