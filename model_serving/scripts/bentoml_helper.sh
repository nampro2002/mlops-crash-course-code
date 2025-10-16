#!/bin/bash
set -euo pipefail

cmd=${1-}

usage() {
    cat <<'EOF'
bentoml_helper.sh <command> [options]
Available commands:
 serve          serve a bentoml service
Available options:
 --port=x       bentoml service's port
 --reload       auto reload bentoml service
EOF
}

if [[ -z "$cmd" ]]; then
    echo "Missing command"
    usage
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

serve() {
    cd "$PROJECT_ROOT/src"
    bentoml serve bentoml_service:svc "$@"
}

shift

case "$cmd" in
serve)
    serve "$@"
    ;;
*)
    echo "Unknown command: $cmd"
    usage
    exit 1
    ;;
esac
