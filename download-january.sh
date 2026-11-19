#!/usr/bin/env bash

cd -- "$(dirname -- "${BASH_SOURCE[0]}")" || exit 1
python=python3
if [[ -x .venv/bin/python ]]; then
  python=.venv/bin/python
fi

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  exec "$python" -m src.ingest "$@"
fi

if [[ -z ${FIRMS_MAP_KEY:-} ]]; then
  if [[ ! -r ./.env ]]; then
    printf '%s\n' 'Set FIRMS_MAP_KEY or create a readable .env beside this script.' >&2
    exit 1
  fi
  source ./.env || exit 1
  FIRMS_MAP_KEY=${FIRMS_MAP_KEY:-${FIRMS_API_KEY:-${MAP_KEY:-}}}
fi

: "${FIRMS_MAP_KEY:?Set FIRMS_MAP_KEY, FIRMS_API_KEY, or MAP_KEY in .env}"
export FIRMS_MAP_KEY
exec "$python" -m src.ingest "$@"
