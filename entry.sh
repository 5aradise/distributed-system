#!/usr/bin/env sh

bin=$1
shift

if [ -z "$bin" ]; then
  echo "binary is not defined"
  exit 1
fi

# shellcheck disable=SC2068
exec ./"$bin" $@
