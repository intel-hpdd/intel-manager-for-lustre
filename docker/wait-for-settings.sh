#!/bin/bash

set -e

until [ -f /var/lib/chroma/iml-settings.conf ]; do
  echo "Waiting for settings."
  sleep 1
done

TMP=$PROXY_HOST

set -a
source /var/lib/chroma/iml-settings.conf
set +a


if [[ ! -z "$TMP" ]]; then
  export PROXY_HOST=$TMP
fi

echo "Starting service."
exec $@