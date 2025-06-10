#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <CRAWL_ID> (e.g. CC-MAIN-2017-13)"
    exit 1
fi

CRAWL="$1"
