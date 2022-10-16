#!/bin/bash

curl $1 > $2
zstdcat --memory=2048MB $2 | jq -r '. | [.id, .subreddit, .created_utc, .body] | @tsv' > $3
