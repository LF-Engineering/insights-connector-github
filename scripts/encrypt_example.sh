#!/bin/bash
export AWS_REGION="`cat ./secrets/AWS_REGION.dev.secret`"
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.dev.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.dev.secret`"
export ENCRYPTION_KEY="`cat ./secrets/ENCRYPTION_KEY.dev.secret`"
export ENCRYPTION_BYTES="`cat ./secrets/ENCRYPTION_BYTES.dev.secret`"
export ESURL="`cat ./secrets/ES_URL.prod.secret`"
export STREAM=''
curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"GitHub:https://github.com/magma/magma-website pull_request"}}}' | jq -rS '.' || exit 1
curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"GitHub:https://github.com/magma/magma-website"}}}' | jq -rS '.' || exit 2
./encrypt "`cat /etc/github/oauths`" > ./secrets/tokens.encrypted.secret || exit 3
./github --github-es-url="${ESURL}" --github-debug=0 --github-categories='pull_request' --github-tokens="`cat ./secrets/tokens.encrypted.secret`" --github-stream="${STREAM}" --github-org=magma --github-repo=magma-website
# ./github --github-es-url="${ESURL}" --github-debug=0 --github-categories='pull_request' --github-tokens="`cat ./secrets/tokens.encrypted.secret`" --github-date-from=2020-10-25 --github-date-to=2020-10-27 --github-stream="${STREAM}" --github-org=magma --github-repo=magma-website
