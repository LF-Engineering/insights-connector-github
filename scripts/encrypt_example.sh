#!/bin/bash
export AWS_REGION="`cat ./secrets/AWS_REGION.dev.secret`"
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.dev.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.dev.secret`"
export ENCRYPTION_KEY="`cat ./secrets/ENCRYPTION_KEY.dev.secret`"
export ENCRYPTION_BYTES="`cat ./secrets/ENCRYPTION_BYTES.dev.secret`"
export ESURL="`cat ./secrets/ES_URL.prod.secret`"
export STREAM=''
export GITHUB_NO_INCREMENTAL=1
#curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"GitHub:https://github.com/zowe/zebra pull_request"}}}' | jq -rS '.' || exit 1
#curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"GitHub:https://github.com/zowe/zebra"}}}' | jq -rS '.' || exit 2
# ./encrypt "`cat /etc/github/oauth`" > ./secrets/tokens.encrypted.secret || exit 3
# ./github --github-es-url="${ESURL}" --github-debug=0 --github-categories='pull_request' --github-tokens="`cat ./secrets/tokens.encrypted.secret`" --github-stream="${STREAM}" --github-org=lukaszgryglicki --github-repo=test-api 2>&1 | tee run.log
./encrypt "`cat /etc/github/oauths`" > ./secrets/tokens.encrypted.secret || exit 3
#./github --github-es-url="${ESURL}" --github-debug=0 --github-categories='issue,pull_request,repository' --github-tokens="`cat ./secrets/tokens.encrypted.secret`" --github-stream="${STREAM}" --github-org=cncf --github-repo=toc 2>&1 | tee run.log
time ./github --github-es-url="${ESURL}" --github-debug=0 --github-categories='pull_request' --github-tokens="`cat ./secrets/tokens.encrypted.secret`" --github-stream="${STREAM}" --github-org=materialx --github-repo=MaterialX 2>&1 | tee run.log
