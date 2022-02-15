#!/bin/bash
export AWS_REGION="`cat ./secrets/AWS_REGION.dev.secret`"
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.dev.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.dev.secret`"
export ENCRYPTION_KEY=encryption-key-name
export ENCRYPTION_BYTES=encryption-bytes
./encrypt "`cat /etc/github/oauths`" > ./secrets/tokens.encrypted.secret || exit 1
./github --github-es-url="`cat ./secrets/ES_URL.prod.secret`" --github-debug=1 --github-categories='pull_request' --github-tokens="`cat ./secrets/tokens.encrypted.secret`" --github-date-from=2020-10-25 --github-date-to=2020-10-27 --github-stream='' --github-org=magma --github-repo=magma-website
