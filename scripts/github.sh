#!/bin/bash
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
./github --github-org=cncf --github-repo=devstats --github-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --github-tokens="`cat ./secrets/tokens.secret`" --github-categories='repository,issue,pull_request' --github-cache-path='/tmp/github-users-cache' $*
