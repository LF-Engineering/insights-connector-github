#!/bin/bash
# example run: GITHUB_STREAM=xyz ./scripts/git.sh
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
# AWSENV=prod|test|dev
if [ -z "${AWSENV}" ]
then
  AWSENV=dev
fi
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.${AWSENV}.secret`"
export AWS_REGION="`cat ./secrets/AWS_REGION.${AWSENV}.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.${AWSENV}.secret`"
# Other example args:
# --github-org=cncf
# --github-repo=devstats
# --github-cache-path='/tmp/github-users-cache'
./github --git-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --github-stream='' $*
