#!/bin/bash
clear; ./github --github-org=kubernetes --github-repo=test-infra --github-es-url="`cat ./secrets/ES_URL.test.secret`" --github-cache-path='/tmp/github-users-cache' --github-debug=0 --github-categories='repository,issue,pull_request' --github-tokens="`cat ./secrets/tokens.secret`" --github-date-from=2015-01-01 --github-ncpus=16 | tee run.log
