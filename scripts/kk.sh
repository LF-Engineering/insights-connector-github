#!/bin/bash
time ./github --github-org kubernetes --github-repo kubernetes --github-tokens=`cat /etc/github/oauths` --github-categories='repository,issue,pull_request' --github-stream='' 2>&1 | tee -a kk.log
# time ./github --github-org kubernetes --github-repo test-infra --github-tokens=`cat /etc/github/oauths` --github-categories='repository,issue' --github-debug=1 --github-stream='' 2>&1 | tee -a kk.log
