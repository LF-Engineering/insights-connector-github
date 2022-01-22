#!/bin/bash
./github --github-org kubernetes --github-repo kubernetes --github-tokens=`cat /etc/github/oauths` --github-categories='irepository,issue,pull_request' --github-stream='' 2>&1 | tee -a kk.log
