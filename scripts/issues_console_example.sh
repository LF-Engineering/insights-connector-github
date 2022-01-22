#!/bin/bash
# ./github --github-org cncf --github-repo devstatscode --github-tokens=`cat /etc/github/oauth` --github-categories='issue' --github-debug=2 --github-stream=''
./github --github-org cncf --github-repo toc --github-tokens=`cat /etc/github/oauths` --github-categories='issue' --github-date-from "2022-01"
