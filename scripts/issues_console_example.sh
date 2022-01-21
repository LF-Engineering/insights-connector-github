#!/bin/bash
./github --github-org cncf --github-repo devstatscode --github-tokens=`cat /etc/github/oauth` --github-categories='issue' --github-debug=2 --github-stream=''
