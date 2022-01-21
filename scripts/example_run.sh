#!/bin/bash
clear; GITHUB_TAGS="c,d,e" ./scripts/github.sh --github-date-from "2019-01" --github-pack-size=500 --github-project=DevStats --github-debug=2 --github-categories='repository,issue,pull_request' --github-tokens="`cat ./secrets/tokens.secret`" --github-stream=''
