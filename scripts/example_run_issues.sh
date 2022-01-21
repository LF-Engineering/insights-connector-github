#!/bin/bash
# FIXME
# clear; ./scripts/github.sh --github-debug=1 --github-categories='issue' --github-tokens="`cat ./secrets/tokens.secret` --github-stream=''"
clear; ./scripts/github.sh --github-debug=0 --github-categories='issue' --github-tokens="`cat ./secrets/token.secret`" --github-date-from=2021 --github-stream=''
