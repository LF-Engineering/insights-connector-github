#!/bin/bash
# FIXME
# clear; ./scripts/github.sh --github-debug=1 --github-categories='issue' --github-tokens="`cat ./secrets/tokens.secret`"
clear; ./scripts/github.sh --github-debug=2 --github-categories='issue,repository' --github-tokens="`cat ./secrets/token.secret`"
