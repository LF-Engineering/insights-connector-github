#!/bin/bash
clear; ./scripts/github.sh --github-debug=2 --github-categories='issue,repository' --github-tokens="`cat ./secrets/tokens.secret`"
