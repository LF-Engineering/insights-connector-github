#!/bin/bash
clear; ./scripts/github.sh --github-debug=2 --github-categories=repository --github-tokens="`cat ./secrets/token.secret`"
