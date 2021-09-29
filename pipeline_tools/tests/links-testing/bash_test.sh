#!/bin/bash

x=()

jq -nc '$ARGS.positional' --args ${x[@]} > foo.json