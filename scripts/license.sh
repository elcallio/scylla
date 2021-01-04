#!/bin/bash

#
git ls-files \
    | xargs ls -Fd \
    | grep -vE '\.db$|\.crc32$' \
    | grep -v '^scripts/license.awk$' \
    | grep -v '[/@]$' \
    | sed 's/\*$//' \
    | xargs file --mime-type --no-pad  | sed -n '/: text/ { s/: text.*$//; p}' \
    | xargs awk -i inplace -f scripts/license.awk
