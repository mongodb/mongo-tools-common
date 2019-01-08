#!/bin/bash
set -o errexit
set -o pipefail

golint $(go list ./... | sed -e 's!github.com/mongodb/mongo-tools-common!.!') \
  | grep -v 'should have comment' \
  | grep -v 'comment on exported' \
  | grep -v 'Id.*should be.*ID' \
  | perl -nle 'push @errs, $_; print $_ if $_ } END { exit 1 if @errs'
