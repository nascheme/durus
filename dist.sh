#!/bin/sh
# Generate source distribution package, using clean git checkout
# This ensures no spurious files or changes are included in package.
set -e
TMP=$(mktemp -d --tmpdir=.)
git archive master | tar -x -C $TMP
./git-changelog > $TMP/CHANGES.txt
(cd $TMP && python setup.py sdist)
cp -v $TMP/dist/* dist
rm -r $TMP
