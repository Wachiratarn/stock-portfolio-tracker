#!/bin/bash
set -e

echo "Building Lambda layer..."
rm -rf layer lambda-layer.zip
mkdir -p layer/python

pip install \
  yfinance \
  pandas \
  pyarrow \
  -t layer/python \
  --platform manylinux2014_x86_64 \
  --python-version 3.12 \
  --only-binary=:all: \
  --quiet

cd layer && zip -r ../lambda-layer.zip python/
echo "Done: lambda-layer.zip ($(du -sh ../lambda-layer.zip | cut -f1))"
