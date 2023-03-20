#!/bin/bash

echo "Replacing airbyte/source-parsersvc-connector:$(git rev-parse --short HEAD)"
git pull
docker build . -t airbyte/source-parsersvc-connector:$(git rev-parse --short HEAD)
echo "New container ready: airbyte/source-parsersvc-connector:$(git rev-parse --short HEAD)"
