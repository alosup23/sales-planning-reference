#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  echo "usage: $0 <image-tag>"
  exit 1
fi

IMAGE_TAG="$1"
REPOSITORY_URI="427304877733.dkr.ecr.ap-southeast-5.amazonaws.com/sales-planning-demo-api"

export DOTNET_ROOT="/Users/aloysius/Documents/New project/.dotnet8"

"$DOTNET_ROOT/dotnet" publish \
  "/Users/aloysius/Documents/New project/apps/api/src/SalesPlanning.Api/SalesPlanning.Api.csproj" \
  -c Release \
  -r linux-x64 \
  -t:PublishContainer \
  -p:ContainerRuntimeIdentifier=linux-x64 \
  -p:ContainerRegistry="${REPOSITORY_URI%/sales-planning-demo-api}" \
  -p:ContainerRepository="sales-planning-demo-api" \
  -p:ContainerImageTag="$IMAGE_TAG"
