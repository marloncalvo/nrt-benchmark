#!/bin/bash
set -e

mvn package
docker compose -f docker-compose.yml -f docker-compose.run.yml up -d --build
