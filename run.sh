#!/bin/bash
set -e

mvn package
mvn -Pspark package
mvn -Pflink package
docker compose -f docker-compose.yml -f docker-compose.run.yml up -d --build
