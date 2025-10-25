#!/bin/bash
set -e # Salir inmediatamente si un comando falla

mensaje=${1:-"Actualización"}
branch=$(git rev-parse --abbrev-ref HEAD)
git add .
git commit -m "$mensaje"
git push origin "$branch"
