#!/bin/bash
# Script de inicio para Uvicorn en Render
# $PORT es una variable de entorno proporcionada por Render

echo "Starting Uvicorn server..."
exec uvicorn main:app --host 0.0.0.0 --port $PORT