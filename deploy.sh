#!/bin/bash
echo "🚀 Running database migrations..."
alembic upgrade head

echo "✅ Starting application..."
uvicorn main:app --host 0.0.0.0 --port $PORT
