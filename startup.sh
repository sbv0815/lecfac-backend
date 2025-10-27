#!/bin/bash
# startup.sh - Limpia caché de Python antes de iniciar

echo "🧹 Limpiando caché de Python..."

# Eliminar TODOS los archivos .pyc y carpetas __pycache__
find . -type f -name '*.pyc' -delete
find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true

echo "✅ Caché eliminado"

# Listar módulos de matching
echo "📋 Módulos de matching disponibles:"
ls -la | grep -E "product|matching"

echo "🚀 Iniciando servidor..."

# Iniciar servidor
python main.py
