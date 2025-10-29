#!/usr/bin/env python3
"""
Instala psycopg si no está disponible
"""

import subprocess
import sys

print("🔧 Verificando e instalando dependencias...")
print()

try:
    import psycopg
    print("✅ psycopg ya está instalado")
except ImportError:
    print("📦 Instalando psycopg...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg[binary]", "--break-system-packages"])
    print("✅ psycopg instalado correctamente")

print()
print("✅ Todo listo para ejecutar el diagnóstico")
