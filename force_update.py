# force_update.py
"""
Script para forzar actualización de módulos en Railway
Cambia un timestamp para que Railway detecte cambios
"""

import sys
from datetime import datetime

# Este comentario cambia en cada ejecución para forzar rebuild
# Última actualización: 2025-10-27 19:15:00

print(f"🔄 Force update ejecutado: {datetime.now()}")
print("✅ Este archivo fuerza a Railway a recompilar todos los módulos")

if __name__ == "__main__":
    sys.exit(0)
