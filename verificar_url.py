"""
Script para verificar DATABASE_URL en Railway
"""
import os
from urllib.parse import urlparse

# Simular carga de .env
from dotenv import load_dotenv
load_dotenv()

database_url = os.getenv('DATABASE_URL')

print("=" * 60)
print("🔍 VERIFICACIÓN DE DATABASE_URL")
print("=" * 60)
print()

if not database_url:
    print("❌ DATABASE_URL no encontrada")
else:
    print(f"✅ DATABASE_URL encontrada")
    print(f"📝 Valor completo:")
    print(f"   {database_url}")
    print()

    # Parsear URL
    url = urlparse(database_url)

    print("🔍 Componentes parseados:")
    print(f"   Scheme:   {url.scheme}")
    print(f"   Hostname: {url.hostname}")
    print(f"   Port:     {url.port or 5432}")
    print(f"   Database: {url.path[1:] if url.path else 'N/A'}")
    print(f"   User:     {url.username}")
    print(f"   Password: {'*' * len(url.password) if url.password else 'N/A'}")
    print()

    if not url.hostname:
        print("🚨 PROBLEMA: hostname es None")
        print("   La URL no tiene un host válido")
    else:
        print("✅ Hostname válido detectado")

print("=" * 60)
