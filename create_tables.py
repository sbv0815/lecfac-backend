#!/usr/bin/env python3
"""
Script para crear todas las tablas necesarias en la base de datos
"""

import os
import sys

# Asegurarse de que podemos importar database.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import create_tables, test_database_connection

def main():
    print("=" * 50)
    print("🚀 INICIANDO CREACIÓN DE TABLAS")
    print("=" * 50)
    
    # Verificar variables de entorno
    print("\n📋 Verificando configuración:")
    print(f"DATABASE_TYPE: {os.environ.get('DATABASE_TYPE', 'No configurado')}")
    print(f"DATABASE_URL: {'Configurado' if os.environ.get('DATABASE_URL') else 'No configurado'}")
    
    # Probar conexión
    print("\n🔍 Probando conexión a base de datos...")
    if test_database_connection():
        print("✅ Conexión exitosa")
    else:
        print("❌ Error de conexión")
        return 1
    
    # Crear tablas
    print("\n📊 Creando tablas...")
    try:
        create_tables()
        print("✅ Tablas creadas exitosamente")
        return 0
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    print("\n" + "=" * 50)
    if exit_code == 0:
        print("✅ PROCESO COMPLETADO EXITOSAMENTE")
    else:
        print("❌ PROCESO COMPLETADO CON ERRORES")
    print("=" * 50)
    sys.exit(exit_code)
