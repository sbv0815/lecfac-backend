"""
============================================================================
INSTALADOR DE SISTEMA DE APRENDIZAJE AUTOMÁTICO V2.0
============================================================================
Crea las 4 tablas necesarias para el sistema de aprendizaje:
- correcciones_aprendidas
- validaciones_pendientes_usuario
- historial_validaciones
- productos_revision_admin

✅ Compatible con PostgreSQL Y SQLite
✅ Lee variables de entorno (.env)
✅ Seguro: IF NOT EXISTS
============================================================================
"""

import os
import sys

# ============================================
# CARGAR VARIABLES DE ENTORNO
# ============================================
from dotenv import load_dotenv
load_dotenv()

print(f"🔍 DATABASE_TYPE: {os.getenv('DATABASE_TYPE', 'sqlite')}")
print(f"🔍 DATABASE_URL configurada: {'SÍ' if os.getenv('DATABASE_URL') else 'NO'}")

from database import get_db_connection

def crear_tablas_aprendizaje():
    """
    Crea las 4 tablas del sistema de aprendizaje
    Compatible con PostgreSQL y SQLite
    """

    print("=" * 80)
    print("🚀 INSTALADOR DE SISTEMA DE APRENDIZAJE V2.0")
    print("=" * 80)
    print("Este script creará 4 tablas nuevas:")
    print("  • correcciones_aprendidas")
    print("  • validaciones_pendientes_usuario")
    print("  • historial_validaciones")
    print("  • productos_revision_admin")
    print("⚠️  SEGURO: Usa IF NOT EXISTS - No borra datos")
    print("")

    # Conectar
    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return False

    cursor = conn.cursor()
    database_type = os.getenv('DATABASE_TYPE', 'sqlite').lower()

    try:
        print("📦 CREANDO TABLAS...")

        # ============================================
        # 1. CORRECCIONES_APRENDIDAS
        # ============================================
        print("\n1️⃣ correcciones_aprendidas")

        if database_type == "postgresql":
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS correcciones_aprendidas (
                    id SERIAL PRIMARY KEY,
                    ocr_original TEXT NOT NULL,
                    ocr_normalizado TEXT NOT NULL,
                    nombre_validado TEXT NOT NULL,
                    establecimiento VARCHAR(100),
                    confianza DECIMAL(3, 2) DEFAULT 0.70,
                    veces_confirmado INTEGER DEFAULT 0,
                    veces_rechazado INTEGER DEFAULT 0,
                    fecha_primera_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_ultima_confirmacion TIMESTAMP,
                    activo BOOLEAN DEFAULT TRUE,

                    UNIQUE(ocr_normalizado, establecimiento)
                )
            """)
        else:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS correcciones_aprendidas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ocr_original TEXT NOT NULL,
                    ocr_normalizado TEXT NOT NULL,
                    nombre_validado TEXT NOT NULL,
                    establecimiento TEXT,
                    confianza REAL DEFAULT 0.70,
                    veces_confirmado INTEGER DEFAULT 0,
                    veces_rechazado INTEGER DEFAULT 0,
                    fecha_primera_vez DATETIME DEFAULT CURRENT_TIMESTAMP,
                    fecha_ultima_confirmacion DATETIME,
                    activo INTEGER DEFAULT 1,

                    UNIQUE(ocr_normalizado, establecimiento)
                )
            """)

        conn.commit()
        print("   ✅ Creada")

        # ============================================
        # 2. VALIDACIONES_PENDIENTES_USUARIO
        # ============================================
        print("\n2️⃣ validaciones_pendientes_usuario")

        if database_type == "postgresql":
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS validaciones_pendientes_usuario (
                    id SERIAL PRIMARY KEY,
                    usuario_id INTEGER NOT NULL,
                    factura_id INTEGER NOT NULL,
                    item_factura_id INTEGER,
                    nombre_ocr TEXT,
                    nombre_sugerido TEXT NOT NULL,
                    nivel_confianza DECIMAL(3, 2),
                    motivo_duda TEXT,
                    estado VARCHAR(20) DEFAULT 'pendiente',
                    respuesta_usuario TEXT,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_respuesta TIMESTAMP,

                    CHECK (estado IN ('pendiente', 'confirmado', 'corregido', 'ignorado'))
                )
            """)
        else:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS validaciones_pendientes_usuario (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario_id INTEGER NOT NULL,
                    factura_id INTEGER NOT NULL,
                    item_factura_id INTEGER,
                    nombre_ocr TEXT,
                    nombre_sugerido TEXT NOT NULL,
                    nivel_confianza REAL,
                    motivo_duda TEXT,
                    estado TEXT DEFAULT 'pendiente',
                    respuesta_usuario TEXT,
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    fecha_respuesta DATETIME,

                    CHECK (estado IN ('pendiente', 'confirmado', 'corregido', 'ignorado'))
                )
            """)

        conn.commit()
        print("   ✅ Creada")

        # ============================================
        # 3. HISTORIAL_VALIDACIONES
        # ============================================
        print("\n3️⃣ historial_validaciones")

        if database_type == "postgresql":
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historial_validaciones (
                    id SERIAL PRIMARY KEY,
                    factura_id INTEGER NOT NULL,
                    item_factura_id INTEGER,
                    ocr_original TEXT,
                    nombre_python TEXT,
                    nombre_perplexity TEXT,
                    nombre_final TEXT NOT NULL,
                    tuvo_correccion_python BOOLEAN DEFAULT FALSE,
                    fue_validado_perplexity BOOLEAN DEFAULT FALSE,
                    confianza_final DECIMAL(3, 2),
                    fuente_final VARCHAR(20),
                    tiempo_procesamiento_ms INTEGER,
                    costo_perplexity DECIMAL(6, 4),
                    fecha_procesamiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    CHECK (fuente_final IN ('python', 'aprendizaje', 'perplexity', 'usuario', 'admin'))
                )
            """)
        else:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historial_validaciones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    factura_id INTEGER NOT NULL,
                    item_factura_id INTEGER,
                    ocr_original TEXT,
                    nombre_python TEXT,
                    nombre_perplexity TEXT,
                    nombre_final TEXT NOT NULL,
                    tuvo_correccion_python INTEGER DEFAULT 0,
                    fue_validado_perplexity INTEGER DEFAULT 0,
                    confianza_final REAL,
                    fuente_final TEXT,
                    tiempo_procesamiento_ms INTEGER,
                    costo_perplexity REAL,
                    fecha_procesamiento DATETIME DEFAULT CURRENT_TIMESTAMP,

                    CHECK (fuente_final IN ('python', 'aprendizaje', 'perplexity', 'usuario', 'admin'))
                )
            """)

        conn.commit()
        print("   ✅ Creada")

        # ============================================
        # 4. PRODUCTOS_REVISION_ADMIN
        # ============================================
        print("\n4️⃣ productos_revision_admin")

        if database_type == "postgresql":
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS productos_revision_admin (
                    id SERIAL PRIMARY KEY,
                    producto_maestro_id INTEGER,
                    motivo_revision TEXT NOT NULL,
                    detalles TEXT,
                    prioridad INTEGER DEFAULT 5,
                    estado VARCHAR(20) DEFAULT 'pendiente',
                    asignado_a INTEGER,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_revision TIMESTAMP,
                    notas_admin TEXT,

                    CHECK (prioridad >= 1 AND prioridad <= 10),
                    CHECK (estado IN ('pendiente', 'en_revision', 'corregido', 'descartado'))
                )
            """)
        else:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS productos_revision_admin (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_maestro_id INTEGER,
                    motivo_revision TEXT NOT NULL,
                    detalles TEXT,
                    prioridad INTEGER DEFAULT 5,
                    estado TEXT DEFAULT 'pendiente',
                    asignado_a INTEGER,
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    fecha_revision DATETIME,
                    notas_admin TEXT,

                    CHECK (prioridad >= 1 AND prioridad <= 10),
                    CHECK (estado IN ('pendiente', 'en_revision', 'corregido', 'descartado'))
                )
            """)

        conn.commit()
        print("   ✅ Creada")

        # ============================================
        # CREAR ÍNDICES (solo PostgreSQL)
        # ============================================
        if database_type == "postgresql":
            print("\n📊 Creando índices...")

            indices = [
                "CREATE INDEX IF NOT EXISTS idx_correcciones_normalizado ON correcciones_aprendidas(ocr_normalizado, establecimiento)",
                "CREATE INDEX IF NOT EXISTS idx_validaciones_usuario ON validaciones_pendientes_usuario(usuario_id, estado)",
                "CREATE INDEX IF NOT EXISTS idx_historial_factura ON historial_validaciones(factura_id)",
                "CREATE INDEX IF NOT EXISTS idx_revision_estado ON productos_revision_admin(estado, prioridad DESC)",
            ]

            for idx_sql in indices:
                try:
                    cursor.execute(idx_sql)
                    conn.commit()
                except Exception as e:
                    print(f"   ⚠️ Índice: {e}")

            print("   ✅ Índices creados")

        cursor.close()
        conn.close()

        print("\n" + "=" * 80)
        print("✅ INSTALACIÓN COMPLETADA")
        print("=" * 80)
        print("🎉 Sistema de aprendizaje listo")
        print("🚀 Próximos pasos:")
        print("   1. Reinicia tu aplicación")
        print("   2. Prueba normalización masiva")
        print("   3. El sistema aprenderá automáticamente")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\n❌ Error creando tablas: {e}")
        import traceback
        traceback.print_exc()

        if conn:
            conn.rollback()
            cursor.close()
            conn.close()

        return False


if __name__ == "__main__":
    crear_tablas_aprendizaje()
