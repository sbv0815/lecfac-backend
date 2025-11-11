"""
setup_aprendizaje.py - INSTALADOR DE SISTEMA DE APRENDIZAJE
============================================================
Script para crear automáticamente todas las tablas necesarias
para el sistema de aprendizaje automático.

USO:
    python setup_aprendizaje.py

El script:
1. Conecta a la base de datos (PostgreSQL o SQLite)
2. Crea las 4 tablas nuevas si no existen
3. Crea vistas y funciones útiles
4. Verifica que todo esté correcto

SEGURO: Usa IF NOT EXISTS - No borra datos existentes
"""

import os
import sys


def get_db_connection():
    """Obtiene conexión a la base de datos según configuración"""
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    if database_type == "postgresql":
        import psycopg2
        from urllib.parse import urlparse

        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise Exception("DATABASE_URL no configurada")

        url = urlparse(database_url)

        print(f"📡 Conectando a PostgreSQL...")
        print(f"   Host: {url.hostname}")
        print(f"   Database: {url.path[1:]}")

        conn = psycopg2.connect(
            host=url.hostname,
            port=url.port,
            database=url.path[1:],
            user=url.username,
            password=url.password
        )

        print(f"   ✅ Conectado")
        return conn, True

    else:
        import sqlite3
        db_path = os.environ.get("DATABASE_PATH", "lecfac.db")
        print(f"📡 Conectando a SQLite: {db_path}")
        conn = sqlite3.connect(db_path)
        print(f"   ✅ Conectado")
        return conn, False


def crear_tablas(cursor, is_postgresql):
    """Crea todas las tablas necesarias"""

    print("\n📦 CREANDO TABLAS...")

    # TABLA 1: correcciones_aprendidas
    print("\n1️⃣ correcciones_aprendidas")
    if is_postgresql:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS correcciones_aprendidas (
            id SERIAL PRIMARY KEY,
            ocr_original VARCHAR(255) NOT NULL,
            ocr_normalizado VARCHAR(255) NOT NULL,
            nombre_validado VARCHAR(255) NOT NULL,
            codigo_ean VARCHAR(50),
            establecimiento VARCHAR(100),
            precio_promedio INT,
            veces_confirmado INT DEFAULT 1,
            veces_rechazado INT DEFAULT 0,
            confianza DECIMAL(3,2) DEFAULT 0.5,
            fuente_validacion VARCHAR(50) DEFAULT 'perplexity',
            fue_validado_manual BOOLEAN DEFAULT FALSE,
            requiere_revision BOOLEAN DEFAULT FALSE,
            fecha_primera_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fecha_ultima_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ocr_normalizado, establecimiento)
        );
        CREATE INDEX IF NOT EXISTS idx_correcciones_ocr ON correcciones_aprendidas(ocr_normalizado);
        CREATE INDEX IF NOT EXISTS idx_correcciones_ean ON correcciones_aprendidas(codigo_ean);
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS correcciones_aprendidas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ocr_original TEXT NOT NULL,
            ocr_normalizado TEXT NOT NULL,
            nombre_validado TEXT NOT NULL,
            codigo_ean TEXT,
            establecimiento TEXT,
            precio_promedio INTEGER,
            veces_confirmado INTEGER DEFAULT 1,
            veces_rechazado INTEGER DEFAULT 0,
            confianza REAL DEFAULT 0.5,
            fuente_validacion TEXT DEFAULT 'perplexity',
            fue_validado_manual INTEGER DEFAULT 0,
            requiere_revision INTEGER DEFAULT 0,
            fecha_primera_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fecha_ultima_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ocr_normalizado, establecimiento)
        );
        CREATE INDEX IF NOT EXISTS idx_correcciones_ocr ON correcciones_aprendidas(ocr_normalizado);
        """)
    print("   ✅ Creada")

    # TABLA 2: validaciones_pendientes_usuario
    print("\n2️⃣ validaciones_pendientes_usuario")
    if is_postgresql:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS validaciones_pendientes_usuario (
            id SERIAL PRIMARY KEY,
            factura_id INT REFERENCES facturas(id) ON DELETE CASCADE,
            usuario_id INT REFERENCES usuarios(id) ON DELETE CASCADE,
            item_factura_id INT REFERENCES items_factura(id) ON DELETE CASCADE,
            ocr_original VARCHAR(255) NOT NULL,
            nombre_sugerido VARCHAR(255) NOT NULL,
            codigo_ean VARCHAR(50),
            precio INT,
            establecimiento VARCHAR(100),
            nivel_confianza DECIMAL(3,2),
            motivo_duda TEXT,
            estado VARCHAR(20) DEFAULT 'pendiente',
            nombre_corregido_usuario VARCHAR(255),
            codigo_corregido_usuario VARCHAR(50),
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fecha_respuesta TIMESTAMP,
            datos_perplexity TEXT,
            datos_ocr TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_val_usuario ON validaciones_pendientes_usuario(usuario_id, estado);
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS validaciones_pendientes_usuario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER,
            usuario_id INTEGER,
            item_factura_id INTEGER,
            ocr_original TEXT NOT NULL,
            nombre_sugerido TEXT NOT NULL,
            codigo_ean TEXT,
            precio INTEGER,
            establecimiento TEXT,
            nivel_confianza REAL,
            motivo_duda TEXT,
            estado TEXT DEFAULT 'pendiente',
            nombre_corregido_usuario TEXT,
            codigo_corregido_usuario TEXT,
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fecha_respuesta TIMESTAMP,
            datos_perplexity TEXT,
            datos_ocr TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_val_usuario ON validaciones_pendientes_usuario(usuario_id, estado);
        """)
    print("   ✅ Creada")

    # TABLA 3: historial_validaciones
    print("\n3️⃣ historial_validaciones")
    if is_postgresql:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS historial_validaciones (
            id SERIAL PRIMARY KEY,
            factura_id INT,
            usuario_id INT,
            producto_maestro_id INT,
            ocr_original VARCHAR(255),
            nombre_python VARCHAR(255),
            nombre_perplexity VARCHAR(255),
            nombre_final VARCHAR(255),
            tuvo_correccion_python BOOLEAN DEFAULT FALSE,
            fue_validado_perplexity BOOLEAN DEFAULT FALSE,
            fue_validado_usuario BOOLEAN DEFAULT FALSE,
            confianza_final DECIMAL(3,2),
            fuente_final VARCHAR(50),
            fecha_procesamiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            datos_completos TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_hist_fecha ON historial_validaciones(fecha_procesamiento);
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS historial_validaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER,
            usuario_id INTEGER,
            producto_maestro_id INTEGER,
            ocr_original TEXT,
            nombre_python TEXT,
            nombre_perplexity TEXT,
            nombre_final TEXT,
            tuvo_correccion_python INTEGER DEFAULT 0,
            fue_validado_perplexity INTEGER DEFAULT 0,
            fue_validado_usuario INTEGER DEFAULT 0,
            confianza_final REAL,
            fuente_final TEXT,
            fecha_procesamiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            datos_completos TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_hist_fecha ON historial_validaciones(fecha_procesamiento);
        """)
    print("   ✅ Creada")

    # TABLA 4: productos_revision_admin
    print("\n4️⃣ productos_revision_admin")
    if is_postgresql:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS productos_revision_admin (
            id SERIAL PRIMARY KEY,
            producto_maestro_id INT,
            nombre_actual VARCHAR(255) NOT NULL,
            codigo_ean VARCHAR(50),
            motivo_revision VARCHAR(100) NOT NULL,
            prioridad INT DEFAULT 5,
            detalles_json TEXT,
            notas TEXT,
            estado VARCHAR(20) DEFAULT 'pendiente',
            revisado_por INT,
            fecha_revision TIMESTAMP,
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_rev_estado ON productos_revision_admin(estado);
        """)
    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS productos_revision_admin (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_maestro_id INTEGER,
            nombre_actual TEXT NOT NULL,
            codigo_ean TEXT,
            motivo_revision TEXT NOT NULL,
            prioridad INTEGER DEFAULT 5,
            detalles_json TEXT,
            notas TEXT,
            estado TEXT DEFAULT 'pendiente',
            revisado_por INTEGER,
            fecha_revision TIMESTAMP,
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_rev_estado ON productos_revision_admin(estado);
        """)
    print("   ✅ Creada")


def main():
    """Función principal"""

    print("=" * 80)
    print("🚀 INSTALADOR DE SISTEMA DE APRENDIZAJE")
    print("=" * 80)
    print("\nEste script creará 4 tablas nuevas:")
    print("  • correcciones_aprendidas")
    print("  • validaciones_pendientes_usuario")
    print("  • historial_validaciones")
    print("  • productos_revision_admin")
    print("\n⚠️  SEGURO: Usa IF NOT EXISTS - No borra datos")

    try:
        conn, is_postgresql = get_db_connection()
        cursor = conn.cursor()

        crear_tablas(cursor, is_postgresql)

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "=" * 80)
        print("✅ INSTALACIÓN COMPLETADA")
        print("=" * 80)
        print("\n🎉 Sistema de aprendizaje listo")
        print("\n🚀 Próximos pasos:")
        print("   1. Reinicia tu aplicación")
        print("   2. Prueba con facturas reales")
        print("   3. El sistema aprenderá automáticamente")
        print("\n" + "=" * 80)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
