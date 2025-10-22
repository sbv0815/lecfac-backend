"""
Script para crear/actualizar todas las tablas necesarias
Funciona tanto con SQLite como PostgreSQL
"""

from database import get_db_connection
import os


def setup_database():
    """Crea o actualiza todas las tablas necesarias"""

    conn = get_db_connection()
    cursor = conn.cursor()

    print("=" * 70)
    print("🗄️  CONFIGURANDO BASE DE DATOS")
    print("=" * 70)

    # Detectar tipo de BD
    db_type = os.environ.get("DATABASE_TYPE", "sqlite")
    print(f"📊 Tipo de BD: {db_type}")

    try:
        # ========================================
        # TABLA: usuarios
        # ========================================
        print("\n📋 Verificando tabla 'usuarios'...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        print("✅ Tabla 'usuarios' lista")

        # ========================================
        # TABLA: facturas
        # ========================================
        print("📋 Verificando tabla 'facturas'...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS facturas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                establecimiento TEXT,
                total_factura REAL,
                fecha_compra DATE,
                fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tiene_imagen BOOLEAN DEFAULT 0,
                ruta_imagen TEXT,
                estado_validacion TEXT DEFAULT 'pendiente',
                motivo_rechazo TEXT,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
            )
        """
        )
        print("✅ Tabla 'facturas' lista")

        # Verificar si existen las columnas estado_validacion y motivo_rechazo
        cursor.execute("PRAGMA table_info(facturas)")
        columns = [col[1] for col in cursor.fetchall()]

        if "estado_validacion" not in columns:
            print("➕ Agregando columna 'estado_validacion'...")
            cursor.execute(
                "ALTER TABLE facturas ADD COLUMN estado_validacion TEXT DEFAULT 'pendiente'"
            )
            print("✅ Columna agregada")
        else:
            print("ℹ️  Columna 'estado_validacion' ya existe")

        if "motivo_rechazo" not in columns:
            print("➕ Agregando columna 'motivo_rechazo'...")
            cursor.execute("ALTER TABLE facturas ADD COLUMN motivo_rechazo TEXT")
            print("✅ Columna agregada")
        else:
            print("ℹ️  Columna 'motivo_rechazo' ya existe")

        # ========================================
        # TABLA: items_factura
        # ========================================
        print("📋 Verificando tabla 'items_factura'...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS items_factura (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                factura_id INTEGER NOT NULL,
                usuario_id INTEGER NOT NULL,
                nombre_producto TEXT NOT NULL,
                codigo_ean TEXT,
                cantidad REAL DEFAULT 1,
                precio_unitario REAL,
                precio_total REAL,
                categoria TEXT,
                marca TEXT,
                FOREIGN KEY (factura_id) REFERENCES facturas(id) ON DELETE CASCADE,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
            )
        """
        )
        print("✅ Tabla 'items_factura' lista")

        # ========================================
        # TABLA: productos_master (catálogo)
        # ========================================
        print("📋 Verificando tabla 'productos_master'...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS productos_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_ean TEXT UNIQUE,
                nombre_normalizado TEXT NOT NULL,
                marca TEXT,
                categoria TEXT,
                presentacion TEXT,
                precio_promedio REAL,
                veces_visto INTEGER DEFAULT 1,
                primera_vez_visto TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ultima_vez_visto TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        print("✅ Tabla 'productos_master' lista")

        # ========================================
        # TABLA: inventario (stock actual)
        # ========================================
        print("📋 Verificando tabla 'inventario'...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS inventario (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                producto_master_id INTEGER,
                nombre_producto TEXT NOT NULL,
                cantidad_actual REAL DEFAULT 0,
                precio_ultima_compra REAL,
                fecha_ultima_compra DATE,
                stock_minimo REAL DEFAULT 0,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
                FOREIGN KEY (producto_master_id) REFERENCES productos_master(id)
            )
        """
        )
        print("✅ Tabla 'inventario' lista")

        # ========================================
        # ÍNDICES para mejorar rendimiento
        # ========================================
        print("\n📊 Creando índices...")

        indices = [
            "CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)",
            "CREATE INDEX IF NOT EXISTS idx_facturas_estado ON facturas(estado_validacion)",
            "CREATE INDEX IF NOT EXISTS idx_items_factura ON items_factura(factura_id)",
            "CREATE INDEX IF NOT EXISTS idx_items_usuario ON items_factura(usuario_id)",
            "CREATE INDEX IF NOT EXISTS idx_productos_ean ON productos_master(codigo_ean)",
            "CREATE INDEX IF NOT EXISTS idx_inventario_usuario ON inventario(usuario_id)",
        ]

        for idx_query in indices:
            cursor.execute(idx_query)

        print("✅ Índices creados")

        # ========================================
        # COMMIT
        # ========================================
        conn.commit()

        # ========================================
        # VERIFICACIÓN FINAL
        # ========================================
        print("\n📋 Verificación final:")

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        print(f"\n✅ Tablas creadas ({len(tables)}):")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"   - {table[0]}: {count} registros")

        print("\n" + "=" * 70)
        print("✅ BASE DE DATOS CONFIGURADA CORRECTAMENTE")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        conn.rollback()
        return False

    finally:
        conn.close()


if __name__ == "__main__":
    setup_database()
