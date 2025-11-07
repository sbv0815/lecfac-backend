# crear_tablas_consolidacion.py
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

# Usar la URL de producción de Railway
DATABASE_URL = os.getenv("DATABASE_URL")

async def crear_tablas_nuevas():
    """
    Crea las nuevas tablas para el sistema de consolidación
    sin afectar las tablas existentes
    """
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print("Conectado a la base de datos de producción")
        print("Creando nuevas tablas...\n")

        # TABLA 1: Productos Referencia (tu fuente de verdad manual)
        print("1. Creando tabla productos_referencia...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS productos_referencia (
                id SERIAL PRIMARY KEY,
                codigo_ean VARCHAR(13) UNIQUE,
                nombre_oficial VARCHAR(200) NOT NULL,
                marca VARCHAR(100),
                categoria VARCHAR(100),
                peso_neto DECIMAL(10,2),
                unidad_medida VARCHAR(10),
                fuente VARCHAR(100) DEFAULT 'manual',
                verificado BOOLEAN DEFAULT TRUE,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notas TEXT
            );
        """)
        print("   ✓ Tabla productos_referencia creada")

        # TABLA 2: Productos Maestros Consolidados
        print("2. Creando tabla productos_maestros_v2...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS productos_maestros_v2 (
                id SERIAL PRIMARY KEY,
                codigo_ean VARCHAR(13) UNIQUE,
                nombre_consolidado VARCHAR(200) NOT NULL,
                marca VARCHAR(100),
                categoria_id INTEGER,
                peso_neto DECIMAL(10,2),
                unidad_medida VARCHAR(10),
                confianza_datos DECIMAL(3,2) DEFAULT 0.5,
                veces_visto INTEGER DEFAULT 1,
                fecha_primera_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                estado VARCHAR(20) DEFAULT 'pendiente',
                CHECK (confianza_datos >= 0 AND confianza_datos <= 1),
                CHECK (estado IN ('pendiente', 'verificado', 'conflicto', 'descartado'))
            );
        """)
        print("   ✓ Tabla productos_maestros_v2 creada")

        # Índices para productos_maestros_v2
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_productos_maestros_v2_ean
            ON productos_maestros_v2(codigo_ean);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_productos_maestros_v2_estado
            ON productos_maestros_v2(estado);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_productos_maestros_v2_nombre
            ON productos_maestros_v2(nombre_consolidado);
        """)
        print("   ✓ Índices creados para productos_maestros_v2")

        # TABLA 3: Códigos Alternativos (PLUs, códigos internos)
        print("3. Creando tabla codigos_alternativos...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS codigos_alternativos (
                id SERIAL PRIMARY KEY,
                producto_maestro_id INTEGER REFERENCES productos_maestros_v2(id) ON DELETE CASCADE,
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                codigo_local VARCHAR(50) NOT NULL,
                tipo_codigo VARCHAR(20) DEFAULT 'PLU',
                veces_visto INTEGER DEFAULT 1,
                fecha_primera_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_ultima_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CHECK (tipo_codigo IN ('PLU', 'interno', 'ean_local', 'otro')),
                UNIQUE(codigo_local, establecimiento_id)
            );
        """)
        print("   ✓ Tabla codigos_alternativos creada")

        # Índices para codigos_alternativos
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_codigos_alt_producto
            ON codigos_alternativos(producto_maestro_id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_codigos_alt_codigo
            ON codigos_alternativos(codigo_local, establecimiento_id);
        """)
        print("   ✓ Índices creados para codigos_alternativos")

        # TABLA 4: Variantes de Nombres
        print("4. Creando tabla variantes_nombres...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS variantes_nombres (
                id SERIAL PRIMARY KEY,
                producto_maestro_id INTEGER REFERENCES productos_maestros_v2(id) ON DELETE CASCADE,
                nombre_variante VARCHAR(200) NOT NULL,
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                veces_visto INTEGER DEFAULT 1,
                fecha_primera_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_ultima_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(nombre_variante, establecimiento_id, producto_maestro_id)
            );
        """)
        print("   ✓ Tabla variantes_nombres creada")

        # Índices para variantes_nombres
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variantes_producto
            ON variantes_nombres(producto_maestro_id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_variantes_nombre
            ON variantes_nombres(nombre_variante);
        """)
        print("   ✓ Índices creados para variantes_nombres")

        # TABLA 5: Precios Históricos V2
        print("5. Creando tabla precios_historicos_v2...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS precios_historicos_v2 (
                id SERIAL PRIMARY KEY,
                producto_maestro_id INTEGER REFERENCES productos_maestros_v2(id) ON DELETE CASCADE,
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                precio DECIMAL(10,2) NOT NULL,
                fecha_factura DATE NOT NULL,
                factura_id INTEGER REFERENCES facturas(id),
                item_factura_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CHECK (precio >= 0)
            );
        """)
        print("   ✓ Tabla precios_historicos_v2 creada")

        # Índices para precios_historicos_v2
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_precios_v2_producto
            ON precios_historicos_v2(producto_maestro_id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_precios_v2_establecimiento
            ON precios_historicos_v2(establecimiento_id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_precios_v2_fecha
            ON precios_historicos_v2(fecha_factura);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_precios_v2_producto_fecha
            ON precios_historicos_v2(producto_maestro_id, fecha_factura);
        """)
        print("   ✓ Índices creados para precios_historicos_v2")

        # TABLA 6: Log de Mejoras de Nombres (para auditoría)
        print("6. Creando tabla log_mejoras_nombres...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS log_mejoras_nombres (
                id SERIAL PRIMARY KEY,
                producto_maestro_id INTEGER REFERENCES productos_maestros_v2(id),
                nombre_original VARCHAR(200) NOT NULL,
                nombre_mejorado VARCHAR(200) NOT NULL,
                metodo VARCHAR(50),
                confianza DECIMAL(3,2),
                fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("   ✓ Tabla log_mejoras_nombres creada")

        print("\n" + "="*60)
        print("✓ TODAS LAS TABLAS NUEVAS CREADAS EXITOSAMENTE")
        print("="*60)

        # Verificar que las tablas se crearon
        print("\nVerificando tablas creadas...")
        tablas = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN (
                'productos_referencia',
                'productos_maestros_v2',
                'codigos_alternativos',
                'variantes_nombres',
                'precios_historicos_v2',
                'log_mejoras_nombres'
            )
            ORDER BY table_name;
        """)

        print("\nTablas encontradas:")
        for tabla in tablas:
            print(f"  ✓ {tabla['table_name']}")

        # Contar registros en tablas antiguas (para referencia)
        print("\n" + "="*60)
        print("Estado actual de tablas ANTIGUAS (no modificadas):")
        print("="*60)

        count_facturas = await conn.fetchval("SELECT COUNT(*) FROM facturas")
        print(f"  facturas: {count_facturas} registros")

        count_items = await conn.fetchval("SELECT COUNT(*) FROM items_factura")
        print(f"  items_factura: {count_items} registros")

        count_productos = await conn.fetchval("SELECT COUNT(*) FROM productos_maestros")
        print(f"  productos_maestros: {count_productos} registros")

        count_establecimientos = await conn.fetchval("SELECT COUNT(*) FROM establecimientos")
        print(f"  establecimientos: {count_establecimientos} registros")

        print("\n✓ Todas las tablas antiguas permanecen intactas")
        print("\n" + "="*60)
        print("RESUMEN:")
        print("="*60)
        print("• 6 tablas nuevas creadas")
        print("• 0 tablas antiguas modificadas")
        print("• Listo para comenzar migración de datos")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        await conn.close()
        print("\nConexión cerrada")


if __name__ == "__main__":
    print("="*60)
    print("CREACIÓN DE TABLAS NUEVAS - SISTEMA DE CONSOLIDACIÓN")
    print("="*60)
    print("\nEste script creará las siguientes tablas:")
    print("  1. productos_referencia")
    print("  2. productos_maestros_v2")
    print("  3. codigos_alternativos")
    print("  4. variantes_nombres")
    print("  5. precios_historicos_v2")
    print("  6. log_mejoras_nombres")
    print("\nNinguna tabla existente será modificada.\n")

    respuesta = input("¿Continuar? (si/no): ").lower()

    if respuesta in ['si', 's', 'yes', 'y']:
        asyncio.run(crear_tablas_nuevas())
    else:
        print("Operación cancelada")
