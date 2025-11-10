#!/usr/bin/env python3
"""
Script para crear tabla categorias directamente
Ejecutar: python crear_categorias_simple.py
"""

import os
import sys

# Agregar el directorio actual al path para importar database
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import get_db_connection

def crear_tabla_categorias():
    """Crea tabla categorias con las 15 categorías básicas"""

    print("\n" + "="*70)
    print("🏗️ CREANDO TABLA CATEGORIAS")
    print("="*70)

    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return False

    cursor = conn.cursor()

    try:
        # 1. Crear tabla
        print("\n1️⃣ Creando tabla categorias...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS categorias (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) UNIQUE NOT NULL,
                descripcion TEXT,
                icono VARCHAR(50),
                orden INTEGER DEFAULT 0,
                activo BOOLEAN DEFAULT TRUE,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("   ✅ Tabla creada")

        # 2. Insertar categorías
        print("\n2️⃣ Insertando categorías básicas...")

        categorias = [
            ('Lácteos', 'Leche, yogurt, queso', '🥛', 1),
            ('Carnes', 'Carnes y pescado', '🥩', 2),
            ('Frutas y Verduras', 'Frescos', '🍎', 3),
            ('Panadería', 'Pan y repostería', '🍞', 4),
            ('Bebidas', 'Jugos y gaseosas', '🥤', 5),
            ('Despensa', 'Granos y enlatados', '🥫', 6),
            ('Aseo Personal', 'Cuidado personal', '🧴', 7),
            ('Aseo Hogar', 'Limpieza', '🧹', 8),
            ('Snacks', 'Galletas y dulces', '🍪', 9),
            ('Congelados', 'Productos congelados', '🧊', 10),
            ('Farmacia', 'Medicamentos', '💊', 11),
            ('Bebé', 'Productos bebé', '👶', 12),
            ('Mascotas', 'Cuidado mascotas', '🐕', 13),
            ('Licores', 'Bebidas alcohólicas', '🍺', 14),
            ('Otros', 'Varios', '📦', 99)
        ]

        insertadas = 0
        for nombre, descripcion, icono, orden in categorias:
            try:
                cursor.execute("""
                    INSERT INTO categorias (nombre, descripcion, icono, orden)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (nombre) DO NOTHING
                """, (nombre, descripcion, icono, orden))

                if cursor.rowcount > 0:
                    insertadas += 1
                    print(f"   ✅ {icono} {nombre}")
                else:
                    print(f"   ⚠️  {icono} {nombre} (ya existe)")

            except Exception as e:
                print(f"   ❌ Error con {nombre}: {e}")

        conn.commit()
        print(f"\n   📊 Total insertadas: {insertadas}")

        # 3. Crear índice
        print("\n3️⃣ Creando índice...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_productos_v2_categoria
            ON productos_maestros_v2(categoria_id)
        """)
        conn.commit()
        print("   ✅ Índice creado")

        # 4. Verificar
        print("\n4️⃣ Verificando...")

        cursor.execute("SELECT COUNT(*) FROM categorias")
        total = cursor.fetchone()[0]
        print(f"   📊 Total categorías en BD: {total}")

        cursor.execute("""
            SELECT COUNT(*) FROM productos_maestros_v2
            WHERE categoria_id IS NOT NULL
        """)
        con_categoria = cursor.fetchone()[0]
        print(f"   ✅ Productos con categoría: {con_categoria}")

        cursor.execute("""
            SELECT COUNT(*) FROM productos_maestros_v2
            WHERE categoria_id IS NULL
        """)
        sin_categoria = cursor.fetchone()[0]
        print(f"   ⚠️  Productos sin categoría: {sin_categoria}")

        # 5. Mostrar categorías
        print("\n5️⃣ Categorías disponibles:")
        cursor.execute("""
            SELECT id, icono, nombre, orden
            FROM categorias
            ORDER BY orden
        """)

        for row in cursor.fetchall():
            print(f"   {row[0]:2d}. {row[1]} {row[2]}")

        cursor.close()
        conn.close()

        print("\n" + "="*70)
        print("✅ TABLA CATEGORIAS CREADA EXITOSAMENTE")
        print("="*70)
        print("\n💡 Ahora puedes:")
        print("   1. Verificar: https://tu-app.railway.app/api/v2/productos/")
        print("   2. Ver dashboard: https://tu-app.railway.app/productos.html")
        print("\n")

        return True

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

        try:
            conn.rollback()
            cursor.close()
            conn.close()
        except:
            pass

        return False


if __name__ == "__main__":
    print("\n🚀 Script de Creación de Tabla Categorias")
    print("   Compatible con Railway")

    exito = crear_tabla_categorias()

    if exito:
        print("✅ Script completado exitosamente")
        sys.exit(0)
    else:
        print("❌ Script falló")
        sys.exit(1)
