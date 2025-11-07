#!/usr/bin/env python3
"""
update_ean_length_lecfac.py
Script para actualizar la longitud del campo codigo_ean en LecFac
"""

import psycopg2
from urllib.parse import urlparse

# Tu DATABASE_URL
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def main():
    print("🔧 Actualizando longitud de codigo_ean en LecFac")
    print("=" * 50)

    # Parsear la URL
    parsed = urlparse(DATABASE_URL)

    try:
        # Conectar a la base de datos
        print("\n🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port,
            database=parsed.path[1:],
            user=parsed.username,
            password=parsed.password
        )

        cursor = conn.cursor()
        print("✅ Conexión establecida")

        # Ejecutar los cambios
        print("\n🔄 Aplicando cambios...")

        try:
            # Actualizar productos_maestros
            print("  - Actualizando productos_maestros.codigo_ean...")
            cursor.execute("""
                ALTER TABLE productos_maestros
                ALTER COLUMN codigo_ean TYPE VARCHAR(20)
            """)
            print("    ✅ OK")

            # Actualizar items_factura
            print("  - Actualizando items_factura.codigo_leido...")
            cursor.execute("""
                ALTER TABLE items_factura
                ALTER COLUMN codigo_leido TYPE VARCHAR(20)
            """)
            print("    ✅ OK")

            # Confirmar cambios
            conn.commit()
            print("\n✅ ¡Cambios aplicados exitosamente!")

            # Verificar los cambios
            print("\n📊 Verificando cambios...")

            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'productos_maestros'
                AND column_name = 'codigo_ean'
            """)
            result = cursor.fetchone()
            if result:
                print(f"  - productos_maestros.codigo_ean: {result[1]}({result[2]})")

            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'items_factura'
                AND column_name = 'codigo_leido'
            """)
            result = cursor.fetchone()
            if result:
                print(f"  - items_factura.codigo_leido: {result[1]}({result[2]})")

            print("\n🎉 Ahora puedes guardar:")
            print("  ✅ Códigos EAN estándar (13 dígitos)")
            print("  ✅ Códigos de Ara con 0 inicial (14 dígitos)")
            print("  ✅ Otros códigos hasta 20 dígitos")

        except psycopg2.Error as e:
            conn.rollback()
            print(f"\n❌ Error al aplicar cambios: {e}")
            return

        # Cerrar conexión
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"\n❌ Error de conexión: {e}")
        return

    print("\n🏁 Script finalizado exitosamente")

if __name__ == "__main__":
    main()
