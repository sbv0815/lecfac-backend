"""
migrar_constraint_aprendizaje.py
Agrega el UNIQUE constraint necesario para el sistema de aprendizaje
"""

import os
import sys
from database import get_db_connection


def migrar_constraint():
    """Agrega el constraint único a correcciones_aprendidas"""

    print("="*80)
    print("🔧 MIGRACIÓN: Agregar UNIQUE constraint a correcciones_aprendidas")
    print("="*80)

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("\n1️⃣ Verificando tabla correcciones_aprendidas...")

        # Verificar si la tabla existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'correcciones_aprendidas'
            )
        """)

        if not cursor.fetchone()[0]:
            print("❌ Tabla correcciones_aprendidas no existe")
            return False

        print("   ✅ Tabla existe")

        # Verificar si el constraint ya existe
        print("\n2️⃣ Verificando constraint existente...")
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'correcciones_aprendidas'
              AND constraint_type = 'UNIQUE'
              AND constraint_name = 'unique_correccion'
        """)

        if cursor.fetchone():
            print("   ℹ️  Constraint 'unique_correccion' ya existe")
            return True

        print("   ⚠️  Constraint no existe, creando...")

        # Limpiar duplicados primero
        print("\n3️⃣ Limpiando duplicados existentes...")
        cursor.execute("""
            DELETE FROM correcciones_aprendidas a
            USING correcciones_aprendidas b
            WHERE a.id < b.id
              AND a.ocr_normalizado = b.ocr_normalizado
              AND COALESCE(a.establecimiento, '') = COALESCE(b.establecimiento, '')
        """)

        duplicados_eliminados = cursor.rowcount
        print(f"   🗑️  {duplicados_eliminados} duplicados eliminados")

        # Crear el constraint
        print("\n4️⃣ Creando UNIQUE constraint...")
        cursor.execute("""
            ALTER TABLE correcciones_aprendidas
            ADD CONSTRAINT unique_correccion
            UNIQUE (ocr_normalizado, establecimiento)
        """)

        conn.commit()
        print("   ✅ Constraint creado exitosamente")

        # Verificar
        print("\n5️⃣ Verificando constraint...")
        cursor.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'correcciones_aprendidas'
              AND constraint_name = 'unique_correccion'
        """)

        resultado = cursor.fetchone()
        if resultado:
            print(f"   ✅ Constraint verificado: {resultado[0]} ({resultado[1]})")
        else:
            print("   ❌ Error: Constraint no se creó correctamente")
            return False

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("✅ MIGRACIÓN COMPLETADA EXITOSAMENTE")
        print("="*80)
        print("🎉 El sistema de aprendizaje ahora funcionará correctamente")
        print("")

        return True

    except Exception as e:
        print(f"\n❌ ERROR EN MIGRACIÓN: {e}")
        import traceback
        traceback.print_exc()

        if conn:
            try:
                conn.rollback()
            except:
                pass

        return False


if __name__ == "__main__":
    print("\n🚀 Iniciando migración de base de datos...\n")

    exito = migrar_constraint()

    if exito:
        print("✅ Migración exitosa - Sistema listo para aprender")
        sys.exit(0)
    else:
        print("❌ Migración fallida - Revisa los errores arriba")
        sys.exit(1)
