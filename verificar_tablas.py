# verificar_tablas.py
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def verificar_estructura():
    """
    Verifica que todas las tablas estén creadas correctamente
    """
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print("="*60)
        print("VERIFICACIÓN DE ESTRUCTURA DE BASE DE DATOS")
        print("="*60)

        # Listar todas las tablas
        tablas = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)

        print("\n📋 TODAS LAS TABLAS EN LA BASE DE DATOS:")
        print("-"*60)
        for tabla in tablas:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {tabla['table_name']}")
            print(f"  {tabla['table_name']:<30} {count:>10} registros")

        # Verificar relaciones (foreign keys)
        print("\n🔗 RELACIONES (FOREIGN KEYS):")
        print("-"*60)
        fks = await conn.fetch("""
            SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name IN (
                'productos_maestros_v2',
                'codigos_alternativos',
                'variantes_nombres',
                'precios_historicos_v2'
            )
            ORDER BY tc.table_name;
        """)

        for fk in fks:
            print(f"  {fk['table_name']}.{fk['column_name']}")
            print(f"    → {fk['foreign_table_name']}.{fk['foreign_column_name']}")

        # Verificar índices
        print("\n📊 ÍNDICES CREADOS:")
        print("-"*60)
        indices = await conn.fetch("""
            SELECT
                tablename,
                indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename IN (
                'productos_maestros_v2',
                'codigos_alternativos',
                'variantes_nombres',
                'precios_historicos_v2'
            )
            ORDER BY tablename, indexname;
        """)

        current_table = None
        for idx in indices:
            if idx['tablename'] != current_table:
                print(f"\n  {idx['tablename']}:")
                current_table = idx['tablename']
            print(f"    - {idx['indexname']}")

        print("\n" + "="*60)
        print("✓ VERIFICACIÓN COMPLETADA")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(verificar_estructura())
