#!/usr/bin/env python3
"""
reset_lecfac_completo.py
Script definitivo que encuentra TODAS las dependencias y hace reset completo
"""

import psycopg2
from datetime import datetime

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def encontrar_dependencias():
    """Encuentra todas las tablas que tienen foreign keys"""
    print("\n🔍 Analizando dependencias...")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Query para encontrar TODAS las foreign keys
    cur.execute("""
        SELECT
            tc.table_name as tabla_hija,
            kcu.column_name as columna_hija,
            ccu.table_name AS tabla_padre,
            ccu.column_name AS columna_padre
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        ORDER BY tabla_padre, tabla_hija;
    """)

    dependencias = {}
    for row in cur.fetchall():
        tabla_hija, col_hija, tabla_padre, col_padre = row
        if tabla_padre not in dependencias:
            dependencias[tabla_padre] = []
        dependencias[tabla_padre].append(tabla_hija)

    # Mostrar dependencias
    print("\n📋 Dependencias encontradas:")
    for padre, hijas in dependencias.items():
        print(f"   {padre} <- {', '.join(set(hijas))}")

    cur.close()
    conn.close()

    return dependencias

def obtener_orden_borrado(dependencias):
    """Determina el orden correcto para borrar tablas"""
    # Tablas que queremos borrar (no usuarios)
    tablas_objetivo = [
        'processing_jobs', 'inventario_usuario', 'items_factura',
        'facturas', 'precios_productos', 'productos_por_establecimiento',
        'codigos_normalizados', 'correcciones_productos',
        'codigos_locales', 'matching_logs', 'historial_compras_usuario',
        'productos_maestros', 'establecimientos', 'patrones_compra',
        'gastos_mensuales', 'alertas_usuario', 'presupuesto_usuario',
        'auditoria_productos', 'historial_cambios_productos'
    ]

    # Construir orden basado en dependencias
    orden = []
    procesadas = set()

    # Función recursiva para procesar dependencias
    def procesar_tabla(tabla):
        if tabla in procesadas or tabla not in tablas_objetivo:
            return

        # Primero procesar las tablas que dependen de esta
        if tabla in dependencias:
            for hija in dependencias[tabla]:
                if hija != tabla:  # Evitar loops
                    procesar_tabla(hija)

        # Luego agregar esta tabla
        if tabla not in procesadas and tabla in tablas_objetivo:
            orden.append(tabla)
            procesadas.add(tabla)

    # Procesar todas las tablas
    for tabla in tablas_objetivo:
        procesar_tabla(tabla)

    return orden

def reset_completo():
    """Hace el reset completo en el orden correcto"""
    print("\n🧹 Iniciando reset completo...")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Obtener dependencias y orden
    dependencias = encontrar_dependencias()
    orden = obtener_orden_borrado(dependencias)

    print(f"\n📝 Orden de borrado determinado ({len(orden)} tablas)")

    # Borrar en el orden correcto
    total_borrados = 0
    for tabla in orden:
        try:
            # Verificar si existe
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (tabla,))

            if cur.fetchone()[0]:
                cur.execute(f"DELETE FROM {tabla}")
                borrados = cur.rowcount
                total_borrados += borrados
                print(f"   ✅ {tabla}: {borrados} registros eliminados")
            else:
                print(f"   ⚠️ {tabla}: no existe")

        except Exception as e:
            print(f"   ❌ {tabla}: {str(e).split('DETAIL')[0]}")
            conn.rollback()

    # Resetear usuarios
    try:
        cur.execute("""
            UPDATE usuarios
            SET facturas_aportadas = 0,
                productos_aportados = 0,
                puntos_contribucion = 0
        """)
        print(f"\n✅ Contadores de usuarios reseteados")
    except Exception as e:
        print(f"❌ Error reseteando usuarios: {e}")

    # Resetear secuencias
    print("\n🔄 Reseteando secuencias...")
    secuencias = [
        'establecimientos_id_seq',
        'productos_maestros_id_seq',
        'productos_por_establecimiento_id_seq',
        'facturas_id_seq',
        'items_factura_id_seq',
        'precios_productos_id_seq',
        'inventario_usuario_id_seq'
    ]

    for seq in secuencias:
        try:
            cur.execute(f"ALTER SEQUENCE IF EXISTS {seq} RESTART WITH 1")
            print(f"   ✅ {seq}")
        except Exception as e:
            print(f"   ⚠️ {seq}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n✅ Reset completado: {total_borrados} registros eliminados en total")

def verificar_estado_final():
    """Verifica el estado final"""
    print("\n📊 Estado final de la base de datos:")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    tablas_verificar = [
        'usuarios', 'establecimientos', 'productos_maestros',
        'facturas', 'items_factura', 'inventario_usuario'
    ]

    print("-" * 50)
    for tabla in tablas_verificar:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cur.fetchone()[0]
            print(f"{tabla:<25} {count:>10} registros")
        except:
            print(f"{tabla:<25} {'ERROR':>10}")
    print("-" * 50)

    cur.close()
    conn.close()

def main():
    print("=" * 70)
    print("LECFAC - RESET COMPLETO DEFINITIVO")
    print("=" * 70)
    print("\nEste script detectará TODAS las dependencias y borrará")
    print("los datos en el orden correcto.")

    # Confirmar
    respuesta = input("\n⚠️  ¿Continuar? (escribe 'RESET TOTAL'): ")

    if respuesta != 'RESET TOTAL':
        print("❌ Cancelado")
        return

    # Ejecutar
    reset_completo()
    verificar_estado_final()

    print("\n✨ ¡Base de datos lista para empezar de cero!")
    print("\nPuedes ahora:")
    print("1. Escanear facturas y ver el flujo completo del OCR")
    print("2. Verificar que los user_id se asignan correctamente")
    print("3. Ver cómo se construye la base comunitaria paso a paso")

if __name__ == "__main__":
    main()
