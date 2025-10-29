"""
Monitor de Anomalías en Facturas
=================================

Sistema para detectar, registrar y analizar anomalías en el procesamiento de facturas.
Registra duplicados detectados, correcciones aplicadas y estadísticas por establecimiento.

Autor: LecFac
Versión: 1.0.0
Fecha: 2025-01-18
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple


def get_db_connection():
    """Obtiene conexión a la base de datos"""
    # Importar aquí para evitar dependencias circulares
    from database import get_db_connection as get_conn
    return get_conn()


def guardar_reporte_anomalia(
    factura_id: int,
    establecimiento: str,
    metricas: Dict
) -> bool:
    """
    Guarda un reporte de anomalía detectada en una factura

    Args:
        factura_id: ID de la factura
        establecimiento: Nombre del establecimiento
        metricas: Dict con métricas de la anomalía

    Returns:
        True si se guardó correctamente, False en caso contrario
    """
    print(f"📊 Guardando reporte de anomalía para factura {factura_id}...")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Extraer datos de métricas
        productos_originales = metricas.get("productos_originales", 0)
        productos_corregidos = metricas.get("productos_corregidos", 0)
        productos_eliminados = metricas.get("productos_eliminados", 0)
        diferencia_inicial = metricas.get("diferencia_inicial", 0)
        diferencia_final = metricas.get("diferencia_final", 0)
        porcentaje_inicial = metricas.get("porcentaje_inicial", 0)
        porcentaje_final = metricas.get("porcentaje_final", 0)

        # Determinar tipo de anomalía
        tipo_anomalia = "duplicados" if productos_eliminados > 0 else "descuadre"

        # Determinar severidad
        if porcentaje_final < 5:
            severidad = "baja"
        elif porcentaje_final < 15:
            severidad = "media"
        else:
            severidad = "alta"

        # Crear descripción
        descripcion = (
            f"Factura procesada con {productos_eliminados} duplicados eliminados. "
            f"Productos: {productos_originales} → {productos_corregidos}. "
            f"Diferencia: {porcentaje_inicial:.1f}% → {porcentaje_final:.1f}%"
        )

        # Verificar si la tabla existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'reportes_anomalias'
                )
            """)
            tabla_existe = cursor.fetchone()[0]

            if not tabla_existe:
                print("⚠️ Tabla reportes_anomalias no existe, creándola...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS reportes_anomalias (
                        id SERIAL PRIMARY KEY,
                        factura_id INTEGER NOT NULL,
                        establecimiento VARCHAR(255),
                        tipo_anomalia VARCHAR(50),
                        severidad VARCHAR(20),
                        descripcion TEXT,
                        productos_originales INTEGER,
                        productos_corregidos INTEGER,
                        productos_eliminados INTEGER,
                        diferencia_inicial DECIMAL(10,2),
                        diferencia_final DECIMAL(10,2),
                        porcentaje_inicial DECIMAL(5,2),
                        porcentaje_final DECIMAL(5,2),
                        fecha_deteccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        estado VARCHAR(20) DEFAULT 'pendiente'
                    )
                """)
                conn.commit()
                print("✅ Tabla reportes_anomalias creada")

            # Insertar reporte
            cursor.execute("""
                INSERT INTO reportes_anomalias (
                    factura_id, establecimiento, tipo_anomalia, severidad,
                    descripcion, productos_originales, productos_corregidos,
                    productos_eliminados, diferencia_inicial, diferencia_final,
                    porcentaje_inicial, porcentaje_final
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                factura_id, establecimiento, tipo_anomalia, severidad,
                descripcion, productos_originales, productos_corregidos,
                productos_eliminados, diferencia_inicial, diferencia_final,
                porcentaje_inicial, porcentaje_final
            ))

            reporte_id = cursor.fetchone()[0]

        else:
            # SQLite
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reportes_anomalias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    factura_id INTEGER NOT NULL,
                    establecimiento TEXT,
                    tipo_anomalia TEXT,
                    severidad TEXT,
                    descripcion TEXT,
                    productos_originales INTEGER,
                    productos_corregidos INTEGER,
                    productos_eliminados INTEGER,
                    diferencia_inicial REAL,
                    diferencia_final REAL,
                    porcentaje_inicial REAL,
                    porcentaje_final REAL,
                    fecha_deteccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    estado TEXT DEFAULT 'pendiente'
                )
            """)

            cursor.execute("""
                INSERT INTO reportes_anomalias (
                    factura_id, establecimiento, tipo_anomalia, severidad,
                    descripcion, productos_originales, productos_corregidos,
                    productos_eliminados, diferencia_inicial, diferencia_final,
                    porcentaje_inicial, porcentaje_final
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                factura_id, establecimiento, tipo_anomalia, severidad,
                descripcion, productos_originales, productos_corregidos,
                productos_eliminados, diferencia_inicial, diferencia_final,
                porcentaje_inicial, porcentaje_final
            ))

            reporte_id = cursor.lastrowid

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Reporte de anomalía guardado (ID: {reporte_id})")
        return True

    except Exception as e:
        print(f"❌ Error guardando reporte de anomalía: {e}")
        import traceback
        traceback.print_exc()
        return False


def obtener_estadisticas_por_establecimiento() -> List[Tuple]:
    """
    Obtiene estadísticas de anomalías por establecimiento

    Returns:
        Lista de tuplas con estadísticas por establecimiento
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar si tabla existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'reportes_anomalias'
                )
            """)
            if not cursor.fetchone()[0]:
                cursor.close()
                conn.close()
                return []

            cursor.execute("""
                SELECT
                    establecimiento,
                    COUNT(*) as num_facturas,
                    AVG(porcentaje_final) as ratio_promedio,
                    SUM(CASE WHEN productos_eliminados > 0 THEN 1 ELSE 0 END) as facturas_corregidas,
                    AVG(productos_eliminados) as promedio_duplicados
                FROM reportes_anomalias
                GROUP BY establecimiento
                ORDER BY num_facturas DESC
                LIMIT 20
            """)
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='reportes_anomalias'
            """)
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return []

            cursor.execute("""
                SELECT
                    establecimiento,
                    COUNT(*) as num_facturas,
                    AVG(porcentaje_final) as ratio_promedio,
                    SUM(CASE WHEN productos_eliminados > 0 THEN 1 ELSE 0 END) as facturas_corregidas,
                    AVG(productos_eliminados) as promedio_duplicados
                FROM reportes_anomalias
                GROUP BY establecimiento
                ORDER BY num_facturas DESC
                LIMIT 20
            """)

        resultados = cursor.fetchall()
        cursor.close()
        conn.close()

        return resultados

    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return []


def obtener_anomalias_pendientes(limit: int = 50) -> List[Dict]:
    """
    Obtiene anomalías pendientes de revisión

    Args:
        limit: Número máximo de resultados

    Returns:
        Lista de anomalías pendientes
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar si tabla existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'reportes_anomalias'
                )
            """)
            if not cursor.fetchone()[0]:
                cursor.close()
                conn.close()
                return []

            cursor.execute("""
                SELECT
                    id,
                    factura_id,
                    establecimiento,
                    tipo_anomalia,
                    severidad,
                    descripcion,
                    productos_eliminados,
                    porcentaje_final,
                    fecha_deteccion
                FROM reportes_anomalias
                WHERE estado = 'pendiente'
                ORDER BY fecha_deteccion DESC
                LIMIT %s
            """, (limit,))
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='reportes_anomalias'
            """)
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return []

            cursor.execute("""
                SELECT
                    id,
                    factura_id,
                    establecimiento,
                    tipo_anomalia,
                    severidad,
                    descripcion,
                    productos_eliminados,
                    porcentaje_final,
                    fecha_deteccion
                FROM reportes_anomalias
                WHERE estado = 'pendiente'
                ORDER BY fecha_deteccion DESC
                LIMIT ?
            """, (limit,))

        anomalias = []
        for row in cursor.fetchall():
            anomalias.append({
                "id": row[0],
                "factura_id": row[1],
                "establecimiento": row[2],
                "tipo_anomalia": row[3],
                "severidad": row[4],
                "descripcion": row[5],
                "productos_eliminados": row[6],
                "porcentaje_final": float(row[7]) if row[7] else 0,
                "fecha_deteccion": str(row[8]) if row[8] else None
            })

        cursor.close()
        conn.close()

        return anomalias

    except Exception as e:
        print(f"❌ Error obteniendo anomalías pendientes: {e}")
        return []


def marcar_anomalia_revisada(anomalia_id: int, estado: str = "revisada") -> bool:
    """
    Marca una anomalía como revisada

    Args:
        anomalia_id: ID de la anomalía
        estado: Nuevo estado (revisada, ignorada, corregida)

    Returns:
        True si se actualizó correctamente
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                UPDATE reportes_anomalias
                SET estado = %s
                WHERE id = %s
            """, (estado, anomalia_id))
        else:
            cursor.execute("""
                UPDATE reportes_anomalias
                SET estado = ?
                WHERE id = ?
            """, (estado, anomalia_id))

        conn.commit()
        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"❌ Error marcando anomalía como revisada: {e}")
        return False


# ==========================================
# TESTING
# ==========================================
if __name__ == "__main__":
    print("🧪 Testing anomaly_monitor.py")
    print("=" * 60)

    # Test: Guardar reporte de anomalía
    print("\n📋 Test: Guardar reporte de anomalía")

    metricas_test = {
        "productos_originales": 10,
        "productos_corregidos": 8,
        "productos_eliminados": 2,
        "diferencia_inicial": 5000,
        "diferencia_final": 500,
        "porcentaje_inicial": 15.5,
        "porcentaje_final": 2.3
    }

    # Nota: Este test requiere conexión a BD
    print("⚠️ Test requiere conexión a base de datos")
    print("   Para testing completo, ejecutar con BD disponible")

    print("\n" + "=" * 60)
    print("✅ Módulo cargado correctamente")
