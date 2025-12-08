"""
learning_system.py - Sistema de Aprendizaje de Correcciones OCR
================================================================
Este módulo permite que LecFac "aprenda" de las correcciones del usuario.

FLUJO DE APRENDIZAJE:
1. Usuario escanea factura con "P HIG ROSAL30H 12UND"
2. Sistema no lo reconoce → lo guarda con producto_maestro_id temporal
3. Usuario corrige en admin → "PAPEL HIGIÉNICO ROSAL 30M X12"
4. Sistema APRENDE: guarda el alias en productos_alias
5. Próxima vez que vea "P HIG ROSAL30H 12UND" → ya sabe qué producto es

INTEGRACIÓN:
- Se usa ANTES del matching por similitud
- Si encuentra alias → retorna producto con confianza alta
- Si no encuentra → continúa con el flujo normal

AUTOR: LecFac Team
VERSIÓN: 1.0
FECHA: 2025-12-08
================================================================
"""

import os
import re
from typing import Dict, Optional, List, Tuple
from datetime import datetime


def normalizar_texto_para_alias(texto: str) -> str:
    """
    Normaliza un texto OCR para búsqueda en alias.
    - Lowercase
    - Elimina espacios extras
    - Elimina caracteres especiales
    """
    if not texto:
        return ""

    # Lowercase y trim
    texto = texto.lower().strip()

    # Reemplazar múltiples espacios por uno solo
    texto = re.sub(r"\s+", " ", texto)

    # Eliminar caracteres especiales excepto espacios y alfanuméricos
    texto = re.sub(r"[^\w\s]", "", texto)

    return texto


def buscar_producto_por_alias(
    cursor, texto_ocr: str, establecimiento_id: int = None, codigo: str = None
) -> Optional[Dict]:
    """
    Busca un producto en la tabla de alias aprendidos.

    Args:
        cursor: Cursor de base de datos
        texto_ocr: Texto tal como lo leyó el OCR
        establecimiento_id: ID del establecimiento (opcional)
        codigo: Código leído (opcional)

    Returns:
        Dict con producto_maestro_id, nombre_consolidado, confianza, fuente
        o None si no encontró
    """
    try:
        alias_normalizado = normalizar_texto_para_alias(texto_ocr)

        if not alias_normalizado:
            return None

        # Primero buscar por código si lo tenemos
        if codigo and len(codigo) >= 3:
            cursor.execute(
                """
                SELECT
                    pa.producto_maestro_id,
                    pm.nombre_consolidado,
                    pm.codigo_ean,
                    pa.confianza,
                    pa.fuente,
                    pa.veces_usado
                FROM productos_alias pa
                JOIN productos_maestros_v2 pm ON pa.producto_maestro_id = pm.id
                WHERE pa.codigo_asociado = %s
                  AND (pa.establecimiento_id IS NULL OR pa.establecimiento_id = %s)
                ORDER BY
                    CASE WHEN pa.establecimiento_id = %s THEN 0 ELSE 1 END,
                    pa.confianza DESC
                LIMIT 1
            """,
                (codigo, establecimiento_id, establecimiento_id),
            )

            resultado = cursor.fetchone()
            if resultado:
                # Actualizar estadísticas de uso
                cursor.execute(
                    """
                    UPDATE productos_alias
                    SET veces_usado = veces_usado + 1,
                        ultima_vez_usado = CURRENT_TIMESTAMP
                    WHERE codigo_asociado = %s
                """,
                    (codigo,),
                )

                return {
                    "producto_maestro_id": resultado[0],
                    "nombre_consolidado": resultado[1],
                    "codigo_ean": resultado[2],
                    "confianza": float(resultado[3]),
                    "fuente": f"alias_codigo_{resultado[4]}",
                    "veces_usado": resultado[5],
                }

        # Buscar por texto normalizado
        cursor.execute(
            """
            SELECT
                pa.producto_maestro_id,
                pm.nombre_consolidado,
                pm.codigo_ean,
                pa.confianza,
                pa.fuente,
                pa.veces_usado
            FROM productos_alias pa
            JOIN productos_maestros_v2 pm ON pa.producto_maestro_id = pm.id
            WHERE pa.alias_normalizado = %s
              AND (pa.establecimiento_id IS NULL OR pa.establecimiento_id = %s)
            ORDER BY
                CASE WHEN pa.establecimiento_id = %s THEN 0 ELSE 1 END,
                pa.confianza DESC,
                pa.veces_usado DESC
            LIMIT 1
        """,
            (alias_normalizado, establecimiento_id, establecimiento_id),
        )

        resultado = cursor.fetchone()

        if resultado:
            # Actualizar estadísticas de uso
            cursor.execute(
                """
                UPDATE productos_alias
                SET veces_usado = veces_usado + 1,
                    ultima_vez_usado = CURRENT_TIMESTAMP
                WHERE alias_normalizado = %s
                  AND (establecimiento_id IS NULL OR establecimiento_id = %s)
            """,
                (alias_normalizado, establecimiento_id),
            )

            print(
                f"   🧠 APRENDIDO: '{texto_ocr[:30]}' → '{resultado[1][:30]}' (confianza: {resultado[3]:.0%})"
            )

            return {
                "producto_maestro_id": resultado[0],
                "nombre_consolidado": resultado[1],
                "codigo_ean": resultado[2],
                "confianza": float(resultado[3]),
                "fuente": f"alias_{resultado[4]}",
                "veces_usado": resultado[5],
            }

        return None

    except Exception as e:
        print(f"   ⚠️ Error buscando alias: {e}")
        return None


def aprender_correccion(
    cursor,
    conn,
    texto_ocr: str,
    producto_maestro_id: int,
    establecimiento_id: int = None,
    codigo: str = None,
    usuario_id: int = None,
    fuente: str = "correccion_usuario",
) -> bool:
    """
    Aprende una corrección guardándola como alias.

    Args:
        cursor: Cursor de base de datos
        conn: Conexión de base de datos
        texto_ocr: Texto original del OCR
        producto_maestro_id: ID del producto correcto
        establecimiento_id: ID del establecimiento (opcional)
        codigo: Código asociado (opcional)
        usuario_id: ID del usuario que hizo la corrección
        fuente: Origen de la corrección

    Returns:
        True si se guardó correctamente
    """
    try:
        alias_normalizado = normalizar_texto_para_alias(texto_ocr)

        if not alias_normalizado or len(alias_normalizado) < 3:
            print(f"   ⚠️ Texto muy corto para aprender: '{texto_ocr}'")
            return False

        # Determinar confianza según fuente
        confianza_map = {
            "correccion_admin": 0.99,
            "correccion_usuario": 0.95,
            "ocr_automatico": 0.80,
            "importacion": 0.90,
            "manual": 0.95,
        }
        confianza = confianza_map.get(fuente, 0.80)

        # Insertar o actualizar alias
        cursor.execute(
            """
            INSERT INTO productos_alias (
                producto_maestro_id,
                alias_texto,
                alias_normalizado,
                codigo_asociado,
                establecimiento_id,
                fuente,
                confianza,
                creado_por,
                veces_usado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1)
            ON CONFLICT (alias_normalizado, establecimiento_id)
            DO UPDATE SET
                producto_maestro_id = EXCLUDED.producto_maestro_id,
                veces_usado = productos_alias.veces_usado + 1,
                ultima_vez_usado = CURRENT_TIMESTAMP,
                fecha_actualizacion = CURRENT_TIMESTAMP,
                confianza = LEAST(0.99, productos_alias.confianza + 0.05)
            RETURNING id
        """,
            (
                producto_maestro_id,
                texto_ocr[:255],
                alias_normalizado[:255],
                codigo[:50] if codigo else None,
                establecimiento_id,
                fuente,
                confianza,
                usuario_id,
            ),
        )

        alias_id = cursor.fetchone()[0]
        conn.commit()

        # Obtener nombre del producto para log
        cursor.execute(
            "SELECT nombre_consolidado FROM productos_maestros_v2 WHERE id = %s",
            (producto_maestro_id,),
        )
        nombre_producto = cursor.fetchone()
        nombre = nombre_producto[0] if nombre_producto else "?"

        print(f"   🧠 APRENDIDO: '{texto_ocr[:40]}' → '{nombre[:40]}' (ID: {alias_id})")

        # Actualizar estadísticas
        cursor.execute(
            """
            INSERT INTO aprendizaje_stats (fecha, correcciones_aprendidas)
            VALUES (CURRENT_DATE, 1)
            ON CONFLICT (fecha) DO UPDATE SET
                correcciones_aprendidas = aprendizaje_stats.correcciones_aprendidas + 1
        """
        )
        conn.commit()

        return True

    except Exception as e:
        print(f"   ❌ Error aprendiendo corrección: {e}")
        import traceback

        traceback.print_exc()
        conn.rollback()
        return False


def aprender_desde_item_factura(
    cursor,
    conn,
    item_factura_id: int,
    producto_maestro_id_correcto: int,
    usuario_id: int = None,
) -> bool:
    """
    Aprende una corrección a partir de un item de factura.
    Útil para el admin cuando corrige un item.

    Args:
        cursor: Cursor de base de datos
        conn: Conexión de base de datos
        item_factura_id: ID del item en items_factura
        producto_maestro_id_correcto: ID del producto correcto
        usuario_id: ID del usuario que hizo la corrección
    """
    try:
        # Obtener datos del item
        cursor.execute(
            """
            SELECT
                itf.nombre_leido,
                itf.codigo_leido,
                f.establecimiento_id
            FROM items_factura itf
            JOIN facturas f ON itf.factura_id = f.id
            WHERE itf.id = %s
        """,
            (item_factura_id,),
        )

        item = cursor.fetchone()
        if not item:
            print(f"   ⚠️ Item {item_factura_id} no encontrado")
            return False

        nombre_ocr, codigo, establecimiento_id = item

        if not nombre_ocr:
            print(f"   ⚠️ Item {item_factura_id} sin nombre")
            return False

        # Aprender la corrección
        return aprender_correccion(
            cursor=cursor,
            conn=conn,
            texto_ocr=nombre_ocr,
            producto_maestro_id=producto_maestro_id_correcto,
            establecimiento_id=establecimiento_id,
            codigo=codigo,
            usuario_id=usuario_id,
            fuente="correccion_admin",
        )

    except Exception as e:
        print(f"   ❌ Error aprendiendo desde item: {e}")
        return False


def registrar_matching_exitoso(
    cursor,
    conn,
    texto_ocr: str,
    producto_maestro_id: int,
    establecimiento_id: int = None,
    codigo: str = None,
    confianza: float = 0.80,
) -> bool:
    """
    Registra un matching exitoso como alias con confianza menor.
    Se llama cuando el sistema hace match pero NO desde corrección manual.

    Esto permite que el sistema "confirme" sus propios matches con el tiempo.
    """
    try:
        # Solo registrar si la confianza es razonablemente alta
        if confianza < 0.70:
            return False

        alias_normalizado = normalizar_texto_para_alias(texto_ocr)

        if not alias_normalizado or len(alias_normalizado) < 3:
            return False

        # Verificar si ya existe un alias con mayor confianza
        cursor.execute(
            """
            SELECT confianza FROM productos_alias
            WHERE alias_normalizado = %s
              AND (establecimiento_id IS NULL OR establecimiento_id = %s)
        """,
            (alias_normalizado, establecimiento_id),
        )

        existente = cursor.fetchone()

        if existente and existente[0] >= confianza:
            # Ya existe con igual o mayor confianza, solo actualizar uso
            cursor.execute(
                """
                UPDATE productos_alias
                SET veces_usado = veces_usado + 1,
                    ultima_vez_usado = CURRENT_TIMESTAMP
                WHERE alias_normalizado = %s
                  AND (establecimiento_id IS NULL OR establecimiento_id = %s)
            """,
                (alias_normalizado, establecimiento_id),
            )
            conn.commit()
            return True

        # Insertar o actualizar con confianza del matching automático (menor)
        cursor.execute(
            """
            INSERT INTO productos_alias (
                producto_maestro_id,
                alias_texto,
                alias_normalizado,
                codigo_asociado,
                establecimiento_id,
                fuente,
                confianza,
                veces_usado
            ) VALUES (%s, %s, %s, %s, %s, 'ocr_automatico', %s, 1)
            ON CONFLICT (alias_normalizado, establecimiento_id)
            DO UPDATE SET
                veces_usado = productos_alias.veces_usado + 1,
                ultima_vez_usado = CURRENT_TIMESTAMP
                -- NO actualizar producto_maestro_id ni confianza si viene de automático
        """,
            (
                producto_maestro_id,
                texto_ocr[:255],
                alias_normalizado[:255],
                codigo[:50] if codigo else None,
                establecimiento_id,
                confianza,
            ),
        )

        conn.commit()
        return True

    except Exception as e:
        # No fallar silenciosamente, pero no interrumpir el flujo
        print(f"   ⚠️ Error registrando matching: {e}")
        return False


def obtener_estadisticas_aprendizaje(cursor) -> Dict:
    """
    Obtiene estadísticas del sistema de aprendizaje.
    """
    try:
        # Total de alias
        cursor.execute("SELECT COUNT(*) FROM productos_alias")
        total_alias = cursor.fetchone()[0]

        # Alias por fuente
        cursor.execute(
            """
            SELECT fuente, COUNT(*), AVG(confianza)
            FROM productos_alias
            GROUP BY fuente
        """
        )
        por_fuente = {
            row[0]: {"count": row[1], "avg_confianza": float(row[2] or 0)}
            for row in cursor.fetchall()
        }

        # Alias más usados
        cursor.execute(
            """
            SELECT
                pa.alias_texto,
                pm.nombre_consolidado,
                pa.veces_usado,
                pa.confianza
            FROM productos_alias pa
            JOIN productos_maestros_v2 pm ON pa.producto_maestro_id = pm.id
            ORDER BY pa.veces_usado DESC
            LIMIT 10
        """
        )
        mas_usados = [
            {
                "alias": row[0],
                "producto": row[1],
                "usos": row[2],
                "confianza": float(row[3]),
            }
            for row in cursor.fetchall()
        ]

        # Estadísticas del día
        cursor.execute(
            """
            SELECT
                COALESCE(SUM(correcciones_aprendidas), 0),
                COALESCE(SUM(matchings_por_alias), 0)
            FROM aprendizaje_stats
            WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
        """
        )
        stats_semana = cursor.fetchone()

        return {
            "total_alias": total_alias,
            "por_fuente": por_fuente,
            "mas_usados": mas_usados,
            "correcciones_semana": stats_semana[0] if stats_semana else 0,
            "matchings_por_alias_semana": stats_semana[1] if stats_semana else 0,
        }

    except Exception as e:
        print(f"Error obteniendo estadísticas: {e}")
        return {"error": str(e)}


def importar_correcciones_masivas(cursor, conn, correcciones: List[Dict]) -> Dict:
    """
    Importa múltiples correcciones de una vez.

    Args:
        correcciones: Lista de dicts con:
            - texto_ocr: str
            - producto_maestro_id: int
            - establecimiento_id: int (opcional)
            - codigo: str (opcional)

    Returns:
        Dict con estadísticas de importación
    """
    importados = 0
    errores = 0

    for correccion in correcciones:
        try:
            exito = aprender_correccion(
                cursor=cursor,
                conn=conn,
                texto_ocr=correccion["texto_ocr"],
                producto_maestro_id=correccion["producto_maestro_id"],
                establecimiento_id=correccion.get("establecimiento_id"),
                codigo=correccion.get("codigo"),
                fuente="importacion",
            )
            if exito:
                importados += 1
            else:
                errores += 1
        except Exception as e:
            print(f"Error importando: {e}")
            errores += 1

    return {"importados": importados, "errores": errores, "total": len(correcciones)}


# =============================================================================
# TEST
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🧠 SISTEMA DE APRENDIZAJE - TEST")
    print("=" * 60)

    # Test de normalización
    textos_test = [
        "P HIG ROSAL30H 12UND",
        "  HUEVOS  ORO   AA  X30UND  ",
        "ACE VEG GIRAS 1LT!!!",
    ]

    print("\n📝 Test de normalización:")
    for texto in textos_test:
        normalizado = normalizar_texto_para_alias(texto)
        print(f"   '{texto}' → '{normalizado}'")

    print("\n✅ Tests completados")
    print(
        "\nPara integrar con product_matcher, agregar al inicio de buscar_o_crear_producto_inteligente:"
    )
    print(
        """
    # PASO 0: Buscar en alias aprendidos
    from learning_system import buscar_producto_por_alias

    alias_match = buscar_producto_por_alias(cursor, nombre_ocr, establecimiento_id, codigo)
    if alias_match and alias_match['confianza'] >= 0.80:
        return {
            'producto_id': alias_match['producto_maestro_id'],
            'nombre': alias_match['nombre_consolidado'],
            'fuente': alias_match['fuente'],
            'confianza': alias_match['confianza']
        }
    """
    )
