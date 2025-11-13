"""
plu_consolidator.py - Consolidación inteligente de productos PLU
========================================================================
Sistema de aprendizaje automático para productos sin EAN

🎯 OBJETIVO:
Consolidar variaciones de nombres OCR basándose en el código PLU

EJEMPLO:
- PLU: 1570809 + "AREPA DODO PAISA" → 10 veces
- PLU: 1570809 + "AREPA DONA PAISA" → 50 veces
- PLU: 1570809 + "AREPA DORA PAISA" → 30 veces
→ Nombre oficial: "AREPA DONA PAISA" (el más frecuente)

⚠️ FILOSOFÍA DE DISEÑO:
- NO modifica el flujo existente
- Solo se activa con flag ENABLE_PLU_CONSOLIDATION
- Completamente reversible
"""

import os
from typing import Optional, Dict

# ═══════════════════════════════════════════════════════════════
# CONFIGURACIÓN - Fácil de activar/desactivar
# ═══════════════════════════════════════════════════════════════
ENABLE_PLU_CONSOLIDATION = os.environ.get("ENABLE_PLU_CONSOLIDATION", "false").lower() == "true"


def buscar_nombre_mas_frecuente_por_plu(
    plu_code: str,
    establecimiento: str,
    cursor
) -> Optional[Dict[str, any]]:
    """
    Busca el nombre más frecuente para un código PLU específico

    Args:
        plu_code: Código PLU (ej: "1570809")
        establecimiento: Cadena (ej: "OLIMPICA")
        cursor: Cursor de base de datos

    Returns:
        Dict con nombre_oficial y frecuencia, o None si no hay datos
    """

    # ⚠️ SAFETY CHECK: Solo activar si flag está habilitado
    if not ENABLE_PLU_CONSOLIDATION:
        return None

    try:
        import os
        is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
        param = "%s" if is_postgresql else "?"

        # Buscar todos los nombres usados para este PLU
        cursor.execute(f"""
            SELECT
                pm.nombre_normalizado,
                COUNT(*) as frecuencia,
                MAX(pm.ultima_actualizacion) as ultima_vez
            FROM productos_maestros pm
            INNER JOIN items_factura if ON if.producto_maestro_id = pm.id
            INNER JOIN facturas_procesadas fp ON if.factura_id = fp.id
            WHERE pm.codigo_ean = {param}
              AND fp.establecimiento_nombre ILIKE {param}
            GROUP BY pm.nombre_normalizado
            ORDER BY frecuencia DESC, ultima_vez DESC
            LIMIT 5
        """, (plu_code, f"%{establecimiento}%"))

        resultados = cursor.fetchall()

        if not resultados or len(resultados) == 0:
            return None

        # El más frecuente está primero
        nombre_mas_usado = resultados[0][0]
        frecuencia = resultados[0][1]

        print(f"   🔍 PLU {plu_code} consolidación:")
        print(f"      Nombre más usado: {nombre_mas_usado}")
        print(f"      Frecuencia: {frecuencia} veces")

        # Si hay variantes, mostrarlas
        if len(resultados) > 1:
            print(f"      Variantes encontradas:")
            for i, (nombre_var, freq_var, _) in enumerate(resultados[1:], 1):
                print(f"         {i}. {nombre_var} ({freq_var} veces)")

        return {
            'nombre_oficial': nombre_mas_usado,
            'frecuencia': frecuencia,
            'variantes': len(resultados),
            'confianza': min(0.70 + (frecuencia * 0.05), 0.95)
        }

    except Exception as e:
        print(f"   ⚠️ Error en consolidación PLU: {e}")
        return None


def aplicar_consolidacion_plu(
    codigo: str,
    nombre_ocr: str,
    tipo_codigo: str,
    establecimiento: str,
    cursor
) -> Optional[str]:
    """
    Aplica consolidación PLU si está habilitada

    Args:
        codigo: Código del producto
        nombre_ocr: Nombre leído por OCR
        tipo_codigo: "EAN", "PLU" o "DESCONOCIDO"
        establecimiento: Cadena
        cursor: Cursor de BD

    Returns:
        Nombre consolidado o None si no aplica
    """

    # SAFETY CHECK 1: Flag deshabilitado
    if not ENABLE_PLU_CONSOLIDATION:
        return None

    # SAFETY CHECK 2: Solo para PLU
    if tipo_codigo != 'PLU':
        return None

    # SAFETY CHECK 3: Debe tener código
    if not codigo or not codigo.strip():
        return None

    # Buscar nombre más frecuente
    resultado = buscar_nombre_mas_frecuente_por_plu(
        plu_code=codigo,
        establecimiento=establecimiento,
        cursor=cursor
    )

    if not resultado:
        return None

    # Solo usar si tiene confianza suficiente
    if resultado['confianza'] < 0.70:
        return None

    print(f"   ✅ CONSOLIDACIÓN PLU APLICADA")
    print(f"      OCR original: {nombre_ocr}")
    print(f"      Nombre consolidado: {resultado['nombre_oficial']}")
    print(f"      Confianza: {resultado['confianza']:.0%}")

    return resultado['nombre_oficial']


# ═══════════════════════════════════════════════════════════════
# STATUS DE CARGA
# ═══════════════════════════════════════════════════════════════
print("="*80)
if ENABLE_PLU_CONSOLIDATION:
    print("✅ plu_consolidator.py CARGADO - CONSOLIDACIÓN PLU HABILITADA")
else:
    print("⚠️  plu_consolidator.py CARGADO - CONSOLIDACIÓN PLU DESHABILITADA")
print("   Para habilitar: export ENABLE_PLU_CONSOLIDATION=true")
print("="*80)
