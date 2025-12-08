"""
GUÍA DE INTEGRACIÓN - Sistema de Aprendizaje
=============================================

Este documento explica cómo integrar el sistema de aprendizaje con tu código existente.

ARCHIVOS NECESARIOS:
1. migrations/001_productos_alias.sql  → Ejecutar en PostgreSQL
2. learning_system.py                  → Módulo de aprendizaje
3. api_aprendizaje.py                  → Endpoints del admin
4. Modificaciones a product_matcher.py → Ver abajo

=============================================
"""

# =============================================================================
# PASO 1: Ejecutar la migración SQL
# =============================================================================
"""
Conecta a tu PostgreSQL de Railway y ejecuta:

psql $DATABASE_URL < migrations/001_productos_alias.sql

Esto crea:
- Tabla productos_alias (guarda los alias aprendidos)
- Tabla correcciones_pendientes (para revisión)
- Tabla aprendizaje_stats (estadísticas)
- Funciones SQL auxiliares
"""

# =============================================================================
# PASO 2: Modificar product_matcher.py
# =============================================================================
"""
En tu función buscar_o_crear_producto_inteligente(), agrega esto
AL INICIO, ANTES de cualquier otra búsqueda:
"""

# --- CÓDIGO A AGREGAR EN product_matcher.py ---


def buscar_o_crear_producto_inteligente(
    codigo: str,
    nombre_ocr: str,
    precio: int,
    establecimiento_id: int,
    establecimiento_nombre: str,
    cursor,
    conn,
) -> dict:
    """
    Busca o crea un producto con el siguiente orden de prioridad:

    1. 🧠 ALIAS APRENDIDOS (NUEVO) - Busca en correcciones previas
    2. 📦 PAPA - Productos ya validados
    3. 🔍 AUDITORÍA por EAN
    4. 🔍 AUDITORÍA por nombre
    5. 🌐 WEB VTEX
    6. 💾 CACHE
    7. 🆕 CREAR NUEVO
    """

    print(f"\n{'='*60}")
    print(f"🔍 BUSCANDO PRODUCTO")
    print(f"   Código: {codigo or 'N/A'}")
    print(f"   Nombre OCR: {nombre_ocr[:50]}")
    print(f"   Precio: ${precio:,}")
    print(f"{'='*60}")

    # =========================================================================
    # 🧠 PASO 0: BUSCAR EN ALIAS APRENDIDOS (NUEVO)
    # =========================================================================
    try:
        from learning_system import (
            buscar_producto_por_alias,
            registrar_matching_exitoso,
        )

        alias_match = buscar_producto_por_alias(
            cursor=cursor,
            texto_ocr=nombre_ocr,
            establecimiento_id=establecimiento_id,
            codigo=codigo,
        )

        if alias_match and alias_match.get("confianza", 0) >= 0.80:
            print(f"   🧠 MATCH POR ALIAS APRENDIDO!")
            print(f"      Producto: {alias_match['nombre_consolidado'][:40]}")
            print(f"      Confianza: {alias_match['confianza']:.0%}")
            print(f"      Fuente: {alias_match['fuente']}")
            print(f"      Usos previos: {alias_match.get('veces_usado', 0)}")

            return {
                "producto_id": alias_match["producto_maestro_id"],
                "nombre": alias_match["nombre_consolidado"],
                "fuente": alias_match["fuente"],
                "confianza": alias_match["confianza"],
                "es_nuevo": False,
            }
    except ImportError:
        # learning_system no instalado, continuar sin él
        print(f"   ⚠️ Sistema de aprendizaje no disponible")
    except Exception as e:
        print(f"   ⚠️ Error en búsqueda de alias: {e}")

    # =========================================================================
    # Continuar con el flujo normal existente...
    # =========================================================================

    # ... (resto de tu código de product_matcher)

    # =========================================================================
    # AL FINAL: Cuando encuentres un match exitoso, registrarlo para aprendizaje
    # =========================================================================
    """
    Después de encontrar un match (por EAN, similitud, etc.), agregar:

    try:
        from learning_system import registrar_matching_exitoso
        registrar_matching_exitoso(
            cursor=cursor,
            conn=conn,
            texto_ocr=nombre_ocr,
            producto_maestro_id=producto_id,
            establecimiento_id=establecimiento_id,
            codigo=codigo,
            confianza=confianza_del_match
        )
    except:
        pass  # No fallar si el aprendizaje falla
    """


# =============================================================================
# PASO 3: Agregar endpoints al main.py
# =============================================================================
"""
En tu main.py, agrega:

from api_aprendizaje import router as aprendizaje_router
app.include_router(aprendizaje_router)

Esto habilita:
- POST /api/admin/corregir-item     → Corregir y aprender
- POST /api/admin/aprender          → Enseñar alias manual
- GET  /api/admin/aprendizaje/stats → Ver estadísticas
- GET  /api/admin/alias/{id}        → Ver alias de producto
- DELETE /api/admin/alias/{id}      → Eliminar alias malo
"""


# =============================================================================
# PASO 4: Modificar el Admin Dashboard
# =============================================================================
"""
En tu dashboard de admin, cuando el usuario corrige un item:

1. Usuario ve item con producto incorrecto
2. Usuario selecciona el producto correcto
3. Dashboard hace POST a /api/admin/corregir-item
4. Sistema actualiza el item Y aprende la corrección
5. Próxima vez que vea ese texto OCR → ya sabe qué es

Ejemplo de llamada desde el admin:

fetch('/api/admin/corregir-item', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
        item_factura_id: 12345,
        producto_maestro_id_correcto: 678,
        usuario_id: 1
    })
})
"""


# =============================================================================
# FLUJO COMPLETO DE APRENDIZAJE
# =============================================================================
"""
ANTES (sin aprendizaje):
========================
1. Usuario escanea "P HIG ROSAL30H 12UND"
2. OCR no reconoce → crea producto nuevo con nombre feo
3. Admin corrige manualmente
4. Usuario escanea otra factura con "P HIG ROSAL30H 12UND"
5. OCR no reconoce → MISMO ERROR
6. Admin tiene que corregir de nuevo... ∞

DESPUÉS (con aprendizaje):
==========================
1. Usuario escanea "P HIG ROSAL30H 12UND"
2. Sistema busca en alias → no encuentra
3. Sistema usa matching normal → crea/asigna producto
4. Admin corrige → "PAPEL HIGIÉNICO ROSAL 30M X12"
5. Sistema APRENDE: guarda alias en productos_alias
6. Usuario escanea otra factura con "P HIG ROSAL30H 12UND"
7. Sistema busca en alias → ¡ENCUENTRA!
8. Asigna automáticamente el producto correcto ✅
9. Admin no tiene que hacer nada 🎉
"""


# =============================================================================
# EJEMPLO: Cómo se ve en la práctica
# =============================================================================
"""
=== PRIMERA VEZ ===
📄 Procesando factura OXXO #1234
🔍 BUSCANDO PRODUCTO
   Código: N/A
   Nombre OCR: P HIG ROSAL30H 12UND

   🧠 Buscando en alias aprendidos... No encontrado
   📦 Buscando en PAPA... No encontrado
   🔍 Buscando en auditoría por nombre... Encontrado con 72% similitud
   ✅ Asignado a: PAPEL HIGIÉNICO ROSAL ULTRACONFORT 30M X12

[Admin corrige porque el match no era exacto]
   🧠 APRENDIDO: 'P HIG ROSAL30H 12UND' → 'PAPEL HIGIÉNICO ROSAL PLUS 30M X12'

=== SEGUNDA VEZ ===
📄 Procesando factura OXXO #1235
🔍 BUSCANDO PRODUCTO
   Código: N/A
   Nombre OCR: P HIG ROSAL30H 12UND

   🧠 MATCH POR ALIAS APRENDIDO!
      Producto: PAPEL HIGIÉNICO ROSAL PLUS 30M X12
      Confianza: 95%
      Fuente: alias_correccion_admin
      Usos previos: 1
   ✅ Producto asignado correctamente (sin intervención del admin)
"""


# =============================================================================
# ESTADÍSTICAS QUE PUEDES VER
# =============================================================================
"""
GET /api/admin/aprendizaje/stats retorna:

{
    "total_alias": 1547,
    "por_fuente": {
        "correccion_admin": {"count": 234, "avg_confianza": 0.98},
        "correccion_usuario": {"count": 89, "avg_confianza": 0.94},
        "ocr_automatico": {"count": 1224, "avg_confianza": 0.82}
    },
    "mas_usados": [
        {"alias": "P HIG ROSAL30H", "producto": "PAPEL HIGIÉNICO ROSAL", "usos": 156},
        {"alias": "HUEV ORO AA X30", "producto": "HUEVOS ORO TIPO AA X30", "usos": 89}
    ],
    "correcciones_semana": 45,
    "matchings_por_alias_semana": 1203
}
"""


if __name__ == "__main__":
    print("=" * 60)
    print("📚 GUÍA DE INTEGRACIÓN - Sistema de Aprendizaje")
    print("=" * 60)
    print(
        """
Este archivo es solo documentación.

Para implementar el sistema de aprendizaje:

1. Ejecuta la migración SQL en tu base de datos
2. Copia learning_system.py a tu proyecto
3. Copia api_aprendizaje.py a tu proyecto
4. Modifica product_matcher.py según las instrucciones
5. Agrega el router a main.py
6. Actualiza tu dashboard de admin

¡El sistema empezará a aprender de cada corrección!
    """
    )
