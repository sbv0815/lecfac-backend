# integracion_auditoria_facturas.py
"""
INTEGRACIÓN: Auditoría automática después de procesar factura

Este código debe agregarse DESPUÉS de que claude_invoice.py procese la factura
"""

from auditoria_automatica import AuditoriaAutomatica, ReporteAuditoria
from typing import Dict, List


async def procesar_factura_con_auditoria(
    factura_id: int,
    establecimiento: str,
    total_factura: float,
    productos: List[Dict],
    usar_ia: bool = False,
) -> Dict:
    """
    Procesa una factura recién subida con auditoría automática

    Flujo:
    1. Claude Vision ya extrajo los datos (claude_invoice.py)
    2. Validar datos con auditoría automática
    3. Si puntaje < 80% y usar_ia=True → Auditoría IA
    4. Actualizar estado en BD
    5. Retornar resultado

    Args:
        factura_id: ID de la factura en BD
        establecimiento: Nombre del establecimiento
        total_factura: Total de la factura
        productos: Lista de productos extraídos por Claude Vision
        usar_ia: Si True, usa Claude API para casos complejos

    Returns:
        Dict con resultado de la auditoría
    """
    try:
        # 1. Preparar datos para auditoría
        factura_data = {
            "id": factura_id,
            "establecimiento": establecimiento,
            "total_factura": total_factura,
        }

        items_data = []
        for prod in productos:
            items_data.append(
                {
                    "nombre": prod.get("nombre", ""),
                    "codigo_ean": prod.get("codigo", ""),
                    "cantidad": prod.get("cantidad", 1),
                    "precio_unitario": prod.get("precio", 0),
                    "precio_total": prod.get("valor", 0),
                }
            )

        # 2. Ejecutar auditoría automática (código, sin costo)
        print(f"\n🔍 Auditando factura #{factura_id}...")
        reporte = ReporteAuditoria.generar_reporte_factura(factura_data, items_data)

        print(f"📊 Puntaje de calidad: {reporte['puntaje_calidad']}/100")
        print(f"✅ Estado sugerido: {reporte['estado_sugerido']}")

        # 3. Si el puntaje es bajo y se solicitó IA → Usar Claude
        if reporte["puntaje_calidad"] < 80 and usar_ia:
            print("🤖 Aplicando auditoría IA...")
            from api_auditoria_ia import AuditoriaIA

            productos_normalizados = []
            for item in items_data:
                if item.get("nombre"):
                    resultado_ia = AuditoriaIA.normalizar_nombre_con_ia(item["nombre"])

                    if resultado_ia["confianza"] >= 0.8:
                        item["nombre_normalizado"] = resultado_ia["nombre_normalizado"]
                        item["marca"] = resultado_ia["marca"]
                        item["categoria"] = resultado_ia["categoria"]
                        item["confianza_ia"] = resultado_ia["confianza"]

                    productos_normalizados.append(item)

            reporte["productos_normalizados"] = productos_normalizados
            reporte["auditoria_ia_aplicada"] = True
        else:
            reporte["auditoria_ia_aplicada"] = False

        # 4. Actualizar estado en BD
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        nuevo_estado = reporte["estado_sugerido"]
        cursor.execute(
            "UPDATE facturas SET estado_validacion = ? WHERE id = ?",
            (nuevo_estado, factura_id),
        )
        conn.commit()
        conn.close()

        print(f"✅ Factura #{factura_id} → Estado: {nuevo_estado}\n")

        return {
            "success": True,
            "factura_id": factura_id,
            "puntaje_calidad": reporte["puntaje_calidad"],
            "estado": nuevo_estado,
            "problemas": reporte["problemas"],
            "sugerencias": reporte["sugerencias"],
            "auditoria_ia_aplicada": reporte["auditoria_ia_aplicada"],
        }

    except Exception as e:
        import traceback

        traceback.print_exc()
        return {"success": False, "factura_id": factura_id, "error": str(e)}


# ==========================================
# CÓDIGO PARA AGREGAR EN TU API DE FACTURAS
# ==========================================

"""
INSTRUCCIONES DE INTEGRACIÓN:

1. En tu archivo donde procesas facturas (probablemente api_facturas.py o similar),
   después de guardar la factura en BD, agregar:

```python
from integracion_auditoria_facturas import procesar_factura_con_auditoria

@router.post("/facturas/subir")
async def subir_factura(...):
    # ... tu código existente para procesar el video con claude_invoice.py ...

    # Después de guardar en BD:
    factura_id = cursor.lastrowid

    # 🚀 AUDITAR AUTOMÁTICAMENTE (sin IA para que sea rápido)
    resultado_auditoria = await procesar_factura_con_auditoria(
        factura_id=factura_id,
        establecimiento=result["data"]["establecimiento"],
        total_factura=result["data"]["total"],
        productos=result["data"]["productos"],
        usar_ia=False  # Cambiar a True solo si quieres usar Claude API (costo)
    )

    # Opcional: agregar info de auditoría a la respuesta
    return {
        "success": True,
        "factura_id": factura_id,
        "mensaje": "Factura procesada y auditada",
        "auditoria": resultado_auditoria
    }
```

2. Asegúrate de que la tabla facturas tiene la columna estado_validacion:

```sql
ALTER TABLE facturas ADD COLUMN estado_validacion TEXT DEFAULT 'pendiente';
```

3. Listo! Ahora cada factura se audita automáticamente al subirla.
"""


# ==========================================
# NORMALIZACIÓN DE PRODUCTOS DURANTE CARGA
# ==========================================


def normalizar_productos_durante_carga(productos: List[Dict]) -> List[Dict]:
    """
    Normaliza nombres de productos ANTES de guardarlos en BD

    Esto mejora la calidad de datos desde el primer momento
    """
    audit = AuditoriaAutomatica()
    productos_normalizados = []

    for prod in productos:
        # Normalizar nombre
        nombre_original = prod.get("nombre", "")
        nombre_limpio = audit.normalizar_nombre_producto(nombre_original)

        # Detectar categoría si no tiene código EAN
        categoria = None
        if not prod.get("codigo"):
            categoria = audit.detectar_categoria(nombre_limpio)
            if categoria:
                # Generar código interno
                codigo_interno = audit.generar_codigo_interno(nombre_limpio, categoria)
                prod["codigo_interno"] = codigo_interno
                prod["categoria"] = categoria

        # Normalizar establecimiento
        if "establecimiento" in prod:
            establecimiento_normalizado, es_conocido = audit.normalizar_establecimiento(
                prod["establecimiento"]
            )
            prod["establecimiento_normalizado"] = establecimiento_normalizado
            prod["establecimiento_conocido"] = es_conocido

        prod["nombre_normalizado"] = nombre_limpio
        prod["nombre_original"] = nombre_original

        productos_normalizados.append(prod)

    return productos_normalizados


# ==========================================
# EJEMPLO COMPLETO DE INTEGRACIÓN
# ==========================================

"""
EJEMPLO COMPLETO: Cómo se vería tu endpoint de subir factura con todo integrado

```python
from fastapi import APIRouter, UploadFile, File, HTTPException
from claude_invoice import parse_invoice_with_claude
from integracion_auditoria_facturas import (
    procesar_factura_con_auditoria,
    normalizar_productos_durante_carga
)
from database import get_db_connection
import shutil
import os

router = APIRouter()

@router.post("/api/facturas/subir")
async def subir_factura(
    usuario_id: int,
    video: UploadFile = File(...),
    auditar_con_ia: bool = False  # Opcional: permite al usuario elegir
):
    try:
        # 1. Guardar video temporalmente
        temp_path = f"/tmp/{video.filename}"
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(video.file, buffer)

        # 2. Procesar con Claude Vision
        print("📹 Procesando video con Claude Vision...")
        result = parse_invoice_with_claude(temp_path)

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])

        data = result["data"]

        # 3. Normalizar productos ANTES de guardar
        print("🧹 Normalizando productos...")
        productos_normalizados = normalizar_productos_durante_carga(data["productos"])

        # 4. Guardar factura en BD
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            '''
            INSERT INTO facturas (
                usuario_id,
                establecimiento,
                total_factura,
                fecha_compra,
                tiene_imagen,
                estado_validacion
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                usuario_id,
                data["establecimiento"],
                data["total"],
                data.get("fecha", "2025-01-01"),
                True,
                "pendiente"  # Se actualizará con la auditoría
            )
        )

        factura_id = cursor.lastrowid

        # 5. Guardar productos
        for prod in productos_normalizados:
            cursor.execute(
                '''
                INSERT INTO items_factura (
                    factura_id,
                    usuario_id,
                    nombre_producto,
                    codigo_ean,
                    cantidad,
                    precio_unitario,
                    precio_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    factura_id,
                    usuario_id,
                    prod["nombre_normalizado"],
                    prod.get("codigo", ""),
                    prod.get("cantidad", 1),
                    prod.get("precio", 0),
                    prod.get("valor", 0)
                )
            )

        conn.commit()
        conn.close()

        # 6. 🚀 AUDITAR AUTOMÁTICAMENTE
        print("🔍 Auditando factura...")
        resultado_auditoria = await procesar_factura_con_auditoria(
            factura_id=factura_id,
            establecimiento=data["establecimiento"],
            total_factura=data["total"],
            productos=productos_normalizados,
            usar_ia=auditar_con_ia
        )

        # 7. Limpiar archivo temporal
        os.remove(temp_path)

        # 8. Responder
        return {
            "success": True,
            "factura_id": factura_id,
            "productos_guardados": len(productos_normalizados),
            "establecimiento": data["establecimiento"],
            "total": data["total"],
            "auditoria": {
                "puntaje_calidad": resultado_auditoria["puntaje_calidad"],
                "estado": resultado_auditoria["estado"],
                "problemas": len(resultado_auditoria["problemas"]),
                "ia_usada": resultado_auditoria["auditoria_ia_aplicada"]
            },
            "mensaje": "Factura procesada y auditada exitosamente"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
```
"""

print("✅ Módulo de integración de auditoría cargado")
