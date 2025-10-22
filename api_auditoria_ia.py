# api_auditoria_ia.py
"""
API de auditoría inteligente que combina validaciones automáticas con IA de Claude
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Optional
import anthropic
import os
from database import get_db_connection
from auditoria_automatica import AuditoriaAutomatica, ReporteAuditoria

router = APIRouter(prefix="/api/auditoria", tags=["auditoria"])

# Cliente de Anthropic (asegúrate de tener ANTHROPIC_API_KEY en .env)
client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


class AuditoriaIA:
    """Auditoría avanzada usando Claude API"""

    @staticmethod
    def normalizar_nombre_con_ia(nombre: str, contexto: str = "") -> Dict:
        """
        Usa Claude para normalizar nombres de productos complejos

        Args:
            nombre: Nombre del producto a normalizar
            contexto: Contexto adicional (ej: "producto de supermercado")

        Returns:
            {
                "nombre_normalizado": str,
                "marca": str,
                "presentacion": str,
                "categoria": str,
                "confianza": float (0-1)
            }
        """
        prompt = f"""Analiza este nombre de producto de supermercado y normalizalo:

PRODUCTO: "{nombre}"

Necesito que extraigas:
1. **Nombre normalizado**: Escrito correctamente (mayúsculas/minúsculas apropiadas)
2. **Marca**: Si tiene marca explícita
3. **Presentación**: Tamaño/peso (ej: "1L", "500gr", "x12 unidades")
4. **Categoría**: Una de estas: LACTEOS, ASEO, BEBIDAS, CARNES, FRUTAS, VERDURAS, GRANOS, SNACKS, PANADERIA, OTRO

Responde SOLO en formato JSON:
{{
  "nombre_normalizado": "...",
  "marca": "..." o null,
  "presentacion": "..." o null,
  "categoria": "..."
}}

Ejemplos:
- "LECHE COLANTA 1100ML" → {{"nombre_normalizado": "Leche Colanta 1100ml", "marca": "Colanta", "presentacion": "1100ml", "categoria": "LACTEOS"}}
- "ARROZ DIANA X 500GR" → {{"nombre_normalizado": "Arroz Diana 500gr", "marca": "Diana", "presentacion": "500gr", "categoria": "GRANOS"}}
- "MANZANA ROJA KILO" → {{"nombre_normalizado": "Manzana Roja", "marca": null, "presentacion": "kg", "categoria": "FRUTAS"}}
"""

        try:
            message = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=500,
                temperature=0,  # Queremos respuestas consistentes
                messages=[{"role": "user", "content": prompt}],
            )

            # Extraer respuesta JSON
            respuesta = message.content[0].text.strip()

            # Remover markdown si existe
            if respuesta.startswith("```json"):
                respuesta = respuesta[7:]
            if respuesta.endswith("```"):
                respuesta = respuesta[:-3]

            import json

            resultado = json.loads(respuesta.strip())

            # Agregar confianza (alta porque Claude fue explícito)
            resultado["confianza"] = 0.95

            return resultado

        except Exception as e:
            print(f"Error en normalización IA: {e}")
            # Fallback a normalización por código
            return {
                "nombre_normalizado": AuditoriaAutomatica.normalizar_nombre_producto(
                    nombre
                ),
                "marca": None,
                "presentacion": None,
                "categoria": "OTRO",
                "confianza": 0.3,
            }

    @staticmethod
    def buscar_duplicados_con_ia(
        producto: str, catalogo_candidatos: List[str]
    ) -> List[Dict]:
        """
        Usa Claude para identificar productos duplicados con nombres diferentes

        Args:
            producto: Nombre del producto a buscar
            catalogo_candidatos: Lista de productos similares del catálogo

        Returns:
            Lista de matches con score de similitud
        """
        if not catalogo_candidatos:
            return []

        prompt = f"""Eres un experto en productos de supermercado.

PRODUCTO NUEVO: "{producto}"

PRODUCTOS EN CATÁLOGO:
{chr(10).join([f"{i+1}. {p}" for i, p in enumerate(catalogo_candidatos)])}

¿Cuáles de estos productos del catálogo son el MISMO producto que el nuevo?
Ten en cuenta variaciones de nombre, abreviaturas, errores ortográficos, etc.

Responde en formato JSON:
{{
  "matches": [
    {{"indice": 1, "score": 0.95, "razon": "Mismo producto con diferente formato"}},
    ...
  ]
}}

Solo incluye matches con score >= 0.7
Si no hay matches, devuelve: {{"matches": []}}
"""

        try:
            message = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=1000,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )

            respuesta = message.content[0].text.strip()

            if respuesta.startswith("```json"):
                respuesta = respuesta[7:]
            if respuesta.endswith("```"):
                respuesta = respuesta[:-3]

            import json

            resultado = json.loads(respuesta.strip())

            return resultado.get("matches", [])

        except Exception as e:
            print(f"Error en búsqueda de duplicados IA: {e}")
            return []


# ==========================================
# ENDPOINTS
# ==========================================


@router.post("/procesar-factura/{factura_id}")
async def procesar_factura_completa(factura_id: int, usar_ia: bool = True):
    """
    Procesa una factura con auditoría completa (código + IA opcional)

    Flujo:
    1. Validaciones automáticas (código)
    2. Si puntaje < 80% y usar_ia=True → Auditoría IA
    3. Generar reporte final
    4. Actualizar estado en BD
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Obtener factura
        cursor.execute("SELECT * FROM facturas WHERE id = ?", (factura_id,))
        factura_row = cursor.fetchone()
        if not factura_row:
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        factura = {
            "id": factura_row[0],
            "usuario_id": factura_row[1],
            "establecimiento": factura_row[2],
            "total_factura": factura_row[3],
            "fecha_compra": factura_row[4],
        }

        # 2. Obtener items
        cursor.execute(
            "SELECT * FROM items_factura WHERE factura_id = ?", (factura_id,)
        )
        items = []
        for row in cursor.fetchall():
            items.append(
                {
                    "id": row[0],
                    "nombre": row[3],
                    "codigo_ean": row[4],
                    "cantidad": row[5],
                    "precio_unitario": row[6],
                    "precio_total": row[7],
                }
            )

        # 3. Auditoría automática (código)
        reporte_auto = ReporteAuditoria.generar_reporte_factura(factura, items)

        # 4. Si puntaje bajo y se solicitó IA → Auditoría IA
        items_procesados = []
        if reporte_auto["puntaje_calidad"] < 80 and usar_ia:
            print(f"🤖 Aplicando auditoría IA a factura {factura_id}...")

            for item in items:
                # Normalizar nombre con IA
                resultado_ia = AuditoriaIA.normalizar_nombre_con_ia(item["nombre"])

                item_procesado = {
                    **item,
                    "nombre_normalizado_ia": resultado_ia["nombre_normalizado"],
                    "marca_detectada": resultado_ia["marca"],
                    "presentacion_detectada": resultado_ia["presentacion"],
                    "categoria_detectada": resultado_ia["categoria"],
                    "confianza_ia": resultado_ia["confianza"],
                }

                # Si la confianza es alta, actualizar en BD
                if resultado_ia["confianza"] >= 0.8:
                    # TODO: Actualizar item en BD con datos normalizados
                    pass

                items_procesados.append(item_procesado)

            reporte_auto["auditoria_ia_aplicada"] = True
            reporte_auto["items_normalizados"] = items_procesados
        else:
            reporte_auto["auditoria_ia_aplicada"] = False

        # 5. Actualizar estado en BD
        nuevo_estado = reporte_auto["estado_sugerido"]
        cursor.execute(
            "UPDATE facturas SET estado_validacion = ? WHERE id = ?",
            (nuevo_estado, factura_id),
        )
        conn.commit()
        conn.close()

        return {
            "success": True,
            "factura_id": factura_id,
            "reporte": reporte_auto,
        }

    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/procesar-lote")
async def procesar_lote_facturas(
    limite: int = 50, solo_pendientes: bool = True, usar_ia: bool = False
):
    """
    Procesa un lote de facturas pendientes de revisión

    Args:
        limite: Máximo de facturas a procesar
        solo_pendientes: Si True, solo procesa facturas con estado != 'validada'
        usar_ia: Si True, usa Claude API para casos complejos (consume créditos)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener facturas pendientes
        if solo_pendientes:
            cursor.execute(
                """
                SELECT id FROM facturas
                WHERE COALESCE(estado_validacion, 'pendiente') != 'validada'
                ORDER BY fecha_cargue ASC
                LIMIT ?
            """,
                (limite,),
            )
        else:
            cursor.execute(
                "SELECT id FROM facturas ORDER BY fecha_cargue DESC LIMIT ?", (limite,)
            )

        facturas_ids = [row[0] for row in cursor.fetchall()]
        conn.close()

        # Procesar cada factura
        resultados = []
        for factura_id in facturas_ids:
            try:
                resultado = await procesar_factura_completa(factura_id, usar_ia=usar_ia)
                resultados.append(resultado)
            except Exception as e:
                resultados.append(
                    {
                        "success": False,
                        "factura_id": factura_id,
                        "error": str(e),
                    }
                )

        # Resumen
        procesadas = sum(1 for r in resultados if r["success"])
        errores = len(resultados) - procesadas

        validadas = sum(
            1
            for r in resultados
            if r["success"] and r["reporte"]["estado_sugerido"] == "validada"
        )
        pendientes = sum(
            1
            for r in resultados
            if r["success"] and r["reporte"]["estado_sugerido"] == "pendiente"
        )
        rechazadas = sum(
            1
            for r in resultados
            if r["success"] and r["reporte"]["estado_sugerido"] == "rechazada"
        )

        return {
            "success": True,
            "total_procesadas": procesadas,
            "errores": errores,
            "resumen": {
                "validadas": validadas,
                "pendientes": pendientes,
                "rechazadas": rechazadas,
            },
            "resultados": resultados,
        }

    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cola-revision")
async def obtener_cola_revision(limite: int = 100):
    """
    Obtiene la cola de facturas pendientes de revisión manual
    Ordenadas por prioridad (puntaje más bajo primero)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                f.id,
                f.establecimiento,
                f.total_factura,
                f.fecha_cargue,
                f.estado_validacion,
                COUNT(i.id) as num_productos
            FROM facturas f
            LEFT JOIN items_factura i ON i.factura_id = f.id
            WHERE f.estado_validacion = 'pendiente'
            GROUP BY f.id, f.establecimiento, f.total_factura, f.fecha_cargue, f.estado_validacion
            ORDER BY f.fecha_cargue ASC
            LIMIT ?
        """,
            (limite,),
        )

        cola = []
        for row in cursor.fetchall():
            cola.append(
                {
                    "id": row[0],
                    "establecimiento": row[1],
                    "total": float(row[2]) if row[2] else 0,
                    "fecha_cargue": str(row[3]),
                    "productos": row[5] or 0,
                }
            )

        conn.close()

        return {
            "success": True,
            "total_pendientes": len(cola),
            "facturas": cola,
        }

    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/normalizar-producto")
async def normalizar_producto_individual(nombre: str, usar_ia: bool = True):
    """
    Endpoint para probar normalización de un producto individual
    """
    if usar_ia:
        resultado = AuditoriaIA.normalizar_nombre_con_ia(nombre)
    else:
        resultado = {
            "nombre_normalizado": AuditoriaAutomatica.normalizar_nombre_producto(
                nombre
            ),
            "marca": None,
            "presentacion": None,
            "categoria": AuditoriaAutomatica.detectar_categoria(nombre),
            "confianza": 0.5,
        }

    return {"success": True, "resultado": resultado}


print("✅ Módulo de auditoría IA cargado correctamente")
