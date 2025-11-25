"""
Integración del Scraper Web con el Proceso de OCR - LecFac
============================================================

Este módulo conecta el scraper de supermercados con el proceso de
escaneo de facturas para enriquecer automáticamente los productos.

Flujo:
1. Usuario escanea factura
2. OCR extrae productos (nombre, PLU, precio)
3. Este módulo enriquece cada producto con datos de la web
4. Se guarda en productos_web_enriched
5. Se actualiza productos_maestros con el nombre correcto

Uso en el backend:
    from enrichment_service import enriquecer_factura, enriquecer_producto_individual

    # Después del OCR, enriquecer toda la factura
    productos_enriquecidos = await enriquecer_factura(
        productos_ocr=productos_del_ocr,
        supermercado="Carulla"
    )
"""

import asyncio
from typing import Dict, List, Optional
from datetime import datetime


# Mapeo de nombres de supermercados a claves del scraper
MAPEO_SUPERMERCADOS = {
    # Nombres como vienen del OCR/tiquete
    "carulla": "carulla",
    "éxito": "exito",
    "exito": "exito",
    "jumbo": "jumbo",
    "metro": "jumbo",  # Metro es de Cencosud como Jumbo
    "olimpica": "olimpica",
    "olímpica": "olimpica",
    # Variaciones
    "almacenes exito": "exito",
    "supermercado carulla": "carulla",
    "tiendas jumbo": "jumbo",
    "supertiendas olimpica": "olimpica",
    "sao olimpica": "olimpica",
    "droguerias olimpica": "olimpica",
}


def normalizar_supermercado(nombre: str) -> Optional[str]:
    """Convierte el nombre del supermercado al formato del scraper"""
    if not nombre:
        return None

    nombre_lower = nombre.lower().strip()

    # Buscar en el mapeo
    for key, value in MAPEO_SUPERMERCADOS.items():
        if key in nombre_lower:
            return value

    return None


async def enriquecer_producto_individual(
    nombre_ocr: str,
    codigo_ocr: str = None,
    precio_ocr: int = None,
    supermercado: str = "carulla",
    guardar_bd: bool = True,
) -> Dict:
    """
    Enriquece un único producto del OCR

    Args:
        nombre_ocr: Nombre como aparece en el tiquete
        codigo_ocr: Código PLU o EAN del tiquete
        precio_ocr: Precio del tiquete (entero, pesos colombianos)
        supermercado: Nombre del supermercado
        guardar_bd: Si guardar en la base de datos

    Returns:
        Dict con el producto enriquecido
    """
    try:
        from vtex_scraper import enriquecer_y_guardar, enriquecer_producto

        # Normalizar supermercado
        super_key = normalizar_supermercado(supermercado)

        if not super_key:
            print(f"⚠️ Supermercado '{supermercado}' no soportado para enriquecimiento")
            return {
                "enriquecido": False,
                "nombre_ocr": nombre_ocr,
                "mensaje": f"Supermercado {supermercado} no soportado",
            }

        # Determinar si el código es PLU o EAN
        plu_ocr = None
        ean_ocr = None

        if codigo_ocr:
            codigo_limpio = str(codigo_ocr).strip()
            if len(codigo_limpio) >= 8:
                ean_ocr = codigo_limpio  # Probablemente EAN
            elif len(codigo_limpio) >= 3:
                plu_ocr = codigo_limpio  # Probablemente PLU

        # Enriquecer
        if guardar_bd:
            resultado = await enriquecer_y_guardar(
                nombre_ocr=nombre_ocr,
                plu_ocr=plu_ocr,
                ean_ocr=ean_ocr,
                precio_ocr=precio_ocr,
                supermercado=super_key,
                guardar_candidatos=True,
            )
        else:
            resultado = await enriquecer_producto(
                nombre_ocr=nombre_ocr,
                plu_ocr=plu_ocr,
                ean_ocr=ean_ocr,
                precio_ocr=precio_ocr,
                supermercado=super_key,
            )

        return resultado

    except ImportError as e:
        print(f"❌ Error importando vtex_scraper: {e}")
        return {
            "enriquecido": False,
            "nombre_ocr": nombre_ocr,
            "mensaje": "Módulo vtex_scraper no disponible",
        }
    except Exception as e:
        print(f"❌ Error enriqueciendo {nombre_ocr}: {e}")
        return {"enriquecido": False, "nombre_ocr": nombre_ocr, "mensaje": str(e)}


async def enriquecer_factura(
    productos_ocr: List[Dict],
    supermercado: str,
    guardar_bd: bool = True,
    max_concurrent: int = 3,
) -> List[Dict]:
    """
    Enriquece todos los productos de una factura

    Args:
        productos_ocr: Lista de productos del OCR, cada uno con:
            - nombre: str
            - codigo: str (PLU o EAN)
            - precio: int o float
        supermercado: Nombre del supermercado de la factura
        guardar_bd: Si guardar en la base de datos
        max_concurrent: Máximo de requests concurrentes (para no saturar la API)

    Returns:
        Lista de productos enriquecidos
    """
    if not productos_ocr:
        return []

    print(f"\n{'='*60}")
    print(f"🔄 Enriqueciendo {len(productos_ocr)} productos de {supermercado}")
    print("=" * 60)

    resultados = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async def enriquecer_con_limite(producto: Dict) -> Dict:
        async with semaphore:
            # Pequeña pausa para no saturar la API
            await asyncio.sleep(0.5)

            return await enriquecer_producto_individual(
                nombre_ocr=producto.get("nombre", ""),
                codigo_ocr=producto.get("codigo"),
                precio_ocr=(
                    int(producto.get("precio", 0)) if producto.get("precio") else None
                ),
                supermercado=supermercado,
                guardar_bd=guardar_bd,
            )

    # Ejecutar en paralelo con límite
    tareas = [enriquecer_con_limite(p) for p in productos_ocr]
    resultados = await asyncio.gather(*tareas, return_exceptions=True)

    # Procesar resultados
    enriquecidos = 0
    verificados = 0

    for i, resultado in enumerate(resultados):
        if isinstance(resultado, Exception):
            resultados[i] = {
                "enriquecido": False,
                "nombre_ocr": productos_ocr[i].get("nombre"),
                "mensaje": str(resultado),
            }
        elif resultado.get("enriquecido"):
            enriquecidos += 1
            if resultado.get("verificado"):
                verificados += 1

    print(
        f"\n📊 Resumen: {enriquecidos}/{len(productos_ocr)} enriquecidos, {verificados} verificados"
    )

    return resultados


def actualizar_producto_maestro(
    codigo: str, nombre_enriquecido: str, ean: str = None, supermercado: str = None
) -> bool:
    """
    Actualiza productos_maestros con el nombre correcto del producto

    Esta función debería llamarse después de enriquecer un producto
    para mantener productos_maestros actualizado con nombres correctos.
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        if not conn:
            return False

        cursor = conn.cursor()

        # Actualizar el nombre si el producto existe
        cursor.execute(
            """
            UPDATE productos_maestros
            SET nombre = %s,
                fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE codigo = %s
        """,
            (nombre_enriquecido, codigo),
        )

        # Si tiene EAN y no está guardado, agregarlo
        if ean:
            cursor.execute(
                """
                UPDATE productos_maestros
                SET ean = %s
                WHERE codigo = %s AND (ean IS NULL OR ean = '')
            """,
                (ean, codigo),
            )

        conn.commit()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error actualizando producto_maestro: {e}")
        return False


# ============================================
# FUNCIÓN PARA INTEGRAR EN EL FLUJO DE OCR
# ============================================


async def post_procesar_ocr(
    factura_id: int, productos_ocr: List[Dict], supermercado: str
) -> Dict:
    """
    Función para llamar DESPUÉS del procesamiento OCR

    Integración en main.py:

    ```python
    # Después de procesar el OCR y guardar en BD
    from enrichment_service import post_procesar_ocr

    resultado_enrichment = await post_procesar_ocr(
        factura_id=factura.id,
        productos_ocr=productos_extraidos,
        supermercado=factura.supermercado
    )
    ```
    """
    resultado = {
        "factura_id": factura_id,
        "total_productos": len(productos_ocr),
        "enriquecidos": 0,
        "verificados": 0,
        "guardados_bd": 0,
        "errores": [],
    }

    try:
        # Enriquecer productos
        productos_enriquecidos = await enriquecer_factura(
            productos_ocr=productos_ocr, supermercado=supermercado, guardar_bd=True
        )

        for p in productos_enriquecidos:
            if p.get("enriquecido"):
                resultado["enriquecidos"] += 1

                if p.get("verificado"):
                    resultado["verificados"] += 1

                if p.get("db_id"):
                    resultado["guardados_bd"] += 1

                    # Actualizar producto_maestro con el nombre correcto
                    if p.get("plu_web") and p.get("nombre_completo"):
                        actualizar_producto_maestro(
                            codigo=p["plu_web"],
                            nombre_enriquecido=p["nombre_completo"],
                            ean=p.get("ean_web"),
                            supermercado=supermercado,
                        )

            elif p.get("mensaje"):
                resultado["errores"].append(p["mensaje"])

        print(f"\n✅ Enrichment completado para factura {factura_id}")
        print(
            f"   Enriquecidos: {resultado['enriquecidos']}/{resultado['total_productos']}"
        )
        print(f"   Verificados: {resultado['verificados']}")
        print(f"   Guardados BD: {resultado['guardados_bd']}")

    except Exception as e:
        resultado["errores"].append(str(e))
        print(f"❌ Error en enrichment: {e}")

    return resultado


# ============================================
# PRUEBA
# ============================================


async def test():
    print("=" * 60)
    print("🧪 Test de Enrichment Service")
    print("=" * 60)

    # Simular productos del OCR
    productos_ocr = [
        {"nombre": "AREPA EXTRADELGA SARY", "codigo": "237373", "precio": 7800},
        {"nombre": "LCH ALQUER DESLACT", "codigo": "", "precio": 7100},
        {"nombre": "QSO MOZARELLA COLANT", "codigo": "123456", "precio": 12500},
    ]

    # Enriquecer
    resultados = await enriquecer_factura(
        productos_ocr=productos_ocr,
        supermercado="Carulla",
        guardar_bd=False,  # No guardar en test
    )

    print("\n" + "=" * 60)
    print("📊 RESULTADOS:")
    print("=" * 60)

    for i, r in enumerate(resultados):
        print(f"\n{i+1}. {r.get('nombre_ocr', 'N/A')}")
        if r.get("enriquecido"):
            print(f"   ✅ → {r.get('nombre_completo', 'N/A')[:50]}...")
            print(f"   💰 Precio web: ${r.get('precio_web', 0):,}")
            print(f"   🏷️ PLU: {r.get('plu_web')}")
        else:
            print(f"   ❌ {r.get('mensaje', 'No enriquecido')}")


if __name__ == "__main__":
    asyncio.run(test())
