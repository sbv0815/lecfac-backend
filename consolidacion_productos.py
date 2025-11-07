# consolidacion_productos.py
import anthropic
import os
from typing import Optional, Dict, List
from datetime import datetime
import asyncpg
from difflib import SequenceMatcher

# Cliente de Anthropic
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

async def mejorar_nombre_con_claude(nombre_ocr: str, codigo_ean: Optional[str] = None) -> Dict:
    """
    Usa Claude para limpiar, corregir y estandarizar nombres de productos

    Returns:
        {
            'nombre_mejorado': str,
            'marca': str | None,
            'peso_neto': float | None,
            'unidad_medida': str | None,
            'confianza': float
        }
    """
    prompt = f"""Eres un experto en productos de supermercados colombianos. Tu tarea es analizar y mejorar este nombre de producto escaneado de un recibo.

NOMBRE DEL RECIBO: "{nombre_ocr}"
{f"CÓDIGO EAN: {codigo_ean}" if codigo_ean else "NO TIENE CÓDIGO EAN"}

INSTRUCCIONES:
1. Corrige errores comunes de OCR:
   - ALQUERI → ALQUERIA
   - ALPNA → ALPINA
   - COLANT → COLANTA
   - CREM → CREMA
   - LEC → LECHE
   - YOGUR → YOGURT
   - Palabras cortadas o incompletas

2. Completa palabras truncadas basándote en productos comunes colombianos

3. Estandariza el formato:
   - Todo en MAYÚSCULAS
   - Sin tildes
   - Espacios simples entre palabras

4. Extrae información:
   - Marca (si existe)
   - Peso/volumen con unidad (ej: 500, unidad: "G")
   - Si no puedes determinar algo, déjalo en null

5. Mantén SOLO información relevante: marca, tipo de producto, peso/volumen

EJEMPLOS:
- "CREMA DE LEC ALQUERI" → "CREMA DE LECHE ALQUERIA"
- "ARROZ DIANA X 500" → "ARROZ DIANA 500G"
- "JUGO HIT MORA 200ML" → "JUGO HIT MORA 200ML"

Responde ÚNICAMENTE con un JSON en este formato exacto (sin markdown, sin explicaciones):
{{
  "nombre_mejorado": "NOMBRE CORREGIDO Y COMPLETO",
  "marca": "MARCA o null",
  "peso_neto": numero_o_null,
  "unidad_medida": "G/ML/KG/L/UNIDADES o null",
  "confianza": 0.95
}}

El campo confianza debe ser:
- 0.95-1.0 si todo está claro y correcto
- 0.80-0.94 si hiciste correcciones menores
- 0.60-0.79 si tuviste que adivinar o completar bastante
- 0.40-0.59 si hay mucha incertidumbre"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            temperature=0.3,  # Baja temperatura para respuestas más consistentes
            messages=[{"role": "user", "content": prompt}]
        )

        respuesta = message.content[0].text.strip()

        # Parsear JSON
        import json
        datos = json.loads(respuesta)

        return datos

    except Exception as e:
        print(f"Error al mejorar nombre con Claude: {e}")
        # Fallback: retornar el nombre original con baja confianza
        return {
            "nombre_mejorado": nombre_ocr.upper(),
            "marca": None,
            "peso_neto": None,
            "unidad_medida": None,
            "confianza": 0.3
        }


async def consolidar_por_ean(
    conn: asyncpg.Connection,
    ean: str,
    nombre_ocr: str,
    establecimiento_id: int
) -> int:
    """
    Consolida un producto que tiene código EAN
    Returns: producto_maestro_id
    """
    # 1. Buscar en productos_maestros_v2
    producto = await conn.fetchrow(
        "SELECT * FROM productos_maestros_v2 WHERE codigo_ean = $1",
        ean
    )

    if producto:
        # Producto ya existe - actualizar contador
        await conn.execute(
            """UPDATE productos_maestros_v2
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_actualizacion = NOW()
               WHERE id = $1""",
            producto['id']
        )

        producto_id = producto['id']

    else:
        # Producto nuevo
        # 1. Buscar en productos_referencia (fuente de verdad)
        referencia = await conn.fetchrow(
            "SELECT * FROM productos_referencia WHERE codigo_ean = $1",
            ean
        )

        if referencia:
            # Usar nombre oficial de referencia
            nombre_final = referencia['nombre_oficial']
            marca = referencia['marca']
            confianza = 1.0
            estado = 'verificado'

            # Log: no necesitamos Claude porque tenemos referencia
            print(f"   ℹ️  Usando referencia: {nombre_final}")

        else:
            # Usar Claude para mejorar el nombre
            print(f"   🤖 Mejorando con Claude: {nombre_ocr}")
            mejora = await mejorar_nombre_con_claude(nombre_ocr, ean)

            nombre_final = mejora['nombre_mejorado']
            marca = mejora['marca']
            confianza = mejora['confianza']
            estado = 'verificado' if confianza >= 0.85 else 'pendiente'

            # Log de la mejora
            await conn.execute(
                """INSERT INTO log_mejoras_nombres
                   (nombre_original, nombre_mejorado, metodo, confianza)
                   VALUES ($1, $2, $3, $4)""",
                nombre_ocr, nombre_final, 'claude', confianza
            )

            print(f"      → {nombre_final} (confianza: {confianza:.2f})")

        # Crear nuevo producto maestro
        producto = await conn.fetchrow(
            """INSERT INTO productos_maestros_v2
               (codigo_ean, nombre_consolidado, marca, confianza_datos, estado, veces_visto)
               VALUES ($1, $2, $3, $4, $5, 1)
               RETURNING id""",
            ean, nombre_final, marca, confianza, estado
        )

        producto_id = producto['id']

    # Registrar esta variante de nombre
    await registrar_variante_nombre(conn, producto_id, nombre_ocr, establecimiento_id)

    return producto_id


async def consolidar_por_plu(
    conn: asyncpg.Connection,
    plu: str,
    nombre_ocr: str,
    establecimiento_id: int
) -> int:
    """
    Consolida un producto que tiene código PLU (sin EAN)
    Returns: producto_maestro_id
    """
    # 1. Buscar si ya conocemos este PLU en este establecimiento
    codigo_alt = await conn.fetchrow(
        """SELECT ca.*, pm.*
           FROM codigos_alternativos ca
           JOIN productos_maestros_v2 pm ON ca.producto_maestro_id = pm.id
           WHERE ca.codigo_local = $1 AND ca.establecimiento_id = $2""",
        plu, establecimiento_id
    )

    if codigo_alt:
        # Ya sabemos qué producto es este PLU
        producto_id = codigo_alt['producto_maestro_id']

        await conn.execute(
            """UPDATE codigos_alternativos
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_vez = NOW()
               WHERE codigo_local = $1 AND establecimiento_id = $2""",
            plu, establecimiento_id
        )

        await conn.execute(
            """UPDATE productos_maestros_v2
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_actualizacion = NOW()
               WHERE id = $1""",
            producto_id
        )

    else:
        # PLU nuevo - mejorar nombre con Claude
        print(f"   🤖 Nuevo PLU {plu} - Mejorando con Claude: {nombre_ocr}")
        mejora = await mejorar_nombre_con_claude(nombre_ocr, None)

        nombre_mejorado = mejora['nombre_mejorado']
        marca = mejora['marca']
        confianza = mejora['confianza']

        # Buscar si existe un producto similar (mismo nombre mejorado, mismo establecimiento)
        similar = await conn.fetchrow(
            """SELECT pm.id, pm.nombre_consolidado
               FROM productos_maestros_v2 pm
               LEFT JOIN codigos_alternativos ca ON pm.id = ca.producto_maestro_id
               WHERE ca.establecimiento_id = $1
               AND pm.codigo_ean IS NULL
               AND pm.nombre_consolidado = $2
               LIMIT 1""",
            establecimiento_id, nombre_mejorado
        )

        if similar:
            # Ya existe un producto con ese nombre en ese establecimiento
            producto_id = similar['id']
            print(f"      → Vinculando a producto existente: {similar['nombre_consolidado']}")

        else:
            # Crear nuevo producto sin EAN
            producto = await conn.fetchrow(
                """INSERT INTO productos_maestros_v2
                   (nombre_consolidado, marca, confianza_datos, estado, veces_visto)
                   VALUES ($1, $2, $3, $4, 1)
                   RETURNING id""",
                nombre_mejorado, marca, confianza, 'pendiente'
            )

            producto_id = producto['id']
            print(f"      → Nuevo producto: {nombre_mejorado} (confianza: {confianza:.2f})")

        # Registrar el PLU
        await conn.execute(
            """INSERT INTO codigos_alternativos
               (producto_maestro_id, establecimiento_id, codigo_local, tipo_codigo, veces_visto)
               VALUES ($1, $2, $3, $4, 1)""",
            producto_id, establecimiento_id, plu, 'PLU'
        )

        # Log de la mejora
        await conn.execute(
            """INSERT INTO log_mejoras_nombres
               (producto_maestro_id, nombre_original, nombre_mejorado, metodo, confianza)
               VALUES ($1, $2, $3, $4, $5)""",
            producto_id, nombre_ocr, nombre_mejorado, 'claude', confianza
        )

    # Registrar variante de nombre
    await registrar_variante_nombre(conn, producto_id, nombre_ocr, establecimiento_id)

    return producto_id


async def consolidar_sin_codigo(
    conn: asyncpg.Connection,
    nombre_ocr: str,
    establecimiento_id: int
) -> int:
    """
    Consolida un producto SIN código (ni EAN ni PLU)
    Este es el caso más difícil - solo tenemos el nombre
    Returns: producto_maestro_id
    """
    print(f"   ⚠️  Producto sin código - Solo nombre: {nombre_ocr}")

    # Mejorar nombre con Claude
    mejora = await mejorar_nombre_con_claude(nombre_ocr, None)

    nombre_mejorado = mejora['nombre_mejorado']
    marca = mejora['marca']
    confianza = mejora['confianza'] * 0.8  # Penalizar por no tener código

    # Buscar productos similares en el mismo establecimiento
    similar = await buscar_producto_similar(
        conn, nombre_mejorado, establecimiento_id
    )

    if similar and similar['confianza'] >= 0.90:
        # Match muy seguro
        producto_id = similar['producto_id']
        print(f"      → Match encontrado: {similar['nombre']} (conf: {similar['confianza']:.2f})")

        await conn.execute(
            """UPDATE productos_maestros_v2
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_actualizacion = NOW()
               WHERE id = $1""",
            producto_id
        )

    else:
        # Crear nuevo producto
        producto = await conn.fetchrow(
            """INSERT INTO productos_maestros_v2
               (nombre_consolidado, marca, confianza_datos, estado, veces_visto)
               VALUES ($1, $2, $3, 'conflicto', 1)
               RETURNING id""",
            nombre_mejorado, marca, confianza
        )

        producto_id = producto['id']
        print(f"      → Nuevo producto (sin código): {nombre_mejorado}")

    # Log y variante
    await conn.execute(
        """INSERT INTO log_mejoras_nombres
           (producto_maestro_id, nombre_original, nombre_mejorado, metodo, confianza)
           VALUES ($1, $2, $3, $4, $5)""",
        producto_id, nombre_ocr, nombre_mejorado, 'claude_sin_codigo', confianza
    )

    await registrar_variante_nombre(conn, producto_id, nombre_ocr, establecimiento_id)

    return producto_id


async def buscar_producto_similar(
    conn: asyncpg.Connection,
    nombre: str,
    establecimiento_id: int
) -> Optional[Dict]:
    """
    Busca productos con nombres similares
    """
    # Obtener productos del mismo establecimiento
    productos = await conn.fetch(
        """SELECT DISTINCT pm.id, pm.nombre_consolidado
           FROM productos_maestros_v2 pm
           LEFT JOIN variantes_nombres vn ON pm.id = vn.producto_maestro_id
           WHERE (vn.establecimiento_id = $1 OR pm.codigo_ean IS NOT NULL)
           AND pm.codigo_ean IS NULL""",
        establecimiento_id
    )

    mejor_match = None
    mejor_score = 0

    for p in productos:
        score = SequenceMatcher(None, nombre.upper(), p['nombre_consolidado'].upper()).ratio()

        if score > mejor_score:
            mejor_score = score
            mejor_match = {
                'producto_id': p['id'],
                'nombre': p['nombre_consolidado'],
                'confianza': score
            }

    return mejor_match if mejor_score > 0.85 else None


async def registrar_variante_nombre(
    conn: asyncpg.Connection,
    producto_id: int,
    nombre: str,
    establecimiento_id: int
):
    """
    Registra una variante de nombre vista en una factura
    """
    await conn.execute(
        """INSERT INTO variantes_nombres
           (producto_maestro_id, nombre_variante, establecimiento_id, veces_visto, fecha_ultima_vez)
           VALUES ($1, $2, $3, 1, NOW())
           ON CONFLICT (nombre_variante, establecimiento_id, producto_maestro_id)
           DO UPDATE SET
               veces_visto = variantes_nombres.veces_visto + 1,
               fecha_ultima_vez = NOW()""",
        producto_id, nombre, establecimiento_id
    )


async def registrar_precio(
    conn: asyncpg.Connection,
    producto_id: int,
    establecimiento_id: int,
    precio: float,
    fecha_factura: str,
    factura_id: int,
    item_factura_id: Optional[int] = None
):
    """
    Registra un precio en el histórico
    """
    await conn.execute(
        """INSERT INTO precios_historicos_v2
           (producto_maestro_id, establecimiento_id, precio, fecha_factura, factura_id, item_factura_id)
           VALUES ($1, $2, $3, $4, $5, $6)""",
        producto_id, establecimiento_id, precio, fecha_factura, factura_id, item_factura_id
    )


async def procesar_item_con_consolidacion(
    conn: asyncpg.Connection,
    item_ocr: Dict,
    factura_id: int,
    establecimiento_id: int
) -> int:
    """
    Función principal: procesa un item del OCR y lo consolida
    Returns: producto_maestro_id
    """
    nombre = item_ocr['nombre']
    codigo = item_ocr.get('codigo')
    precio = item_ocr['precio']
    cantidad = item_ocr.get('cantidad', 1)

    print(f"\n📦 Procesando: {nombre[:50]}")

    # Determinar tipo de código
    es_ean = codigo and len(str(codigo)) == 13 and str(codigo).isdigit()
    es_plu = codigo and len(str(codigo)) <= 6 and str(codigo).isdigit()

    # Consolidar según el tipo de código
    if es_ean:
        print(f"   ✓ EAN detectado: {codigo}")
        producto_id = await consolidar_por_ean(conn, str(codigo), nombre, establecimiento_id)

    elif es_plu:
        print(f"   ✓ PLU detectado: {codigo}")
        producto_id = await consolidar_por_plu(conn, str(codigo), nombre, establecimiento_id)

    elif codigo:
        print(f"   ℹ️  Código no estándar: {codigo}")
        # Tratar como PLU genérico
        producto_id = await consolidar_por_plu(conn, str(codigo), nombre, establecimiento_id)

    else:
        print(f"   ⚠️  Sin código")
        producto_id = await consolidar_sin_codigo(conn, nombre, establecimiento_id)

    # Registrar precio
    await registrar_precio(
        conn,
        producto_id,
        establecimiento_id,
        precio,
        datetime.now().date(),
        factura_id
    )

    return producto_id
