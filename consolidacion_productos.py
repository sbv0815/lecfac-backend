# consolidacion_productos.py - VERSIÓN MEJORADA CON NORMALIZACIÓN Y MEJOR MATCHING

import anthropic
import os
from typing import Optional, Dict, List
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher

# Cliente de Anthropic
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


# ============================================================================
# NORMALIZACIÓN DE NOMBRES
# ============================================================================

def normalizar_nombre_producto(nombre: str) -> str:
    """
    Normaliza nombres de productos:
    - Todo MAYÚSCULAS
    - Sin tildes
    - Sin espacios extras
    - Sin caracteres especiales

    Ejemplos:
    "crema de leché" → "CREMA DE LECHE"
    "  Jugo    Hit  " → "JUGO HIT"
    "Arroz-Diana" → "ARROZ DIANA"
    """
    if not nombre or not nombre.strip():
        return "PRODUCTO SIN NOMBRE"

    # Convertir a mayúsculas y limpiar espacios
    nombre = nombre.upper().strip()

    # Quitar tildes y diacríticos
    nombre = ''.join(
        c for c in unicodedata.normalize('NFD', nombre)
        if unicodedata.category(c) != 'Mn'
    )

    # Reemplazar caracteres especiales por espacios
    caracteres_especiales = ['-', '_', '.', ',', '/', '\\', '|']
    for char in caracteres_especiales:
        nombre = nombre.replace(char, ' ')

    # Quitar espacios múltiples
    nombre = ' '.join(nombre.split())

    # Quitar caracteres no alfanuméricos (excepto espacios)
    nombre = ''.join(c for c in nombre if c.isalnum() or c.isspace())

    return nombre


def calcular_similitud_mejorada(nombre1: str, nombre2: str) -> float:
    """
    Calcula similitud entre dos nombres de productos
    Detecta que "crema veche" es similar a "crema de leche"

    Returns: float entre 0.0 y 1.0
    """
    # Normalizar ambos nombres
    n1 = normalizar_nombre_producto(nombre1)
    n2 = normalizar_nombre_producto(nombre2)

    # Si son idénticos
    if n1 == n2:
        return 1.0

    # Si uno contiene completamente al otro (substring)
    if len(n1) > len(n2):
        if n2 in n1:
            # Calcular qué tan grande es el match
            ratio = len(n2) / len(n1)
            return 0.80 + (ratio * 0.15)  # Entre 0.80 y 0.95
    else:
        if n1 in n2:
            ratio = len(n1) / len(n2)
            return 0.80 + (ratio * 0.15)

    # Similitud por palabras en común
    palabras1 = set(n1.split())
    palabras2 = set(n2.split())

    if palabras1 and palabras2:
        palabras_comunes = palabras1.intersection(palabras2)
        total_palabras = len(palabras1.union(palabras2))

        if palabras_comunes:
            similitud_palabras = len(palabras_comunes) / total_palabras

            # Si tienen >70% palabras en común, considerar muy similares
            if similitud_palabras > 0.7:
                return 0.75 + (similitud_palabras * 0.25)

    # Similitud de caracteres (fallback)
    return SequenceMatcher(None, n1, n2).ratio()


# ============================================================================
# MEJORA CON CLAUDE
# ============================================================================

def mejorar_nombre_con_claude(nombre_ocr: str, codigo_ean: Optional[str] = None) -> Dict:
    """
    Usa Claude para limpiar, corregir y estandarizar nombres de productos
    VERSIÓN MEJORADA - Devuelve nombres normalizados
    """
    # Pre-normalizar el input
    nombre_normalizado = normalizar_nombre_producto(nombre_ocr)

    prompt = f"""Eres un experto en productos de supermercados colombianos. Analiza y mejora este nombre de producto.

NOMBRE DEL RECIBO: "{nombre_normalizado}"
{f"CÓDIGO EAN: {codigo_ean}" if codigo_ean else "NO TIENE CÓDIGO EAN"}

INSTRUCCIONES CRÍTICAS:
1. Corrige errores típicos de OCR:
   - VECHE/VEC/LECH → LECHE
   - ALQUERI/ALQUER → ALQUERIA
   - ALPNA/ALPIN → ALPINA
   - COLANT → COLANTA
   - CREM/CRM → CREMA
   - YOGUR/YOGU → YOGURT
   - SEMI → SEMI (semidescremada)
   - Palabras cortadas: completa basándote en productos comunes

2. COMPLETA nombres truncados:
   - "CREMA SEMI" → "CREMA DE LECHE SEMI"
   - "ARROZ DIANA X" → "ARROZ DIANA"
   - "JUGO HIT MOR" → "JUGO HIT MORA"

3. FORMATO OBLIGATORIO:
   - Todo en MAYÚSCULAS
   - Sin tildes ni caracteres especiales
   - Un solo espacio entre palabras
   - NO incluir precios ni códigos

4. EXTRAE:
   - Marca (si existe claramente)
   - Peso/volumen (número + unidad)
   - Si no estás seguro, pon null

5. Productos colombianos comunes:
   - CREMA DE LECHE ALQUERIA/ALPINA/COLANTA
   - LECHE ENTERA/SEMI/DESLACTOSADA
   - ARROZ DIANA/FLORHUILA
   - PANELA DOÑA PANELA
   - HUEVOS SANTA REYES/KIKES

Responde SOLO con JSON (sin markdown, sin explicaciones):
{{
  "nombre_mejorado": "NOMBRE COMPLETO Y CORREGIDO",
  "marca": "MARCA o null",
  "peso_neto": numero_o_null,
  "unidad_medida": "G/ML/KG/L/UNIDADES o null",
  "confianza": 0.95
}}

Confianza:
- 0.95-1.0: Todo claro, producto conocido
- 0.80-0.94: Correcciones menores
- 0.60-0.79: Adivinaste o completaste bastante
- 0.40-0.59: Mucha incertidumbre"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=250,
            temperature=0.2,  # Más bajo = más consistente
            messages=[{"role": "user", "content": prompt}]
        )

        respuesta = message.content[0].text.strip()

        # Limpiar markdown si Claude lo agregó
        if respuesta.startswith('```'):
            respuesta = respuesta.split('```')[1]
            if respuesta.startswith('json'):
                respuesta = respuesta[4:]
            respuesta = respuesta.strip()

        import json
        datos = json.loads(respuesta)

        # CRÍTICO: Normalizar el nombre mejorado también
        datos['nombre_mejorado'] = normalizar_nombre_producto(datos['nombre_mejorado'])

        return datos

    except Exception as e:
        print(f"⚠️ Error al mejorar con Claude: {e}")
        # Fallback: devolver nombre normalizado
        return {
            "nombre_mejorado": nombre_normalizado,
            "marca": None,
            "peso_neto": None,
            "unidad_medida": None,
            "confianza": 0.3
        }


# ============================================================================
# CONSOLIDACIÓN POR EAN
# ============================================================================

def consolidar_por_ean(
    cursor,
    ean: str,
    nombre_ocr: str,
    establecimiento_id: int
) -> int:
    """
    Consolida un producto que tiene código EAN
    ✅ Usa nombres normalizados
    """
    # Normalizar nombre de entrada
    nombre_normalizado = normalizar_nombre_producto(nombre_ocr)

    # 1. Buscar en productos_maestros_v2
    cursor.execute(
        "SELECT * FROM productos_maestros_v2 WHERE codigo_ean = %s",
        (ean,)
    )
    producto = cursor.fetchone()

    if producto:
        # Producto existe - actualizar contador
        cursor.execute(
            """UPDATE productos_maestros_v2
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_actualizacion = NOW()
               WHERE id = %s""",
            (producto['id'],)
        )

        producto_id = producto['id']
        print(f"   ✓ Producto existente: {producto['nombre_consolidado']}")

    else:
        # Producto nuevo - buscar en productos_referencia
        cursor.execute(
            "SELECT * FROM productos_referencia WHERE codigo_ean = %s",
            (ean,)
        )
        referencia = cursor.fetchone()

        if referencia:
            nombre_final = normalizar_nombre_producto(referencia['nombre_oficial'])
            marca = referencia['marca']
            confianza = 1.0
            estado = 'verificado'
            print(f"   ℹ️  Usando referencia: {nombre_final}")
        else:
            # Usar Claude para mejorar el nombre
            print(f"   🤖 Mejorando con Claude: {nombre_normalizado}")
            mejora = mejorar_nombre_con_claude(nombre_normalizado, ean)

            nombre_final = mejora['nombre_mejorado']  # Ya viene normalizado
            marca = mejora['marca']
            confianza = mejora['confianza']
            estado = 'verificado' if confianza >= 0.85 else 'pendiente'

            print(f"      → {nombre_final} (confianza: {confianza:.2f})")

            # Log de la mejora
            try:
                cursor.execute(
                    """INSERT INTO log_mejoras_nombres
                       (nombre_original, nombre_mejorado, metodo, confianza)
                       VALUES (%s, %s, %s, %s)""",
                    (nombre_normalizado, nombre_final, 'claude', confianza)
                )
            except Exception as e:
                print(f"   ⚠️ Error guardando log: {e}")

        # Crear nuevo producto maestro
        cursor.execute(
            """INSERT INTO productos_maestros_v2
               (codigo_ean, nombre_consolidado, marca, confianza_datos, estado, veces_visto)
               VALUES (%s, %s, %s, %s, %s, 1)
               RETURNING id""",
            (ean, nombre_final, marca, confianza, estado)
        )

        producto_id = cursor.fetchone()['id']

    # Registrar variante de nombre
    registrar_variante_nombre(cursor, producto_id, nombre_normalizado, establecimiento_id)

    return producto_id


# ============================================================================
# CONSOLIDACIÓN POR PLU
# ============================================================================

def consolidar_por_plu(
    cursor,
    plu: str,
    nombre_ocr: str,
    establecimiento_id: int
) -> int:
    """
    Consolida un producto que tiene código PLU (sin EAN)
    ✅ Mejorado con normalización y mejor matching
    """
    nombre_normalizado = normalizar_nombre_producto(nombre_ocr)

    # Buscar si ya conocemos este PLU
    cursor.execute(
        """SELECT ca.*, pm.*
           FROM codigos_alternativos ca
           JOIN productos_maestros_v2 pm ON ca.producto_maestro_id = pm.id
           WHERE ca.codigo_local = %s AND ca.establecimiento_id = %s""",
        (plu, establecimiento_id)
    )
    codigo_alt = cursor.fetchone()

    if codigo_alt:
        producto_id = codigo_alt['producto_maestro_id']
        print(f"   ✓ PLU conocido: {codigo_alt['nombre_consolidado']}")

        cursor.execute(
            """UPDATE codigos_alternativos
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_vez = NOW()
               WHERE codigo_local = %s AND establecimiento_id = %s""",
            (plu, establecimiento_id)
        )

        cursor.execute(
            """UPDATE productos_maestros_v2
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_actualizacion = NOW()
               WHERE id = %s""",
            (producto_id,)
        )
    else:
        # PLU nuevo
        print(f"   🤖 Nuevo PLU {plu} - Mejorando: {nombre_normalizado}")
        mejora = mejorar_nombre_con_claude(nombre_normalizado, None)

        nombre_mejorado = mejora['nombre_mejorado']
        marca = mejora['marca']
        confianza = mejora['confianza']

        # Buscar producto similar por NOMBRE (no por PLU)
        cursor.execute(
            """SELECT pm.id, pm.nombre_consolidado
               FROM productos_maestros_v2 pm
               LEFT JOIN codigos_alternativos ca ON pm.id = ca.producto_maestro_id
               WHERE ca.establecimiento_id = %s
               AND pm.codigo_ean IS NULL
               LIMIT 50""",  # Buscar en más productos
            (establecimiento_id,)
        )
        productos_existentes = cursor.fetchall()

        mejor_match = None
        mejor_similitud = 0

        for prod in productos_existentes:
            similitud = calcular_similitud_mejorada(nombre_mejorado, prod['nombre_consolidado'])

            if similitud > mejor_similitud:
                mejor_similitud = similitud
                mejor_match = prod

        # Si hay match > 85%, usar ese producto
        if mejor_match and mejor_similitud >= 0.85:
            producto_id = mejor_match['id']
            print(f"      → Match encontrado: {mejor_match['nombre_consolidado']} ({mejor_similitud:.2f})")
        else:
            # Crear nuevo producto
            cursor.execute(
                """INSERT INTO productos_maestros_v2
                   (nombre_consolidado, marca, confianza_datos, estado, veces_visto)
                   VALUES (%s, %s, %s, %s, 1)
                   RETURNING id""",
                (nombre_mejorado, marca, confianza, 'pendiente')
            )
            producto_id = cursor.fetchone()['id']
            print(f"      → Nuevo producto: {nombre_mejorado} (conf: {confianza:.2f})")

        # Registrar el PLU
        cursor.execute(
            """INSERT INTO codigos_alternativos
               (producto_maestro_id, establecimiento_id, codigo_local, tipo_codigo, veces_visto)
               VALUES (%s, %s, %s, %s, 1)""",
            (producto_id, establecimiento_id, plu, 'PLU')
        )

        # Log
        try:
            cursor.execute(
                """INSERT INTO log_mejoras_nombres
                   (producto_maestro_id, nombre_original, nombre_mejorado, metodo, confianza)
                   VALUES (%s, %s, %s, %s, %s)""",
                (producto_id, nombre_normalizado, nombre_mejorado, 'claude', confianza)
            )
        except Exception as e:
            print(f"   ⚠️ Error guardando log: {e}")

    registrar_variante_nombre(cursor, producto_id, nombre_normalizado, establecimiento_id)

    return producto_id


# ============================================================================
# CONSOLIDACIÓN SIN CÓDIGO
# ============================================================================

def consolidar_sin_codigo(
    cursor,
    nombre_ocr: str,
    establecimiento_id: int
) -> int:
    """
    Consolida un producto SIN código
    ✅ Mejorado con mejor matching
    """
    nombre_normalizado = normalizar_nombre_producto(nombre_ocr)
    print(f"   ⚠️ Sin código - Buscando por nombre: {nombre_normalizado}")

    mejora = mejorar_nombre_con_claude(nombre_normalizado, None)

    nombre_mejorado = mejora['nombre_mejorado']
    marca = mejora['marca']
    confianza = mejora['confianza'] * 0.8  # Penalizar por no tener código

    # Buscar productos similares
    similar = buscar_producto_similar(cursor, nombre_mejorado, establecimiento_id)

    if similar and similar['confianza'] >= 0.90:
        producto_id = similar['producto_id']
        print(f"      → Match: {similar['nombre']} (conf: {similar['confianza']:.2f})")

        cursor.execute(
            """UPDATE productos_maestros_v2
               SET veces_visto = veces_visto + 1,
                   fecha_ultima_actualizacion = NOW()
               WHERE id = %s""",
            (producto_id,)
        )
    else:
        # Crear nuevo producto (estado 'conflicto' por falta de código)
        cursor.execute(
            """INSERT INTO productos_maestros_v2
               (nombre_consolidado, marca, confianza_datos, estado, veces_visto)
               VALUES (%s, %s, %s, 'conflicto', 1)
               RETURNING id""",
            (nombre_mejorado, marca, confianza)
        )
        producto_id = cursor.fetchone()['id']
        print(f"      → Nuevo (sin código): {nombre_mejorado}")

    try:
        cursor.execute(
            """INSERT INTO log_mejoras_nombres
               (producto_maestro_id, nombre_original, nombre_mejorado, metodo, confianza)
               VALUES (%s, %s, %s, %s, %s)""",
            (producto_id, nombre_normalizado, nombre_mejorado, 'claude_sin_codigo', confianza)
        )
    except Exception as e:
        print(f"   ⚠️ Error guardando log: {e}")

    registrar_variante_nombre(cursor, producto_id, nombre_normalizado, establecimiento_id)

    return producto_id


def buscar_producto_similar(cursor, nombre: str, establecimiento_id: int) -> Optional[Dict]:
    """
    Busca productos similares
    ✅ Usa similitud mejorada
    """
    cursor.execute(
        """SELECT DISTINCT pm.id, pm.nombre_consolidado
           FROM productos_maestros_v2 pm
           LEFT JOIN variantes_nombres vn ON pm.id = vn.producto_maestro_id
           WHERE (vn.establecimiento_id = %s OR pm.codigo_ean IS NOT NULL)
           AND pm.codigo_ean IS NULL
           LIMIT 100""",  # Buscar en más productos
        (establecimiento_id,)
    )
    productos = cursor.fetchall()

    mejor_match = None
    mejor_score = 0

    for p in productos:
        score = calcular_similitud_mejorada(nombre, p['nombre_consolidado'])

        if score > mejor_score:
            mejor_score = score
            mejor_match = {
                'producto_id': p['id'],
                'nombre': p['nombre_consolidado'],
                'confianza': score
            }

    return mejor_match if mejor_score > 0.85 else None


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def registrar_variante_nombre(cursor, producto_id: int, nombre: str, establecimiento_id: int):
    """Registra variante de nombre - Normalizado"""
    nombre_normalizado = normalizar_nombre_producto(nombre)

    try:
        cursor.execute(
            """INSERT INTO variantes_nombres
               (producto_maestro_id, nombre_variante, establecimiento_id, veces_visto, fecha_ultima_vez)
               VALUES (%s, %s, %s, 1, NOW())
               ON CONFLICT (nombre_variante, establecimiento_id, producto_maestro_id)
               DO UPDATE SET
                   veces_visto = variantes_nombres.veces_visto + 1,
                   fecha_ultima_vez = NOW()""",
            (producto_id, nombre_normalizado, establecimiento_id)
        )
    except Exception as e:
        print(f"   ⚠️ Error registrando variante: {e}")


def registrar_precio(
    cursor,
    producto_id: int,
    establecimiento_id: int,
    precio: float,
    fecha_factura: str,
    factura_id: int,
    item_factura_id: Optional[int] = None
):
    """Registra precio histórico"""
    try:
        cursor.execute(
            """INSERT INTO precios_historicos_v2
               (producto_maestro_id, establecimiento_id, precio, fecha_factura, factura_id, item_factura_id)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (producto_id, establecimiento_id, precio, fecha_factura, factura_id, item_factura_id)
        )
    except Exception as e:
        print(f"   ⚠️ Error registrando precio: {e}")


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def procesar_item_con_consolidacion(
    cursor,
    item_ocr: Dict,
    factura_id: int,
    establecimiento_id: int
) -> int:
    """
    Función principal de consolidación
    ✅ VERSIÓN MEJORADA con normalización completa

    Args:
        cursor: Cursor de psycopg2
        item_ocr: {'nombre': str, 'codigo': str, 'precio': float, 'cantidad': int}
        factura_id: ID de la factura
        establecimiento_id: ID del establecimiento

    Returns:
        producto_maestro_id: ID del producto consolidado
    """
    # Normalizar nombre ANTES de todo
    nombre_original = item_ocr.get('nombre', 'PRODUCTO SIN NOMBRE')
    nombre = normalizar_nombre_producto(nombre_original)

    codigo = item_ocr.get('codigo', '').strip() if item_ocr.get('codigo') else None
    precio = item_ocr.get('precio', 0)

    print(f"\n📦 Procesando: {nombre[:60]}")
    if codigo:
        print(f"   📟 Código: {codigo}")

    # Detectar tipo de código
    es_ean = codigo and len(str(codigo)) == 13 and str(codigo).isdigit()
    es_plu = codigo and 3 <= len(str(codigo)) <= 6 and str(codigo).isdigit()

    if es_ean:
        print(f"   ✓ EAN detectado")
        producto_id = consolidar_por_ean(cursor, str(codigo), nombre, establecimiento_id)
    elif es_plu:
        print(f"   ✓ PLU detectado")
        producto_id = consolidar_por_plu(cursor, str(codigo), nombre, establecimiento_id)
    elif codigo:
        print(f"   ℹ️ Código no estándar")
        producto_id = consolidar_por_plu(cursor, str(codigo), nombre, establecimiento_id)
    else:
        print(f"   ⚠️ Sin código")
        producto_id = consolidar_sin_codigo(cursor, nombre, establecimiento_id)

    # Registrar precio
    registrar_precio(
        cursor,
        producto_id,
        establecimiento_id,
        precio,
        datetime.now().date(),
        factura_id
    )

    return producto_id
