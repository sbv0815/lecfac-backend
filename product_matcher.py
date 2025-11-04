"""
product_matcher.py - VERSIÓN MEJORADA
Sistema de matching y normalización de productos
"""

import re
from unidecode import unidecode


def normalizar_nombre_producto(nombre: str) -> str:
    """
    Normaliza el nombre del producto para facilitar matching

    MEJORAS:
    - Elimina espacios extra
    - Convierte a minúsculas
    - Elimina acentos
    - Elimina caracteres especiales
    - Normaliza variaciones comunes (gr, ml, und, etc)
    """
    if not nombre:
        return ""

    # Convertir a minúsculas
    nombre = nombre.lower()

    # Eliminar acentos
    nombre = unidecode(nombre)

    # Eliminar caracteres especiales pero mantener espacios
    nombre = re.sub(r'[^\w\s]', ' ', nombre)

    # Normalizar unidades de medida
    nombre = re.sub(r'\b(\d+)\s*(gr?|gramos?)\b', r'\1g', nombre)
    nombre = re.sub(r'\b(\d+)\s*(ml|mililitros?)\b', r'\1ml', nombre)
    nombre = re.sub(r'\b(\d+)\s*(kg|kilos?|kilogramos?)\b', r'\1kg', nombre)
    nombre = re.sub(r'\b(\d+)\s*(lt?|litros?)\b', r'\1l', nombre)
    nombre = re.sub(r'\b(\d+)\s*(und?|unidades?|u)\b', r'\1und', nombre)
    nombre = re.sub(r'\b(\d+)\s*(cm|centimetros?)\b', r'\1cm', nombre)

    # Normalizar palabras comunes
    nombre = re.sub(r'\bx\s*(\d+)', r'\1und', nombre)  # "x 6" -> "6und"
    nombre = re.sub(r'\bpaq\b', 'paquete', nombre)
    nombre = re.sub(r'\bboll?a\b', 'bolsa', nombre)

    # Eliminar espacios múltiples
    nombre = re.sub(r'\s+', ' ', nombre)

    # Quitar espacios al inicio y final
    nombre = nombre.strip()

    return nombre


def extraer_caracteristicas(nombre: str) -> dict:
    """
    Extrae características clave del producto

    Returns:
        dict con: nombre_base, peso, unidades, sabor, marca, etc.
    """
    nombre_norm = normalizar_nombre_producto(nombre)

    caracteristicas = {
        "nombre_original": nombre,
        "nombre_normalizado": nombre_norm,
        "peso_g": None,
        "volumen_ml": None,
        "unidades": None,
        "marca": None,
        "sabor": None,
    }

    # Extraer peso en gramos
    match_peso = re.search(r'(\d+)g\b', nombre_norm)
    if match_peso:
        caracteristicas["peso_g"] = int(match_peso.group(1))

    # Extraer volumen en ml
    match_vol = re.search(r'(\d+)ml\b', nombre_norm)
    if match_vol:
        caracteristicas["volumen_ml"] = int(match_vol.group(1))

    # Extraer unidades
    match_und = re.search(r'(\d+)und\b', nombre_norm)
    if match_und:
        caracteristicas["unidades"] = int(match_und.group(1))

    return caracteristicas


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """
    Calcula similitud entre dos nombres de productos

    Returns:
        float entre 0.0 (totalmente diferente) y 1.0 (idéntico)
    """
    # Normalizar ambos nombres
    n1 = normalizar_nombre_producto(nombre1)
    n2 = normalizar_nombre_producto(nombre2)

    # Si son exactamente iguales
    if n1 == n2:
        return 1.0

    # Si uno contiene al otro
    if n1 in n2 or n2 in n1:
        # Calcular qué tan similar es la longitud
        len_ratio = min(len(n1), len(n2)) / max(len(n1), len(n2))
        return 0.8 + (0.2 * len_ratio)

    # Calcular similitud por palabras
    palabras1 = set(n1.split())
    palabras2 = set(n2.split())

    palabras_comunes = palabras1.intersection(palabras2)
    total_palabras = palabras1.union(palabras2)

    if not total_palabras:
        return 0.0

    similitud_jaccard = len(palabras_comunes) / len(total_palabras)

    return similitud_jaccard


def buscar_o_crear_producto_inteligente(
    codigo: str,
    nombre: str,
    precio: int,
    establecimiento: str,
    cursor,
    conn
) -> int:
    """
    Busca un producto existente o crea uno nuevo

    LÓGICA MEJORADA:
    1. Si tiene código EAN válido (8+ dígitos), buscar por código
    2. Si no tiene código, buscar por similitud de nombre (>80%)
    3. Si no encuentra nada, crear producto nuevo

    Returns:
        int: producto_maestro_id
    """
    import os

    nombre_normalizado = normalizar_nombre_producto(nombre)

    print(f"🔍 buscar_o_crear_producto_inteligente()")
    print(f"   Código: {codigo}")
    print(f"   Nombre original: {nombre}")
    print(f"   Nombre normalizado: {nombre_normalizado}")
    print(f"   Precio: ${precio:,}")

    # PASO 1: Buscar por código EAN si existe
    if codigo and len(codigo) >= 8 and codigo.isdigit():
        print(f"   🔍 Buscando por código EAN: {codigo}")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT id, nombre_normalizado, precio_promedio_global
                FROM productos_maestros
                WHERE codigo_ean = %s
                LIMIT 1
            """, (codigo,))
        else:
            cursor.execute("""
                SELECT id, nombre_normalizado, precio_promedio_global
                FROM productos_maestros
                WHERE codigo_ean = ?
                LIMIT 1
            """, (codigo,))

        row = cursor.fetchone()
        if row:
            producto_id = row[0]
            print(f"   ✅ Producto encontrado por EAN: ID {producto_id}")

            # Actualizar precio promedio
            actualizar_precio_promedio(producto_id, precio, cursor, conn)

            return producto_id

    # PASO 2: Buscar por similitud de nombre
    print(f"   🔍 Buscando por similitud de nombre...")

    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT id, nombre_normalizado, codigo_ean, precio_promedio_global
            FROM productos_maestros
            WHERE nombre_normalizado ILIKE %s
            LIMIT 10
        """, (f"%{nombre_normalizado[:20]}%",))
    else:
        cursor.execute("""
            SELECT id, nombre_normalizado, codigo_ean, precio_promedio_global
            FROM productos_maestros
            WHERE nombre_normalizado LIKE ?
            LIMIT 10
        """, (f"%{nombre_normalizado[:20]}%",))

    candidatos = cursor.fetchall()

    mejor_match = None
    mejor_similitud = 0.0

    for row in candidatos:
        producto_id, nombre_db, codigo_db, precio_db = row

        similitud = calcular_similitud(nombre_normalizado, nombre_db)

        print(f"   📊 Similitud con '{nombre_db}': {similitud:.2f}")

        # Si la similitud es muy alta (>85%), considerar match
        if similitud > 0.85 and similitud > mejor_similitud:
            mejor_similitud = similitud
            mejor_match = producto_id

    if mejor_match:
        print(f"   ✅ Producto similar encontrado: ID {mejor_match} (similitud: {mejor_similitud:.2f})")

        # Actualizar precio promedio
        actualizar_precio_promedio(mejor_match, precio, cursor, conn)

        return mejor_match

    # PASO 3: Crear producto nuevo
    print(f"   ➕ Creando producto nuevo...")

    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                precio_promedio_global,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion
            ) VALUES (%s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
        """, (codigo or None, nombre_normalizado, precio))

        producto_id = cursor.fetchone()[0]
    else:
        cursor.execute("""
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                precio_promedio_global,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion
            ) VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (codigo or None, nombre_normalizado, precio))

        producto_id = cursor.lastrowid

    conn.commit()

    print(f"   ✅ Producto creado: ID {producto_id}")

    return producto_id


def actualizar_precio_promedio(producto_id: int, nuevo_precio: int, cursor, conn):
    """
    Actualiza el precio promedio de un producto
    """
    import os

    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            UPDATE productos_maestros
            SET precio_promedio_global = (
                (precio_promedio_global * total_reportes + %s) / (total_reportes + 1)
            ),
            total_reportes = total_reportes + 1,
            ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (nuevo_precio, producto_id))
    else:
        cursor.execute("""
            UPDATE productos_maestros
            SET precio_promedio_global = (
                (precio_promedio_global * total_reportes + ?) / (total_reportes + 1)
            ),
            total_reportes = total_reportes + 1,
            ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (nuevo_precio, producto_id))

    conn.commit()
    print(f"   💰 Precio promedio actualizado para producto {producto_id}")


# ==========================================
# FUNCIONES DE CONSOLIDACIÓN MANUAL
# ==========================================

def fusionar_productos_duplicados(producto_principal_id: int, productos_a_fusionar: list, cursor, conn):
    """
    Fusiona varios productos duplicados en uno solo

    Args:
        producto_principal_id: ID del producto que se mantendrá
        productos_a_fusionar: Lista de IDs de productos a fusionar en el principal
        cursor: Cursor de BD
        conn: Conexión a BD
    """
    import os

    print(f"🔀 Fusionando productos en ID {producto_principal_id}")
    print(f"   Productos a fusionar: {productos_a_fusionar}")

    for producto_id in productos_a_fusionar:
        # Actualizar items_factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                UPDATE items_factura
                SET producto_maestro_id = %s
                WHERE producto_maestro_id = %s
            """, (producto_principal_id, producto_id))

            # Actualizar inventario_usuario
            cursor.execute("""
                UPDATE inventario_usuario
                SET producto_maestro_id = %s
                WHERE producto_maestro_id = %s
            """, (producto_principal_id, producto_id))

            # Eliminar producto duplicado
            cursor.execute("""
                DELETE FROM productos_maestros
                WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                UPDATE items_factura
                SET producto_maestro_id = ?
                WHERE producto_maestro_id = ?
            """, (producto_principal_id, producto_id))

            cursor.execute("""
                UPDATE inventario_usuario
                SET producto_maestro_id = ?
                WHERE producto_maestro_id = ?
            """, (producto_principal_id, producto_id))

            cursor.execute("""
                DELETE FROM productos_maestros
                WHERE id = ?
            """, (producto_id,))

    conn.commit()

    print(f"   ✅ Productos fusionados correctamente")


def detectar_duplicados_por_similitud(cursor, umbral: float = 0.90) -> list:
    """
    Detecta productos duplicados por similitud de nombre

    Returns:
        list: Lista de tuplas (id1, nombre1, id2, nombre2, similitud)
    """
    import os

    print(f"🔍 Detectando duplicados (umbral: {umbral})")

    # Obtener todos los productos
    cursor.execute("""
        SELECT id, nombre_normalizado, codigo_ean
        FROM productos_maestros
        ORDER BY id
    """)

    productos = cursor.fetchall()
    duplicados = []

    # Comparar cada producto con los demás
    for i in range(len(productos)):
        for j in range(i + 1, len(productos)):
            id1, nombre1, codigo1 = productos[i]
            id2, nombre2, codigo2 = productos[j]

            # Si tienen el mismo código EAN, son el mismo producto
            if codigo1 and codigo2 and codigo1 == codigo2:
                similitud = 1.0
            else:
                similitud = calcular_similitud(nombre1, nombre2)

            if similitud >= umbral:
                duplicados.append((id1, nombre1, id2, nombre2, similitud))

    print(f"   ✅ {len(duplicados)} duplicados detectados")

    return duplicados
