"""
product_matcher.py - VERSIÓN MEJORADA V3
Sistema de matching y normalización de productos

CAMBIOS V3:
- ✅ GUARDA EN AMBAS TABLAS: productos_maestros (legacy) y productos_maestros_v2 (nueva)
- ✅ Sincronización automática entre tablas
- Busca PLUs cortos por nombre (no por código)
- Soporte para productos con múltiples PLUs por establecimiento
- Lógica: 1 producto maestro → N códigos PLU (uno por establecimiento)
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


def clasificar_codigo_tipo(codigo: str) -> str:
    """
    Clasifica el tipo de código basándose en su longitud

    Returns:
        'EAN' para códigos de 8+ dígitos
        'PLU' para códigos de 3-7 dígitos
        'DESCONOCIDO' para otros casos
    """
    if not codigo or not isinstance(codigo, str):
        return 'DESCONOCIDO'

    codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))

    if not codigo_limpio:
        return 'DESCONOCIDO'

    longitud = len(codigo_limpio)

    if longitud >= 8:
        return 'EAN'
    elif 3 <= longitud <= 7:
        return 'PLU'
    else:
        return 'DESCONOCIDO'


def crear_producto_en_ambas_tablas(
    codigo_ean: str,
    nombre_normalizado: str,
    precio: int,
    cursor,
    conn
) -> int:
    """
    ✅ NUEVO: Crea producto en AMBAS tablas simultáneamente

    Args:
        codigo_ean: Código EAN (puede ser None para PLUs)
        nombre_normalizado: Nombre normalizado del producto
        precio: Precio inicial
        cursor: Cursor de BD
        conn: Conexión a BD

    Returns:
        int: ID del producto en productos_maestros (LEGACY) ⬅️ CAMBIO CRÍTICO
    """
    import os

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    # PASO 1: Crear en productos_maestros (legacy) ⬅️ PRIMERO
    if is_postgresql:
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
        """, (codigo_ean, nombre_normalizado, precio))
        producto_legacy_id = cursor.fetchone()[0]
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
        """, (codigo_ean, nombre_normalizado, precio))
        producto_legacy_id = cursor.lastrowid

    # PASO 2: Crear en productos_maestros_v2 (nueva tabla)
    if is_postgresql:
        cursor.execute("""
            INSERT INTO productos_maestros_v2 (
                codigo_ean,
                nombre_consolidado,
                marca,
                categoria_id
            ) VALUES (%s, %s, NULL, NULL)
            RETURNING id
        """, (codigo_ean, nombre_normalizado))
        producto_v2_id = cursor.fetchone()[0]
    else:
        cursor.execute("""
            INSERT INTO productos_maestros_v2 (
                codigo_ean,
                nombre_consolidado,
                marca,
                categoria_id
            ) VALUES (?, ?, NULL, NULL)
        """, (codigo_ean, nombre_normalizado))
        producto_v2_id = cursor.lastrowid

    conn.commit()

    print(f"   ✅ Producto creado en AMBAS tablas:")
    print(f"      - productos_maestros (legacy): ID {producto_legacy_id} ⬅️ Este se retorna")
    print(f"      - productos_maestros_v2: ID {producto_v2_id}")

    return producto_legacy_id  # ⬅️ RETORNAR ID LEGACY, NO V2


def buscar_o_crear_producto_inteligente(
    codigo: str,
    nombre: str,
    precio: int,
    establecimiento: str,
    cursor,
    conn
) -> int:
    """
    Busca un producto existente o crea uno nuevo EN AMBAS TABLAS

    LÓGICA V3 MEJORADA:
    1. Clasificar código como EAN o PLU
    2. Si es EAN (8+ dígitos):
       - Buscar por codigo_ean en productos_maestros (LEGACY)
       - Si no existe, crear en AMBAS tablas
    3. Si es PLU (3-7 dígitos):
       - Buscar por similitud de nombre (>85%)
       - Si no existe, crear en AMBAS tablas SIN codigo_ean
    4. Si no tiene código:
       - Buscar por similitud de nombre
       - Si no existe, crear en AMBAS tablas SIN codigo_ean

    Returns:
        int: producto_maestro_id de productos_maestros (LEGACY) ⬅️ CAMBIO CRÍTICO
    """
    import os

    nombre_normalizado = normalizar_nombre_producto(nombre)
    tipo_codigo = clasificar_codigo_tipo(codigo)

    print(f"🔍 buscar_o_crear_producto_inteligente() V3")
    print(f"   Código: {codigo} → Tipo: {tipo_codigo}")
    print(f"   Nombre original: {nombre}")
    print(f"   Nombre normalizado: {nombre_normalizado}")
    print(f"   Precio: ${precio:,}")
    print(f"   Establecimiento: {establecimiento}")

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    # =================================================================
    # CASO 1: CÓDIGO EAN (8+ dígitos) - Buscar por codigo_ean
    # =================================================================
    if tipo_codigo == 'EAN':
        print(f"   🔍 Buscando por código EAN en productos_maestros (legacy): {codigo}")

        if is_postgresql:
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
            nombre_existente = row[1]
            print(f"   ✅ Producto encontrado por EAN: ID {producto_id} (legacy)")
            print(f"      Nombre en BD: {nombre_existente}")

            # Actualizar precio promedio
            actualizar_precio_promedio_legacy(codigo, precio, cursor, conn)

            # Asegurar que existe en productos_maestros_v2
            sincronizar_a_v2(producto_id, codigo, nombre_normalizado, cursor, conn)

            return producto_id

        # No existe, crear nuevo producto con este EAN EN AMBAS TABLAS
        print(f"   ➕ Creando producto nuevo con EAN: {codigo}")
        producto_id = crear_producto_en_ambas_tablas(
            codigo_ean=codigo,
            nombre_normalizado=nombre_normalizado,
            precio=precio,
            cursor=cursor,
            conn=conn
        )

        return producto_id

    # =================================================================
    # CASO 2: CÓDIGO PLU (3-7 dígitos) o SIN CÓDIGO - Buscar por nombre
    # =================================================================
    print(f"   🔍 Código PLU o sin código - Buscando por similitud de nombre en productos_maestros (legacy)...")

    # Buscar productos similares por nombre
    if is_postgresql:
        cursor.execute("""
            SELECT id, nombre_normalizado, codigo_ean, precio_promedio_global
            FROM productos_maestros
            WHERE nombre_normalizado ILIKE %s
            LIMIT 20
        """, (f"%{nombre_normalizado[:30]}%",))
    else:
        cursor.execute("""
            SELECT id, nombre_normalizado, codigo_ean, precio_promedio_global
            FROM productos_maestros
            WHERE nombre_normalizado LIKE ?
            LIMIT 20
        """, (f"%{nombre_normalizado[:30]}%",))

    candidatos = cursor.fetchall()

    mejor_match = None
    mejor_similitud = 0.0
    umbral_similitud = 0.85  # 85% de similitud mínima

    for row in candidatos:
        producto_id, nombre_db, codigo_db, precio_db = row

        similitud = calcular_similitud(nombre_normalizado, nombre_db)

        if similitud > umbral_similitud:
            print(f"   📊 Similitud con '{nombre_db}': {similitud:.2f} (ID: {producto_id})")

            if similitud > mejor_similitud:
                mejor_similitud = similitud
                mejor_match = producto_id

    if mejor_match:
        print(f"   ✅ Producto similar encontrado: ID {mejor_match} (legacy, similitud: {mejor_similitud:.2f})")

        # Actualizar precio promedio
        actualizar_precio_promedio_legacy(None, precio, cursor, conn, nombre_normalizado)

        # Asegurar que existe en productos_maestros_v2
        sincronizar_a_v2(mejor_match, None, nombre_normalizado, cursor, conn)

        return mejor_match

    # No se encontró producto similar, crear uno nuevo SIN codigo_ean EN AMBAS TABLAS
    print(f"   ➕ Creando producto nuevo SIN código EAN (será PLU específico por establecimiento)")

    producto_id = crear_producto_en_ambas_tablas(
        codigo_ean=None,
        nombre_normalizado=nombre_normalizado,
        precio=precio,
        cursor=cursor,
        conn=conn
    )

    print(f"      (Los códigos PLU se guardarán en productos_por_establecimiento)")

    return producto_id


def sincronizar_a_v2(producto_legacy_id: int, codigo_ean: str, nombre: str, cursor, conn):
    """
    Asegura que un producto de productos_maestros también exista en productos_maestros_v2

    Args:
        producto_legacy_id: ID en productos_maestros
        codigo_ean: Código EAN (puede ser None)
        nombre: Nombre del producto
        cursor: Cursor de BD
        conn: Conexión a BD
    """
    import os

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    # Verificar si ya existe en v2
    if is_postgresql:
        if codigo_ean:
            cursor.execute("""
                SELECT id FROM productos_maestros_v2
                WHERE codigo_ean = %s
                LIMIT 1
            """, (codigo_ean,))
        else:
            cursor.execute("""
                SELECT id FROM productos_maestros_v2
                WHERE nombre_consolidado ILIKE %s
                LIMIT 1
            """, (nombre,))
    else:
        if codigo_ean:
            cursor.execute("""
                SELECT id FROM productos_maestros_v2
                WHERE codigo_ean = ?
                LIMIT 1
            """, (codigo_ean,))
        else:
            cursor.execute("""
                SELECT id FROM productos_maestros_v2
                WHERE nombre_consolidado LIKE ?
                LIMIT 1
            """, (nombre,))

    if cursor.fetchone():
        # Ya existe en v2
        return

    # No existe, crear en v2
    if is_postgresql:
        cursor.execute("""
            INSERT INTO productos_maestros_v2 (
                codigo_ean,
                nombre_consolidado,
                marca,
                categoria_id
            ) VALUES (%s, %s, NULL, NULL)
        """, (codigo_ean, nombre))
    else:
        cursor.execute("""
            INSERT INTO productos_maestros_v2 (
                codigo_ean,
                nombre_consolidado,
                marca,
                categoria_id
            ) VALUES (?, ?, NULL, NULL)
        """, (codigo_ean, nombre))

    conn.commit()
    print(f"   🔄 Sincronizado a productos_maestros_v2 (legacy ID: {producto_legacy_id})")


def actualizar_precio_promedio_legacy(codigo_ean: str, nuevo_precio: int, cursor, conn, nombre: str = None):
    """
    Actualiza el precio promedio en la tabla legacy (productos_maestros)
    """
    import os

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    # Buscar en tabla legacy
    if codigo_ean:
        if is_postgresql:
            cursor.execute("""
                SELECT id FROM productos_maestros
                WHERE codigo_ean = %s
                LIMIT 1
            """, (codigo_ean,))
        else:
            cursor.execute("""
                SELECT id FROM productos_maestros
                WHERE codigo_ean = ?
                LIMIT 1
            """, (codigo_ean,))
    elif nombre:
        if is_postgresql:
            cursor.execute("""
                SELECT id FROM productos_maestros
                WHERE nombre_normalizado ILIKE %s
                LIMIT 1
            """, (nombre,))
        else:
            cursor.execute("""
                SELECT id FROM productos_maestros
                WHERE nombre_normalizado LIKE ?
                LIMIT 1
            """, (nombre,))
    else:
        return

    row = cursor.fetchone()
    if not row:
        return

    producto_legacy_id = row[0]

    if is_postgresql:
        cursor.execute("""
            UPDATE productos_maestros
            SET precio_promedio_global = (
                (precio_promedio_global * total_reportes + %s) / (total_reportes + 1)
            ),
            total_reportes = total_reportes + 1,
            ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (nuevo_precio, producto_legacy_id))
    else:
        cursor.execute("""
            UPDATE productos_maestros
            SET precio_promedio_global = (
                (precio_promedio_global * total_reportes + ?) / (total_reportes + 1)
            ),
            total_reportes = total_reportes + 1,
            ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (nuevo_precio, producto_legacy_id))

    conn.commit()
    print(f"   💰 Precio promedio actualizado en tabla legacy: ID {producto_legacy_id}")


def actualizar_precio_promedio(producto_id: int, nuevo_precio: int, cursor, conn):
    """
    LEGACY: Mantiene compatibilidad con código antiguo
    """
    actualizar_precio_promedio_legacy(None, nuevo_precio, cursor, conn)


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


print("=" * 80)
print("✅ product_matcher.py V3 CARGADO")
print("=" * 80)
print("🔄 GUARDA EN AMBAS TABLAS:")
print("   • productos_maestros (legacy)")
print("   • productos_maestros_v2 (nueva)")
print("✅ Sincronización automática entre tablas")
print("=" * 80)
