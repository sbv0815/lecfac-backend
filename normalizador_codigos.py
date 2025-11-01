"""
MÓDULO DE NORMALIZACIÓN DE CÓDIGOS
Para manejar diferentes tipos de códigos según establecimiento
"""

def normalizar_codigo_por_establecimiento(codigo: str, establecimiento: str) -> tuple:
    """
    Normaliza códigos según establecimiento y detecta tipo

    Args:
        codigo: Código leído del OCR
        establecimiento: Nombre del establecimiento

    Returns:
        tuple: (codigo_normalizado, tipo_codigo, confianza)

    Tipos de código:
        - EAN: Código de barras estándar internacional (8-13 dígitos)
        - PLU: Price Look-Up, usado para frutas/verduras (3-7 dígitos)
        - INTERNO: Código interno del establecimiento
        - sin_codigo: No tiene código válido

    Confianza:
        - 100: EAN estándar
        - 95: EAN normalizado (ej: ARA sin 0 inicial)
        - 70: Código interno validado
        - 50: PLU (puede repetirse entre establecimientos)
        - 30: Código desconocido
        - 0: Sin código
    """
    if not codigo:
        return (None, 'sin_codigo', 0)

    codigo = str(codigo).strip()
    establecimiento_lower = establecimiento.lower() if establecimiento else ""

    # ===========================================
    # 1. ARA / JERONIMO MARTINS
    # ===========================================
    if any(x in establecimiento_lower for x in ['ara', 'jeronimo', 'martins']):
        # ARA antepone un 0 extra
        if codigo.startswith('0') and len(codigo) >= 11:
            codigo_limpio = codigo.lstrip('0')
            if 8 <= len(codigo_limpio) <= 13:
                print(f"   🔧 ARA normalizado: {codigo} → {codigo_limpio}")
                return (codigo_limpio, 'EAN', 95)

    # ===========================================
    # 2. D1
    # ===========================================
    elif 'd1' in establecimiento_lower:
        # D1 usa códigos internos de 6-8 dígitos
        if codigo.isdigit() and 6 <= len(codigo) <= 8:
            return (f"D1_{codigo}", 'INTERNO', 70)
        # Si tiene EAN estándar, usarlo
        elif codigo.isdigit() and 8 <= len(codigo) <= 13:
            return (codigo, 'EAN', 100)

    # ===========================================
    # 3. ALKOSTO / MAKRO
    # ===========================================
    elif any(x in establecimiento_lower for x in ['alkosto', 'makro']):
        # A veces usan códigos de 7 dígitos para productos al peso
        if codigo.isdigit() and len(codigo) == 7:
            return (f"ALKOSTO_{codigo}", 'INTERNO', 70)

    # ===========================================
    # 4. EAN ESTÁNDAR (8-13 dígitos)
    # ===========================================
    if codigo.isdigit() and 8 <= len(codigo) <= 13:
        return (codigo, 'EAN', 100)

    # ===========================================
    # 5. PLU (3-7 dígitos) - productos frescos
    # ===========================================
    # Estos códigos son específicos por establecimiento
    if codigo.isdigit() and 3 <= len(codigo) <= 7:
        # Prefijar con establecimiento para evitar conflictos
        # Ej: "123" en Jumbo ≠ "123" en Éxito
        prefijo = establecimiento_lower.split()[0][:10]  # Primeras 10 letras
        codigo_con_prefijo = f"PLU_{prefijo}_{codigo}"
        return (codigo_con_prefijo, 'PLU', 50)

    # ===========================================
    # 6. CÓDIGO DESCONOCIDO (pero válido)
    # ===========================================
    if codigo.isdigit() and len(codigo) > 0:
        # Prefijarlo con establecimiento para seguridad
        prefijo = establecimiento_lower.split()[0][:10]
        codigo_con_prefijo = f"{prefijo}_{codigo}"
        return (codigo_con_prefijo, 'DESCONOCIDO', 30)

    # ===========================================
    # 7. SIN CÓDIGO
    # ===========================================
    return (None, 'sin_codigo', 0)


def buscar_o_crear_producto_inteligente(
    cursor, conn,
    codigo: str,
    tipo_codigo: str,
    nombre: str,
    establecimiento: str,
    precio: int
) -> tuple:
    """
    Busca o crea producto según el tipo de código

    Args:
        cursor: Cursor de BD
        conn: Conexión de BD
        codigo: Código normalizado
        tipo_codigo: Tipo ('EAN', 'PLU', 'INTERNO', 'sin_codigo')
        nombre: Nombre del producto
        establecimiento: Establecimiento
        precio: Precio del producto

    Returns:
        tuple: (producto_maestro_id, accion)
        accion: 'encontrado_ean', 'encontrado_nombre', 'creado_nuevo'
    """

    # ===========================================
    # CASO 1: EAN (búsqueda global)
    # ===========================================
    if tipo_codigo == 'EAN' and codigo:
        cursor.execute("""
            SELECT id FROM productos_maestros
            WHERE codigo_ean = %s
        """, (codigo,))

        resultado = cursor.fetchone()
        if resultado:
            print(f"   ✅ Producto encontrado por EAN: ID={resultado[0]}")
            return (resultado[0], 'encontrado_ean')

    # ===========================================
    # CASO 2: PLU o INTERNO (búsqueda por código + establecimiento)
    # ===========================================
    if tipo_codigo in ['PLU', 'INTERNO'] and codigo:
        # El código ya viene prefijado con el establecimiento
        cursor.execute("""
            SELECT id FROM productos_maestros
            WHERE codigo_ean = %s
        """, (codigo,))

        resultado = cursor.fetchone()
        if resultado:
            print(f"   ✅ Producto local encontrado: ID={resultado[0]}")
            return (resultado[0], 'encontrado_codigo_local')

    # ===========================================
    # CASO 3: Sin código o no encontrado - buscar por NOMBRE SIMILAR
    # ===========================================
    if nombre and len(nombre) >= 3:
        nombre_busqueda = nombre.lower().strip()

        # Buscar productos con nombres similares
        cursor.execute("""
            SELECT id, nombre_normalizado
            FROM productos_maestros
            WHERE LOWER(nombre_normalizado) LIKE %s
            LIMIT 5
        """, (f"%{nombre_busqueda[:20]}%",))

        resultados = cursor.fetchall()

        if resultados:
            # Si hay coincidencias muy cercanas, usar el primero
            for row in resultados:
                nombre_existente = row[1].lower()
                # Coincidencia de al menos 70% de las palabras
                palabras_busqueda = set(nombre_busqueda.split())
                palabras_existente = set(nombre_existente.split())

                if len(palabras_busqueda & palabras_existente) >= len(palabras_busqueda) * 0.7:
                    print(f"   ✅ Producto encontrado por nombre: ID={row[0]} ('{row[1]}')")
                    return (row[0], 'encontrado_nombre')

    # ===========================================
    # CASO 4: CREAR NUEVO PRODUCTO
    # ===========================================
    try:
        cursor.execute("""
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                precio_promedio_global,
                total_reportes,
                primera_vez_reportado
            ) VALUES (%s, %s, %s, 1, CURRENT_TIMESTAMP)
            RETURNING id
        """, (codigo if codigo else None, nombre, precio))

        nuevo_id = cursor.fetchone()[0]
        conn.commit()

        tipo_msg = "EAN" if tipo_codigo == 'EAN' else tipo_codigo
        print(f"   ➕ Producto nuevo creado: ID={nuevo_id} ({tipo_msg})")
        return (nuevo_id, 'creado_nuevo')

    except Exception as e:
        print(f"   ❌ Error creando producto: {e}")
        conn.rollback()
        raise



# ===========================================
# EJEMPLO DE USO
# ===========================================
if __name__ == "__main__":
    # Ejemplos de normalización

    tests = [
        ("07042623245", "JERONIMO MARTINS COLOMBIA"),
        ("7042623245", "JUMBO"),
        ("123", "ÉXITO"),
        ("12345", "D1"),
        ("7702265014", "CARULLA"),
        ("", "ALKOSTO"),
    ]

    print("\n🧪 PRUEBAS DE NORMALIZACIÓN:")
    print("="*80)

    for codigo, establecimiento in tests:
        codigo_norm, tipo, confianza = normalizar_codigo_por_establecimiento(codigo, establecimiento)
        codigo_display = codigo_norm if codigo_norm else "None"
        print(f"{establecimiento:30} | {codigo:15} → {codigo_display:20} | {tipo:12} | Confianza: {confianza}%")

    print("="*80)
