"""
MÓDULO DE NORMALIZACIÓN DE CÓDIGOS - V2 (Compatible psycopg3)
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
    """
    if not codigo:
        return (None, 'sin_codigo', 0)

    codigo = str(codigo).strip()
    establecimiento_lower = establecimiento.lower() if establecimiento else ""

    # ARA / JERONIMO MARTINS
    if any(x in establecimiento_lower for x in ['ara', 'jeronimo', 'martins']):
        if codigo.startswith('0') and len(codigo) >= 11:
            codigo_limpio = codigo.lstrip('0')
            if 8 <= len(codigo_limpio) <= 13:
                print(f"   🔧 ARA normalizado: {codigo} → {codigo_limpio}")
                return (codigo_limpio, 'EAN', 95)

    # D1
    elif 'd1' in establecimiento_lower:
        if codigo.isdigit() and 6 <= len(codigo) <= 8:
            return (f"D1_{codigo}", 'INTERNO', 70)
        elif codigo.isdigit() and 8 <= len(codigo) <= 13:
            return (codigo, 'EAN', 100)

    # ALKOSTO / MAKRO
    elif any(x in establecimiento_lower for x in ['alkosto', 'makro']):
        if codigo.isdigit() and len(codigo) == 7:
            return (f"ALKOSTO_{codigo}", 'INTERNO', 70)

    # EAN ESTÁNDAR (8-13 dígitos)
    if codigo.isdigit() and 8 <= len(codigo) <= 13:
        return (codigo, 'EAN', 100)

    # PLU (3-7 dígitos) - productos frescos
    if codigo.isdigit() and 3 <= len(codigo) <= 7:
        prefijo = establecimiento_lower.split()[0][:10]
        codigo_con_prefijo = f"PLU_{prefijo}_{codigo}"
        return (codigo_con_prefijo, 'PLU', 50)

    # CÓDIGO DESCONOCIDO (pero válido)
    if codigo.isdigit() and len(codigo) > 0:
        prefijo = establecimiento_lower.split()[0][:10]
        codigo_con_prefijo = f"{prefijo}_{codigo}"
        return (codigo_con_prefijo, 'DESCONOCIDO', 30)

    # SIN CÓDIGO
    return (None, 'sin_codigo', 0)


def buscar_o_crear_producto_inteligente(
    cursor, conn,
    codigo: str,
    tipo_codigo: str,
    nombre: str,
    establecimiento: str,
    precio: int,
    codigo_raw: str = None
) -> tuple:
    """
    Busca o crea producto según el tipo de código
    ✅ COMPATIBLE CON PSYCOPG3
    """

    # ===========================================
    # PASO 0: Obtener establecimiento_id
    # ===========================================
    try:
        cursor.execute("""
            SELECT id FROM establecimientos
            WHERE LOWER(TRIM(nombre_normalizado)) = LOWER(TRIM(%s))
            LIMIT 1
        """, (establecimiento,))

        # ✅ CORRECCIÓN: Verificar si hay resultado antes de fetchone()
        resultado_est = cursor.fetchone()

        if resultado_est:
            establecimiento_id = resultado_est[0]
            print(f"   🏪 Establecimiento encontrado: {establecimiento} (ID={establecimiento_id})")
        else:
            # Crear establecimiento si no existe
            print(f"   ➕ Creando establecimiento: {establecimiento}")
            cursor.execute("""
                INSERT INTO establecimientos (nombre_normalizado, activo)
                VALUES (%s, TRUE)
                RETURNING id
            """, (establecimiento,))
            establecimiento_id = cursor.fetchone()[0]
            conn.commit()
            print(f"   ✅ Establecimiento creado: ID={establecimiento_id}")

    except Exception as e:
        print(f"   ❌ Error obteniendo establecimiento: {e}")
        establecimiento_id = None

    # ===========================================
    # CASO 1: EAN (búsqueda global)
    # ===========================================
    if tipo_codigo == 'EAN' and codigo:
        try:
            cursor.execute("""
                SELECT id FROM productos_maestros
                WHERE codigo_ean = %s
            """, (codigo,))

            resultado = cursor.fetchone()
            if resultado:
                print(f"   ✅ Producto encontrado por EAN: ID={resultado[0]}")
                return (resultado[0], 'encontrado_ean')
        except Exception as e:
            print(f"   ⚠️ Error buscando por EAN: {e}")

    # ===========================================
    # CASO 2: PLU o INTERNO (búsqueda en codigos_locales)
    # ===========================================
    if tipo_codigo in ['PLU', 'INTERNO', 'DESCONOCIDO'] and codigo_raw and establecimiento_id:
        try:
            cursor.execute("""
                SELECT cl.producto_maestro_id, pm.nombre_normalizado, cl.veces_visto
                FROM codigos_locales cl
                JOIN productos_maestros pm ON cl.producto_maestro_id = pm.id
                WHERE cl.codigo_local = %s
                  AND cl.establecimiento_id = %s
                  AND cl.activo = TRUE
            """, (codigo_raw, establecimiento_id))

            resultado = cursor.fetchone()
            if resultado:
                producto_id = resultado[0]
                nombre_existente = resultado[1]
                veces_visto = resultado[2]

                # Actualizar estadísticas
                cursor.execute("""
                    UPDATE codigos_locales
                    SET veces_visto = veces_visto + 1,
                        ultima_vez_visto = CURRENT_TIMESTAMP
                    WHERE codigo_local = %s
                      AND establecimiento_id = %s
                """, (codigo_raw, establecimiento_id))
                conn.commit()

                print(f"   ✅ Código local encontrado: {codigo_raw} → Producto #{producto_id} (visto {veces_visto+1} veces)")
                return (producto_id, 'encontrado_codigo_local')
        except Exception as e:
            print(f"   ⚠️ Error buscando código local: {e}")

    # ===========================================
    # CASO 3: Buscar por NOMBRE SIMILAR
    # ===========================================
    if nombre and len(nombre) >= 3:
        try:
            nombre_busqueda = nombre.lower().strip()

            cursor.execute("""
                SELECT id, nombre_normalizado
                FROM productos_maestros
                WHERE LOWER(nombre_normalizado) LIKE %s
                LIMIT 5
            """, (f"%{nombre_busqueda[:20]}%",))

            resultados = cursor.fetchall()

            if resultados:
                for row in resultados:
                    producto_id = row[0]
                    nombre_existente = row[1].lower()

                    # Coincidencia de al menos 70% de las palabras
                    palabras_busqueda = set(nombre_busqueda.split())
                    palabras_existente = set(nombre_existente.split())

                    if len(palabras_busqueda & palabras_existente) >= len(palabras_busqueda) * 0.7:
                        print(f"   ✅ Producto encontrado por nombre: ID={producto_id} ('{row[1]}')")

                        # Registrar el código local si existe
                        if codigo_raw and establecimiento_id and tipo_codigo in ['PLU', 'INTERNO', 'DESCONOCIDO']:
                            try:
                                cursor.execute("""
                                    INSERT INTO codigos_locales (
                                        producto_maestro_id,
                                        establecimiento_id,
                                        codigo_local,
                                        descripcion_local,
                                        veces_visto,
                                        activo
                                    ) VALUES (%s, %s, %s, %s, 1, TRUE)
                                    ON CONFLICT (establecimiento_id, codigo_local)
                                    DO UPDATE SET veces_visto = codigos_locales.veces_visto + 1
                                """, (producto_id, establecimiento_id, codigo_raw, nombre))
                                conn.commit()
                                print(f"   📝 Código local aprendido: {codigo_raw} → Producto #{producto_id}")
                            except Exception as e:
                                print(f"   ⚠️ No se pudo registrar código local: {e}")
                                conn.rollback()

                        return (producto_id, 'encontrado_nombre')
        except Exception as e:
            print(f"   ⚠️ Error buscando por nombre: {e}")

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
        """, (codigo if tipo_codigo == 'EAN' else None, nombre, precio))

        nuevo_id = cursor.fetchone()[0]
        conn.commit()

        tipo_msg = "EAN" if tipo_codigo == 'EAN' else tipo_codigo
        print(f"   ➕ Producto nuevo creado: ID={nuevo_id} ({tipo_msg})")

        # Si tiene código local (PLU/INTERNO), registrarlo
        if codigo_raw and establecimiento_id and tipo_codigo in ['PLU', 'INTERNO', 'DESCONOCIDO']:
            try:
                cursor.execute("""
                    INSERT INTO codigos_locales (
                        producto_maestro_id,
                        establecimiento_id,
                        codigo_local,
                        descripcion_local,
                        veces_visto,
                        activo
                    ) VALUES (%s, %s, %s, %s, 1, TRUE)
                """, (nuevo_id, establecimiento_id, codigo_raw, nombre))
                conn.commit()
                print(f"   📝 Código local registrado: {codigo_raw} en {establecimiento}")
            except Exception as e:
                print(f"   ⚠️ No se pudo registrar código local: {e}")
                conn.rollback()

        return (nuevo_id, 'creado_nuevo')

    except Exception as e:
        print(f"   ❌ Error creando producto: {e}")
        conn.rollback()
        return (None, 'error')


print("✅ normalizador_codigos V2 cargado (compatible psycopg3)")
