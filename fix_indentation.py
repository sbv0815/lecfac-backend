# fix_indentation.py
with open('main.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Buscar la línea que tiene "for producto in productos_unicos:"
# dentro de process_video_background_task
start_idx = None
for i, line in enumerate(lines):
    if 'for producto in productos_unicos:' in line and i > 2000:
        start_idx = i
        break

if start_idx:
    print(f"✅ Encontrado en línea {start_idx}")

    # Código correcto con indentación correcta
    codigo_correcto = """            for producto in productos_unicos:
                try:
                    codigo = producto.get("codigo", "")
                    nombre = producto.get("nombre", "Sin nombre")
                    precio = producto.get("precio") or producto.get("valor", 0)
                    cantidad = producto.get("cantidad", 1)

                    if not nombre or nombre.strip() == "":
                        print(f"⚠️ Producto sin nombre, omitiendo")
                        productos_fallidos += 1
                        continue

                    try:
                        cantidad = int(cantidad)
                        if cantidad <= 0:
                            cantidad = 1
                    except (ValueError, TypeError):
                        cantidad = 1

                    try:
                        precio = float(precio)
                        if precio < 0:
                            print(f"⚠️ Precio negativo para '{nombre}', omitiendo")
                            productos_fallidos += 1
                            continue
                    except (ValueError, TypeError):
                        precio = 0

                    if precio == 0:
                        print(f"⚠️ Precio cero para '{nombre}', omitiendo")
                        productos_fallidos += 1
                        continue

                    # Buscar o crear producto
                    producto_maestro_id = None

                    if codigo and len(codigo) >= 3:
                        producto_maestro_id = buscar_o_crear_producto_inteligente(
                            codigo=codigo,
                            nombre=nombre,
                            precio=int(precio),
                            establecimiento=establecimiento,
                            cursor=cursor,
                            conn=conn
                        )

                        if not producto_maestro_id:
                            print(f"   ⚠️ SKIP: No se pudo crear producto maestro para '{nombre}'")
                            productos_fallidos += 1
                            continue

                        print(f"   ✅ Producto Maestro ID: {producto_maestro_id} - {nombre}")

                    # Guardar en items_factura
                    if os.environ.get("DATABASE_TYPE") == "postgresql":
                        cursor.execute(
                            \"\"\"
                            INSERT INTO items_factura (
                                factura_id, usuario_id, producto_maestro_id,
                                codigo_leido, nombre_leido, cantidad, precio_pagado
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            \"\"\",
                            (
                                factura_id,
                                usuario_id,
                                producto_maestro_id,
                                codigo or None,
                                nombre,
                                cantidad,
                                precio,
                            ),
                        )
                    else:
                        cursor.execute(
                            \"\"\"
                            INSERT INTO items_factura (
                                factura_id, usuario_id, producto_maestro_id,
                                codigo_leido, nombre_leido, cantidad, precio_pagado
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            \"\"\",
                            (
                                factura_id,
                                usuario_id,
                                producto_maestro_id,
                                codigo or None,
                                nombre,
                                cantidad,
                                precio,
                            ),
                        )

                    productos_guardados += 1

                except Exception as e:
                    print(f"❌ Error guardando '{nombre}': {str(e)}")
                    import traceback
                    traceback.print_exc()
                    productos_fallidos += 1

                    if "constraint" in str(e).lower():
                        conn.rollback()
                        conn = get_db_connection()
                        cursor = conn.cursor()

                    continue
"""

    # Encontrar el final del bloque actual
    end_idx = start_idx + 1
    indent_count = 0
    for i in range(start_idx + 1, len(lines)):
        if 'if os.environ.get("DATABASE_TYPE")' in lines[i] and 'postgresql' in lines[i]:
            # Buscar hasta el continue final
            for j in range(i, min(i + 100, len(lines))):
                if lines[j].strip() == 'continue' and indent_count == 0:
                    end_idx = j + 1
                    break
            break

    print(f"Reemplazando líneas {start_idx} a {end_idx}")

    # Reemplazar
    new_lines = lines[:start_idx] + [codigo_correcto + '\n'] + lines[end_idx:]

    # Guardar
    with open('main.py', 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

    print("✅ Archivo corregido")
else:
    print("❌ No se encontró el bloque")
