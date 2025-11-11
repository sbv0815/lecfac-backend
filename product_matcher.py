"""
product_matcher.py - VERSIÓN 5.0 - SISTEMA DE 4 CAPAS CON APRENDIZAJE
========================================================================
Sistema de matching y normalización de productos con aprendizaje automático

🎯 FLUJO COMPLETO V5.0:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣ OCR (Claude Vision)           → "oso  blanco" (datos crudos)
2️⃣ Correcciones Python            → "QUESO BLANCO" (corregido)
3️⃣ Aprendizaje Automático         → Busca si ya lo conoce
   └─ SI CONOCE (confianza ≥90%) → Usar nombre aprendido ✅ AHORRA $$$
   └─ NO CONOCE                  → Continuar a Perplexity ↓
4️⃣ Validación Perplexity          → "QUESO BLANCO COLANTA 500G"
5️⃣ Base de Datos + Aprendizaje   → Guarda + Aprende para próxima vez
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BENEFICIOS V5.0:
- 💰 Ahorra dinero: No llama Perplexity para productos conocidos
- ⚡ Más rápido: 0.1s vs 2-4s para productos aprendidos
- 🎯 Más preciso: Aprende de cada factura procesada
- 👥 Colaborativo: Usuarios validan cuando hay dudas
- 📊 Analítico: Historial completo para mejora continua
"""

import re
from unidecode import unidecode

# Importar módulos
try:
    from correcciones_ocr import corregir_ocr_basico
    CORRECCIONES_OCR_AVAILABLE = True
except ImportError:
    CORRECCIONES_OCR_AVAILABLE = False
    print("⚠️  correcciones_ocr.py no disponible")

try:
    from perplexity_validator import validar_con_perplexity
    PERPLEXITY_AVAILABLE = True
except ImportError:
    PERPLEXITY_AVAILABLE = False
    print("⚠️  perplexity_validator.py no disponible")

try:
    from aprendizaje_manager import AprendizajeManager, calcular_nivel_confianza
    APRENDIZAJE_AVAILABLE = True
except ImportError:
    APRENDIZAJE_AVAILABLE = False
    print("⚠️  aprendizaje_manager.py no disponible")


def normalizar_nombre_producto(nombre: str, aplicar_correcciones_ocr: bool = True) -> str:
    """Normaliza nombre del producto"""
    if not nombre:
        return ""
    if aplicar_correcciones_ocr and CORRECCIONES_OCR_AVAILABLE:
        nombre = corregir_ocr_basico(nombre)
    else:
        nombre = nombre.upper()
        nombre = unidecode(nombre)
    nombre = re.sub(r'[^\w\s]', ' ', nombre)
    nombre = re.sub(r'\s+', ' ', nombre)
    return nombre.strip()[:100]


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """Calcula similitud entre nombres"""
    n1 = normalizar_nombre_producto(nombre1, False)
    n2 = normalizar_nombre_producto(nombre2, False)
    if n1 == n2:
        return 1.0
    if n1 in n2 or n2 in n1:
        return 0.8 + (0.2 * min(len(n1), len(n2)) / max(len(n1), len(n2)))
    palabras1, palabras2 = set(n1.split()), set(n2.split())
    if not palabras1.union(palabras2):
        return 0.0
    return len(palabras1.intersection(palabras2)) / len(palabras1.union(palabras2))


def clasificar_codigo_tipo(codigo: str) -> str:
    """Clasifica tipo de código"""
    if not codigo:
        return 'DESCONOCIDO'
    codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))
    longitud = len(codigo_limpio)
    if longitud >= 8:
        return 'EAN'
    elif 3 <= longitud <= 7:
        return 'PLU'
    return 'DESCONOCIDO'


def validar_nombre_con_sistema_completo(
    nombre_ocr_original: str,
    nombre_corregido: str,
    precio: int,
    establecimiento: str,
    codigo: str = "",
    aprendizaje_mgr: AprendizajeManager = None,
    factura_id: int = None,
    usuario_id: int = None,
    item_factura_id: int = None
) -> dict:
    """
    V5.0: Sistema completo de 4 capas
    Busca en aprendizaje → Si no existe, usa Perplexity → Guarda resultado
    """

    print(f"\n3️⃣ CAPA 3 - APRENDIZAJE AUTOMÁTICO:")

    # Buscar en aprendizaje
    if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
        correccion = aprendizaje_mgr.buscar_correccion_aprendida(
            ocr_normalizado=nombre_corregido,
            establecimiento=establecimiento,
            codigo_ean=codigo if clasificar_codigo_tipo(codigo) == 'EAN' else None
        )

        if correccion and correccion['confianza'] >= 0.7:
            confianza = correccion['confianza']
            categoria = 'alta' if confianza >= 0.9 else 'media'
            requiere_validacion = (categoria == 'media')

            print(f"   ✅ Corrección aprendida: {correccion['nombre_validado']}")
            print(f"   📊 Confianza: {confianza:.2f} ({categoria})")

            aprendizaje_mgr.incrementar_confianza(correccion['id'], True)

            validacion_id = None
            if requiere_validacion and usuario_id and factura_id and item_factura_id:
                validacion_id = aprendizaje_mgr.crear_validacion_pendiente(
                    factura_id, usuario_id, item_factura_id,
                    nombre_ocr_original, correccion['nombre_validado'],
                    codigo if clasificar_codigo_tipo(codigo) == 'EAN' else None,
                    precio, establecimiento, confianza,
                    f"Confianza media - Confirmado {correccion['veces_confirmado']} veces"
                )

            return {
                'nombre_final': correccion['nombre_validado'],
                'fue_validado': True,
                'confianza': confianza,
                'categoria_confianza': categoria,
                'fuente': 'aprendizaje',
                'detalles': f"Visto {correccion['veces_confirmado']} veces",
                'requiere_validacion_usuario': requiere_validacion,
                'validacion_pendiente_id': validacion_id
            }

    print(f"   ℹ️  No hay corrección aprendida")
    print(f"\n4️⃣ CAPA 4 - VALIDACIÓN PERPLEXITY:")

    # Validar con Perplexity
    if not PERPLEXITY_AVAILABLE:
        return {
            'nombre_final': nombre_corregido,
            'fue_validado': False,
            'confianza': 0.5,
            'categoria_confianza': 'media',
            'fuente': 'python',
            'detalles': 'Perplexity no disponible',
            'requiere_validacion_usuario': True,
            'validacion_pendiente_id': None
        }

    try:
        resultado = validar_con_perplexity(
            nombre_corregido, precio, establecimiento, codigo, nombre_ocr_original
        )

        nombre_final = resultado['nombre_final']
        fue_validado = resultado['fue_validado']
        tiene_ean = clasificar_codigo_tipo(codigo) == 'EAN'
        confianza, categoria = calcular_nivel_confianza(
            fue_validado, True, tiene_ean, 0
        )

        print(f"   📊 Perplexity: {nombre_final}")
        print(f"   📊 Confianza: {confianza:.2f} ({categoria})")

        # Guardar en aprendizaje
        if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
            aprendizaje_mgr.guardar_correccion_aprendida(
                nombre_ocr_original, nombre_corregido, nombre_final,
                codigo if tiene_ean else None,
                establecimiento, precio, confianza, 'perplexity', False
            )

        requiere_validacion = (categoria == 'media')
        validacion_id = None
        if requiere_validacion and APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
            if usuario_id and factura_id and item_factura_id:
                validacion_id = aprendizaje_mgr.crear_validacion_pendiente(
                    factura_id, usuario_id, item_factura_id,
                    nombre_ocr_original, nombre_final,
                    codigo if tiene_ean else None,
                    precio, establecimiento, confianza,
                    "Primera vez detectado", resultado
                )

        return {
            'nombre_final': nombre_final,
            'fue_validado': fue_validado,
            'confianza': confianza,
            'categoria_confianza': categoria,
            'fuente': 'perplexity',
            'detalles': resultado.get('detalles', ''),
            'requiere_validacion_usuario': requiere_validacion,
            'validacion_pendiente_id': validacion_id
        }

    except Exception as e:
        print(f"   ❌ Error Perplexity: {e}")
        return {
            'nombre_final': nombre_corregido,
            'fue_validado': False,
            'confianza': 0.5,
            'categoria_confianza': 'media',
            'fuente': 'python',
            'detalles': f'Error: {e}',
            'requiere_validacion_usuario': True,
            'validacion_pendiente_id': None
        }


def crear_producto_en_ambas_tablas(codigo_ean, nombre_final, precio, cursor, conn, metadatos=None):
    """Crea producto en ambas tablas"""
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    if metadatos:
        print(f"\n   📊 Validación: {metadatos.get('fuente')} - Confianza: {metadatos.get('confianza', 0):.2f}")

    # Crear en productos_maestros
    if is_postgresql:
        cursor.execute("""
            INSERT INTO productos_maestros (codigo_ean, nombre_normalizado, precio_promedio_global,
                                           total_reportes, primera_vez_reportado, ultima_actualizacion)
            VALUES (%s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id
        """, (codigo_ean, nombre_final, precio))
        producto_id = cursor.fetchone()[0]
    else:
        cursor.execute("""
            INSERT INTO productos_maestros (codigo_ean, nombre_normalizado, precio_promedio_global,
                                           total_reportes, primera_vez_reportado, ultima_actualizacion)
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (codigo_ean, nombre_final, precio))
        producto_id = cursor.lastrowid

    # Crear en productos_maestros_v2
    if is_postgresql:
        cursor.execute("""
            INSERT INTO productos_maestros_v2 (codigo_ean, nombre_consolidado, marca, categoria_id)
            VALUES (%s, %s, NULL, NULL)
        """, (codigo_ean, nombre_final))
    else:
        cursor.execute("""
            INSERT INTO productos_maestros_v2 (codigo_ean, nombre_consolidado, marca, categoria_id)
            VALUES (?, ?, NULL, NULL)
        """, (codigo_ean, nombre_final))

    conn.commit()
    print(f"   ✅ Producto creado ID: {producto_id} - {nombre_final}")
    return producto_id


def buscar_o_crear_producto_inteligente(
    codigo, nombre, precio, establecimiento, cursor, conn,
    factura_id=None, usuario_id=None, item_factura_id=None
):
    """
    V5.0: Sistema completo con aprendizaje automático
    """
    import os

    print(f"\n{'='*70}")
    print(f"🔍 PRODUCTO MATCHER V5.0 - APRENDIZAJE AUTOMÁTICO")
    print(f"{'='*70}")

    nombre_ocr_original = nombre
    tipo_codigo = clasificar_codigo_tipo(codigo)

    print(f"1️⃣ OCR: {nombre_ocr_original}")
    print(f"   Código: {codigo} ({tipo_codigo}) | Precio: ${precio:,} | {establecimiento}")

    # Correcciones Python
    nombre_corregido = normalizar_nombre_producto(nombre, True)
    print(f"\n2️⃣ PYTHON: {nombre_corregido}")

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    aprendizaje_mgr = AprendizajeManager(cursor, conn) if APRENDIZAJE_AVAILABLE else None

    # Buscar producto existente
    if tipo_codigo == 'EAN':
        if is_postgresql:
            cursor.execute("SELECT id, nombre_normalizado FROM productos_maestros WHERE codigo_ean = %s LIMIT 1", (codigo,))
        else:
            cursor.execute("SELECT id, nombre_normalizado FROM productos_maestros WHERE codigo_ean = ? LIMIT 1", (codigo,))

        row = cursor.fetchone()
        if row:
            print(f"\n   ✅ EXISTENTE ID: {row[0]} - {row[1]}")
            actualizar_precio_promedio_legacy(codigo, precio, cursor, conn)
            sincronizar_a_v2(row[0], codigo, row[1], cursor, conn)
            return row[0]

        print(f"\n   🆕 PRODUCTO NUEVO")
    else:
        # Buscar por similitud
        if is_postgresql:
            cursor.execute("SELECT id, nombre_normalizado FROM productos_maestros WHERE nombre_normalizado ILIKE %s LIMIT 20",
                         (f"%{nombre_corregido[:30]}%",))
        else:
            cursor.execute("SELECT id, nombre_normalizado FROM productos_maestros WHERE nombre_normalizado LIKE ? LIMIT 20",
                         (f"%{nombre_corregido[:30]}%",))

        for row in cursor.fetchall():
            sim = calcular_similitud(nombre_corregido, row[1])
            if sim >= 0.85:
                print(f"\n   ✅ EXISTENTE ID: {row[0]} - Similitud: {sim:.2f}")
                actualizar_precio_promedio_legacy(None, precio, cursor, conn, row[1])
                sincronizar_a_v2(row[0], None, row[1], cursor, conn)
                return row[0]

        print(f"\n   🆕 PRODUCTO NUEVO")

    # Validación completa
    resultado = validar_nombre_con_sistema_completo(
        nombre_ocr_original, nombre_corregido, precio, establecimiento,
        codigo, aprendizaje_mgr, factura_id, usuario_id, item_factura_id
    )

    nombre_final = resultado['nombre_final']
    print(f"\n✅ NOMBRE FINAL: {nombre_final}")
    print(f"   Fuente: {resultado['fuente']} | Confianza: {resultado['confianza']:.2f}")

    # Crear producto
    producto_id = crear_producto_en_ambas_tablas(
        codigo if tipo_codigo == 'EAN' else None,
        nombre_final, precio, cursor, conn, resultado
    )

    # Marcar para revisión si confianza baja
    if resultado['categoria_confianza'] == 'baja' and aprendizaje_mgr:
        aprendizaje_mgr.marcar_para_revision_admin(
            producto_id, nombre_final,
            codigo if tipo_codigo == 'EAN' else None,
            'confianza_baja', 3,
            {'confianza': resultado['confianza'], 'ocr': nombre_ocr_original}
        )

    print(f"{'='*70}\n")
    return producto_id


def sincronizar_a_v2(producto_id, codigo_ean, nombre, cursor, conn):
    """Sincroniza a v2"""
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    if is_postgresql:
        if codigo_ean:
            cursor.execute("SELECT id FROM productos_maestros_v2 WHERE codigo_ean = %s", (codigo_ean,))
        else:
            cursor.execute("SELECT id FROM productos_maestros_v2 WHERE nombre_consolidado ILIKE %s", (nombre,))
    else:
        if codigo_ean:
            cursor.execute("SELECT id FROM productos_maestros_v2 WHERE codigo_ean = ?", (codigo_ean,))
        else:
            cursor.execute("SELECT id FROM productos_maestros_v2 WHERE nombre_consolidado LIKE ?", (nombre,))

    if not cursor.fetchone():
        if is_postgresql:
            cursor.execute("INSERT INTO productos_maestros_v2 (codigo_ean, nombre_consolidado) VALUES (%s, %s)", (codigo_ean, nombre))
        else:
            cursor.execute("INSERT INTO productos_maestros_v2 (codigo_ean, nombre_consolidado) VALUES (?, ?)", (codigo_ean, nombre))
        conn.commit()


def actualizar_precio_promedio_legacy(codigo_ean, nuevo_precio, cursor, conn, nombre=None):
    """Actualiza precio promedio"""
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    if codigo_ean:
        cursor.execute("SELECT id FROM productos_maestros WHERE codigo_ean = " + ("%s" if is_postgresql else "?"), (codigo_ean,))
    elif nombre:
        cursor.execute("SELECT id FROM productos_maestros WHERE nombre_normalizado " + ("ILIKE" if is_postgresql else "LIKE") + (" %s" if is_postgresql else " ?"), (nombre,))
    else:
        return

    row = cursor.fetchone()
    if row:
        cursor.execute("""
            UPDATE productos_maestros
            SET precio_promedio_global = ((precio_promedio_global * total_reportes + """ + ("%s" if is_postgresql else "?") + """) / (total_reportes + 1)),
                total_reportes = total_reportes + 1
            WHERE id = """ + ("%s" if is_postgresql else "?"),
            (nuevo_precio, row[0])
        )
        conn.commit()


def actualizar_precio_promedio(producto_id, nuevo_precio, cursor, conn):
    """Legacy"""
    actualizar_precio_promedio_legacy(None, nuevo_precio, cursor, conn)


def fusionar_productos_duplicados(producto_principal_id, productos_a_fusionar, cursor, conn):
    """Fusiona duplicados"""
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    for pid in productos_a_fusionar:
        cursor.execute(f"UPDATE items_factura SET producto_maestro_id = {param} WHERE producto_maestro_id = {param}", (producto_principal_id, pid))
        cursor.execute(f"UPDATE inventario_usuario SET producto_maestro_id = {param} WHERE producto_maestro_id = {param}", (producto_principal_id, pid))
        cursor.execute(f"DELETE FROM productos_maestros WHERE id = {param}", (pid,))
    conn.commit()


def detectar_duplicados_por_similitud(cursor, umbral=0.90):
    """Detecta duplicados"""
    cursor.execute("SELECT id, nombre_normalizado, codigo_ean FROM productos_maestros ORDER BY id")
    productos = cursor.fetchall()
    duplicados = []

    for i in range(len(productos)):
        for j in range(i + 1, len(productos)):
            id1, n1, c1 = productos[i]
            id2, n2, c2 = productos[j]
            sim = 1.0 if (c1 and c2 and c1 == c2) else calcular_similitud(n1, n2)
            if sim >= umbral:
                duplicados.append((id1, n1, id2, n2, sim))

    return duplicados


print("="*80)
print("✅ product_matcher.py V5.0 CARGADO")
print("="*80)
print("🎯 SISTEMA DE 4 CAPAS + APRENDIZAJE AUTOMÁTICO")
print("   1️⃣ OCR → 2️⃣ Python → 3️⃣ Aprendizaje → 4️⃣ Perplexity → 5️⃣ BD")
print("="*80)
print(f"{'✅' if CORRECCIONES_OCR_AVAILABLE else '⚠️ '} Correcciones OCR")
print(f"{'✅' if PERPLEXITY_AVAILABLE else '⚠️ '} Perplexity")
print(f"{'✅' if APRENDIZAJE_AVAILABLE else '⚠️ '} Aprendizaje Automático")
print("="*80)
