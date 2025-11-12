"""
product_matcher.py - VERSIÓN 6.0 - INTEGRACIÓN CON APRENDIZAJE V2.0
========================================================================
Sistema de matching y normalización de productos con aprendizaje automático

🎯 FLUJO COMPLETO V6.0:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣ OCR (Claude Vision)           → "oso  blanco" (datos crudos)
2️⃣ Correcciones Python            → "QUESO BLANCO" (corregido)
3️⃣ Búsqueda Aprendizaje           → ¿Ya lo conocemos?
   ├─ ENCONTRADO (conf ≥70%)     → Usar nombre aprendido ✅ AHORRA $$$
   └─ NO ENCONTRADO              → Continuar ↓
4️⃣ Validación Perplexity          → "QUESO BLANCO COLANTA 500G"
5️⃣ Guardar Aprendizaje            → Aprende para próxima vez
6️⃣ Base de Datos                  → Guarda en productos_maestros
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CAMBIOS V6.0:
- ✅ Compatible con aprendizaje_manager.py V2.0
- ✅ Usa esquema real de Railway (verificado)
- ✅ Búsqueda inteligente: EAN → Específico → Genérico
- ✅ Guarda automáticamente en aprendizaje después de Perplexity
- ✅ Maneja confianza y feedback correctamente
"""

import re
from unidecode import unidecode
from typing import Optional, Dict, Any

# Importar módulos
CORRECCIONES_OCR_AVAILABLE = False
try:
    from perplexity_validator import validar_con_perplexity
    PERPLEXITY_AVAILABLE = True
except ImportError:
    PERPLEXITY_AVAILABLE = False
    print("⚠️  perplexity_validator.py no disponible")

try:
    from aprendizaje_manager import AprendizajeManager
    APRENDIZAJE_AVAILABLE = True
except ImportError:
    APRENDIZAJE_AVAILABLE = False
    print("⚠️  aprendizaje_manager.py no disponible")
from typing import Optional, Dict


def normalizar_nombre_producto(nombre: str, aplicar_correcciones_ocr: bool = True) -> str:
    """
    Normaliza nombre del producto para búsquedas

    Args:
        nombre: Nombre original del producto
        aplicar_correcciones_ocr: Si debe aplicar correcciones OCR (deprecado, siempre usa normalización básica)

    Returns:
        Nombre normalizado (uppercase, sin tildes, sin caracteres especiales)
    """
    if not nombre:
        return ""

    # Normalización básica (sin dependencia de correcciones_ocr)
    nombre = nombre.upper()
    nombre = unidecode(nombre)

    # Limpiar caracteres especiales
    nombre = re.sub(r'[^\w\s]', ' ', nombre)
    nombre = re.sub(r'\s+', ' ', nombre)

    return nombre.strip()[:100]


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """
    Calcula similitud entre dos nombres de productos

    Returns:
        Float entre 0.0 y 1.0 (1.0 = idénticos)
    """
    n1 = normalizar_nombre_producto(nombre1, False)
    n2 = normalizar_nombre_producto(nombre2, False)

    # Nombres idénticos
    if n1 == n2:
        return 1.0

    # Uno contiene al otro
    if n1 in n2 or n2 in n1:
        return 0.8 + (0.2 * min(len(n1), len(n2)) / max(len(n1), len(n2)))

    # Similitud por palabras en común
    palabras1 = set(n1.split())
    palabras2 = set(n2.split())

    if not palabras1.union(palabras2):
        return 0.0

    return len(palabras1.intersection(palabras2)) / len(palabras1.union(palabras2))


def clasificar_codigo_tipo(codigo: str) -> str:
    """
    Clasifica el tipo de código del producto

    Returns:
        'EAN', 'PLU', o 'DESCONOCIDO'
    """
    if not codigo:
        return 'DESCONOCIDO'

    codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))
    longitud = len(codigo_limpio)

    if longitud >= 8:  # EAN-8, EAN-13, UPC
        return 'EAN'
    elif 3 <= longitud <= 7:  # PLU codes
        return 'PLU'

    return 'DESCONOCIDO'


def detectar_cadena(establecimiento: str) -> str:
    """
    Detecta la cadena principal del establecimiento

    Args:
        establecimiento: Nombre completo del establecimiento

    Returns:
        Cadena normalizada (JUMBO, EXITO, etc.)
    """
    if not establecimiento:
        return "DESCONOCIDO"

    establecimiento_upper = establecimiento.upper()

    # Mapeo de cadenas conocidas
    cadenas = {
        'JUMBO': 'JUMBO',
        'EXITO': 'EXITO',
        'CARULLA': 'CARULLA',
        'OLIMPICA': 'OLIMPICA',
        'D1': 'D1',
        'ARA': 'ARA',
        'CRUZ VERDE': 'CRUZ VERDE',
        'FARMATODO': 'FARMATODO',
        'SUPERMERCADOS PREMIUM':'SUPERMERCADOS PREMIUM',
    }

    for cadena_key, cadena_value in cadenas.items():
        if cadena_key in establecimiento_upper:
            return cadena_value

    return establecimiento_upper.split()[0] if establecimiento_upper else "DESCONOCIDO"

def buscar_en_productos_referencia(codigo_ean: str, cursor) -> Optional[Dict[str, Any]]:
    """
    Busca producto en la tabla de referencia oficial

    Args:
        codigo_ean: Código EAN-13 del producto
        cursor: Cursor de base de datos

    Returns:
        Dict con datos oficiales del producto o None si no existe
    """
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    if not codigo_ean or len(codigo_ean) < 8:
        return None

    try:
        cursor.execute(f"""
            SELECT
                codigo_ean,
                nombre,
                marca,
                categoria,
                presentacion,
                unidad_medida
            FROM productos_referencia
            WHERE codigo_ean = {param}
            LIMIT 1
        """, (codigo_ean,))

        resultado = cursor.fetchone()

        if not resultado:
            return None

        # Extraer campos
        ean = resultado[0]
        nombre = resultado[1] or ""
        marca = resultado[2] or ""
        categoria = resultado[3] or ""
        presentacion = resultado[4] or ""
        unidad_medida = resultado[5] or ""

        # Construir nombre completo: MARCA NOMBRE PRESENTACION UNIDAD_MEDIDA
        # Ejemplo: "KIKES HUEVO ROJO AA 30 UNIDADES"
        partes = []

        if marca:
            partes.append(marca.upper().strip())
        if nombre:
            partes.append(nombre.upper().strip())
        if presentacion:
            partes.append(presentacion.upper().strip())
        if unidad_medida and unidad_medida.upper() not in ['UNIDAD', 'UND', 'U']:
            # Solo agregar unidad si no es redundante
            partes.append(unidad_medida.upper().strip())

        nombre_oficial = " ".join(partes)

        return {
            'codigo_ean': ean,
            'nombre_oficial': nombre_oficial,
            'marca': marca,
            'nombre': nombre,
            'presentacion': presentacion,
            'categoria': categoria,
            'unidad_medida': unidad_medida,
            'fuente': 'productos_referencia'
        }

    except Exception as e:
        print(f"   ⚠️ Error buscando en productos_referencia: {e}")
        import traceback
        traceback.print_exc()
        return None

def validar_nombre_con_sistema_completo(
    nombre_ocr_original: str,
    nombre_corregido: str,
    precio: int,
    establecimiento: str,
    codigo: str = "",
    aprendizaje_mgr = None,
    factura_id: int = None,
    usuario_id: int = None,
    item_factura_id: int = None,
    cursor = None  # ← NUEVO
) -> dict:
    """
    V6.1: Sistema completo con productos_referencia como fuente prioritaria

    Flujo:
    1. Busca en productos_referencia (OFICIAL - máxima prioridad)
    2. Busca en aprendizaje (productos validados previamente)
    3. Si no encuentra → Perplexity
    4. Guarda resultado en aprendizaje para próxima vez
    """

    print(f"\n🔍 VALIDACIÓN SISTEMA COMPLETO:")
    print(f"   OCR Original: {nombre_ocr_original}")
    print(f"   Corregido: {nombre_corregido}")
    print(f"   Establecimiento: {establecimiento}")
    print(f"   Código: {codigo or 'Sin código'}")

    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)

    # ========================================================================
    # PASO 1: BUSCAR EN PRODUCTOS_REFERENCIA (FUENTE OFICIAL)
    # ========================================================================
    if tipo_codigo == 'EAN' and codigo and cursor:
        print(f"\n1️⃣ CAPA 1 - PRODUCTOS REFERENCIA (OFICIAL):")

        producto_oficial = buscar_en_productos_referencia(codigo, cursor)

        if producto_oficial:
            print(f"   ✅ ENCONTRADO EN PRODUCTOS REFERENCIA")
            print(f"   📝 Nombre oficial: {producto_oficial['nombre_oficial']}")
            print(f"   🏷️  Marca: {producto_oficial.get('marca', 'N/A')}")
            print(f"   📦 Presentación: {producto_oficial.get('presentacion', 'N/A')}")
            print(f"   📂 Categoría: {producto_oficial.get('categoria', 'N/A')}")
            print(f"   💰 Ahorro: $0.005 USD (no se llamó Perplexity)")

            # Guardar en aprendizaje para acelerar próximas búsquedas
            if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
                aprendizaje_mgr.guardar_correccion_aprendida(
                    ocr_original=nombre_ocr_original,
                    ocr_normalizado=nombre_corregido,
                    nombre_validado=producto_oficial['nombre_oficial'],
                    establecimiento=cadena,
                    confianza_inicial=0.95,  # Máxima confianza
                    codigo_ean=codigo
                )

            return {
                'nombre_final': producto_oficial['nombre_oficial'],
                'fue_validado': True,
                'confianza': 0.95,
                'categoria_confianza': 'alta',
                'fuente': 'productos_referencia',
                'detalles': f"Código EAN oficial: {codigo}",
                'ahorro_dinero': True,
                'producto_oficial': producto_oficial
            }
        else:
            print(f"   ℹ️  No encontrado en productos_referencia")

    # ========================================================================
    # PASO 2: BUSCAR EN APRENDIZAJE
    # ========================================================================
    print(f"\n2️⃣ CAPA 2 - APRENDIZAJE AUTOMÁTICO:")

    if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
        correccion = aprendizaje_mgr.buscar_correccion_aprendida(
            ocr_normalizado=nombre_corregido,
            establecimiento=cadena,
            codigo_ean=codigo if tipo_codigo == 'EAN' else None
        )

        if correccion and correccion['confianza'] >= 0.70:
            confianza = correccion['confianza']
            categoria = 'alta' if confianza >= 0.85 else 'media'

            print(f"   ✅ ENCONTRADO EN APRENDIZAJE")
            print(f"   📝 Nombre: {correccion['nombre_validado']}")
            print(f"   📊 Confianza: {confianza:.2f} ({categoria})")
            print(f"   📈 Confirmado: {correccion['veces_confirmado']} veces")
            print(f"   🔍 Fuente: {correccion.get('fuente_busqueda', 'desconocido')}")
            print(f"   💰 Ahorro: $0.005 USD")

            # Incrementar confianza por uso exitoso
            aprendizaje_mgr.incrementar_confianza(correccion['id'], True)

            return {
                'nombre_final': correccion['nombre_validado'],
                'fue_validado': True,
                'confianza': confianza,
                'categoria_confianza': categoria,
                'fuente': 'aprendizaje',
                'detalles': f"Usado {correccion['veces_confirmado']} veces",
                'aprendizaje_id': correccion['id'],
                'ahorro_dinero': True
            }
        else:
            print(f"   ℹ️  No encontrado en aprendizaje (o confianza baja)")
    else:
        print(f"   ⚠️  Sistema de aprendizaje no disponible")

    # ========================================================================
    # PASO 3: VALIDAR CON PERPLEXITY (ÚLTIMO RECURSO)
    # ========================================================================
    print(f"\n3️⃣ CAPA 3 - VALIDACIÓN PERPLEXITY:")

    if not PERPLEXITY_AVAILABLE:
        print(f"   ⚠️  Perplexity no disponible")
        return {
            'nombre_final': nombre_corregido,
            'fue_validado': False,
            'confianza': 0.50,
            'categoria_confianza': 'baja',
            'fuente': 'python',
            'detalles': 'Perplexity no disponible',
            'ahorro_dinero': False
        }

    try:
        resultado_perplexity = validar_con_perplexity(
            nombre_corregido,
            precio,
            cadena,
            codigo,
            nombre_ocr_original
        )

        nombre_final = resultado_perplexity.get('nombre_final', nombre_corregido)
        fue_validado = resultado_perplexity.get('fue_validado', False)

        # Calcular confianza
        tiene_ean = (tipo_codigo == 'EAN')
        confianza = 0.85 if fue_validado else 0.60
        if tiene_ean:
            confianza = min(confianza + 0.10, 0.95)

        categoria = 'alta' if confianza >= 0.85 else 'media'

        print(f"   ✅ VALIDADO CON PERPLEXITY")
        print(f"   📝 Nombre final: {nombre_final}")
        print(f"   📊 Confianza: {confianza:.2f} ({categoria})")
        print(f"   💰 Costo: ~$0.005 USD")

        # ====================================================================
        # PASO 4: GUARDAR EN APRENDIZAJE
        # ====================================================================
        if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
            print(f"\n4️⃣ GUARDANDO EN APRENDIZAJE:")

            aprendizaje_id = aprendizaje_mgr.guardar_correccion_aprendida(
                ocr_original=nombre_ocr_original,
                ocr_normalizado=nombre_corregido,
                nombre_validado=nombre_final,
                establecimiento=cadena,
                confianza_inicial=confianza,
                codigo_ean=codigo if tipo_codigo == 'EAN' else None
            )

            if aprendizaje_id:
                print(f"   ✅ Guardado en aprendizaje (ID: {aprendizaje_id})")
                print(f"   💡 Próxima vez será instantáneo y gratis")
            else:
                print(f"   ⚠️  No se pudo guardar en aprendizaje")

        return {
            'nombre_final': nombre_final,
            'fue_validado': fue_validado,
            'confianza': confianza,
            'categoria_confianza': categoria,
            'fuente': 'perplexity',
            'detalles': resultado_perplexity.get('detalles', ''),
            'ahorro_dinero': False
        }

    except Exception as e:
        print(f"   ❌ Error en Perplexity: {e}")
        return {
            'nombre_final': nombre_corregido,
            'fue_validado': False,
            'confianza': 0.50,
            'categoria_confianza': 'baja',
            'fuente': 'python',
            'detalles': f'Error: {str(e)}',
            'ahorro_dinero': False
        }


def crear_producto_en_ambas_tablas(
    codigo_ean,
    nombre_final,
    precio,
    cursor,
    conn,
    metadatos=None
):
    """
    Crea producto en productos_maestros y productos_maestros_v2
    CON MANEJO ROBUSTO DE ERRORES - V6.1

    Args:
        codigo_ean: Código EAN del producto (puede ser None)
        nombre_final: Nombre validado del producto
        precio: Precio del producto
        cursor: Cursor de la base de datos
        conn: Conexión a la base de datos
        metadatos: Dict con información adicional (opcional)

    Returns:
        ID del producto creado o None si falla
    """
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    if metadatos:
        print(f"\n   📊 Metadatos de validación:")
        print(f"      Fuente: {metadatos.get('fuente', 'desconocido')}")
        print(f"      Confianza: {metadatos.get('confianza', 0):.2f}")
        if metadatos.get('ahorro_dinero'):
            print(f"      💰 Ahorro: $0.005 USD (no se llamó Perplexity)")

    try:
        # Crear en productos_maestros
        if is_postgresql:
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean,
                    nombre_normalizado,
                    precio_promedio_global,
                    total_reportes,
                    primera_vez_reportado,
                    ultima_actualizacion
                )
                VALUES (%s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id
            """, (codigo_ean, nombre_final, precio))
        else:
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean,
                    nombre_normalizado,
                    precio_promedio_global,
                    total_reportes,
                    primera_vez_reportado,
                    ultima_actualizacion
                )
                VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (codigo_ean, nombre_final, precio))

        # Obtener ID
        resultado = cursor.fetchone()

        if not resultado:
            print(f"   ❌ ERROR: No se obtuvo ID del INSERT")
            print(f"      Producto: {nombre_final}")
            print(f"      Intentando obtener ID manualmente...")

            # Intentar buscar el producto recién creado
            if is_postgresql:
                cursor.execute("""
                    SELECT id FROM productos_maestros
                    WHERE nombre_normalizado = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (nombre_final,))
            else:
                cursor.execute("""
                    SELECT id FROM productos_maestros
                    WHERE nombre_normalizado = ?
                    ORDER BY id DESC
                    LIMIT 1
                """, (nombre_final,))

            resultado = cursor.fetchone()

            if not resultado:
                print(f"   ❌ CRÍTICO: No se pudo obtener ID del producto")
                conn.rollback()
                return None

        producto_id = resultado[0]
        print(f"   ✅ Producto creado ID: {producto_id}")
        print(f"      Nombre: {nombre_final}")

        # Crear en productos_maestros_v2
        try:
            if is_postgresql:
                cursor.execute("""
                    INSERT INTO productos_maestros_v2 (
                        codigo_ean,
                        nombre_consolidado,
                        marca,
                        categoria_id
                    )
                    VALUES (%s, %s, NULL, NULL)
                    ON CONFLICT (codigo_ean) DO NOTHING
                """, (codigo_ean, nombre_final))
            else:
                cursor.execute("""
                    INSERT OR IGNORE INTO productos_maestros_v2 (
                        codigo_ean,
                        nombre_consolidado,
                        marca,
                        categoria_id
                    )
                    VALUES (?, ?, NULL, NULL)
                """, (codigo_ean, nombre_final))
        except Exception as e:
            print(f"   ⚠️ Error en productos_maestros_v2 (no crítico): {e}")
            # No es crítico, continuar

        conn.commit()
        return producto_id

    except Exception as e:
        print(f"   ❌ Error creando producto '{nombre_final}': {e}")
        import traceback
        traceback.print_exc()

        try:
            conn.rollback()
        except:
            pass

        return None


def sincronizar_a_v2(producto_id, codigo_ean, nombre, cursor, conn):
    """Sincroniza producto a tabla v2 (legacy)"""
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    if is_postgresql:
        if codigo_ean:
            cursor.execute(
                "SELECT id FROM productos_maestros_v2 WHERE codigo_ean = %s",
                (codigo_ean,)
            )
        else:
            cursor.execute(
                "SELECT id FROM productos_maestros_v2 WHERE nombre_consolidado ILIKE %s",
                (nombre,)
            )
    else:
        if codigo_ean:
            cursor.execute(
                "SELECT id FROM productos_maestros_v2 WHERE codigo_ean = ?",
                (codigo_ean,)
            )
        else:
            cursor.execute(
                "SELECT id FROM productos_maestros_v2 WHERE nombre_consolidado LIKE ?",
                (nombre,)
            )

    if not cursor.fetchone():
        if is_postgresql:
            cursor.execute(
                "INSERT INTO productos_maestros_v2 (codigo_ean, nombre_consolidado) VALUES (%s, %s)",
                (codigo_ean, nombre)
            )
        else:
            cursor.execute(
                "INSERT INTO productos_maestros_v2 (codigo_ean, nombre_consolidado) VALUES (?, ?)",
                (codigo_ean, nombre)
            )
        conn.commit()


def actualizar_precio_promedio_legacy(codigo_ean, nuevo_precio, cursor, conn, nombre=None):
    """Actualiza precio promedio (legacy)"""
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    if codigo_ean:
        cursor.execute(
            "SELECT id FROM productos_maestros WHERE codigo_ean = " +
            ("%s" if is_postgresql else "?"),
            (codigo_ean,)
        )
    elif nombre:
        cursor.execute(
            "SELECT id FROM productos_maestros WHERE nombre_normalizado " +
            ("ILIKE" if is_postgresql else "LIKE") +
            (" %s" if is_postgresql else " ?"),
            (nombre,)
        )
    else:
        return

    row = cursor.fetchone()
    if row:
        cursor.execute(
            """
            UPDATE productos_maestros
            SET precio_promedio_global = (
                (precio_promedio_global * total_reportes + """ +
                ("%s" if is_postgresql else "?") + """) / (total_reportes + 1)
            ),
            total_reportes = total_reportes + 1
            WHERE id = """ + ("%s" if is_postgresql else "?"),
            (nuevo_precio, row[0])
        )
        conn.commit()


def fusionar_productos_duplicados(producto_principal_id, productos_a_fusionar, cursor, conn):
    """Fusiona productos duplicados"""
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    for pid in productos_a_fusionar:
        # Actualizar referencias en items_factura
        cursor.execute(
            f"UPDATE items_factura SET producto_maestro_id = {param} WHERE producto_maestro_id = {param}",
            (producto_principal_id, pid)
        )

        # Actualizar referencias en inventario_usuario
        cursor.execute(
            f"UPDATE inventario_usuario SET producto_maestro_id = {param} WHERE producto_maestro_id = {param}",
            (producto_principal_id, pid)
        )

        # Eliminar producto duplicado
        cursor.execute(
            f"DELETE FROM productos_maestros WHERE id = {param}",
            (pid,)
        )

    conn.commit()


def detectar_duplicados_por_similitud(cursor, umbral=0.90):
    """Detecta productos duplicados por similitud"""
    cursor.execute(
        "SELECT id, nombre_normalizado, codigo_ean FROM productos_maestros ORDER BY id"
    )
    productos = cursor.fetchall()
    duplicados = []

    for i in range(len(productos)):
        for j in range(i + 1, len(productos)):
            id1, n1, c1 = productos[i]
            id2, n2, c2 = productos[j]

            # Si tienen mismo EAN, son duplicados
            if c1 and c2 and c1 == c2:
                sim = 1.0
            else:
                sim = calcular_similitud(n1, n2)

            if sim >= umbral:
                duplicados.append((id1, n1, id2, n2, sim))

    return duplicados


def buscar_o_crear_producto_inteligente(
    codigo: str,
    nombre: str,
    precio: int,
    establecimiento: str,
    cursor,
    conn,
    factura_id: int = None,
    usuario_id: int = None,
    item_factura_id: int = None
) -> int:
    """
    Función de compatibilidad para main.py
    Busca o crea un producto usando el sistema completo

    Args:
        codigo: Código del producto (EAN o PLU)
        nombre: Nombre del producto (del OCR)
        precio: Precio del producto
        establecimiento: Nombre del establecimiento
        cursor: Cursor de base de datos
        conn: Conexión a base de datos
        factura_id: ID de la factura (opcional)
        usuario_id: ID del usuario (opcional)
        item_factura_id: ID del item (opcional)

    Returns:
        ID del producto en productos_maestros
    """
    import os

    print(f"\n🔍 BUSCAR O CREAR PRODUCTO:")
    print(f"   Código: {codigo or 'Sin código'}")
    print(f"   Nombre: {nombre[:50]}")
    print(f"   Precio: ${precio:,}")
    print(f"   Establecimiento: {establecimiento}")

    # Normalizar nombre
    nombre_normalizado = normalizar_nombre_producto(nombre, True)
    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    # ========================================================================
    # PASO 1: BUSCAR PRODUCTO EXISTENTE
    # ========================================================================

    producto_id = None

    # Buscar por EAN
    if tipo_codigo == 'EAN' and codigo:
        cursor.execute(
            f"SELECT id, nombre_normalizado FROM productos_maestros WHERE codigo_ean = {param}",
            (codigo,)
        )
        resultado = cursor.fetchone()

        if resultado:
            producto_id = resultado[0]
            print(f"   ✅ Encontrado por EAN: ID={producto_id}")
            return producto_id

    # Buscar por nombre similar
    # Buscar por nombre similar (más flexible)
    cursor.execute(f"""
        SELECT id, nombre_normalizado, codigo_ean
        FROM productos_maestros
        WHERE nombre_normalizado {('ILIKE' if is_postgresql else 'LIKE')} {param}
       OR {param} {('ILIKE' if is_postgresql else 'LIKE')} '%' || nombre_normalizado || '%'
        LIMIT 10
    """, (f"%{nombre_normalizado[:50]}%", nombre_normalizado))

    candidatos = cursor.fetchall()

    for cand_id, cand_nombre, cand_ean in candidatos:
        similitud = calcular_similitud(nombre_normalizado, cand_nombre)

        if similitud >= 0.90:
            producto_id = cand_id
            print(f"   ✅ Encontrado por similitud: ID={producto_id} (sim={similitud:.2f})")
            return producto_id

    # ========================================================================
    # PASO 2: NO ENCONTRADO → VALIDAR Y CREAR
    # ========================================================================

    print(f"   ℹ️  Producto no encontrado → Validando...")
# Inicializar AprendizajeManager si está disponible
    aprendizaje_mgr = None
    print(f"   🧠 APRENDIZAJE_AVAILABLE: {APRENDIZAJE_AVAILABLE}")

    if APRENDIZAJE_AVAILABLE:
        try:
            from aprendizaje_manager import AprendizajeManager
            aprendizaje_mgr = AprendizajeManager(cursor, conn)
            print(f"   ✅ AprendizajeManager inicializado correctamente")
        except Exception as e:
            print(f"   ❌ Error inicializando AprendizajeManager: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"   ⚠️  APRENDIZAJE_AVAILABLE = False")

    # Validar con sistema completo
    resultado_validacion = validar_nombre_con_sistema_completo(
        nombre_ocr_original=nombre,
        nombre_corregido=nombre_normalizado,
        precio=precio,
        establecimiento=cadena,
        codigo=codigo,
        aprendizaje_mgr=aprendizaje_mgr,
        factura_id=factura_id,
        usuario_id=usuario_id,
        item_factura_id=item_factura_id
    )

    nombre_final = resultado_validacion['nombre_final']

    print(f"\n➕ CREANDO PRODUCTO:")
    print(f"   Nombre validado: {nombre_final}")
    print(f"   Fuente: {resultado_validacion['fuente']}")
    print(f"   Confianza: {resultado_validacion['confianza']:.2f}")

    # Crear producto
    producto_id = crear_producto_en_ambas_tablas(
        codigo_ean=codigo if tipo_codigo == 'EAN' else None,
        nombre_final=nombre_final,
        precio=precio,
        cursor=cursor,
        conn=conn,
        metadatos=resultado_validacion
    )

    if not producto_id:
        print(f"   ❌ CRÍTICO: No se pudo crear producto '{nombre_final}'")
        return None  # ← AGREGAR ESTA LÍNEA SI NO ESTÁ

    return producto_id

# ==============================================================================
# MENSAJE DE CARGA
# ==============================================================================

print("="*80)
print("✅ product_matcher.py V6.0 CARGADO")
print("="*80)
print("🎯 SISTEMA INTEGRADO CON APRENDIZAJE V2.0")
print("   1️⃣ OCR → 2️⃣ Python → 3️⃣ Aprendizaje → 4️⃣ Perplexity → 5️⃣ BD")
print("="*80)
print(f"{'✅' if PERPLEXITY_AVAILABLE else '⚠️ '} Perplexity")
print(f"{'✅' if APRENDIZAJE_AVAILABLE else '⚠️ '} Aprendizaje Automático V2.0")
print("="*80)
