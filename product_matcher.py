"""
product_matcher.py - VERSIÓN 6.1 - INTEGRACIÓN COMPLETA
========================================================================
Sistema de matching y normalización de productos con aprendizaje automático

🎯 FLUJO COMPLETO V6.1:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣ Productos Referencia (OFICIAL)  → Datos oficiales con EAN
2️⃣ Aprendizaje Automático          → Productos validados previamente
3️⃣ Productos Maestros              → Búsqueda en catálogo existente
4️⃣ Validación Perplexity           → Último recurso (cuesta $$$)
5️⃣ Guardar Aprendizaje             → Aprende para próxima vez
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CAMBIOS V6.1:
- ✅ Integración con productos_referencia (fuente oficial)
- ✅ Manejo robusto de errores en crear_producto_en_ambas_tablas
- ✅ Imports correctos de typing
- ✅ Threshold de similitud ajustado a 0.90
- ✅ Búsqueda mejorada con OR en SQL
    Ojala y funcione.
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


def normalizar_nombre_producto(nombre: str, aplicar_correcciones_ocr: bool = True) -> str:
    """
    Normaliza nombre del producto para búsquedas
    """
    if not nombre:
        return ""

    nombre = nombre.upper()
    nombre = unidecode(nombre)
    nombre = re.sub(r'[^\w\s]', ' ', nombre)
    nombre = re.sub(r'\s+', ' ', nombre)

    return nombre.strip()[:100]


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """
    Calcula similitud entre dos nombres de productos
    """
    n1 = normalizar_nombre_producto(nombre1, False)
    n2 = normalizar_nombre_producto(nombre2, False)

    if n1 == n2:
        return 1.0

    if n1 in n2 or n2 in n1:
        return 0.8 + (0.2 * min(len(n1), len(n2)) / max(len(n1), len(n2)))

    palabras1 = set(n1.split())
    palabras2 = set(n2.split())

    if not palabras1.union(palabras2):
        return 0.0

    return len(palabras1.intersection(palabras2)) / len(palabras1.union(palabras2))


def clasificar_codigo_tipo(codigo: str) -> str:
    """
    Clasifica el tipo de código del producto
    """
    if not codigo:
        return 'DESCONOCIDO'

    codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))
    longitud = len(codigo_limpio)

    if longitud >= 8:
        return 'EAN'
    elif 3 <= longitud <= 7:
        return 'PLU'

    return 'DESCONOCIDO'


def detectar_cadena(establecimiento: str) -> str:
    """
    Detecta la cadena principal del establecimiento
    """
    if not establecimiento:
        return "DESCONOCIDO"

    establecimiento_upper = establecimiento.upper()

    cadenas = {
        'JUMBO': 'JUMBO',
        'EXITO': 'EXITO',
        'CARULLA': 'CARULLA',
        'OLIMPICA': 'OLIMPICA',
        'D1': 'D1',
        'ARA': 'ARA',
        'CRUZ VERDE': 'CRUZ VERDE',
        'FARMATODO': 'FARMATODO'
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

        # Construir nombre completo
        partes = []

        if marca:
            partes.append(marca.upper().strip())
        if nombre:
            partes.append(nombre.upper().strip())
        if presentacion:
            partes.append(presentacion.upper().strip())
        if unidad_medida and unidad_medida.upper() not in ['UNIDAD', 'UND', 'U']:
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
    cursor = None
) -> dict:
    """
    V6.1: Sistema completo con productos_referencia como fuente prioritaria
    """

    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)

    # ========================================================================
    # PASO 1: BUSCAR EN PRODUCTOS_REFERENCIA (FUENTE OFICIAL)
    # ========================================================================
    if tipo_codigo == 'EAN' and codigo and cursor:
        producto_oficial = buscar_en_productos_referencia(codigo, cursor)

        if producto_oficial:
            print(f"   ✅ ENCONTRADO EN PRODUCTOS REFERENCIA")
            print(f"   📝 Nombre oficial: {producto_oficial['nombre_oficial']}")
            print(f"   🏷️  Marca: {producto_oficial.get('marca', 'N/A')}")
            print(f"   💰 Ahorro: $0.005 USD")

            # Guardar en aprendizaje
            if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
                aprendizaje_mgr.guardar_correccion_aprendida(
                    ocr_original=nombre_ocr_original,
                    ocr_normalizado=nombre_corregido,
                    nombre_validado=producto_oficial['nombre_oficial'],
                    establecimiento=cadena,
                    confianza_inicial=0.95,
                    codigo_ean=codigo
                )

            return {
                'nombre_final': producto_oficial['nombre_oficial'],
                'fue_validado': True,
                'confianza': 0.95,
                'categoria_confianza': 'alta',
                'fuente': 'productos_referencia',
                'detalles': f"Código EAN oficial: {codigo}",
                'ahorro_dinero': True
            }

    # ========================================================================
    # PASO 2: BUSCAR EN APRENDIZAJE
    # ========================================================================
    if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
        correccion = aprendizaje_mgr.buscar_correccion_aprendida(
            ocr_normalizado=nombre_corregido,
            establecimiento=cadena,
            codigo_ean=codigo if tipo_codigo == 'EAN' else None
        )

        if correccion and correccion['confianza'] >= 0.70:
            confianza = correccion['confianza']
            categoria = 'alta' if confianza >= 0.85 else 'media'

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

    # ========================================================================
    # PASO 3: VALIDAR CON PERPLEXITY
    # ========================================================================
    if not PERPLEXITY_AVAILABLE:
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

        tiene_ean = (tipo_codigo == 'EAN')
        confianza = 0.85 if fue_validado else 0.60
        if tiene_ean:
            confianza = min(confianza + 0.10, 0.95)

        categoria = 'alta' if confianza >= 0.85 else 'media'

        # Guardar en aprendizaje
        if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
            aprendizaje_mgr.guardar_correccion_aprendida(
                ocr_original=nombre_ocr_original,
                ocr_normalizado=nombre_corregido,
                nombre_validado=nombre_final,
                establecimiento=cadena,
                confianza_inicial=confianza,
                codigo_ean=codigo if tipo_codigo == 'EAN' else None
            )

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
    V6.1 - CON MANEJO ROBUSTO DE ERRORES
    """
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

    if metadatos:
        fuente = metadatos.get('fuente', 'desconocido')
        if metadatos.get('ahorro_dinero'):
            print(f"      💰 Ahorro: $0.005 USD (fuente: {fuente})")

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

        # Obtener ID con manejo de errores
        resultado = cursor.fetchone()

        if not resultado:
            print(f"   ❌ ERROR: fetchone() retornó None")

            # Fallback: buscar manualmente
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
                print(f"   ❌ CRÍTICO: No se pudo obtener ID")
                conn.rollback()
                return None

        producto_id = resultado[0]
        print(f"   ✅ Producto creado ID: {producto_id}")

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
            print(f"   ⚠️ Error v2 (no crítico): {e}")

        conn.commit()
        return producto_id

    except Exception as e:
        print(f"   ❌ Error creando '{nombre_final}': {e}")
        import traceback
        traceback.print_exc()

        try:
            conn.rollback()
        except:
            pass

        return None


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
) -> Optional[int]:
    """
    Función principal de matching de productos V6.1
    """
    import os

    print(f"\n🔍 BUSCAR O CREAR PRODUCTO:")
    print(f"   Código: {codigo or 'Sin código'}")
    print(f"   Nombre: {nombre[:50]}")
    print(f"   Precio: ${precio:,}")
    print(f"   Establecimiento: {establecimiento}")

    nombre_normalizado = normalizar_nombre_producto(nombre, True)
    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    # ========================================================================
    # PASO 1: BUSCAR PRODUCTO EXISTENTE
    # ========================================================================

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

    # Inicializar AprendizajeManager
    aprendizaje_mgr = None

    if APRENDIZAJE_AVAILABLE:
        try:
            from aprendizaje_manager import AprendizajeManager
            aprendizaje_mgr = AprendizajeManager(cursor, conn)
        except Exception as e:
            print(f"   ⚠️ Error AprendizajeManager: {e}")

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
        item_factura_id=item_factura_id,
        cursor=cursor
    )

    nombre_final = resultado_validacion['nombre_final']

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
        print(f"   ❌ CRÍTICO: No se pudo crear '{nombre_final}'")
        return None

    return producto_id


# ==============================================================================
# MENSAJE DE CARGA
# ==============================================================================

print("="*80)
print("✅ product_matcher.py V6.1 CARGADO")
print("="*80)
print("🎯 SISTEMA INTEGRADO COMPLETO")
print("   1️⃣ Productos Referencia → 2️⃣ Aprendizaje → 3️⃣ Perplexity → 4️⃣ BD")
print("="*80)
print(f"{'✅' if PERPLEXITY_AVAILABLE else '⚠️ '} Perplexity")
print(f"{'✅' if APRENDIZAJE_AVAILABLE else '⚠️ '} Aprendizaje Automático")
print("="*80)


