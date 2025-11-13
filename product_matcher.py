"""
product_matcher.py - VERSIÓN 7.0 - Sistema de Consolidación PLU
========================================================================
Sistema de matching y normalización de productos con aprendizaje automático

🎯 FLUJO COMPLETO V7.0:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣ Productos Referencia (OFICIAL)  → Datos oficiales con EAN
2️⃣ Consolidación PLU (OPCIONAL)    → Agrupa variantes por código PLU
3️⃣ Aprendizaje Automático          → Productos validados previamente
4️⃣ OCR Directo                     → Usar lo que leyó Claude Vision
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CAMBIOS V7.0:
- ✅ Consolidación PLU integrada (controlada por flag)
- ✅ Perplexity DESHABILITADO (inventaba texto que no existía)
- ✅ Fix tuple index out of range en query SQL
- ✅ Validación robusta de tuplas antes de unpack
- ✅ Ahorro de $0.005 por producto

FILOSOFÍA:
- NO rompe funcionalidad existente
- Consolidación PLU es OPCIONAL (flag)
- Completamente reversible
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

try:
    from plu_consolidator import aplicar_consolidacion_plu, ENABLE_PLU_CONSOLIDATION
    PLU_CONSOLIDATOR_AVAILABLE = True
except ImportError:
    PLU_CONSOLIDATOR_AVAILABLE = False
    ENABLE_PLU_CONSOLIDATION = False
    print("⚠️  plu_consolidator.py no disponible")


def normalizar_nombre_producto(nombre: str, aplicar_correcciones_ocr: bool = True) -> str:
    """Normaliza nombre del producto para búsquedas"""
    if not nombre:
        return ""

    nombre = nombre.upper()
    nombre = unidecode(nombre)
    nombre = re.sub(r'[^\w\s]', ' ', nombre)
    nombre = re.sub(r'\s+', ' ', nombre)

    return nombre.strip()[:100]


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """Calcula similitud entre dos nombres de productos"""
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
    """Clasifica el tipo de código del producto"""
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
    """Detecta la cadena principal del establecimiento"""
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
    """Busca producto en la tabla de referencia oficial"""
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
    V7.0: Sistema sin Perplexity - NO inventa texto

    FLUJO:
    1️⃣ Productos Referencia (EAN oficial) → Nombre oficial correcto
    2️⃣ Aprendizaje Automático → Correcciones previas validadas
    3️⃣ OCR Directo → Usar lo que leyó Claude Vision SIN modificar

    ❌ PERPLEXITY DESHABILITADO: Inventaba texto que no existía
    """

    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)

    # ═══════════════════════════════════════════════════════════════
    # PASO 1: BUSCAR EN PRODUCTOS_REFERENCIA (FUENTE OFICIAL)
    # ═══════════════════════════════════════════════════════════════
    if tipo_codigo == 'EAN' and codigo and cursor:
        producto_oficial = buscar_en_productos_referencia(codigo, cursor)

        if producto_oficial:
            print(f"   ✅ ENCONTRADO EN PRODUCTOS REFERENCIA")
            print(f"   📝 Nombre oficial: {producto_oficial['nombre_oficial']}")
            print(f"   🏷️  Marca: {producto_oficial.get('marca', 'N/A')}")
            print(f"   💰 Ahorro: $0.005 USD (Perplexity deshabilitado)")

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

    # ═══════════════════════════════════════════════════════════════
    # PASO 2: BUSCAR EN APRENDIZAJE AUTOMÁTICO
    # ═══════════════════════════════════════════════════════════════
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

            print(f"   ✅ ENCONTRADO EN APRENDIZAJE")
            print(f"   📝 Nombre validado: {correccion['nombre_validado']}")
            print(f"   🎯 Confianza: {confianza:.0%}")
            print(f"   💰 Ahorro: $0.005 USD (Perplexity deshabilitado)")

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

    # ═══════════════════════════════════════════════════════════════
    # PASO 3: USAR NOMBRE OCR DIRECTO (SIN PERPLEXITY)
    # ═══════════════════════════════════════════════════════════════
    print(f"   📝 USANDO NOMBRE OCR DIRECTO (sin validación externa)")
    print(f"   ⚡ Perplexity deshabilitado para evitar invención de texto")
    print(f"   💰 Ahorro: $0.005 USD")

    # Determinar confianza basada en tipo de código
    tiene_ean = (tipo_codigo == 'EAN')
    confianza = 0.75 if tiene_ean else 0.70  # Alta confianza si tiene EAN
    categoria = 'alta' if confianza >= 0.75 else 'media'

    # Guardar en aprendizaje para próxima vez
    if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
        aprendizaje_mgr.guardar_correccion_aprendida(
            ocr_original=nombre_ocr_original,
            ocr_normalizado=nombre_corregido,
            nombre_validado=nombre_corregido,  # ← Usar lo que leyó Claude Vision
            establecimiento=cadena,
            confianza_inicial=confianza,
            codigo_ean=codigo if tipo_codigo == 'EAN' else None
        )

    return {
        'nombre_final': nombre_corregido,  # ← USAR NOMBRE OCR SIN MODIFICAR
        'fue_validado': True,
        'confianza': confianza,
        'categoria_confianza': categoria,
        'fuente': 'ocr_directo',
        'detalles': 'Perplexity deshabilitado - usando OCR directo',
        'ahorro_dinero': True  # ← Ahorro de $0.005 por producto
    }


def crear_producto_en_ambas_tablas(cursor, conn, nombre_normalizado, codigo_ean=None, marca=None, categoria=None):
    """
    Crea producto en productos_maestros con manejo robusto de errores
    V7.0 - FIX DEFINITIVO: tuple index out of range
    """
    try:
        # ✅ VALIDAR PARÁMETROS (no pueden ser None)
        if not nombre_normalizado or not nombre_normalizado.strip():
            print(f"   ❌ ERROR: nombre_normalizado vacío")
            return None

        # Construir nombre final
        nombre_final = nombre_normalizado.strip().upper()
        if marca and marca.strip():
            nombre_final = f"{marca.strip().upper()} {nombre_final}"

        # ✅ CONVERTIR None A VALORES SQL VÁLIDOS
        codigo_ean_safe = codigo_ean if codigo_ean and codigo_ean.strip() else None
        marca_safe = marca if marca and marca.strip() else None
        categoria_safe = categoria if categoria and categoria.strip() else None

        print(f"   📝 Creando producto: {nombre_final}")

        # ✅ FIX: USAR RETURNING id CORRECTAMENTE
        cursor.execute("""
            INSERT INTO productos_maestros (
                codigo_ean, nombre_normalizado, marca, categoria,
                precio_promedio_global, total_reportes,
                primera_vez_reportado, ultima_actualizacion
            ) VALUES (%s, %s, %s, %s, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
        """, (codigo_ean_safe, nombre_final, marca_safe, categoria_safe))

        # ✅ FIX CRÍTICO: VALIDAR ANTES DE ACCEDER
        resultado = cursor.fetchone()

        if not resultado:
            print(f"   ❌ ERROR: INSERT no retornó ID")
            conn.rollback()
            return None

        if len(resultado) == 0:
            print(f"   ❌ ERROR: RETURNING id retornó tupla vacía")
            conn.rollback()
            return None

        # ✅ ACCESO SEGURO AL ID
        producto_id = resultado[0]

        if not producto_id or producto_id <= 0:
            print(f"   ❌ ERROR: ID inválido: {producto_id}")
            conn.rollback()
            return None

        conn.commit()
        print(f"   ✅ Producto creado exitosamente: ID {producto_id}")
        return producto_id

    except IndexError as e:
        print(f"   ❌ IndexError en crear_producto_en_ambas_tablas: {e}")
        print(f"   🔍 resultado = {resultado if 'resultado' in locals() else 'NO DEFINIDO'}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return None

    except Exception as e:
        print(f"   ❌ Error en crear_producto_en_ambas_tablas: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
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
    Función principal de matching de productos V7.0
    Incluye consolidación PLU opcional
    """
    import os

    print(f"\n🔍 BUSCAR O CREAR PRODUCTO:")
    print(f"   Código: {codigo or 'Sin código'}")
    print(f"   Nombre: {nombre[:50]}")
    print(f"   Precio: ${precio:,}")
    print(f"   Establecimiento: {establecimiento}")

    try:
        nombre_normalizado = normalizar_nombre_producto(nombre, True)
        tipo_codigo = clasificar_codigo_tipo(codigo)
        cadena = detectar_cadena(establecimiento)

        is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
        param = "%s" if is_postgresql else "?"

        # ═══════════════════════════════════════════════════════════════
        # PASO 0: CONSOLIDACIÓN PLU (OPCIONAL - Solo si flag activo)
        # ═══════════════════════════════════════════════════════════════
        if PLU_CONSOLIDATOR_AVAILABLE and ENABLE_PLU_CONSOLIDATION:
            nombre_consolidado_plu = aplicar_consolidacion_plu(
                codigo=codigo,
                nombre_ocr=nombre_normalizado,
                tipo_codigo=tipo_codigo,
                establecimiento=cadena,
                cursor=cursor
            )

            # Si encontró consolidación, usar ese nombre
            if nombre_consolidado_plu:
                print(f"   🎯 Usando nombre consolidado por PLU")
                # Buscar si ya existe con el nombre consolidado
                cursor.execute(f"""
                    SELECT id FROM productos_maestros
                    WHERE nombre_normalizado = {param}
                      AND codigo_ean = {param}
                    LIMIT 1
                """, (nombre_consolidado_plu, codigo))

                resultado = cursor.fetchone()
                if resultado and len(resultado) >= 1:
                    producto_id = resultado[0]
                    print(f"   ✅ Producto consolidado encontrado: ID={producto_id}")
                    return producto_id

        # ═══════════════════════════════════════════════════════════════
        # PASO 1: BUSCAR POR EAN
        # ═══════════════════════════════════════════════════════════════
        if tipo_codigo == 'EAN' and codigo:
            try:
                cursor.execute(
                    f"SELECT id, nombre_normalizado FROM productos_maestros WHERE codigo_ean = {param}",
                    (codigo,)
                )
                resultado = cursor.fetchone()

                if resultado and len(resultado) >= 1:
                    producto_id = resultado[0]
                    print(f"   ✅ Encontrado por EAN: ID={producto_id}")
                    return producto_id
            except Exception as e:
                print(f"   ⚠️ Error buscando por EAN: {e}")

        # ═══════════════════════════════════════════════════════════════
        # PASO 2: BUSCAR POR NOMBRE SIMILAR
        # ═══════════════════════════════════════════════════════════════
        try:
            # ✅ FIX CRÍTICO: Usar solo UN parámetro para evitar tuple index error
            search_pattern = f"%{nombre_normalizado[:50]}%"
            cursor.execute(f"""
                SELECT id, nombre_normalizado, codigo_ean
                FROM productos_maestros
                WHERE nombre_normalizado {('ILIKE' if is_postgresql else 'LIKE')} {param}
                LIMIT 10
            """, (search_pattern,))

            candidatos = cursor.fetchall()

            # ✅ FIX CRÍTICO: VALIDAR CADA TUPLA ANTES DE UNPACK
            for candidato in candidatos:
                # Validar que la tupla tenga al menos 3 elementos
                if not candidato or len(candidato) < 3:
                    continue

                # Unpack seguro
                cand_id = candidato[0]
                cand_nombre = candidato[1]
                cand_ean = candidato[2]

                # Validar que los valores no sean None
                if not cand_id or not cand_nombre:
                    continue

                similitud = calcular_similitud(nombre_normalizado, cand_nombre)

                if similitud >= 0.90:
                    producto_id = cand_id
                    print(f"   ✅ Encontrado por similitud: ID={producto_id} (sim={similitud:.2f})")
                    return producto_id

        except Exception as e:
            print(f"   ⚠️ Error buscando por similitud: {e}")
            import traceback
            traceback.print_exc()

        # ═══════════════════════════════════════════════════════════════
        # PASO 3: NO ENCONTRADO → VALIDAR Y CREAR
        # ═══════════════════════════════════════════════════════════════
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

        # Crear producto CON PARÁMETROS CORRECTOS
        producto_id = crear_producto_en_ambas_tablas(
            cursor=cursor,
            conn=conn,
            nombre_normalizado=nombre_final,
            codigo_ean=codigo if tipo_codigo == 'EAN' else None,
            marca=None,
            categoria=None
        )

        if not producto_id:
            print(f"   ❌ SKIP: No se pudo crear '{nombre_final}'")
            return None

        print(f"   ✅ Producto nuevo creado: ID={producto_id}")
        return producto_id

    except Exception as e:
        print(f"   ❌ ERROR CRÍTICO en buscar_o_crear_producto_inteligente: {e}")
        import traceback
        traceback.print_exc()
        return None


# ═══════════════════════════════════════════════════════════════
# MENSAJE DE CARGA
# ═══════════════════════════════════════════════════════════════
print("="*80)
print("✅ product_matcher.py V7.0 - Sistema de Consolidación PLU")
print("="*80)
print("🎯 FLUJO OPTIMIZADO:")
print("   1️⃣ Productos Referencia (EAN oficial)")
if PLU_CONSOLIDATOR_AVAILABLE and ENABLE_PLU_CONSOLIDATION:
    print("   2️⃣ Consolidación PLU (ACTIVA)")
else:
    print("   2️⃣ Consolidación PLU (INACTIVA)")
print("   3️⃣ Aprendizaje Automático")
print("   4️⃣ OCR Directo")
print("="*80)
print(f"❌ Perplexity: DESHABILITADO (inventaba texto)")
print(f"{'✅' if PLU_CONSOLIDATOR_AVAILABLE and ENABLE_PLU_CONSOLIDATION else '⚠️ '} Consolidación PLU: {'ACTIVA' if ENABLE_PLU_CONSOLIDATION else 'INACTIVA'}")
print(f"{'✅' if APRENDIZAJE_AVAILABLE else '⚠️ '} Aprendizaje Automático")
print("="*80)
if not ENABLE_PLU_CONSOLIDATION:
    print("💡 Para activar consolidación PLU:")
    print("   export ENABLE_PLU_CONSOLIDATION=true")
    print("="*80)
