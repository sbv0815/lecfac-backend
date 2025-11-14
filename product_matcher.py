"""
product_matcher.py - VERSIÓN 8.0 - Sistema con Jerarquía de Validación
========================================================================
Sistema de matching con validación por fuentes confiables

🎯 FLUJO COMPLETO V8.0:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣ Productos Referencia (OFICIAL)  → Datos oficiales con EAN (99% conf)
2️⃣ Historial PLU                   → PLUs frecuentes en BD (80% conf)
3️⃣ Aprendizaje Automático          → Correcciones validadas (70%+ conf)
4️⃣ OCR Corregido                   → Claude + correcciones estáticas (60% conf)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CAMBIOS V8.0:
- ✅ Búsqueda en historial de PLUs por establecimiento
- ✅ Marcado para revisión admin cuando confianza < 80%
- ✅ Sistema de jerarquía de fuentes de verdad
- ✅ NO aprende automáticamente sin validación
- ✅ Mejor tracking de por qué se eligió cada nombre

FILOSOFÍA:
- Solo aprender de fuentes CONFIABLES (referencia oficial o admin)
- Marcar para revisión cuando hay dudas
- Historial de PLU como segunda fuente de verdad
- NO inventar ni adivinar
"""

import re
from unidecode import unidecode
from typing import Optional, Dict, Any, Tuple
import traceback

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


def normalizar_nombre_producto(
    nombre: str, aplicar_correcciones_ocr: bool = True
) -> str:
    """Normaliza nombre del producto para búsquedas"""
    if not nombre:
        return ""

    nombre = nombre.upper()
    nombre = unidecode(nombre)
    nombre = re.sub(r"[^\w\s]", " ", nombre)
    nombre = re.sub(r"\s+", " ", nombre)

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
        return "DESCONOCIDO"

    codigo_limpio = "".join(filter(str.isdigit, str(codigo)))
    longitud = len(codigo_limpio)

    if longitud >= 8:
        return "EAN"
    elif 3 <= longitud <= 7:
        return "PLU"

    return "DESCONOCIDO"


def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena principal del establecimiento"""
    if not establecimiento:
        return "DESCONOCIDO"

    establecimiento_upper = establecimiento.upper()

    cadenas = {
        "JUMBO": "JUMBO",
        "EXITO": "EXITO",
        "CARULLA": "CARULLA",
        "OLIMPICA": "OLIMPICA",
        "D1": "D1",
        "ARA": "ARA",
        "CRUZ VERDE": "CRUZ VERDE",
        "FARMATODO": "FARMATODO",
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
        cursor.execute(
            f"""
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
        """,
            (codigo_ean,),
        )

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
        if unidad_medida and unidad_medida.upper() not in ["UNIDAD", "UND", "U"]:
            partes.append(unidad_medida.upper().strip())

        nombre_oficial = " ".join(partes)

        return {
            "codigo_ean": ean,
            "nombre_oficial": nombre_oficial,
            "marca": marca,
            "nombre": nombre,
            "presentacion": presentacion,
            "categoria": categoria,
            "unidad_medida": unidad_medida,
            "fuente": "productos_referencia",
        }

    except Exception as e:
        print(f"   ⚠️ Error buscando en productos_referencia: {e}")
        return None


def buscar_nombre_por_plu_historial(
    codigo_plu: str, establecimiento_id: int, cursor
) -> Optional[Dict[str, Any]]:
    """
    Busca el nombre más común para un PLU en el historial de compras

    Args:
        codigo_plu: Código PLU a buscar
        establecimiento_id: ID del establecimiento
        cursor: Cursor de BD

    Returns:
        Dict con nombre más común y estadísticas, o None
    """
    if not codigo_plu or not establecimiento_id:
        return None

    try:
        cursor.execute(
            """
            SELECT
                pm.nombre_normalizado,
                COUNT(*) as frecuencia,
                MAX(if2.fecha_creacion) as ultima_vez
            FROM items_factura if2
            JOIN productos_maestros pm ON if2.producto_maestro_id = pm.id
            JOIN facturas f ON if2.factura_id = f.id
            WHERE if2.codigo_leido = %s
              AND f.establecimiento_id = %s
            GROUP BY pm.nombre_normalizado
            ORDER BY frecuencia DESC, ultima_vez DESC
            LIMIT 1
        """,
            (codigo_plu, establecimiento_id),
        )

        resultado = cursor.fetchone()

        if resultado and len(resultado) >= 3:
            frecuencia = resultado[1] or 1
            return {
                "nombre": resultado[0],
                "frecuencia": frecuencia,
                "ultima_vez": resultado[2],
                "fuente": "historial_plu",
                "confianza": min(
                    0.85, 0.65 + (frecuencia * 0.05)
                ),  # Más frecuencia = más confianza
            }

        return None

    except Exception as e:
        print(f"   ⚠️ Error buscando PLU en historial: {e}")
        return None


def marcar_para_revision_admin(
    cursor,
    conn,
    producto_maestro_id: int,
    nombre_ocr: str,
    nombre_sugerido: str,
    codigo: str,
    establecimiento: str,
    razon: str,
) -> bool:
    """
    Marca un producto para revisión por administrador

    Returns:
        True si se marcó correctamente, False si hubo error
    """
    try:
        # Verificar si la tabla existe y tiene las columnas necesarias
        cursor.execute(
            """
            INSERT INTO productos_revision_admin (
                producto_maestro_id,
                nombre_ocr_original,
                nombre_sugerido,
                codigo_producto,
                establecimiento,
                motivo_revision,
                razon_revision,
                estado,
                fecha_creacion
             ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pendiente', CURRENT_TIMESTAMP)
            ON CONFLICT (producto_maestro_id)
            DO UPDATE SET
                nombre_ocr_original = EXCLUDED.nombre_ocr_original,
                nombre_sugerido = EXCLUDED.nombre_sugerido,
                 motivo_revision = EXCLUDED.motivo_revision,
                razon_revision = EXCLUDED.razon_revision,
                fecha_creacion = CURRENT_TIMESTAMP,
                estado = 'pendiente'
        """,
            (
                producto_maestro_id,
                nombre_ocr[:200] if nombre_ocr else "",
                nombre_sugerido[:200] if nombre_sugerido else "",
                codigo[:50] if codigo else "",
                establecimiento[:100] if establecimiento else "",
                razon[:500] if razon else "Sin especificar",
                razon[:500] if razon else "Sin especificar",
            ),
        )
        conn.commit()
        print(f"      📋 Marcado para revisión: {nombre_ocr[:40]}")
        return True
    except Exception as e:
        print(f"      ⚠️ Error marcando para revisión: {e}")
        # No hacer rollback para no perder el producto
        return False


def validar_nombre_con_sistema_completo(
    nombre_ocr_original: str,
    nombre_corregido: str,
    precio: int,
    establecimiento: str,
    codigo: str = "",
    aprendizaje_mgr=None,
    factura_id: int = None,
    usuario_id: int = None,
    item_factura_id: int = None,
    cursor=None,
    establecimiento_id: int = None,
) -> dict:
    """
    V8.0: Sistema con jerarquía de validación

    FLUJO:
    1️⃣ Productos Referencia (EAN oficial) → Nombre oficial (99% conf)
    2️⃣ Historial PLU → Nombre más frecuente (80% conf)
    3️⃣ Aprendizaje Automático → Correcciones validadas (70%+ conf)
    4️⃣ OCR Corregido → Sin validación externa (60% conf)

    Marca para revisión admin si:
    - EAN no está en productos_referencia
    - PLU es nuevo o poco frecuente
    - Hay discrepancia entre OCR y historial
    """

    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)
    marcar_revision = False
    razon_revision = ""

    # ═══════════════════════════════════════════════════════════════
    # PASO 1: BUSCAR EN PRODUCTOS_REFERENCIA (FUENTE OFICIAL - 99%)
    # ═══════════════════════════════════════════════════════════════
    if tipo_codigo == "EAN" and codigo and cursor:
        producto_oficial = buscar_en_productos_referencia(codigo, cursor)

        if producto_oficial:
            print(f"   ✅ ENCONTRADO EN PRODUCTOS REFERENCIA")
            print(f"   📝 Nombre oficial: {producto_oficial['nombre_oficial']}")
            print(f"   🏷️  Marca: {producto_oficial.get('marca', 'N/A')}")
            print(f"   🎯 Confianza: 99% (fuente oficial)")

            # Guardar en aprendizaje con máxima confianza
            if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
                try:
                    aprendizaje_mgr.guardar_correccion_aprendida(
                        ocr_original=nombre_ocr_original,
                        ocr_normalizado=nombre_corregido,
                        nombre_validado=producto_oficial["nombre_oficial"],
                        establecimiento=cadena,
                        confianza_inicial=0.99,
                        codigo_ean=codigo,
                    )
                except Exception as e:
                    print(f"      ⚠️ Error guardando aprendizaje: {e}")

            return {
                "nombre_final": producto_oficial["nombre_oficial"],
                "fue_validado": True,
                "confianza": 0.99,
                "categoria_confianza": "muy_alta",
                "fuente": "productos_referencia",
                "detalles": f"Código EAN oficial: {codigo}",
                "necesita_revision": False,
                "razon_revision": "",
            }
        else:
            # EAN no está en referencia → Marcar para agregar
            marcar_revision = True
            razon_revision = f"EAN {codigo} no está en productos_referencia - agregar datos oficiales"

    # ═══════════════════════════════════════════════════════════════
    # PASO 2: BUSCAR PLU EN HISTORIAL (80% conf)
    # ═══════════════════════════════════════════════════════════════
    if tipo_codigo == "PLU" and codigo and cursor and establecimiento_id:
        resultado_plu = buscar_nombre_por_plu_historial(
            codigo, establecimiento_id, cursor
        )

        if resultado_plu and resultado_plu["frecuencia"] >= 2:
            # PLU visto al menos 2 veces antes
            print(f"   ✅ PLU ENCONTRADO EN HISTORIAL")
            print(f"   📝 Nombre histórico: {resultado_plu['nombre']}")
            print(f"   📊 Frecuencia: {resultado_plu['frecuencia']} veces")
            print(f"   🎯 Confianza: {resultado_plu['confianza']:.0%}")

            # Si el nombre OCR es MUY diferente al histórico, revisar
            similitud = calcular_similitud(nombre_corregido, resultado_plu["nombre"])
            if similitud < 0.70:
                print(
                    f"   ⚠️ Discrepancia: OCR='{nombre_corregido[:30]}' vs Historial='{resultado_plu['nombre'][:30]}'"
                )
                marcar_revision = True
                razon_revision = (
                    f"Discrepancia OCR vs Historial (similitud {similitud:.0%})"
                )

            return {
                "nombre_final": resultado_plu["nombre"],
                "fue_validado": True,
                "confianza": resultado_plu["confianza"],
                "categoria_confianza": (
                    "alta" if resultado_plu["confianza"] >= 0.80 else "media"
                ),
                "fuente": "historial_plu",
                "detalles": f"PLU {codigo} visto {resultado_plu['frecuencia']} veces en establecimiento",
                "necesita_revision": marcar_revision,
                "razon_revision": razon_revision,
            }
        elif tipo_codigo == "PLU":
            # PLU nuevo o poco frecuente
            marcar_revision = True
            razon_revision = (
                f"PLU {codigo} es nuevo o poco frecuente (menos de 2 apariciones)"
            )

    # ═══════════════════════════════════════════════════════════════
    # PASO 3: BUSCAR EN APRENDIZAJE AUTOMÁTICO (70%+ conf)
    # ═══════════════════════════════════════════════════════════════
    if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
        try:
            correccion = aprendizaje_mgr.buscar_correccion_aprendida(
                ocr_normalizado=nombre_corregido,
                establecimiento=cadena,
                codigo_ean=codigo if tipo_codigo == "EAN" else None,
            )

            if correccion and correccion["confianza"] >= 0.80:
                confianza = correccion["confianza"]

                aprendizaje_mgr.incrementar_confianza(correccion["id"], True)

                print(f"   ✅ ENCONTRADO EN APRENDIZAJE")
                print(f"   📝 Nombre validado: {correccion['nombre_validado']}")
                print(f"   🎯 Confianza: {confianza:.0%}")

                return {
                    "nombre_final": correccion["nombre_validado"],
                    "fue_validado": True,
                    "confianza": confianza,
                    "categoria_confianza": "alta" if confianza >= 0.85 else "media",
                    "fuente": "aprendizaje",
                    "detalles": f"Validado {correccion['veces_confirmado']} veces previamente",
                    "aprendizaje_id": correccion["id"],
                    "necesita_revision": False,
                    "razon_revision": "",
                }
        except Exception as e:
            print(f"   ⚠️ Error consultando aprendizaje: {e}")

    # ═══════════════════════════════════════════════════════════════
    # PASO 4: USAR NOMBRE OCR CORREGIDO (60% conf - SIN VALIDACIÓN)
    # ═══════════════════════════════════════════════════════════════
    print(f"   📝 USANDO NOMBRE OCR CORREGIDO (sin validación externa)")

    # Determinar confianza basada en tipo de código
    tiene_ean = tipo_codigo == "EAN"
    confianza = 0.65 if tiene_ean else 0.60
    categoria = "media" if confianza >= 0.65 else "baja"

    # Marcar para revisión si no hay fuente confiable
    if not marcar_revision:
        marcar_revision = True
        razon_revision = "Producto nuevo sin validación externa - requiere revisión"

    # ⚠️ NO guardar en aprendizaje automáticamente
    # Solo guardar cuando admin valide o cuando se confirme por otra fuente
    print(f"   ⚠️ NO se guarda en aprendizaje (requiere validación)")

    return {
        "nombre_final": nombre_corregido,
        "fue_validado": False,
        "confianza": confianza,
        "categoria_confianza": categoria,
        "fuente": "ocr_corregido",
        "detalles": "Sin validación externa - usar correcciones estáticas",
        "necesita_revision": marcar_revision,
        "razon_revision": razon_revision,
    }


def crear_producto_en_ambas_tablas(
    cursor, conn, nombre_normalizado, codigo_ean=None, marca=None, categoria=None
):
    """
    Crea producto en productos_maestros con manejo robusto de errores
    V8.0 - Sin cambios respecto a V7.0
    """
    try:
        if not nombre_normalizado or not nombre_normalizado.strip():
            print(f"   ❌ ERROR: nombre_normalizado vacío")
            return None

        nombre_final = nombre_normalizado.strip().upper()
        if marca and marca.strip():
            nombre_final = f"{marca.strip().upper()} {nombre_final}"

        codigo_ean_safe = codigo_ean if codigo_ean and codigo_ean.strip() else None
        marca_safe = marca if marca and marca.strip() else None
        categoria_safe = categoria if categoria and categoria.strip() else None

        print(f"   📝 Creando producto: {nombre_final}")

        cursor.execute(
            """
            INSERT INTO productos_maestros (
                codigo_ean, nombre_normalizado, marca, categoria,
                precio_promedio_global, total_reportes,
                primera_vez_reportado, ultima_actualizacion
            ) VALUES (%s, %s, %s, %s, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
        """,
            (codigo_ean_safe, nombre_final, marca_safe, categoria_safe),
        )

        resultado = cursor.fetchone()

        if not resultado or len(resultado) == 0:
            print(f"   ❌ ERROR: INSERT no retornó ID")
            conn.rollback()
            return None

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
        traceback.print_exc()
        conn.rollback()
        return None

    except Exception as e:
        print(f"   ❌ Error en crear_producto_en_ambas_tablas: {e}")
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
    item_factura_id: int = None,
    establecimiento_id: int = None,
) -> Optional[int]:
    """
    Función principal de matching de productos V8.0
    Incluye jerarquía de validación y marcado para revisión admin
    """
    import os

    print(f"\n🔍 BUSCAR O CREAR PRODUCTO V8.0:")
    print(f"   Código: {codigo or 'Sin código'}")
    print(f"   Nombre: {nombre[:50]}")
    print(f"   Precio: ${precio:,}")
    print(f"   Establecimiento: {establecimiento}")
    if establecimiento_id:
        print(f"   Establecimiento ID: {establecimiento_id}")

    # ✅ FIX: Definir variables ANTES de usarlas
    nombre_normalizado = normalizar_nombre_producto(nombre, True)
    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    # ═══════════════════════════════════════════════════════════════
    # PASO 1.5: BUSCAR PRODUCTO YA REVISADO POR ADMIN
    # ═══════════════════════════════════════════════════════════════
    try:
        cursor.execute(
            """
            SELECT id, nombre_normalizado
            FROM productos_maestros
            WHERE revisado_admin = TRUE
              AND nombre_normalizado ILIKE %s
            ORDER BY fecha_revision DESC
            LIMIT 1
        """,
            (f"%{nombre_normalizado[:30]}%",),
        )

        revisado = cursor.fetchone()
        if revisado and len(revisado) >= 2:
            similitud = calcular_similitud(nombre_normalizado, revisado[1])
            if similitud >= 0.85:
                producto_id = revisado[0]
                print(f"   ✅ Producto REVISADO por admin: ID={producto_id}")
                return producto_id
    except Exception as e:
        print(f"   ⚠️ Error buscando productos revisados: {e}")

    try:
        # ✅ Variables ya definidas arriba, no repetir

        # ═══════════════════════════════════════════════════════════════
        # PASO 0: CONSOLIDACIÓN PLU (OPCIONAL)
        # ═══════════════════════════════════════════════════════════════
        if PLU_CONSOLIDATOR_AVAILABLE and ENABLE_PLU_CONSOLIDATION:
            nombre_consolidado_plu = aplicar_consolidacion_plu(
                codigo=codigo,
                nombre_ocr=nombre_normalizado,
                tipo_codigo=tipo_codigo,
                establecimiento=cadena,
                cursor=cursor,
            )

            if nombre_consolidado_plu:
                print(f"   🎯 Usando nombre consolidado por PLU")
                cursor.execute(
                    f"""
                    SELECT id FROM productos_maestros
                    WHERE nombre_normalizado = {param}
                      AND codigo_ean = {param}
                    LIMIT 1
                """,
                    (nombre_consolidado_plu, codigo),
                )

                resultado = cursor.fetchone()
                if resultado and len(resultado) >= 1:
                    producto_id = resultado[0]
                    print(f"   ✅ Producto consolidado encontrado: ID={producto_id}")
                    return producto_id

        # ═══════════════════════════════════════════════════════════════
        # PASO 1: BUSCAR POR EAN EXISTENTE
        # ═══════════════════════════════════════════════════════════════
        if tipo_codigo == "EAN" and codigo:
            try:
                cursor.execute(
                    f"SELECT id, nombre_normalizado FROM productos_maestros WHERE codigo_ean = {param}",
                    (codigo,),
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
            search_pattern = f"%{nombre_normalizado[:50]}%"
            cursor.execute(
                f"""
                SELECT id, nombre_normalizado, codigo_ean
                FROM productos_maestros
                WHERE nombre_normalizado {('ILIKE' if is_postgresql else 'LIKE')} {param}
                LIMIT 10
            """,
                (search_pattern,),
            )

            candidatos = cursor.fetchall()

            for candidato in candidatos:
                if not candidato or len(candidato) < 3:
                    continue

                cand_id = candidato[0]
                cand_nombre = candidato[1]

                if not cand_id or not cand_nombre:
                    continue

                similitud = calcular_similitud(nombre_normalizado, cand_nombre)

                if similitud >= 0.90:
                    producto_id = cand_id
                    print(
                        f"   ✅ Encontrado por similitud: ID={producto_id} (sim={similitud:.2f})"
                    )
                    return producto_id

        except Exception as e:
            print(f"   ⚠️ Error buscando por similitud: {e}")
            traceback.print_exc()

        # ═══════════════════════════════════════════════════════════════
        # PASO 3: NO ENCONTRADO → VALIDAR Y CREAR
        # ═══════════════════════════════════════════════════════════════
        print(f"   ℹ️  Producto no encontrado → Validando con sistema completo...")

        # Inicializar AprendizajeManager
        aprendizaje_mgr = None

        if APRENDIZAJE_AVAILABLE:
            try:
                aprendizaje_mgr = AprendizajeManager(cursor, conn)
            except Exception as e:
                print(f"   ⚠️ Error AprendizajeManager: {e}")

        # Validar con sistema completo V8.0
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
            cursor=cursor,
            establecimiento_id=establecimiento_id,
        )

        nombre_final = resultado_validacion["nombre_final"]
        print(f"   📊 Fuente: {resultado_validacion['fuente']}")
        print(f"   🎯 Confianza: {resultado_validacion['confianza']:.0%}")

        # Crear producto
        producto_id = crear_producto_en_ambas_tablas(
            cursor=cursor,
            conn=conn,
            nombre_normalizado=nombre_final,
            codigo_ean=codigo if tipo_codigo == "EAN" else None,
            marca=None,
            categoria=None,
        )

        if not producto_id:
            print(f"   ❌ SKIP: No se pudo crear '{nombre_final}'")
            return None

        # ✅ NUEVO: Marcar para revisión si necesario
        if resultado_validacion.get("necesita_revision", False) and producto_id:
            try:
                marcar_para_revision_admin(
                    cursor=cursor,
                    conn=conn,
                    producto_maestro_id=producto_id,
                    nombre_ocr=nombre,
                    nombre_sugerido=nombre_final,
                    codigo=codigo or "",
                    establecimiento=cadena,
                    razon=resultado_validacion.get("razon_revision", "Sin especificar"),
                )
            except Exception as e:
                print(f"      ⚠️ No se pudo marcar para revisión: {e}")

        print(f"   ✅ Producto nuevo creado: ID={producto_id}")
        return producto_id

    except Exception as e:
        print(f"   ❌ ERROR CRÍTICO en buscar_o_crear_producto_inteligente: {e}")
        traceback.print_exc()
        return None


# ═══════════════════════════════════════════════════════════════
# MENSAJE DE CARGA
# ═══════════════════════════════════════════════════════════════
print("=" * 80)
print("✅ product_matcher.py V8.0 - Sistema con Jerarquía de Validación")
print("=" * 80)
print("🎯 FLUJO DE VALIDACIÓN:")
print("   1️⃣ Productos Referencia (EAN oficial) → 99% confianza")
print("   2️⃣ Historial PLU (frecuencia en BD) → 80% confianza")
print("   3️⃣ Aprendizaje Automático (validados) → 70%+ confianza")
print("   4️⃣ OCR Corregido (sin validación) → 60% confianza")
print("=" * 80)
print("📋 REVISIÓN ADMIN:")
print("   • EANs no en productos_referencia → Agregar datos oficiales")
print("   • PLUs nuevos o poco frecuentes → Validar nombre")
print("   • Discrepancias OCR vs Historial → Resolver conflicto")
print("   • Productos sin validación externa → Confirmar nombre")
print("=" * 80)
print(f"❌ Perplexity: DESHABILITADO (inventaba texto)")
print(
    f"{'✅' if PLU_CONSOLIDATOR_AVAILABLE and ENABLE_PLU_CONSOLIDATION else '⚠️ '} Consolidación PLU: {'ACTIVA' if ENABLE_PLU_CONSOLIDATION else 'INACTIVA'}"
)
print(f"{'✅' if APRENDIZAJE_AVAILABLE else '⚠️ '} Aprendizaje Automático")
print("=" * 80)
