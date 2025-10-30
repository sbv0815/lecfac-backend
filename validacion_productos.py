"""
SISTEMA DE VALIDACIÓN AUTOMÁTICA CON CLAUDE AI
===============================================
Función que integra el sistema de matching inteligente con Claude AI
para normalizar nombres de productos desde facturas OCR.

Autor: Santiago + Claude
Fecha: 2025-10-30
Sistema: LecFac
"""

import os
import anthropic
from typing import Optional, Dict, Tuple
from datetime import datetime


def validar_producto_con_claude(codigo_leido: str, nombre_leido: str,
                                 establecimiento: str = None) -> Dict:
    """
    Valida un producto usando Claude AI para obtener el nombre real.

    Args:
        codigo_leido: Código EAN o PLU del OCR
        nombre_leido: Nombre como lo leyó el OCR (ej: "LECA KLER L")
        establecimiento: Nombre del establecimiento (opcional)

    Returns:
        Dict con:
        - nombre_normalizado: Nombre real del producto
        - marca: Marca identificada
        - categoria: Categoría del producto
        - subcategoria: Subcategoría
        - confianza: Nivel de confianza (0-100)
        - razonamiento: Explicación de Claude
    """

    print(f"\n🤖 Validando con Claude AI:")
    print(f"   Código: {codigo_leido}")
    print(f"   Nombre OCR: {nombre_leido}")
    print(f"   Establecimiento: {establecimiento or 'N/A'}")

    try:
        # Cliente Anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")

        client = anthropic.Anthropic(api_key=api_key)

        # Prompt especializado para productos colombianos
        prompt = f"""Eres un experto en productos de supermercados COLOMBIANOS.

Te doy un nombre de producto que salió del OCR de una factura (puede tener errores de lectura) y necesito que identifiques el producto REAL.

**DATOS DEL OCR:**
- Código EAN/PLU: {codigo_leido or 'No disponible'}
- Nombre leído: "{nombre_leido}"
- Establecimiento: {establecimiento or 'Supermercado genérico'}

**TU TAREA:**

1. **Identificar el producto real:** Basándote en el código EAN (si existe) y el nombre con errores OCR, determina qué producto es realmente.

2. **Categorizar:** Asigna categoría y subcategoría apropiadas.

**CATEGORÍAS VÁLIDAS EN COLOMBIA:**
- alimentos (subcategorías: lácteos, carnes, frutas-verduras, granos-cereales, panadería, snacks)
- bebidas (subcategorías: gaseosas, jugos, agua, lácteas, alcohólicas)
- aseo-hogar (subcategorías: limpieza, lavandería, desinfección)
- cuidado-personal (subcategorías: higiene, cuidado-piel, cuidado-capilar)
- belleza (subcategorías: maquillaje, cuidado-capilar-fijadores, fragancias)
- mascotas (subcategorías: alimento-perros, alimento-gatos, accesorios)
- bebe (subcategorías: pañales, alimentos, cuidado)
- farmacia (subcategorías: medicamentos, vitaminas, primeros-auxilios)
- otros

**RESPONDE EN FORMATO JSON:**
```json
{{
  "nombre_normalizado": "Nombre completo y correcto del producto",
  "marca": "Marca del producto (si es identificable)",
  "categoria": "categoría válida de la lista",
  "subcategoria": "subcategoría correspondiente",
  "confianza": 95,
  "razonamiento": "Breve explicación de cómo identificaste el producto"
}}
```

**EJEMPLOS:**

Input: "LECA KLER L" (código: 7702113042900)
Output:
```json
{{
  "nombre_normalizado": "Laca KLEER LAC Fijadora Extra Fuerte",
  "marca": "KLEER",
  "categoria": "belleza",
  "subcategoria": "cuidado-capilar-fijadores",
  "confianza": 95,
  "razonamiento": "El código EAN 770211304290 corresponde a productos KLEER. 'LECA' es error OCR de 'Laca'. KLER = KLEER (marca colombiana de fijadores capilares)"
}}
```

Input: "LCHE ALPNA 1L" (código: 7702001234567)
Output:
```json
{{
  "nombre_normalizado": "Leche Alpina Entera 1 Litro",
  "marca": "ALPINA",
  "categoria": "alimentos",
  "subcategoria": "lácteos",
  "confianza": 98,
  "razonamiento": "Error OCR claro: LCHE = Leche, ALPNA = Alpina (marca láctea líder en Colombia)"
}}
```

**IMPORTANTE:**
- Si el código EAN es válido (13 dígitos), úsalo como referencia principal
- Considera errores comunes OCR: O→0, I→1, E→3, S→5, ñ→n
- Marcas colombianas comunes: Alpina, Colanta, Fruco, Zenú, Rica Rondo, Noel, Colombina, Postobón
- Si tienes menos de 70% confianza, indica en el razonamiento

ANALIZA Y RESPONDE SOLO CON JSON:"""

        # Llamar a Claude
        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",  # Sonnet para mejor razonamiento
            max_tokens=1000,
            temperature=0,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )

        response_text = message.content[0].text.strip()
        print(f"   📄 Respuesta Claude: {response_text[:150]}...")

        # Extraer JSON
        import json
        import re

        json_str = response_text
        if "```json" in response_text:
            json_str = response_text.split("```json")[1].split("```")[0]
        elif "```" in response_text:
            json_str = response_text.split("```")[1].split("```")[0]
        elif "{" in response_text:
            start = response_text.find("{")
            end = response_text.rfind("}") + 1
            if start != -1 and end > start:
                json_str = response_text[start:end]

        json_str = json_str.strip()
        resultado = json.loads(json_str)

        # Validar respuesta
        if not resultado.get("nombre_normalizado"):
            raise ValueError("Claude no retornó nombre_normalizado")

        # Asegurar valores por defecto
        resultado.setdefault("marca", None)
        resultado.setdefault("categoria", "otros")
        resultado.setdefault("subcategoria", None)
        resultado.setdefault("confianza", 80)
        resultado.setdefault("razonamiento", "Validación automática")

        print(f"   ✅ Validación exitosa:")
        print(f"      Nombre: {resultado['nombre_normalizado']}")
        print(f"      Marca: {resultado.get('marca', 'N/A')}")
        print(f"      Categoría: {resultado.get('categoria', 'N/A')}")
        print(f"      Confianza: {resultado.get('confianza', 0)}%")

        return resultado

    except Exception as e:
        print(f"   ❌ Error en validación con Claude: {e}")
        import traceback
        traceback.print_exc()

        # Fallback: retornar datos básicos
        return {
            "nombre_normalizado": nombre_leido,
            "marca": None,
            "categoria": "otros",
            "subcategoria": None,
            "confianza": 50,
            "razonamiento": f"Error en validación: {str(e)}"
        }


def buscar_en_memoria(codigo_leido: str, nombre_leido: str, cursor) -> Optional[int]:
    """
    Busca en la tabla codigos_normalizados si ya se validó este producto antes.

    Returns:
        producto_maestro_id si existe, None si no
    """
    try:
        # Buscar por código exacto
        if codigo_leido and len(codigo_leido) >= 4:
            cursor.execute("""
                SELECT producto_maestro_id, confianza, veces_usado
                FROM codigos_normalizados
                WHERE codigo_leido = %s
                AND confianza >= 70
                ORDER BY veces_usado DESC
                LIMIT 1
            """, (codigo_leido,))

            resultado = cursor.fetchone()
            if resultado:
                producto_id, confianza, veces = resultado
                print(f"   ✅ MEMORIA: Encontrado por código (confianza={confianza}%, usado {veces} veces)")

                # Incrementar contador
                cursor.execute("""
                    UPDATE codigos_normalizados
                    SET veces_usado = veces_usado + 1,
                        ultima_vez_usado = CURRENT_TIMESTAMP
                    WHERE codigo_leido = %s AND producto_maestro_id = %s
                """, (codigo_leido, producto_id))

                return producto_id

        # Buscar por nombre similar
        nombre_norm = nombre_leido.upper().strip()
        cursor.execute("""
            SELECT producto_maestro_id, confianza, veces_usado
            FROM codigos_normalizados
            WHERE UPPER(nombre_leido) = %s
            AND confianza >= 70
            ORDER BY veces_usado DESC
            LIMIT 1
        """, (nombre_norm,))

        resultado = cursor.fetchone()
        if resultado:
            producto_id, confianza, veces = resultado
            print(f"   ✅ MEMORIA: Encontrado por nombre (confianza={confianza}%, usado {veces} veces)")

            # Incrementar contador
            cursor.execute("""
                UPDATE codigos_normalizados
                SET veces_usado = veces_usado + 1,
                    ultima_vez_usado = CURRENT_TIMESTAMP
                WHERE UPPER(nombre_leido) = %s AND producto_maestro_id = %s
            """, (nombre_norm, producto_id))

            return producto_id

        print(f"   ℹ️ No encontrado en memoria, validando con Claude...")
        return None

    except Exception as e:
        print(f"   ⚠️ Error buscando en memoria: {e}")
        return None


def guardar_en_memoria(codigo_leido: str, nombre_leido: str, producto_maestro_id: int,
                       confianza: int, cursor, conn) -> bool:
    """
    Guarda el resultado de validación en codigos_normalizados para futuras referencias.
    """
    try:
        # Determinar tipo de código
        tipo_codigo = "DESCONOCIDO"
        if codigo_leido:
            if len(codigo_leido) == 13 and codigo_leido.isdigit():
                tipo_codigo = "EAN13"
            elif len(codigo_leido) >= 3 and codigo_leido.isdigit():
                tipo_codigo = "PLU"
            else:
                tipo_codigo = "INTERNO"

        cursor.execute("""
            INSERT INTO codigos_normalizados (
                codigo_leido,
                nombre_leido,
                producto_maestro_id,
                tipo_codigo,
                confianza,
                veces_usado,
                fecha_aprendizaje,
                ultima_vez_usado
            ) VALUES (%s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (codigo_leido, nombre_leido)
            DO UPDATE SET
                veces_usado = codigos_normalizados.veces_usado + 1,
                ultima_vez_usado = CURRENT_TIMESTAMP,
                confianza = GREATEST(codigos_normalizados.confianza, EXCLUDED.confianza)
        """, (codigo_leido or f"NOCODE_{nombre_leido[:20]}", nombre_leido,
              producto_maestro_id, tipo_codigo, confianza))

        conn.commit()
        print(f"   💾 Guardado en memoria para próximas veces")
        return True

    except Exception as e:
        print(f"   ⚠️ Error guardando en memoria: {e}")
        conn.rollback()
        return False


def procesar_producto_con_validacion(codigo_leido: str, nombre_leido: str, precio: int,
                                     establecimiento_id: Optional[int],
                                     cursor, conn) -> Optional[int]:
    """
    FUNCIÓN PRINCIPAL DE VALIDACIÓN INTELIGENTE

    Flujo:
    1. Busca en memoria (codigos_normalizados)
    2. Si no existe, valida con Claude AI
    3. Crea/actualiza en productos_maestros
    4. Guarda en memoria
    5. Retorna producto_maestro_id

    Args:
        codigo_leido: Código del OCR (puede ser EAN o PLU)
        nombre_leido: Nombre del OCR (puede tener errores)
        precio: Precio del producto
        establecimiento_id: ID del establecimiento (opcional)
        cursor: Cursor de BD
        conn: Conexión a BD

    Returns:
        producto_maestro_id del producto identificado
    """

    print(f"\n{'='*70}")
    print(f"🔍 PROCESANDO PRODUCTO CON VALIDACIÓN INTELIGENTE")
    print(f"{'='*70}")
    print(f"   Código: {codigo_leido or 'N/A'}")
    print(f"   Nombre OCR: {nombre_leido}")
    print(f"   Precio: ${precio:,}")

    try:
        # PASO 1: Buscar en memoria
        producto_id_memoria = buscar_en_memoria(codigo_leido, nombre_leido, cursor)

        if producto_id_memoria:
            print(f"   ⚡ Usando producto de memoria: ID={producto_id_memoria}")
            return producto_id_memoria

        # PASO 2: Validar con Claude AI
        print(f"   🤖 No está en memoria, validando con Claude AI...")

        # Obtener nombre del establecimiento si tenemos el ID
        nombre_establecimiento = None
        if establecimiento_id:
            cursor.execute("SELECT nombre FROM establecimientos WHERE id = %s", (establecimiento_id,))
            resultado = cursor.fetchone()
            if resultado:
                nombre_establecimiento = resultado[0]

        validacion = validar_producto_con_claude(
            codigo_leido=codigo_leido,
            nombre_leido=nombre_leido,
            establecimiento=nombre_establecimiento
        )

        # PASO 3: Buscar/crear en productos_maestros
        nombre_normalizado = validacion["nombre_normalizado"]
        marca = validacion.get("marca")
        categoria = validacion.get("categoria", "otros")
        subcategoria = validacion.get("subcategoria")
        confianza = validacion.get("confianza", 80)

        # Buscar si ya existe el producto
        producto_maestro_id = None

        # Buscar por código EAN si existe
        if codigo_leido and len(codigo_leido) >= 8:
            cursor.execute("""
                SELECT id FROM productos_maestros
                WHERE codigo_ean = %s
                LIMIT 1
            """, (codigo_leido,))

            resultado = cursor.fetchone()
            if resultado:
                producto_maestro_id = resultado[0]
                print(f"   ✅ Producto existe con este EAN: ID={producto_maestro_id}")

                # Actualizar información mejorada
                cursor.execute("""
                    UPDATE productos_maestros
                    SET nombre_normalizado = COALESCE(%s, nombre_normalizado),
                        marca = COALESCE(%s, marca),
                        categoria = COALESCE(%s, categoria),
                        subcategoria = COALESCE(%s, subcategoria),
                        precio_promedio_global = (COALESCE(precio_promedio_global, 0) * COALESCE(total_reportes, 0) + %s) / (COALESCE(total_reportes, 0) + 1),
                        total_reportes = COALESCE(total_reportes, 0) + 1,
                        ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (nombre_normalizado, marca, categoria, subcategoria, precio, producto_maestro_id))
                conn.commit()

        # Si no existe, crear nuevo producto
        if not producto_maestro_id:
            print(f"   ➕ Creando nuevo producto en productos_maestros")
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean,
                    nombre_normalizado,
                    nombre_comercial,
                    marca,
                    categoria,
                    subcategoria,
                    precio_promedio_global,
                    total_reportes,
                    primera_vez_reportado,
                    ultima_actualizacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id
            """, (
                codigo_leido if codigo_leido and len(codigo_leido) >= 8 else None,
                nombre_normalizado,
                nombre_normalizado,
                marca,
                categoria,
                subcategoria,
                precio
            ))

            producto_maestro_id = cursor.fetchone()[0]
            conn.commit()
            print(f"   ✅ Producto creado: ID={producto_maestro_id}")

        # PASO 4: Guardar en memoria
        guardar_en_memoria(
            codigo_leido=codigo_leido,
            nombre_leido=nombre_leido,
            producto_maestro_id=producto_maestro_id,
            confianza=confianza,
            cursor=cursor,
            conn=conn
        )

        print(f"{'='*70}")
        print(f"✅ PRODUCTO PROCESADO EXITOSAMENTE: ID={producto_maestro_id}")
        print(f"{'='*70}\n")

        return producto_maestro_id

    except Exception as e:
        print(f"❌ Error en procesar_producto_con_validacion: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return None


print("✅ validacion_productos.py cargado")
print("   📌 Funciones disponibles:")
print("      - procesar_producto_con_validacion()")
print("      - validar_producto_con_claude()")
print("      - buscar_en_memoria()")
print("      - guardar_en_memoria()")
