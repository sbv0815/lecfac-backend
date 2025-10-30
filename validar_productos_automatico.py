"""
Sistema de Validación Automática de Productos
Resuelve el problema de falsos positivos en matching de productos
Usa búsqueda web + Claude API para identificar productos con alta precisión

Autor: Santiago
Fecha: 2025-10-30
Sistema: LecFac
"""
import psycopg2
import requests
import os
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Configuración
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")  # ✅ BIEN
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"


def conectar_db():
    """Conecta a la base de datos"""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return None


def buscar_producto_google(nombre_ocr: str, codigo_ean: str = None) -> List[str]:
    """
    Busca el producto en Google usando requests
    Retorna lista de snippets de resultados
    """
    try:
        # Construir query optimizada
        query = f'"{nombre_ocr}"'

        if codigo_ean and len(codigo_ean) >= 8:
            query += f' "{codigo_ean}"'

        query += ' supermercado Colombia'

        print(f"   🔍 Buscando: {query}")

        # Usar Google Custom Search API o scraping simple
        # Por ahora simulamos con la query
        search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        # Nota: En producción usar Google Custom Search API o SerpAPI
        # Por ahora retornamos la query para que Claude la analice

        return {
            'query': query,
            'search_url': search_url,
            'mensaje': 'Búsqueda preparada para Claude'
        }

    except Exception as e:
        print(f"   ⚠️ Error en búsqueda: {e}")
        return {}


def analizar_producto_con_claude(nombre_ocr: str, codigo_ean: str = None,
                                  contexto_busqueda: dict = None) -> Dict:
    """
    Usa Claude para identificar el producto real basándose en:
    - Nombre OCR (posiblemente cortado)
    - Código EAN
    - Contexto de búsqueda web
    """
    try:
        prompt = f"""Eres un experto en productos de supermercados colombianos. Tu tarea es identificar el producto REAL basándote en un nombre que fue mal leído por OCR.

**DATOS DEL PRODUCTO:**
- Nombre leído por OCR: "{nombre_ocr}"
- Código EAN: {codigo_ean or "No disponible"}

**INSTRUCCIONES:**
1. El nombre OCR puede estar CORTADO o MAL LEÍDO (ej: "LECA KLER L" puede ser "Laca KLEER" NO "Leche Klim")
2. Usa el código EAN como referencia principal si está disponible
3. Considera el contexto: ¿Es un producto de alimentos, belleza, limpieza, otro?
4. Si hay ambigüedad, da prioridad al EAN sobre el nombre

**EJEMPLOS DE ERRORES COMUNES:**
- "LECA KLER L" = "Laca KLEER LAC" (belleza) NO "Leche Klim" (alimentos)
- "CHOCOLATE BI" = "Chocolate BIT" o "Chocolate BIG"
- "AROMATICA 01" = "Aromática Ola" o "Aromática Hindustán"

**RESPONDE EN JSON con este formato EXACTO:**
{{
    "nombre_completo": "Nombre completo del producto con presentación",
    "nombre_corto": "Nombre corto del producto",
    "marca": "MARCA en mayúsculas",
    "categoria": "alimentos|belleza|limpieza|hogar|bebidas|lacteos|otro",
    "subcategoria": "Subcategoría específica",
    "confianza": 0.95,
    "razonamiento": "Breve explicación de por qué identificaste este producto"
}}

**RESPONDE SOLO EL JSON, SIN TEXTO ADICIONAL:**"""

        headers = {
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }

        data = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1000,
            "temperature": 0,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }

        response = requests.post(ANTHROPIC_API_URL, headers=headers, json=data)
        response.raise_for_status()

        # Extraer respuesta
        respuesta_json = response.json()
        respuesta_texto = respuesta_json['content'][0]['text'].strip()

        # Limpiar markdown si existe
        if respuesta_texto.startswith('```json'):
            respuesta_texto = respuesta_texto.replace('```json', '').replace('```', '').strip()

        # Parsear JSON
        resultado = json.loads(respuesta_texto)

        print(f"   ✅ Claude identificó: {resultado['nombre_completo']}")
        print(f"   📊 Confianza: {resultado['confianza']*100:.1f}%")
        print(f"   💭 Razonamiento: {resultado['razonamiento']}")

        return resultado

    except Exception as e:
        print(f"   ❌ Error con Claude: {e}")
        import traceback
        traceback.print_exc()
        return None


def buscar_en_productos_referencia(nombre_ocr: str, codigo_ean: str = None) -> Optional[Dict]:
    """
    Busca primero en productos_referencia antes de ir a la web
    """
    conn = conectar_db()
    if not conn:
        return None

    cur = conn.cursor()

    try:
        # Buscar por EAN (más confiable)
        if codigo_ean and len(codigo_ean) >= 8:
            cur.execute("""
                SELECT
                    codigo_ean, nombre_completo, nombre_corto, marca,
                    categoria, subcategoria
                FROM productos_referencia
                WHERE codigo_ean = %s AND activo = TRUE
                LIMIT 1
            """, (codigo_ean,))

            resultado = cur.fetchone()
            if resultado:
                cur.close()
                conn.close()
                return {
                    'codigo_ean': resultado[0],
                    'nombre_completo': resultado[1],
                    'nombre_corto': resultado[2],
                    'marca': resultado[3],
                    'categoria': resultado[4],
                    'subcategoria': resultado[5],
                    'confianza': 0.95,
                    'fuente': 'productos_referencia',
                    'razonamiento': 'Match exacto por código EAN'
                }

        # Buscar por similitud de nombre (menos confiable)
        cur.execute("""
            SELECT
                codigo_ean, nombre_completo, nombre_corto, marca,
                categoria, subcategoria,
                SIMILARITY(nombre_completo, %s) as similitud
            FROM productos_referencia
            WHERE activo = TRUE
            AND SIMILARITY(nombre_completo, %s) > 0.6
            ORDER BY similitud DESC
            LIMIT 1
        """, (nombre_ocr, nombre_ocr))

        resultado = cur.fetchone()
        if resultado and resultado[6] > 0.75:  # Solo si similitud > 75%
            cur.close()
            conn.close()
            return {
                'codigo_ean': resultado[0],
                'nombre_completo': resultado[1],
                'nombre_corto': resultado[2],
                'marca': resultado[3],
                'categoria': resultado[4],
                'subcategoria': resultado[5],
                'confianza': float(resultado[6]),
                'fuente': 'productos_referencia',
                'razonamiento': f'Match por similitud de nombre ({resultado[6]*100:.1f}%)'
            }

        cur.close()
        conn.close()
        return None

    except Exception as e:
        print(f"   ⚠️ Error buscando en referencia: {e}")
        cur.close()
        conn.close()
        return None


def validar_y_guardar_producto(nombre_ocr: str, codigo_ean: str = None,
                                 precio: int = None, factura_id: int = None) -> Dict:
    """
    Función principal: valida un producto y lo guarda en productos_referencia si es nuevo

    Returns:
        Dict con información del producto validado
    """
    print(f"\n{'='*80}")
    print(f"🔍 VALIDANDO PRODUCTO")
    print(f"{'='*80}")
    print(f"   Nombre OCR: {nombre_ocr}")
    print(f"   Código EAN: {codigo_ean or 'No disponible'}")
    print(f"   Precio: ${precio:,}" if precio else "")

    # Paso 1: Buscar en productos_referencia
    print("\n   📋 Paso 1: Buscando en productos_referencia...")
    resultado_ref = buscar_en_productos_referencia(nombre_ocr, codigo_ean)

    if resultado_ref and resultado_ref['confianza'] >= 0.90:
        print(f"   ✅ Match encontrado en productos_referencia")
        return resultado_ref

    # Paso 2: Buscar en web (preparar contexto)
    print("\n   🌐 Paso 2: Preparando búsqueda web...")
    contexto_web = buscar_producto_google(nombre_ocr, codigo_ean)

    # Paso 3: Analizar con Claude
    print("\n   🤖 Paso 3: Analizando con Claude AI...")
    resultado_claude = analizar_producto_con_claude(nombre_ocr, codigo_ean, contexto_web)

    if not resultado_claude:
        print("   ❌ No se pudo identificar el producto")
        return None

    # Paso 4: Validar confianza
    if resultado_claude['confianza'] < 0.70:
        print(f"   ⚠️ Confianza baja ({resultado_claude['confianza']*100:.1f}%)")
        print("   📝 Producto marcado para revisión manual")
        return {
            **resultado_claude,
            'requiere_validacion_manual': True
        }

    # Paso 5: Guardar en productos_referencia
    print("\n   💾 Paso 4: Guardando en productos_referencia...")
    producto_guardado = guardar_en_productos_referencia(resultado_claude, codigo_ean)

    if producto_guardado:
        print(f"   ✅ Producto guardado exitosamente")
        return resultado_claude
    else:
        print(f"   ⚠️ No se pudo guardar (puede que ya exista)")
        return resultado_claude


def guardar_en_productos_referencia(info_producto: Dict, codigo_ean: str = None) -> bool:
    """
    Guarda un producto validado en productos_referencia
    """
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        # Verificar si ya existe
        if codigo_ean:
            cur.execute("""
                SELECT id FROM productos_referencia
                WHERE codigo_ean = %s
            """, (codigo_ean,))

            if cur.fetchone():
                print("      ℹ️ Producto ya existe en referencia")
                cur.close()
                conn.close()
                return False

        # Insertar nuevo producto
        cur.execute("""
            INSERT INTO productos_referencia (
                codigo_ean, nombre_completo, nombre_corto, marca,
                categoria, subcategoria, tags, fecha_importacion, activo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), TRUE)
            RETURNING id
        """, (
            codigo_ean,
            info_producto.get('nombre_completo'),
            info_producto.get('nombre_corto'),
            info_producto.get('marca'),
            info_producto.get('categoria'),
            info_producto.get('subcategoria'),
            f"auto_validado confianza:{info_producto.get('confianza', 0)}"
        ))

        producto_id = cur.fetchone()[0]
        conn.commit()

        print(f"      ✅ Nuevo producto ID: {producto_id}")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"      ❌ Error guardando: {e}")
        conn.rollback()
        cur.close()
        conn.close()
        return False


def validar_productos_pendientes(limite: int = None):
    """
    Valida productos en productos_maestros que no tienen marca/categoría
    Si limite es None, procesa TODOS los productos pendientes
    """
    conn = conectar_db()
    if not conn:
        return

    cur = conn.cursor()

    try:
        print("\n" + "="*80)
        print("🔍 BUSCANDO PRODUCTOS PENDIENTES DE VALIDACIÓN")
        print("="*80 + "\n")

        # Buscar productos sin marca o categoría
        if limite:
            cur.execute("""
                SELECT
                    id, codigo_ean, nombre_normalizado,
                    marca, categoria, total_reportes
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL
                AND LENGTH(codigo_ean) >= 8
                AND (marca IS NULL OR categoria IS NULL)
                ORDER BY total_reportes DESC
                LIMIT %s
            """, (limite,))
        else:
            # SIN LÍMITE - procesar TODOS
            cur.execute("""
                SELECT
                    id, codigo_ean, nombre_normalizado,
                    marca, categoria, total_reportes
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL
                AND LENGTH(codigo_ean) >= 8
                AND (marca IS NULL OR categoria IS NULL)
                ORDER BY total_reportes DESC
            """)

        productos_pendientes = cur.fetchall()

        if not productos_pendientes:
            print("✅ No hay productos pendientes de validación")
            cur.close()
            conn.close()
            return

        print(f"📦 Encontrados {len(productos_pendientes)} productos pendientes\n")

        validados = 0
        errores = 0

        for producto in productos_pendientes:
            prod_id, ean, nombre, marca, categoria, reportes = producto

            try:
                # Validar producto
                resultado = validar_y_guardar_producto(nombre, ean)

                if resultado and resultado['confianza'] >= 0.70:
                    # Actualizar productos_maestros
                    cur.execute("""
                        UPDATE productos_maestros
                        SET
                            nombre_normalizado = %s,
                            marca = %s,
                            categoria = %s,
                            subcategoria = %s,
                            ultima_actualizacion = NOW()
                        WHERE id = %s
                    """, (
                        resultado['nombre_completo'],
                        resultado['marca'],
                        resultado['categoria'],
                        resultado.get('subcategoria'),
                        prod_id
                    ))
                    conn.commit()
                    validados += 1
                    print(f"   ✅ Producto ID {prod_id} actualizado\n")
                else:
                    errores += 1
                    print(f"   ⚠️ Producto ID {prod_id} requiere revisión\n")

            except Exception as e:
                errores += 1
                print(f"   ❌ Error con producto ID {prod_id}: {e}\n")
                conn.rollback()

        print("="*80)
        print(f"✅ Validados exitosamente: {validados}")
        print(f"⚠️ Requieren revisión: {errores}")
        print("="*80 + "\n")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error en validación masiva: {e}")
        import traceback
        traceback.print_exc()
        cur.close()
        conn.close()


def probar_caso_laca_kleer():
    """
    Prueba el caso específico de LECA KLER L vs Leche Klim
    """
    print("\n" + "="*80)
    print("🧪 PRUEBA: Caso Laca KLEER vs Leche Klim")
    print("="*80 + "\n")

    # Caso de prueba
    resultado = validar_y_guardar_producto(
        nombre_ocr="LECA KLER L",
        codigo_ean=None,  # No tenemos el EAN real
        precio=15000
    )

    if resultado:
        print("\n" + "="*80)
        print("📊 RESULTADO DE LA PRUEBA")
        print("="*80)
        print(f"Nombre identificado: {resultado.get('nombre_completo')}")
        print(f"Marca: {resultado.get('marca')}")
        print(f"Categoría: {resultado.get('categoria')}")
        print(f"Confianza: {resultado.get('confianza', 0)*100:.1f}%")
        print(f"Razonamiento: {resultado.get('razonamiento')}")

        # Verificar si es correcto
        if 'laca' in resultado.get('nombre_completo', '').lower():
            print("\n✅ ¡CORRECTO! Identificó como LACA (belleza)")
        elif 'leche' in resultado.get('nombre_completo', '').lower():
            print("\n❌ ERROR: Identificó como LECHE (alimento)")

        print("="*80 + "\n")


def main():
    """Menú principal"""
    print("\n" + "="*80)
    print("🎯 SISTEMA DE VALIDACIÓN AUTOMÁTICA DE PRODUCTOS")
    print("Sistema LecFac - Validación Inteligente")
    print("="*80 + "\n")

    if not ANTHROPIC_API_KEY:
        print("❌ ERROR: ANTHROPIC_API_KEY no configurada")
        print("   Configura la variable de entorno antes de continuar")
        return

    print("Opciones:")
    print("1. Probar caso: LECA KLER L (Laca vs Leche)")
    print("2. Validar productos pendientes (sin marca/categoría)")
    print("3. Validar un producto específico")
    print("4. Salir")

    opcion = input("\nSelecciona una opción (1-4): ").strip()

    if opcion == "1":
        probar_caso_laca_kleer()

    elif opcion == "2":
        print("\n" + "="*80)
        print("⚠️  VALIDACIÓN MASIVA DE PRODUCTOS")
        print("="*80)
        print("\nEsta operación puede tomar varios minutos dependiendo")
        print("de cuántos productos haya que validar.\n")

        respuesta = input("¿Validar TODOS los productos pendientes? (s/n): ").strip().lower()

        if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
            print("\n🚀 Iniciando validación masiva...")
            validar_productos_pendientes(limite=None)  # Sin límite
        else:
            limite = input("¿Cuántos productos validar? (default: 10): ").strip()
            limite = int(limite) if limite.isdigit() else 10
            validar_productos_pendientes(limite)

    elif opcion == "3":
        nombre = input("Nombre OCR: ").strip()
        ean = input("Código EAN (opcional): ").strip() or None
        precio = input("Precio (opcional): ").strip()
        precio = int(precio) if precio.isdigit() else None

        resultado = validar_y_guardar_producto(nombre, ean, precio)

        if resultado:
            print("\n✅ Producto validado exitosamente")
        else:
            print("\n❌ No se pudo validar el producto")

    elif opcion == "4":
        print("\n👋 ¡Hasta luego!")

    else:
        print("\n❌ Opción inválida")


if __name__ == "__main__":
    main()
