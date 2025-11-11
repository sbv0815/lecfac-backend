"""
============================================================================
PERPLEXITY VALIDATOR - Sistema de Validación de Nombres de Productos
VERSION 1.0
============================================================================

PROPÓSITO:
Valida nombres de productos detectados por OCR consultando con Perplexity
para obtener el nombre CORRECTO según el supermercado y precio.

ESTRATEGIA:
- Solo se validan productos NUEVOS (que no existen en BD)
- Productos existentes usan el nombre que ya tienen
- Incluye el nombre del supermercado en la consulta para máxima precisión

AUTOR: LecFac Team
FECHA: 2025-11-11
============================================================================
"""

import os
import requests
import json
import time
from typing import Dict, Optional


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

PERPLEXITY_API_KEY = os.environ.get("lefact", "").strip()
PERPLEXITY_MODEL = "llama-3.1-sonar-small-128k-online"
PERPLEXITY_ENDPOINT = "https://api.perplexity.ai/chat/completions"
TIMEOUT_SECONDS = 10


# ==============================================================================
# VALIDACIÓN DE CONFIGURACIÓN
# ==============================================================================

def verificar_configuracion() -> bool:
    """Verifica que las variables de entorno estén configuradas"""
    if not PERPLEXITY_API_KEY:
        print("❌ ERROR: Variable 'lefact' (Perplexity API Key) no configurada")
        return False

    print("✅ Perplexity API Key configurada")
    return True


# ==============================================================================
# FUNCIÓN PRINCIPAL DE VALIDACIÓN
# ==============================================================================

def validar_nombre_producto(
    nombre_ocr: str,
    precio: int,
    supermercado: str,
    codigo: str = ""
) -> Dict[str, any]:
    """
    Valida el nombre de un producto con Perplexity

    Args:
        nombre_ocr: Nombre detectado por OCR (puede tener errores)
        precio: Precio del producto en pesos colombianos
        supermercado: Nombre del supermercado (JUMBO, ÉXITO, etc)
        codigo: Código EAN o PLU (opcional)

    Returns:
        {
            'nombre_validado': str,
            'confianza': str ('alta' | 'media' | 'baja'),
            'fuente': str ('perplexity' | 'ocr_fallback'),
            'tiempo_respuesta': float (segundos),
            'error': str (si hubo error)
        }
    """

    print(f"\n{'='*70}")
    print(f"🔍 VALIDANDO CON PERPLEXITY")
    print(f"{'='*70}")
    print(f"   📝 Nombre OCR: {nombre_ocr}")
    print(f"   💰 Precio: ${precio:,} COP")
    print(f"   🏪 Supermercado: {supermercado}")
    if codigo:
        print(f"   🔖 Código: {codigo}")

    # Verificar configuración
    if not PERPLEXITY_API_KEY:
        print("   ⚠️  API Key no configurada, usando nombre OCR")
        return {
            'nombre_validado': nombre_ocr,
            'confianza': 'baja',
            'fuente': 'ocr_fallback',
            'error': 'API Key no configurada'
        }

    try:
        inicio = time.time()

        # Construir prompt optimizado
        prompt = construir_prompt(nombre_ocr, precio, supermercado, codigo)

        # Llamar a Perplexity API
        response = requests.post(
            PERPLEXITY_ENDPOINT,
            headers={
                "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": PERPLEXITY_MODEL,
                "messages": [
                    {
                        "role": "system",
                        "content": "Eres un experto en productos de supermercados colombianos. Responde SOLO con el nombre del producto, sin explicaciones."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 100,
                "temperature": 0.1,
                "top_p": 0.9,
                "stream": False
            },
            timeout=TIMEOUT_SECONDS
        )

        tiempo_respuesta = time.time() - inicio

        if response.status_code == 200:
            data = response.json()
            nombre_validado = extraer_nombre_respuesta(data)

            # Limpiar y normalizar nombre
            nombre_validado = limpiar_nombre_validado(nombre_validado)

            # Calcular confianza
            confianza = calcular_confianza(nombre_ocr, nombre_validado, precio)

            print(f"   ✅ Validado: {nombre_validado}")
            print(f"   📊 Confianza: {confianza}")
            print(f"   ⏱️  Tiempo: {tiempo_respuesta:.2f}s")
            print(f"{'='*70}\n")

            return {
                'nombre_validado': nombre_validado,
                'confianza': confianza,
                'fuente': 'perplexity',
                'tiempo_respuesta': tiempo_respuesta,
                'nombre_original_ocr': nombre_ocr
            }

        else:
            print(f"   ❌ Error HTTP {response.status_code}")
            print(f"   📄 Respuesta: {response.text[:200]}")

            return {
                'nombre_validado': nombre_ocr,
                'confianza': 'baja',
                'fuente': 'ocr_fallback',
                'error': f'HTTP {response.status_code}: {response.text[:100]}'
            }

    except requests.Timeout:
        print(f"   ⏱️  Timeout ({TIMEOUT_SECONDS}s) - usando nombre OCR")
        return {
            'nombre_validado': nombre_ocr,
            'confianza': 'baja',
            'fuente': 'ocr_fallback',
            'error': 'Timeout'
        }

    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()

        return {
            'nombre_validado': nombre_ocr,
            'confianza': 'baja',
            'fuente': 'ocr_fallback',
            'error': str(e)
        }


# ==============================================================================
# CONSTRUCCIÓN DE PROMPT
# ==============================================================================

def construir_prompt(nombre_ocr: str, precio: int, supermercado: str, codigo: str = "") -> str:
    """
    Construye el prompt optimizado para Perplexity

    ESTRATEGIA:
    - Incluir supermercado para contexto específico
    - Incluir precio para validación de coherencia
    - Incluir código si está disponible
    - Pedir respuesta en formato específico (MAYÚSCULAS, sin tildes)
    """

    # Limpiar supermercado
    supermercado_limpio = supermercado.upper().strip()

    # Construir contexto de código
    contexto_codigo = ""
    if codigo:
        if len(codigo) >= 8:
            contexto_codigo = f"\n- Código EAN: {codigo}"
        else:
            contexto_codigo = f"\n- Código PLU: {codigo}"

    prompt = f"""Busca en internet el producto que se vende en el supermercado {supermercado_limpio} en Colombia con estas características:

DATOS DEL PRODUCTO:
- Nombre detectado: "{nombre_ocr}"
- Precio aproximado: ${precio:,} COP{contexto_codigo}
- Supermercado: {supermercado_limpio}

INSTRUCCIONES:
1. Busca este producto específicamente en {supermercado_limpio} Colombia
2. Verifica que el precio sea coherente con el producto
3. Si el nombre tiene errores de OCR (ej: "QSO BCO"), corrígelo al nombre real (ej: "QUESO BLANCO")
4. Responde con el nombre COMPLETO y CORRECTO del producto
5. Si hay varias presentaciones, usa la más común para ese precio

FORMATO DE RESPUESTA:
- SOLO el nombre del producto
- En MAYÚSCULAS
- Sin tildes (Á→A, É→E, Í→I, Ó→O, Ú→U, Ñ→N)
- Sin símbolos especiales
- Sin explicaciones adicionales

EJEMPLOS DE RESPUESTA CORRECTA:
- "QUESO BLANCO CAMPESINO"
- "CREMA DE LECHE ALPINA"
- "ARROZ DIANA 500G"

RESPONDE SOLO CON EL NOMBRE DEL PRODUCTO:"""

    return prompt


# ==============================================================================
# EXTRACCIÓN Y LIMPIEZA DE RESPUESTA
# ==============================================================================

def extraer_nombre_respuesta(data: dict) -> str:
    """
    Extrae el nombre del producto de la respuesta de Perplexity
    """
    try:
        # Estructura de respuesta de Perplexity
        nombre = data['choices'][0]['message']['content'].strip()

        # Eliminar markdown si existe
        nombre = nombre.replace('**', '').replace('*', '')

        # Tomar solo la primera línea si hay múltiples
        if '\n' in nombre:
            nombre = nombre.split('\n')[0].strip()

        return nombre

    except (KeyError, IndexError) as e:
        print(f"   ⚠️  Error extrayendo nombre: {e}")
        return ""


def limpiar_nombre_validado(nombre: str) -> str:
    """
    Limpia y normaliza el nombre validado por Perplexity

    - Convertir a MAYÚSCULAS
    - Eliminar tildes
    - Eliminar caracteres especiales
    - Eliminar espacios múltiples
    """
    import unicodedata

    if not nombre or len(nombre.strip()) < 2:
        return nombre

    # Convertir a mayúsculas
    nombre = nombre.upper().strip()

    # Eliminar tildes
    nombre = ''.join(
        c for c in unicodedata.normalize('NFD', nombre)
        if unicodedata.category(c) != 'Mn'
    )

    # Reemplazar Ñ que se perdió en la normalización
    # (En Colombia la Ñ es importante)
    # Si detectamos "N~" o similares, restaurar Ñ
    nombre = nombre.replace('~N', 'Ñ').replace('N~', 'Ñ')

    # Eliminar comillas, paréntesis, corchetes
    for char in ['"', "'", '(', ')', '[', ']', '{', '}']:
        nombre = nombre.replace(char, '')

    # Reemplazar guiones y barras por espacios
    for char in ['-', '_', '/', '\\', '|']:
        nombre = nombre.replace(char, ' ')

    # Eliminar espacios múltiples
    nombre = ' '.join(nombre.split())

    # Limitar longitud máxima
    if len(nombre) > 100:
        nombre = nombre[:100].strip()

    return nombre


# ==============================================================================
# CÁLCULO DE CONFIANZA
# ==============================================================================

def calcular_confianza(nombre_ocr: str, nombre_validado: str, precio: int) -> str:
    """
    Calcula nivel de confianza de la validación

    Returns:
        'alta' | 'media' | 'baja'
    """
    # Si el nombre no cambió mucho, confianza alta
    similitud = calcular_similitud_simple(nombre_ocr, nombre_validado)

    if similitud > 0.8:
        return 'alta'
    elif similitud > 0.5:
        return 'media'
    else:
        # Cambió mucho - puede ser corrección importante
        # Si el precio es coherente, aún puede ser alta confianza
        if precio > 1000:  # Producto con precio razonable
            return 'media'
        else:
            return 'baja'


def calcular_similitud_simple(texto1: str, texto2: str) -> float:
    """
    Calcula similitud simple entre dos textos (0.0 a 1.0)
    """
    if not texto1 or not texto2:
        return 0.0

    texto1 = texto1.upper().strip()
    texto2 = texto2.upper().strip()

    if texto1 == texto2:
        return 1.0

    # Similitud por palabras comunes
    palabras1 = set(texto1.split())
    palabras2 = set(texto2.split())

    if not palabras1 or not palabras2:
        return 0.0

    comunes = palabras1.intersection(palabras2)
    total = palabras1.union(palabras2)

    return len(comunes) / len(total) if total else 0.0


# ==============================================================================
# VALIDACIÓN POR LOTES (OPCIONAL)
# ==============================================================================

def validar_productos_batch(productos: list) -> list:
    """
    Valida múltiples productos en lote

    Args:
        productos: Lista de dicts con {nombre, precio, supermercado, codigo}

    Returns:
        Lista de resultados de validación
    """
    resultados = []

    print(f"\n{'='*80}")
    print(f"📦 VALIDACIÓN EN LOTE: {len(productos)} productos")
    print(f"{'='*80}\n")

    for i, prod in enumerate(productos, 1):
        print(f"[{i}/{len(productos)}]", end=" ")

        resultado = validar_nombre_producto(
            nombre_ocr=prod.get('nombre', ''),
            precio=prod.get('precio', 0),
            supermercado=prod.get('supermercado', ''),
            codigo=prod.get('codigo', '')
        )

        resultados.append(resultado)

        # Pequeña pausa para no saturar API
        if i < len(productos):
            time.sleep(0.5)

    # Estadísticas
    validados = sum(1 for r in resultados if r['fuente'] == 'perplexity')
    fallbacks = sum(1 for r in resultados if r['fuente'] == 'ocr_fallback')

    print(f"\n{'='*80}")
    print(f"📊 RESULTADOS DEL LOTE:")
    print(f"   ✅ Validados con Perplexity: {validados}")
    print(f"   ⚠️  Fallback a OCR: {fallbacks}")
    print(f"{'='*80}\n")

    return resultados


# ==============================================================================
# INICIALIZACIÓN
# ==============================================================================

# Verificar configuración al importar
if __name__ != "__main__":
    if verificar_configuracion():
        print("=" * 80)
        print("✅ PERPLEXITY VALIDATOR V1.0 CARGADO")
        print("=" * 80)
        print(f"   🔑 API Key: Configurada")
        print(f"   🤖 Modelo: {PERPLEXITY_MODEL}")
        print(f"   ⏱️  Timeout: {TIMEOUT_SECONDS}s")
        print(f"   🎯 Estrategia: Solo productos NUEVOS")
        print("=" * 80)
    else:
        print("=" * 80)
        print("⚠️  PERPLEXITY VALIDATOR - Configuración Incompleta")
        print("=" * 80)
        print("   ❌ Variable 'lefact' no encontrada")
        print("   ℹ️  Sistema funcionará con fallback a nombres OCR")
        print("=" * 80)


# ==============================================================================
# TESTING
# ==============================================================================

if __name__ == "__main__":
    print("🧪 TESTING PERPLEXITY VALIDATOR\n")

    # Test 1: Producto con error OCR
    print("TEST 1: Error OCR típico")
    resultado = validar_nombre_producto(
        nombre_ocr="QSO BLANCO",
        precio=8600,
        supermercado="OLIMPICA"
    )
    print(f"Resultado: {json.dumps(resultado, indent=2, ensure_ascii=False)}\n")

    # Test 2: Producto con nombre correcto
    print("TEST 2: Nombre correcto")
    resultado = validar_nombre_producto(
        nombre_ocr="ARROZ DIANA",
        precio=4500,
        supermercado="EXITO",
        codigo="7702001023456"
    )
    print(f"Resultado: {json.dumps(resultado, indent=2, ensure_ascii=False)}\n")

    # Test 3: Producto fresco con PLU
    print("TEST 3: Producto fresco")
    resultado = validar_nombre_producto(
        nombre_ocr="MANGO",
        precio=6280,
        supermercado="EXITO",
        codigo="1220"
    )
    print(f"Resultado: {json.dumps(resultado, indent=2, ensure_ascii=False)}\n")
