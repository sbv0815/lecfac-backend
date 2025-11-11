"""
PERPLEXITY VALIDATOR V6 - VALIDACIÓN INTELIGENTE CON 3 CAPAS
=============================================================
Flujo: OCR → Correcciones Python → Validación Perplexity

Este módulo recibe nombres YA CORREGIDOS por Python y usa Perplexity
para obtener el nombre completo del producto verificando:
1. Que exista en el supermercado específico
2. Que el precio sea similar (±20%)
3. Que sea el mismo tipo de producto

Ejemplo:
    OCR:        "oso  blanco"
    Python:     "QUESO BLANCO"  ← Entra aquí
    Perplexity: "QUESO BLANCO COLANTA 500G"  ← Sale aquí (si valida)
"""

import os
import requests
import time
import unicodedata

# ========== CONFIGURACIÓN ==========
PERPLEXITY_API_KEY = os.environ.get("lefact", "").strip()
PERPLEXITY_API_URL = "https://api.perplexity.ai/chat/completions"
PERPLEXITY_MODEL = "sonar"
TIMEOUT_SECONDS = 10
MARGEN_PRECIO_PORCENTAJE = 20  # ±20% para validar precio

# ========== INICIALIZACIÓN ==========
if PERPLEXITY_API_KEY:
    print("="*80)
    print("✅ PERPLEXITY VALIDATOR V6.0 - VALIDACIÓN 3 CAPAS")
    print("="*80)
    print(f"   🔑 API Key: Configurada")
    print(f"   🤖 Modelo: {PERPLEXITY_MODEL}")
    print(f"   ⏱️  Timeout: {TIMEOUT_SECONDS}s")
    print(f"   💰 Margen precio: ±{MARGEN_PRECIO_PORCENTAJE}%")
    print(f"   🎯 Estrategia: OCR → Python → Perplexity")
    print("="*80)
else:
    print("⚠️  Perplexity API Key NO configurada - Solo usará correcciones Python")


def construir_prompt_validacion(nombre_corregido, precio, supermercado, codigo=""):
    """
    Construye el prompt para Perplexity con VALIDACIÓN ESTRICTA.

    Args:
        nombre_corregido: Nombre YA corregido por Python (ej: "QUESO BLANCO")
        precio: Precio del producto en COP
        supermercado: Nombre del establecimiento
        codigo: Código EAN o PLU (opcional)

    Returns:
        str: Prompt optimizado para búsqueda específica
    """
    super_norm = normalizar_nombre_supermercado(supermercado)
    precio_min = int(precio * (1 - MARGEN_PRECIO_PORCENTAJE/100))
    precio_max = int(precio * (1 + MARGEN_PRECIO_PORCENTAJE/100))

    # Construir info de código si existe
    info_codigo = f" con codigo {codigo}" if codigo else ""

    # Prompt ESTRICTO para validación
    prompt = f"""Busca en {super_norm} Colombia el producto exacto: {nombre_corregido}{info_codigo}

Precio factura: ${precio:,} COP (valido entre ${precio_min:,} - ${precio_max:,})

INSTRUCCIONES ESTRICTAS:
1. Busca SOLO en {super_norm} Colombia
2. El producto DEBE ser {nombre_corregido} o muy similar
3. El precio DEBE estar entre ${precio_min:,} - ${precio_max:,} COP
4. Si encuentras el producto con marca/presentacion, responde: NOMBRE COMPLETO EN MAYUSCULAS SIN TILDES
5. Si NO cumple precio o NO existe, responde: NO VALIDADO

Ejemplos:
- Si buscas QUESO BLANCO y encuentras "Queso Blanco Colanta 500g" a $8,500: responde "QUESO BLANCO COLANTA 500G"
- Si buscas ARROZ DIANA pero precio muy diferente: responde "NO VALIDADO"
- Si buscas CREMA DE LECHE pero encuentras varias marcas con precios distintos: responde "NO VALIDADO"

Responde SOLO el nombre completo O "NO VALIDADO". Sin explicaciones."""

    return prompt


def normalizar_nombre_supermercado(supermercado):
    """Normaliza el nombre del supermercado para búsqueda."""
    mapeo = {
        'JUMBO': 'JUMBO',
        'EXITO': 'EXITO',
        'CARULLA': 'CARULLA',
        'OLIMPICA': 'OLIMPICA',
        'D1': 'D1',
        'ARA': 'ARA',
        'CRUZ VERDE': 'CRUZ VERDE'
    }

    super_upper = supermercado.upper().strip()

    # Buscar coincidencia parcial
    for key, value in mapeo.items():
        if key in super_upper:
            return value

    return super_upper


def extraer_nombre_respuesta(data):
    """Extrae el nombre de la respuesta de Perplexity."""
    try:
        if 'choices' in data and data['choices']:
            choice = data['choices'][0]
            if choice and 'message' in choice and 'content' in choice['message']:
                contenido = choice['message']['content'].strip()

                # Quitar markdown
                contenido = contenido.replace('**', '').replace('*', '')

                # Tomar solo primera línea
                nombre = contenido.split('\n')[0].strip()

                return nombre

        return "NO VALIDADO"
    except Exception as e:
        print(f"   ⚠️  Error extrayendo respuesta: {e}")
        return "NO VALIDADO"


def limpiar_nombre_validado(nombre):
    """
    Limpia el nombre validado por Perplexity.
    Quita tildes, caracteres especiales y frases agregadas.
    """
    if not nombre or len(nombre.strip()) < 2:
        return nombre

    nombre = nombre.upper().strip()

    # Si es NO VALIDADO, retornar tal cual
    if "NO VALIDADO" in nombre or "NO ENCONTRADO" in nombre:
        return "NO VALIDADO"

    # Quitar frases comunes que Perplexity agrega
    frases_eliminar = [
        'PRODUCTO SIMILAR ENCONTRADO EN',
        'PRODUCTO ENCONTRADO EN',
        'EN COLOMBIA',
        'COLOMBIA',
        'SIMILAR EN',
        'ENCONTRADO EN',
        'EL PRODUCTO ES',
        'SE TRATA DE',
        'DISPONIBLE EN',
        'VENDIDO EN'
    ]

    for frase in frases_eliminar:
        nombre = nombre.replace(frase, '')

    # Quitar nombres de supermercados si quedaron
    supermercados = ['JUMBO', 'EXITO', 'CARULLA', 'OLIMPICA', 'D1', 'ARA', 'CRUZ VERDE']
    for super_nombre in supermercados:
        nombre = nombre.replace(super_nombre, '')

    # Quitar tildes
    nombre = ''.join(
        c for c in unicodedata.normalize('NFD', nombre)
        if unicodedata.category(c) != 'Mn'
    )

    # Quitar caracteres especiales pero mantener números y letras
    caracteres_permitidos = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '
    nombre = ''.join(c if c in caracteres_permitidos else ' ' for c in nombre)

    # Normalizar espacios
    nombre = ' '.join(nombre.split())

    # Limitar longitud
    if len(nombre) > 100:
        nombre = nombre[:100].strip()

    return nombre


def es_nombre_valido(nombre_validado, nombre_corregido):
    """
    Verifica si el nombre validado es realmente una mejora del corregido.

    Args:
        nombre_validado: Nombre retornado por Perplexity
        nombre_corregido: Nombre que envió Python

    Returns:
        bool: True si es válido y es una mejora
    """
    if not nombre_validado or len(nombre_validado) < 3:
        return False

    if "NO VALIDADO" in nombre_validado or "NO ENCONTRADO" in nombre_validado:
        return False

    # El nombre validado debe contener el corregido o ser muy similar
    val_limpio = nombre_validado.upper().strip()
    corr_limpio = nombre_corregido.upper().strip()

    # Si son idénticos, no es mejora
    if val_limpio == corr_limpio:
        return False

    # El validado debe contener las palabras clave del corregido
    palabras_corregido = set(corr_limpio.split())
    palabras_validado = set(val_limpio.split())

    # Al menos 70% de las palabras del corregido deben estar en el validado
    if len(palabras_corregido) > 0:
        coincidencias = palabras_corregido.intersection(palabras_validado)
        porcentaje_coincidencia = len(coincidencias) / len(palabras_corregido)

        if porcentaje_coincidencia < 0.7:
            return False

    # El validado debe ser más largo (tiene más info)
    if len(val_limpio) <= len(corr_limpio):
        return False

    return True


def validar_con_perplexity(nombre_corregido, precio, supermercado, codigo="", nombre_ocr_original=""):
    """
    FUNCIÓN PRINCIPAL: Valida un producto usando Perplexity.

    Esta función recibe un nombre YA CORREGIDO por Python y usa Perplexity
    para intentar obtener el nombre completo con marca/presentación.

    Args:
        nombre_corregido: Nombre corregido por Python (ej: "QUESO BLANCO")
        precio: Precio en COP
        supermercado: Nombre del establecimiento
        codigo: Código EAN/PLU opcional
        nombre_ocr_original: Nombre original del OCR (para logging)

    Returns:
        dict: {
            'nombre_final': str,        # Nombre a usar (validado o corregido)
            'fue_validado': bool,       # True si Perplexity mejoró el nombre
            'confianza': str,           # 'alta', 'media', 'baja'
            'fuente': str,              # 'perplexity' o 'python'
            'tiempo_respuesta': float,  # Segundos
            'detalles': str            # Info adicional
        }
    """

    # Si no hay API key, usar nombre corregido
    if not PERPLEXITY_API_KEY:
        return {
            'nombre_final': nombre_corregido,
            'fue_validado': False,
            'confianza': 'media',
            'fuente': 'python',
            'detalles': 'Sin API key - usando corrección Python'
        }

    print("\n" + "="*70)
    print("🔍 VALIDACIÓN CON PERPLEXITY")
    print("="*70)
    if nombre_ocr_original:
        print(f"   📝 OCR original: {nombre_ocr_original}")
    print(f"   🔧 Corregido Python: {nombre_corregido}")
    print(f"   💰 Precio: ${precio:,} COP")
    print(f"   🏪 Supermercado: {supermercado}")
    if codigo:
        print(f"   🔢 Código: {codigo}")

    try:
        # Construir prompt
        prompt = construir_prompt_validacion(nombre_corregido, precio, supermercado, codigo)

        # Preparar request
        headers = {
            "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": PERPLEXITY_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": "Eres un asistente que valida productos en supermercados colombianos. Responde SOLO el nombre completo del producto O 'NO VALIDADO'."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": 100,
            "temperature": 0.1
        }

        # Hacer request
        inicio = time.time()
        response = requests.post(
            PERPLEXITY_API_URL,
            json=payload,
            headers=headers,
            timeout=TIMEOUT_SECONDS
        )
        tiempo_respuesta = time.time() - inicio

        # Procesar respuesta
        if response.status_code == 200:
            data = response.json()
            nombre_raw = extraer_nombre_respuesta(data)
            nombre_validado = limpiar_nombre_validado(nombre_raw)

            print(f"   🤖 Respuesta Perplexity: {nombre_raw}")
            print(f"   🧹 Limpiado: {nombre_validado}")

            # Verificar si es válido
            if es_nombre_valido(nombre_validado, nombre_corregido):
                print(f"   ✅ VALIDADO: {nombre_validado}")
                print(f"   ⏱️  Tiempo: {tiempo_respuesta:.2f}s")
                print("="*70)

                return {
                    'nombre_final': nombre_validado,
                    'fue_validado': True,
                    'confianza': 'alta',
                    'fuente': 'perplexity',
                    'tiempo_respuesta': tiempo_respuesta,
                    'detalles': f'Validado en {supermercado}'
                }
            else:
                print(f"   ⚠️  NO VALIDADO - Usando nombre corregido Python")
                print(f"   📝 Mantiene: {nombre_corregido}")
                print(f"   ⏱️  Tiempo: {tiempo_respuesta:.2f}s")
                print("="*70)

                return {
                    'nombre_final': nombre_corregido,
                    'fue_validado': False,
                    'confianza': 'media',
                    'fuente': 'python',
                    'tiempo_respuesta': tiempo_respuesta,
                    'detalles': 'Perplexity no validó - mantiene corrección Python'
                }
        else:
            print(f"   ❌ Error HTTP {response.status_code}")
            print(f"   📄 {response.text[:200]}")
            print(f"   📝 Usando: {nombre_corregido}")
            print("="*70)

            return {
                'nombre_final': nombre_corregido,
                'fue_validado': False,
                'confianza': 'media',
                'fuente': 'python',
                'detalles': f'Error HTTP {response.status_code}'
            }

    except requests.Timeout:
        print(f"   ⏱️  TIMEOUT - Usando nombre corregido Python")
        print(f"   📝 Mantiene: {nombre_corregido}")
        print("="*70)

        return {
            'nombre_final': nombre_corregido,
            'fue_validado': False,
            'confianza': 'media',
            'fuente': 'python',
            'detalles': 'Timeout Perplexity'
        }

    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        print(f"   📝 Usando: {nombre_corregido}")
        print("="*70)

        return {
            'nombre_final': nombre_corregido,
            'fue_validado': False,
            'confianza': 'media',
            'fuente': 'python',
            'detalles': f'Error: {str(e)}'
        }


# ========== FUNCIONES DE COMPATIBILIDAD ==========
# Para mantener compatibilidad con código anterior

def validar_nombre_producto(nombre_ocr, precio, supermercado, codigo=""):
    """
    DEPRECATED: Mantener para compatibilidad pero ahora solo normaliza.
    Usar validar_con_perplexity() para el flujo completo.
    """
    print("⚠️  Usando función DEPRECATED validar_nombre_producto()")
    print("   Considere usar: correcciones_ocr.corregir_ocr_basico() + validar_con_perplexity()")

    # Solo normalizar sin validar
    nombre_normalizado = nombre_ocr.upper().strip()
    nombre_normalizado = ''.join(
        c for c in unicodedata.normalize('NFD', nombre_normalizado)
        if unicodedata.category(c) != 'Mn'
    )

    return {
        'nombre_validado': nombre_normalizado,
        'confianza': 'baja',
        'fuente': 'ocr_normalizado'
    }


# ========== LOG DE CARGA ==========
print("🔥 PERPLEXITY_VALIDATOR.PY V6.0 CARGADO")
print("   📋 Funciones disponibles:")
print("   - validar_con_perplexity() [PRINCIPAL - 3 capas]")
print("   - construir_prompt_validacion()")
print("   - es_nombre_valido()")
print("   - limpiar_nombre_validado()")
