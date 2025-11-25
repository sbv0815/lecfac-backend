# 🛒 Guía Completa: Scraping Carulla para LecFac

## 📋 Índice
1. [Instalación](#instalación)
2. [Uso Básico](#uso-básico)
3. [Integración con LecFac](#integración-con-lecfac)
4. [Consideraciones Legales](#consideraciones-legales)
5. [Buenas Prácticas](#buenas-prácticas)
6. [Troubleshooting](#troubleshooting)

---

## 🔧 Instalación

### 1. Instalar Playwright

```bash
# Instalar Playwright
pip install playwright --break-system-packages

# Instalar navegadores
playwright install chromium
```

### 2. Verificar archivos

Debes tener estos 3 archivos:
- `carulla_scraper.py` - Scraper base
- `lecfac_enricher.py` - Integración con LecFac
- `GUIA_SCRAPING.md` - Este archivo

---

## 🚀 Uso Básico

### Ejemplo 1: Scrapear un producto específico

```python
import asyncio
from carulla_scraper import scrapear_url

# URL de un producto
url = "https://www.carulla.com/queso-fresco-15-pcto-de-descuento-paq-x-30-tajadas-559646/p"

# Ejecutar
producto = asyncio.run(scrapear_url(url))

print(producto)
# Output:
# {
#     'nombre': 'Queso Mozarella FINESSE 30 tajadas (450 gr)',
#     'plu': '426036',
#     'precio': 26100,
#     'supermercado': 'Carulla',
#     'url': 'https://...'
# }
```

### Ejemplo 2: Buscar productos

```python
import asyncio
from carulla_scraper import buscar_productos

# Buscar "queso mozarella" en Carulla
productos = asyncio.run(buscar_productos("queso mozarella", max_productos=5))

for p in productos:
    print(f"{p['nombre']} - PLU: {p['plu']} - ${p['precio']:,}")
```

---

## 🔗 Integración con LecFac

### Flujo Recomendado

```
OCR Extrae Producto
        ↓
¿Existe en BD?
    ↓ NO
Scrapear Carulla
        ↓
Enriquecer con datos completos
        ↓
Guardar en BD
```

### Código de Integración

```python
from lecfac_enricher import ProductEnricher
import asyncio

async def procesar_producto_ocr(producto_ocr):
    """
    Procesa un producto extraído por OCR
    """
    enricher = ProductEnricher()
    
    # Producto del OCR
    producto = {
        'nombre': 'MOZARELL FINESSE',  # Nombre parcial del OCR
        'plu': '426036',
        'precio': 26100,
        'supermercado': 'Carulla'
    }
    
    # Enriquecer con scraping
    producto_enriquecido = await enricher.enriquecer_producto_lecfac(producto)
    
    # Ahora tienes:
    print(producto_enriquecido['nombre_completo'])
    # 'Queso Mozarella FINESSE 30 tajadas (450 gr)'
    
    return producto_enriquecido

# Ejecutar
asyncio.run(procesar_producto_ocr(...))
```

### Integración con FastAPI

```python
# En tu backend (productos_api_v2.py o similar)

from lecfac_enricher import ProductEnricher

enricher = ProductEnricher()

@app.post("/api/productos/enriquecer")
async def enriquecer_producto(producto: dict):
    """
    Endpoint para enriquecer productos con scraping
    """
    try:
        producto_enriquecido = await enricher.enriquecer_producto_lecfac(producto)
        return {
            "success": True,
            "data": producto_enriquecido
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
```

---

## ⚖️ Consideraciones Legales

### ✅ Prácticas Seguras

1. **Rate Limiting Estricto**
   - 3 segundos entre requests (ya implementado)
   - Máximo 100 productos/día por IP
   - Solo productos que no existen en tu BD

2. **Caching Agresivo**
   ```python
   # Guardar productos scrapeados en tu BD
   # NO scrapear el mismo producto dos veces
   ```

3. **User-Agent Honesto**
   ```python
   headers = {
       'User-Agent': 'LecFac Price Comparison Bot (+contacto@lecfac.com)'
   }
   ```

4. **Respetar robots.txt**
   - Revisar: https://www.carulla.com/robots.txt
   - No scrapear áreas prohibidas

### ⚠️ Zona Gris Legal

**Scraping en Colombia:**
- ✅ Datos públicos (precios, nombres)
- ✅ Uso personal/investigación
- ⚠️ Uso comercial (tu caso)
- ❌ Replicar el sitio completo
- ❌ Competir directamente

**Recomendación:**
1. **Contactar a Carulla primero** (mejores prácticas)
2. Scraping como fallback temporal
3. Buscar API oficial en paralelo

---

## 🎯 Buenas Prácticas

### 1. Cuándo Scrapear

✅ **SÍ scrapear:**
- Producto nuevo (no existe en BD)
- PLU sin nombre completo
- Precios desactualizados (>7 días)

❌ **NO scrapear:**
- Productos que ya tienes completos
- Cada vez que usuario escanea factura
- Consultas repetitivas

### 2. Implementación Práctica

```python
async def debe_scrapear(producto_plu: str) -> bool:
    """
    Determina si vale la pena scrapear
    """
    # Buscar en BD
    producto_bd = buscar_en_bd(producto_plu)
    
    if not producto_bd:
        return True  # Producto nuevo
    
    if not producto_bd.get('nombre_completo'):
        return True  # Falta info
    
    if (datetime.now() - producto_bd['ultima_actualizacion']).days > 7:
        return True  # Desactualizado
    
    return False  # Ya tenemos buena info
```

### 3. Cola de Scraping Asíncrona

```python
# En lugar de scrapear en tiempo real:
# 1. Usuario escanea factura
# 2. OCR procesa
# 3. Agregar productos a cola de scraping
# 4. Procesar cola cada noche (off-peak hours)

import asyncio
from datetime import datetime

scraping_queue = []

def agregar_a_cola(producto):
    """Agregar producto a cola de scraping"""
    scraping_queue.append({
        'producto': producto,
        'timestamp': datetime.now()
    })

async def procesar_cola_nocturna():
    """Procesar cola de scraping (ejecutar a las 2 AM)"""
    enricher = ProductEnricher()
    
    for item in scraping_queue:
        producto = await enricher.enriquecer_producto_lecfac(item['producto'])
        guardar_en_bd(producto)
        
        await asyncio.sleep(5)  # Rate limiting generoso
    
    scraping_queue.clear()
```

### 4. Manejo de Errores

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
async def scrapear_con_reintentos(url):
    """Reintentar scraping en caso de fallo"""
    return await scraper.scrape_producto(url)
```

---

## 🔍 Troubleshooting

### Problema 1: "playwright not found"

```bash
# Reinstalar
pip uninstall playwright
pip install playwright --break-system-packages
playwright install chromium
```

### Problema 2: Página no carga

```python
# Aumentar timeout
await page.goto(url, wait_until="networkidle", timeout=30000)
await page.wait_for_timeout(5000)  # 5 segundos
```

### Problema 3: No encuentra precios

```python
# El precio puede estar en diferentes elementos
# Revisar manualmente la página y ajustar los selectores:
precio = await page.locator("[data-testid='price']").text_content()
```

### Problema 4: Rate limiting / Bloqueo IP

```python
# Soluciones:
# 1. Aumentar delay entre requests
self.rate_limit_delay = 5  # 5 segundos

# 2. Rotar User-Agent
# 3. Usar proxies (avanzado)
# 4. Contactar Carulla para API oficial
```

---

## 📊 Métricas Sugeridas

```python
# Trackear efectividad del scraping
metricas = {
    'productos_scrapeados': 0,
    'productos_exitosos': 0,
    'productos_fallidos': 0,
    'tiempo_promedio': 0,
    'errores_por_tipo': {}
}

# Revisar semanalmente:
# - Tasa de éxito (debe ser >80%)
# - Productos únicos (no duplicar scraping)
# - Uso de cache (debe ser >70%)
```

---

## 🚦 Siguiente Paso

### Opción A: Empezar Pequeño (Recomendado)

```bash
# 1. Probar el scraper con 3-5 productos manualmente
python carulla_scraper.py

# 2. Si funciona bien, integrar en flujo de OCR
# 3. Monitorear por 1 semana
# 4. Escalar gradualmente
```

### Opción B: Contactar Carulla Primero

```
Asunto: Propuesta de Colaboración - App Comparación de Precios

Estimados,

Soy desarrollador de LecFac, una app comunitaria que ayuda a 
colombianos a encontrar mejores precios en supermercados.

¿Carulla ofrece una API de productos para desarrolladores?

Estamos dispuestos a:
- Reconocer a Carulla como fuente oficial
- Enlazar directamente al sitio
- Cumplir con términos de uso

Contacto: santiago@lecfac.com
```

---

## 📝 Resumen: Pros y Contras

### ✅ Ventajas del Scraping

- Datos completos y actualizados
- Implementación rápida
- Gratuito
- Control total

### ⚠️ Desventajas del Scraping

- Zona gris legal
- Puede romperse si cambia el sitio
- Riesgo de bloqueo
- Mantenimiento necesario

### 💡 Recomendación Final

**Plan Dual:**
1. Implementar scraping como MVP (esta semana)
2. Contactar Carulla para API oficial (en paralelo)
3. Migrar a API cuando esté disponible

---

## 📞 Soporte

¿Dudas? Revisa:
- Documentación Playwright: https://playwright.dev/python/
- Términos de Carulla: https://www.carulla.com/terminos-y-condiciones
- Contacto Carulla: servicio al cliente

---

**Última actualización:** 25 Nov 2024  
**Versión:** 1.0  
**Autor:** Claude + Santiago
