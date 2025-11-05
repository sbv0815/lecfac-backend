# 🚀 GUÍA DE INTEGRACIÓN - PRODUCTOS V2

## 📋 RESUMEN

Has recibido 2 archivos nuevos para estandarizar tu catálogo de productos:

1. **`productos_mejoras.py`** - Backend con endpoints de duplicados y fusión
2. **`productos_v2.html`** - Interfaz mejorada con detección de duplicados

---

## ⚡ INTEGRACIÓN RÁPIDA (5 minutos)

### **Paso 1: Copiar Backend**

```bash
# Copia productos_mejoras.py a tu carpeta backend
cp productos_mejoras.py /ruta/a/tu/backend/
```

### **Paso 2: Editar main.py**

Agrega estas líneas a tu `main.py`:

```python
# Al inicio del archivo (con las demás importaciones)
from productos_mejoras import router as productos_mejoras_router

# Después de crear tu app (después de app = FastAPI())
app.include_router(productos_mejoras_router)
```

**Ejemplo completo:**

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ✅ AGREGAR ESTA LÍNEA
from productos_mejoras import router as productos_mejoras_router

app = FastAPI()

# CORS (si ya lo tienes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ AGREGAR ESTA LÍNEA
app.include_router(productos_mejoras_router)

# ... resto de tu código
```

### **Paso 3: Copiar HTML**

```bash
# Copia productos_v2.html a tu carpeta de templates/static
cp productos_v2.html /ruta/a/tu/frontend/
```

### **Paso 4: Reiniciar Servidor**

```bash
# Detén tu servidor (Ctrl+C)
# Reinicia
uvicorn main:app --reload
```

---

## ✅ VERIFICAR INSTALACIÓN

### **1. Probar Endpoints**

Abre tu navegador en: `http://localhost:8000/docs`

Deberías ver estos nuevos endpoints en la documentación:

- `GET /api/productos` - Lista productos
- `GET /api/productos/{id}` - Detalles de producto
- `PUT /api/productos/{id}` - Actualizar producto
- `DELETE /api/productos/{id}` - Eliminar producto
- `POST /api/productos/fusionar` - Fusionar productos
- `GET /api/productos/duplicados/ean` - Duplicados por EAN
- `GET /api/productos/duplicados/plu-establecimiento` - Duplicados por PLU
- `GET /api/productos/duplicados/nombres-similares` - Nombres similares
- `GET /api/productos/duplicados/resumen` - Resumen de duplicados
- `GET /api/productos/estadisticas/calidad` - Estadísticas de calidad
- `GET /api/productos/{id}/historial-compras` - Historial de compras

### **2. Probar en Navegador**

```bash
# Prueba un endpoint directamente
curl http://localhost:8000/api/productos/estadisticas/calidad

# Deberías ver un JSON con estadísticas
```

### **3. Abrir Interfaz**

Abre `productos_v2.html` en tu navegador y deberías ver:

- ✅ Estadísticas en cards
- ✅ Tabla de productos
- ✅ Sin errores en consola

---

## 🔧 SOLUCIÓN DE PROBLEMAS

### **Error: "Module not found: productos_mejoras"**

**Solución:**
```bash
# Asegúrate de que productos_mejoras.py esté en la misma carpeta que main.py
ls -la | grep productos_mejoras.py

# Si no está, cópialo
cp productos_mejoras.py .
```

### **Error: "SyntaxError: Failed to execute 'close'"**

**Causa:** El HTML está intentando llamar endpoints que no existen

**Solución:**
1. Verifica que `productos_mejoras.py` esté cargado
2. Verifica que agregaste `app.include_router(productos_mejoras_router)` en main.py
3. Reinicia el servidor

### **Error: "Table 'productos_maestros' doesn't exist"**

**Causa:** Tu base de datos no tiene las tablas necesarias

**Solución:**
```bash
# Ejecuta create_tables() de database.py
python -c "from database import create_tables; create_tables()"
```

### **Error 500 en algún endpoint**

**Solución:**
1. Mira los logs del servidor
2. Verifica que tienes columnas `codigo_plu` en `productos_maestros`
3. Si falta, agrégala:

```sql
-- PostgreSQL
ALTER TABLE productos_maestros ADD COLUMN codigo_plu VARCHAR(20);

-- SQLite
ALTER TABLE productos_maestros ADD COLUMN codigo_plu TEXT;
```

---

## 📊 CARACTERÍSTICAS IMPLEMENTADAS

### **1. Detección de Duplicados**

**Tipos detectados:**
- 🔴 **Mismo EAN, diferentes IDs** (crítico - no debería pasar)
- 🟠 **Mismo PLU en mismo establecimiento** (crítico)
- 🟡 **Nombres similares >85%** (revisar manualmente)

**Cómo usar:**
1. Ir a tab "⚠️ Duplicados"
2. Click "🔍 Analizar Todos los Productos"
3. Revisar lista agrupada por tipo
4. Click "🔗 Fusionar Todo" o seleccionar manualmente

### **2. Fusión de Productos**

**Estrategias:**
- **Más completo:** Mantiene el producto con más datos (EAN, PLU, marca, etc.)
- **Principal:** Mantiene datos del primer producto seleccionado

**Qué se actualiza automáticamente:**
- ✅ `items_factura` - Historial de compras
- ✅ `inventario_usuario` - Consolida cantidades
- ✅ Elimina productos duplicados

**Cómo usar:**
1. Seleccionar productos con checkbox
2. Click "🔗 Fusionar Seleccionados"
3. Elegir estrategia
4. Confirmar

### **3. Edición Inline**

**Cómo usar:**
- Doble click en cualquier celda
- Editar valor
- Enter para guardar
- Actualización instantánea

**Campos editables:**
- Código EAN
- Código PLU
- Nombre normalizado
- Marca
- Categoría

### **4. Dashboard de Calidad**

**Métricas mostradas:**
- Total de productos
- % con EAN completo
- % con marca
- % con categoría
- Productos huérfanos (sin compras)
- Duplicados detectados

**Recomendaciones automáticas:**
- Si % EAN < 80% → "Agregar códigos EAN"
- Si duplicados > 0 → "Fusionar duplicados"
- Si huérfanos > 10 → "Limpiar productos sin uso"

---

## 🎯 CASOS DE USO

### **Caso 1: Limpiar Duplicados de EAN**

**Problema:** Tienes "7702001234567" registrado 3 veces como productos diferentes

**Solución:**
1. Tab "⚠️ Duplicados"
2. Click "🔍 Analizar"
3. Buscar sección "🔴 Duplicados por EAN"
4. Click "🔗 Fusionar Todo" en ese grupo
5. ✅ Listo - ahora es 1 solo producto

### **Caso 2: Fusionar Productos con Nombres Parecidos**

**Problema:** Tienes "LECHE ENTERA 1L" y "Leche Entera 1 Litro" como productos diferentes

**Solución:**
1. Tab "⚠️ Duplicados"
2. Click "🔍 Analizar"
3. Buscar sección "🟡 Nombres Similares"
4. Revisar cada grupo
5. Click "🔗 Fusionar Todo" si son el mismo producto

### **Caso 3: Completar Datos Faltantes**

**Problema:** Muchos productos sin marca o categoría

**Solución:**
1. Tab "📊 Calidad de Datos"
2. Ver recomendaciones
3. Click "Ver productos sin marca"
4. Doble click en celda "Marca"
5. Completar dato
6. Enter para guardar

### **Caso 4: Agregar Código PLU a Productos Frescos**

**Problema:** Productos frescos (frutas, verduras) usan PLU de 4 dígitos

**Solución:**
1. Tab "📋 Lista Completa"
2. Buscar producto
3. Doble click en columna PLU
4. Ingresar código (ej: "4011" para bananas)
5. Enter para guardar

---

## 📈 MEJORES PRÁCTICAS

### **1. Mantén el Catálogo Limpio**

- ✅ Ejecuta "Detectar Duplicados" semanalmente
- ✅ Fusiona duplicados inmediatamente
- ✅ Completa marcas y categorías al crear productos

### **2. Usa Códigos Estándar**

- ✅ **EAN-13:** Para productos empaquetados (13 dígitos)
- ✅ **EAN-8:** Para productos pequeños (8 dígitos)
- ✅ **PLU:** Para productos frescos (4-5 dígitos)

### **3. Normaliza Nombres**

**Bueno:**
- "LECHE ALPINA ENTERA 1L"
- "PAN TAJADO INTEGRAL 500G"

**Malo:**
- "leche alpina"
- "Pan"
- "LECHE ALPINA ENTERA 1 LITRO" (duplicado)

### **4. Revisa Antes de Fusionar**

- ✅ Verifica que los productos sean realmente iguales
- ✅ Revisa el historial de compras (botón 📜)
- ✅ Elige estrategia "Más completo" por defecto

---

## 🔍 QUERIES SQL ÚTILES

### **Ver duplicados manualmente**

```sql
-- Duplicados por EAN
SELECT codigo_ean, COUNT(*) as total
FROM productos_maestros
WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
GROUP BY codigo_ean
HAVING COUNT(*) > 1;

-- Productos sin marca
SELECT id, nombre_normalizado, codigo_ean
FROM productos_maestros
WHERE marca IS NULL OR marca = '';

-- Productos sin compras (huérfanos)
SELECT pm.id, pm.nombre_normalizado
FROM productos_maestros pm
LEFT JOIN items_factura i ON i.producto_maestro_id = pm.id
WHERE i.id IS NULL;
```

---

## 📞 SOPORTE

Si encuentras errores:

1. **Revisa logs del servidor** - Busca líneas que empiecen con "❌"
2. **Verifica la consola del navegador** (F12)
3. **Prueba endpoints en /docs** - Verifica que respondan JSON
4. **Revisa que columna `codigo_plu` exista** en `productos_maestros`

---

## ✅ CHECKLIST DE INTEGRACIÓN

- [ ] `productos_mejoras.py` copiado a carpeta backend
- [ ] `from productos_mejoras import router` agregado a main.py
- [ ] `app.include_router(router)` agregado a main.py
- [ ] Servidor reiniciado
- [ ] `/docs` muestra nuevos endpoints
- [ ] `productos_v2.html` copiado a frontend
- [ ] HTML abre sin errores en consola
- [ ] Estadísticas cargan correctamente
- [ ] Tabla de productos se muestra
- [ ] Botón "Detectar Duplicados" funciona
- [ ] Edición inline funciona
- [ ] Fusión de productos funciona

---

## 🎉 ¡LISTO!

Tu sistema de estandarización está completamente integrado. Ahora puedes:

✅ Detectar duplicados automáticamente
✅ Fusionar productos en segundos
✅ Editar datos con doble click
✅ Monitorear calidad del catálogo
✅ Mantener tu base de datos limpia

---

**¿Dudas? Déjame saber qué necesitas!** 🚀
