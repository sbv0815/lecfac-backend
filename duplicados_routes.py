"""
duplicados_routes.py - Sistema de Detección de Duplicados CORREGIDO
Detecta productos que son el mismo item pero con nombres/códigos diferentes
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from difflib import SequenceMatcher
from datetime import datetime
import traceback
import re

from database import get_db_connection

# ==========================================
# CONFIGURACIÓN DEL ROUTER
# ==========================================
router = APIRouter()

# ==========================================
# MODELOS PYDANTIC
# ==========================================

class FusionProductosRequest(BaseModel):
    """Request para fusionar productos"""
    producto_mantener_id: int
    producto_eliminar_id: int

# ==========================================
# FUNCIONES AUXILIARES
# ==========================================

def calcular_similitud(texto1: str, texto2: str) -> float:
    """Calcula similitud entre dos textos usando SequenceMatcher"""
    if not texto1 or not texto2:
        return 0.0
    
    # Normalizar textos
    t1 = normalizar_nombre_producto(texto1)
    t2 = normalizar_nombre_producto(texto2)
    
    # Calcular similitud
    return SequenceMatcher(None, t1, t2).ratio() * 100

def normalizar_nombre_producto(nombre: str) -> str:
    """
    Normaliza nombre de producto para comparación
    Elimina variaciones comunes que no cambian el producto
    """
    if not nombre:
        return ""
    
    # Convertir a minúsculas
    nombre = nombre.lower().strip()
    
    # Eliminar acentos
    reemplazos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ñ': 'n'
    }
    for old, new in reemplazos.items():
        nombre = nombre.replace(old, new)
    
    # Normalizar unidades de medida comunes
    nombre = re.sub(r'\b(\d+)\s*ml\b', r'\1ml', nombre)
    nombre = re.sub(r'\b(\d+)\s*gr?\b', r'\1g', nombre)
    nombre = re.sub(r'\b(\d+)\s*kg\b', r'\1kg', nombre)
    nombre = re.sub(r'\b(\d+)\s*lt?\b', r'\1l', nombre)
    
    # Convertir unidades equivalentes
    # 1L = 1000ml
    nombre = re.sub(r'\b1\s*l\b', '1000ml', nombre)
    nombre = re.sub(r'\b1\s*lt\b', '1000ml', nombre)
    # 1kg = 1000g
    nombre = re.sub(r'\b1\s*kg\b', '1000g', nombre)
    
    # Eliminar caracteres especiales
    nombre = re.sub(r'[.,\-_/\\]', ' ', nombre)
    
    # Eliminar espacios múltiples
    nombre = ' '.join(nombre.split())
    
    return nombre

def son_precios_similares(precio1: float, precio2: float, tolerancia: float = 0.15) -> bool:
    """
    Verifica si dos precios son similares (dentro del % de tolerancia)
    Por defecto 15% de diferencia
    """
    if precio1 == 0 or precio2 == 0:
        return False
    
    mayor = max(precio1, precio2)
    menor = min(precio1, precio2)
    diferencia_porcentual = (mayor - menor) / mayor
    
    return diferencia_porcentual <= tolerancia

# ==========================================
# ENDPOINTS: PRODUCTOS DUPLICADOS
# ==========================================

@router.get("/admin/duplicados/productos")
async def detectar_productos_duplicados(
    umbral: float = Query(85.0, ge=0, le=100, description="Umbral de similitud (%)"),
    criterio: str = Query("todos", description="Criterio de detección")
):
    """
    Detectar productos duplicados - VERSIÓN CORREGIDA
    
    Un producto es duplicado cuando:
    1. Tiene el mismo código EAN (100% duplicado)
    2. Tiene nombre muy similar (>90%) en el mismo establecimiento
    3. Tiene nombre similar (>85%) y precio similar (±15%)
    
    Criterios:
    - todos: Aplicar todos los criterios
    - codigo: Solo por código EAN
    - nombre: Solo por nombre similar
    - establecimiento: Solo en mismo establecimiento
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener productos desde la tabla productos
        # Agrupamos por nombre normalizado para detectar variaciones
        cursor.execute("""
            SELECT 
                p.id,
                p.nombre,
                p.codigo,
                p.establecimiento,
                p.valor as precio,
                f.cadena,
                COUNT(*) OVER (PARTITION BY p.nombre) as veces_visto
            FROM productos p
            LEFT JOIN facturas f ON p.factura_id = f.id
            WHERE p.nombre IS NOT NULL 
              AND p.nombre != ''
              AND LENGTH(p.nombre) > 3
            ORDER BY p.nombre, p.id
        """)
        
        productos_raw = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Convertir a diccionarios
        productos = []
        for row in productos_raw:
            productos.append({
                "id": row[0],
                "nombre": row[1],
                "codigo": row[2] or "",
                "establecimiento": row[3] or "Desconocido",
                "precio": float(row[4]) if row[4] else 0.0,
                "cadena": row[5] or "",
                "veces_visto": row[6] or 1,
                "ultima_actualizacion": None
            })
        
        print(f"🔍 Analizando {len(productos)} productos...")
        print(f"   Umbral: {umbral}%")
        print(f"   Criterio: {criterio}")
        
        # Detectar duplicados
        duplicados = []
        procesados = set()
        
        for i, prod1 in enumerate(productos):
            for j, prod2 in enumerate(productos[i+1:], start=i+1):
                # Evitar comparar el mismo producto
                if prod1["id"] == prod2["id"]:
                    continue
                
                # Evitar duplicar comparaciones
                par_id = tuple(sorted([prod1["id"], prod2["id"]]))
                if par_id in procesados:
                    continue
                
                # Analizar similitud
                es_duplicado = False
                razones = []
                mismo_codigo = False
                mismo_establecimiento = False
                nombre_similar = False
                precio_similar = False
                similitud_nombre = 0
                
                # 1. CRITERIO PRINCIPAL: Código EAN idéntico
                if prod1["codigo"] and prod2["codigo"] and len(prod1["codigo"]) >= 8:
                    if prod1["codigo"] == prod2["codigo"]:
                        mismo_codigo = True
                        razones.append("Código EAN idéntico")
                        es_duplicado = True
                        similitud_nombre = 100  # Código EAN es definitivo
                
                # 2. Verificar establecimiento/cadena
                if (prod1["establecimiento"] == prod2["establecimiento"] or
                    (prod1["cadena"] and prod2["cadena"] and prod1["cadena"] == prod2["cadena"])):
                    mismo_establecimiento = True
                
                # 3. CRITERIO SECUNDARIO: Similitud de nombre
                similitud_nombre = max(similitud_nombre, calcular_similitud(prod1["nombre"], prod2["nombre"]))
                
                if similitud_nombre >= umbral:
                    nombre_similar = True
                    razones.append(f"Nombre {similitud_nombre:.1f}% similar")
                    
                    # Si tienen nombre MUY similar (>90%) en mismo establecimiento, son duplicados
                    if similitud_nombre >= 90 and mismo_establecimiento:
                        es_duplicado = True
                        razones.append("Mismo establecimiento")
                    
                    # Si tienen nombre similar y precio similar, probablemente duplicados
                    if similitud_nombre >= 85 and son_precios_similares(prod1["precio"], prod2["precio"]):
                        precio_similar = True
                        razones.append("Precio similar")
                        es_duplicado = True
                
                # Aplicar filtros según criterio seleccionado
                if criterio == "codigo":
                    if not mismo_codigo:
                        continue
                elif criterio == "nombre":
                    if not nombre_similar:
                        continue
                elif criterio == "establecimiento":
                    if not mismo_establecimiento:
                        continue
                elif criterio == "todos":
                    if not es_duplicado:
                        continue
                
                # Si pasó los filtros, agregar a la lista
                if es_duplicado or (criterio != "todos" and (mismo_codigo or nombre_similar)):
                    # Determinar cuál producto mantener por defecto
                    # Prioridad: 1) Tiene código, 2) Nombre más completo, 3) Más reciente
                    seleccionado = prod1["id"]
                    if not prod1["codigo"] and prod2["codigo"]:
                        seleccionado = prod2["id"]
                    elif len(prod2["nombre"]) > len(prod1["nombre"]):
                        seleccionado = prod2["id"]
                    
                    duplicados.append({
                        "id": f"dup-{len(duplicados)}",
                        "producto1": prod1,
                        "producto2": prod2,
                        "similitud": round(similitud_nombre, 1),
                        "mismo_codigo": mismo_codigo,
                        "mismo_establecimiento": mismo_establecimiento,
                        "nombre_similar": nombre_similar,
                        "precio_similar": precio_similar,
                        "razon": " + ".join(razones) if razones else "Criterios seleccionados",
                        "seleccionado": seleccionado  # Producto recomendado a mantener
                    })
                    
                    procesados.add(par_id)
        
        print(f"✅ Encontrados {len(duplicados)} pares de duplicados")
        
        return {
            "success": True,
            "total": len(duplicados),
            "duplicados": duplicados,
            "criterios_aplicados": {
                "umbral_similitud": umbral,
                "criterio": criterio,
                "total_productos_analizados": len(productos)
            }
        }
        
    except Exception as e:
        print(f"❌ Error detectando duplicados de productos: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/admin/duplicados/productos/fusionar")
async def fusionar_productos(request: FusionProductosRequest):
    """
    Eliminar producto duplicado
    Simplemente elimina el producto que no queremos mantener
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"\n🗑️ ELIMINANDO PRODUCTO DUPLICADO")
        print(f"   Mantener: #{request.producto_mantener_id}")
        print(f"   Eliminar: #{request.producto_eliminar_id}")
        
        # 1. Verificar que ambos productos existen
        cursor.execute("""
            SELECT id, nombre, codigo FROM productos 
            WHERE id IN (%s, %s)
        """, (request.producto_mantener_id, request.producto_eliminar_id))
        
        productos_encontrados = cursor.fetchall()
        
        if len(productos_encontrados) != 2:
            raise HTTPException(
                status_code=404,
                detail="Uno o ambos productos no encontrados"
            )
        
        # 2. Eliminar el producto duplicado
        cursor.execute("""
            DELETE FROM productos WHERE id = %s
        """, (request.producto_eliminar_id,))
        
        filas_eliminadas = cursor.rowcount
        print(f"   ✅ Producto eliminado")
        
        # 3. Commit
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ ELIMINACIÓN COMPLETADA\n")
        
        return {
            "success": True,
            "message": "Producto duplicado eliminado exitosamente",
            "producto_mantenido": request.producto_mantener_id,
            "producto_eliminado": request.producto_eliminar_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error eliminando producto: {e}")
        print(traceback.format_exc())
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ENDPOINTS: FACTURAS DUPLICADAS
# ==========================================

@router.get("/admin/duplicados/facturas")
async def detectar_facturas_duplicadas(
    criterio: str = Query("all", description="Criterio de detección")
):
    """
    Detectar facturas duplicadas
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener todas las facturas con datos normalizados
        cursor.execute("""
            SELECT 
                f.id,
                COALESCE(f.establecimiento, 'Desconocido') as establecimiento,
                f.fecha_cargue,
                COALESCE(f.total_factura, 0) as total,
                COALESCE(f.tiene_imagen, false) as tiene_imagen,
                COALESCE(f.puntaje_calidad, 0) as calidad_score,
                COUNT(p.id) as num_productos
            FROM facturas f
            LEFT JOIN productos p ON p.factura_id = f.id
            GROUP BY f.id, f.establecimiento, f.fecha_cargue, f.total_factura, f.tiene_imagen, f.puntaje_calidad
            HAVING COUNT(p.id) > 0
            ORDER BY f.fecha_cargue DESC NULLS LAST
            LIMIT 1000
        """)
        
        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1],
                "fecha": row[2],
                "total": float(row[3]),
                "tiene_imagen": row[4],
                "calidad_score": row[5],
                "num_productos": row[6]
            })
        
        cursor.close()
        conn.close()
        
        print(f"🔍 Analizando {len(facturas)} facturas...")
        
        # Detectar duplicados
        duplicados = []
        procesados = set()
        
        for i, fac1 in enumerate(facturas):
            for j, fac2 in enumerate(facturas[i+1:], start=i+1):
                if fac1["id"] == fac2["id"]:
                    continue
                
                par_id = tuple(sorted([fac1["id"], fac2["id"]]))
                if par_id in procesados:
                    continue
                
                es_duplicado = False
                razon = ""
                
                # 1. Mismo establecimiento + fecha + total EXACTO
                if (fac1["establecimiento"] == fac2["establecimiento"] and
                    fac1["fecha"] == fac2["fecha"] and
                    abs(fac1["total"] - fac2["total"]) < 0.01):
                    es_duplicado = True
                    razon = "Misma fecha, establecimiento y total"
                
                # 2. Mismo establecimiento + fecha + productos similares
                elif (fac1["establecimiento"] == fac2["establecimiento"] and
                      fac1["fecha"] == fac2["fecha"] and
                      abs(fac1["num_productos"] - fac2["num_productos"]) <= 2):
                    es_duplicado = True
                    razon = "Misma fecha, establecimiento y productos similares"
                
                # 3. Mismo establecimiento + total + fecha cercana
                elif (fac1["establecimiento"] == fac2["establecimiento"] and
                      abs(fac1["total"] - fac2["total"]) < 0.01):
                    if fac1["fecha"] and fac2["fecha"]:
                        try:
                            dias = abs((fac1["fecha"] - fac2["fecha"]).days)
                            if dias <= 1:
                                es_duplicado = True
                                razon = "Mismo establecimiento, total y fecha cercana"
                        except:
                            pass
                
                # Filtro por criterio
                if criterio == "same_establishment" and fac1["establecimiento"] != fac2["establecimiento"]:
                    continue
                elif criterio == "same_date" and fac1["fecha"] != fac2["fecha"]:
                    continue
                elif criterio == "same_total" and abs(fac1["total"] - fac2["total"]) >= 0.01:
                    continue
                
                if es_duplicado:
                    duplicados.append({
                        "id": f"facdup-{len(duplicados)}",
                        "factura1": fac1,
                        "factura2": fac2,
                        "razon": razon
                    })
                    procesados.add(par_id)
        
        print(f"✅ Encontradas {len(duplicados)} facturas duplicadas")
        
        return {
            "success": True,
            "total": len(duplicados),
            "duplicados": duplicados
        }
        
    except Exception as e:
        print(f"❌ Error detectando facturas duplicadas: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/admin/facturas/{factura_id}")
async def eliminar_factura(factura_id: int):
    """
    Eliminar una factura y todos sus productos
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"\n🗑️ ELIMINANDO FACTURA #{factura_id}")
        
        # Verificar existencia
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        # Eliminar productos
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        productos_eliminados = cursor.rowcount
        
        # Eliminar factura
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Factura eliminada ({productos_eliminados} productos)\n")
        
        return {
            "success": True,
            "message": "Factura eliminada exitosamente",
            "productos_eliminados": productos_eliminados
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))

print("✅ duplicados_routes.py cargado correctamente - VERSIÓN CORREGIDA")
