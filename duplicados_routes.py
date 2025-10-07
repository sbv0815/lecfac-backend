"""
duplicados_routes.py - Endpoints para detección y gestión de duplicados
Incluye detección de productos y facturas duplicadas + fusión/eliminación
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from difflib import SequenceMatcher
from datetime import datetime
import traceback

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

class ProductoDuplicado(BaseModel):
    """Modelo de producto duplicado"""
    id: int
    nombre: str
    codigo: Optional[str]
    establecimiento: str
    precio: float
    veces_visto: int
    ultima_actualizacion: Optional[str]

class DuplicadoProducto(BaseModel):
    """Modelo de par de productos duplicados"""
    id: str
    producto1: ProductoDuplicado
    producto2: ProductoDuplicado
    similitud: float
    mismo_codigo: bool
    mismo_establecimiento: bool
    nombre_similar: bool
    razon: str

# ==========================================
# FUNCIONES AUXILIARES
# ==========================================

def calcular_similitud(texto1: str, texto2: str) -> float:
    """Calcula similitud entre dos textos usando SequenceMatcher"""
    if not texto1 or not texto2:
        return 0.0
    
    # Normalizar textos
    t1 = texto1.lower().strip()
    t2 = texto2.lower().strip()
    
    # Calcular similitud
    return SequenceMatcher(None, t1, t2).ratio() * 100

def normalizar_nombre_producto(nombre: str) -> str:
    """Normaliza nombre de producto para comparación"""
    if not nombre:
        return ""
    
    # Convertir a minúsculas
    nombre = nombre.lower()
    
    # Eliminar caracteres especiales comunes
    reemplazos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ñ': 'n', '.': '', ',': '', '-': ' ', '_': ' '
    }
    
    for old, new in reemplazos.items():
        nombre = nombre.replace(old, new)
    
    # Eliminar espacios múltiples
    nombre = ' '.join(nombre.split())
    
    return nombre

# ==========================================
# ENDPOINTS: PRODUCTOS DUPLICADOS
# ==========================================

@router.get("/admin/duplicados/productos")
async def detectar_productos_duplicados(
    umbral: float = Query(85.0, ge=0, le=100, description="Umbral de similitud (%)"),
    criterio: str = Query("todos", description="Criterio de detección")
):
    """
    Detectar productos duplicados según diferentes criterios
    
    Criterios disponibles:
    - todos: Todos los criterios combinados
    - codigo: Mismo código EAN
    - nombre: Nombres similares
    - establecimiento: Mismo establecimiento
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener todos los productos únicos con sus estadísticas
        cursor.execute("""
            SELECT 
                id,
                nombre,
                codigo,
                establecimiento,
                precio,
                veces_visto,
                ultima_actualizacion
            FROM productos_unicos
            ORDER BY veces_visto DESC
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "nombre": row[1],
                "codigo": row[2],
                "establecimiento": row[3],
                "precio": float(row[4]) if row[4] else 0.0,
                "veces_visto": row[5] or 0,
                "ultima_actualizacion": row[6].isoformat() if row[6] else None
            })
        
        cursor.close()
        conn.close()
        
        print(f"🔍 Analizando {len(productos)} productos únicos...")
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
                
                # 1. Verificar código EAN
                if prod1.get("codigo") and prod2.get("codigo"):
                    if prod1["codigo"] == prod2["codigo"]:
                        mismo_codigo = True
                        razones.append("Mismo código")
                        es_duplicado = True
                
                # 2. Verificar establecimiento
                if prod1["establecimiento"] == prod2["establecimiento"]:
                    mismo_establecimiento = True
                
                # 3. Verificar similitud de nombre
                similitud_nombre = calcular_similitud(prod1["nombre"], prod2["nombre"])
                if similitud_nombre >= umbral:
                    nombre_similar = True
                    razones.append(f"Nombre {similitud_nombre:.1f}% similar")
                    if mismo_establecimiento:
                        es_duplicado = True
                
                # Aplicar filtros según criterio
                if criterio == "codigo" and not mismo_codigo:
                    continue
                elif criterio == "nombre" and not nombre_similar:
                    continue
                elif criterio == "establecimiento" and not mismo_establecimiento:
                    continue
                elif criterio == "todos" and not es_duplicado:
                    continue
                
                # Si es duplicado, agregar a la lista
                if es_duplicado or (criterio != "todos" and (mismo_codigo or nombre_similar)):
                    duplicados.append({
                        "id": f"dup-{len(duplicados)}",
                        "producto1": prod1,
                        "producto2": prod2,
                        "similitud": similitud_nombre,
                        "mismo_codigo": mismo_codigo,
                        "mismo_establecimiento": mismo_establecimiento,
                        "nombre_similar": nombre_similar,
                        "razon": " + ".join(razones) if razones else "Similitud baja"
                    })
                    
                    procesados.add(par_id)
        
        print(f"✅ Encontrados {len(duplicados)} pares de duplicados")
        
        return {
            "success": True,
            "total": len(duplicados),
            "duplicados": duplicados
        }
        
    except Exception as e:
        print(f"❌ Error detectando duplicados de productos: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/admin/duplicados/productos/fusionar")
async def fusionar_productos(request: FusionProductosRequest):
    """
    Fusionar dos productos duplicados
    Mantiene uno y elimina el otro, transfiriendo todo el historial
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"\n🔄 FUSIONANDO PRODUCTOS")
        print(f"   Mantener: #{request.producto_mantener_id}")
        print(f"   Eliminar: #{request.producto_eliminar_id}")
        
        # 1. Verificar que ambos productos existen
        cursor.execute("""
            SELECT id, nombre, codigo FROM productos_unicos 
            WHERE id IN (%s, %s)
        """, (request.producto_mantener_id, request.producto_eliminar_id))
        
        productos_encontrados = cursor.fetchall()
        
        if len(productos_encontrados) != 2:
            raise HTTPException(
                status_code=404,
                detail="Uno o ambos productos no encontrados"
            )
        
        # 2. Validar que tienen el mismo código (si aplica)
        cursor.execute("""
            SELECT codigo FROM productos_unicos WHERE id = %s
        """, (request.producto_mantener_id,))
        codigo_mantener = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT codigo FROM productos_unicos WHERE id = %s
        """, (request.producto_eliminar_id,))
        codigo_eliminar = cursor.fetchone()[0]
        
        if codigo_mantener and codigo_eliminar and codigo_mantener != codigo_eliminar:
            raise HTTPException(
                status_code=400,
                detail="No se pueden fusionar productos con códigos EAN diferentes"
            )
        
        # 3. Actualizar todas las referencias del producto a eliminar
        cursor.execute("""
            UPDATE productos 
            SET producto_unico_id = %s
            WHERE producto_unico_id = %s
        """, (request.producto_mantener_id, request.producto_eliminar_id))
        
        filas_actualizadas = cursor.rowcount
        print(f"   ✅ {filas_actualizadas} registros actualizados")
        
        # 4. Recalcular estadísticas del producto mantenido
        cursor.execute("""
            UPDATE productos_unicos
            SET 
                veces_visto = (
                    SELECT COUNT(*) 
                    FROM productos 
                    WHERE producto_unico_id = %s
                ),
                ultima_actualizacion = (
                    SELECT MAX(fecha) 
                    FROM productos 
                    WHERE producto_unico_id = %s
                ),
                precio = (
                    SELECT precio 
                    FROM productos 
                    WHERE producto_unico_id = %s 
                    ORDER BY fecha DESC 
                    LIMIT 1
                )
            WHERE id = %s
        """, (
            request.producto_mantener_id,
            request.producto_mantener_id,
            request.producto_mantener_id,
            request.producto_mantener_id
        ))
        
        # 5. Eliminar el producto duplicado
        cursor.execute("""
            DELETE FROM productos_unicos 
            WHERE id = %s
        """, (request.producto_eliminar_id,))
        
        print(f"   ✅ Producto #{request.producto_eliminar_id} eliminado")
        
        # 6. Commit
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ FUSIÓN COMPLETADA\n")
        
        return {
            "success": True,
            "message": "Productos fusionados exitosamente",
            "registros_actualizados": filas_actualizadas
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error fusionando productos: {e}")
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
    Detectar facturas duplicadas según diferentes criterios
    
    Criterios disponibles:
    - all: Todos los criterios
    - same_establishment: Mismo establecimiento + fecha + total
    - same_date: Misma fecha + productos similares
    - same_total: Mismo total + establecimiento
    - same_products: Productos idénticos o muy similares
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener todas las facturas
        cursor.execute("""
            SELECT 
                f.id,
                f.establecimiento,
                f.fecha,
                f.total,
                f.tiene_imagen,
                f.calidad_score,
                COUNT(p.id) as num_productos
            FROM facturas f
            LEFT JOIN productos p ON p.factura_id = f.id
            GROUP BY f.id, f.establecimiento, f.fecha, f.total, f.tiene_imagen, f.calidad_score
            ORDER BY f.fecha DESC
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
        
        print(f"🔍 Analizando {len(facturas)} facturas...")
        print(f"   Criterio: {criterio}")
        
        # Detectar duplicados
        duplicados = []
        procesados = set()
        
        for i, fac1 in enumerate(facturas):
            for j, fac2 in enumerate(facturas[i+1:], start=i+1):
                # Evitar comparar la misma factura
                if fac1["id"] == fac2["id"]:
                    continue
                
                # Evitar duplicar comparaciones
                par_id = tuple(sorted([fac1["id"], fac2["id"]]))
                if par_id in procesados:
                    continue
                
                # Analizar similitud
                es_duplicado = False
                razon = ""
                
                # 1. Mismo establecimiento + fecha + total
                if (fac1["establecimiento"] == fac2["establecimiento"] and
                    fac1["fecha"] == fac2["fecha"] and
                    abs(fac1["total"] - fac2["total"]) < 0.01):
                    es_duplicado = True
                    razon = "Mismo establecimiento, fecha y total"
                
                # 2. Mismo establecimiento + fecha + productos similares
                elif (fac1["establecimiento"] == fac2["establecimiento"] and
                      fac1["fecha"] == fac2["fecha"] and
                      abs(fac1["num_productos"] - fac2["num_productos"]) <= 1):
                    es_duplicado = True
                    razon = "Mismo establecimiento, fecha y productos similares"
                
                # 3. Mismo establecimiento + total + fecha cercana (±1 día)
                elif (fac1["establecimiento"] == fac2["establecimiento"] and
                      abs(fac1["total"] - fac2["total"]) < 0.01 and
                      abs((fac1["fecha"] - fac2["fecha"]).days) <= 1):
                    es_duplicado = True
                    razon = "Mismo establecimiento, total y fecha cercana"
                
                # Aplicar filtro según criterio
                if criterio == "same_establishment":
                    if fac1["establecimiento"] != fac2["establecimiento"]:
                        continue
                elif criterio == "same_date":
                    if fac1["fecha"] != fac2["fecha"]:
                        continue
                elif criterio == "same_total":
                    if abs(fac1["total"] - fac2["total"]) >= 0.01:
                        continue
                
                # Si es duplicado, agregar a la lista
                if es_duplicado:
                    duplicados.append({
                        "id": f"facdup-{len(duplicados)}",
                        "factura1": fac1,
                        "factura2": fac2,
                        "razon": razon
                    })
                    
                    procesados.add(par_id)
        
        cursor.close()
        conn.close()
        
        print(f"✅ Encontrados {len(duplicados)} pares de facturas duplicadas")
        
        return {
            "success": True,
            "total": len(duplicados),
            "duplicados": duplicados
        }
        
    except Exception as e:
        print(f"❌ Error detectando duplicados de facturas: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/admin/facturas/{factura_id}")
async def eliminar_factura(factura_id: int):
    """
    Eliminar una factura y todos sus productos asociados
    ADVERTENCIA: Esta acción es permanente
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"\n🗑️ ELIMINANDO FACTURA #{factura_id}")
        
        # 1. Verificar que la factura existe
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        # 2. Eliminar productos asociados
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        productos_eliminados = cursor.rowcount
        print(f"   ✅ {productos_eliminados} productos eliminados")
        
        # 3. Eliminar factura
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        print(f"   ✅ Factura eliminada")
        
        # 4. Commit
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ ELIMINACIÓN COMPLETADA\n")
        
        return {
            "success": True,
            "message": "Factura eliminada exitosamente",
            "productos_eliminados": productos_eliminados
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error eliminando factura: {e}")
        print(traceback.format_exc())
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ENDPOINT ADICIONAL: Obtener imagen de factura
# ==========================================

@router.get("/admin/duplicados/facturas/{factura_id}/imagen")
async def obtener_imagen_factura_duplicados(factura_id: int):
    """
    Obtener imagen de una factura (para el comparador de duplicados)
    Redirige al endpoint principal de imágenes
    """
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"/admin/facturas/{factura_id}/imagen")

print("✅ duplicados_routes.py cargado correctamente")
