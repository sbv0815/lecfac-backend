"""
duplicados_routes.py - Sistema de Detección de Duplicados
CORRECCIÓN: establecimiento está en facturas, NO en productos
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from difflib import SequenceMatcher
from datetime import datetime
import traceback
import re

from database import get_db_connection

router = APIRouter()

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
    
    t1 = normalizar_nombre_producto(texto1)
    t2 = normalizar_nombre_producto(texto2)
    
    return SequenceMatcher(None, t1, t2).ratio() * 100

def normalizar_nombre_producto(nombre: str) -> str:
    """Normaliza nombre de producto para comparación"""
    if not nombre:
        return ""
    
    nombre = nombre.lower().strip()
    
    # Eliminar acentos
    reemplazos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ñ': 'n'
    }
    for old, new in reemplazos.items():
        nombre = nombre.replace(old, new)
    
    # Normalizar unidades
    nombre = re.sub(r'\b(\d+)\s*ml\b', r'\1ml', nombre)
    nombre = re.sub(r'\b(\d+)\s*gr?\b', r'\1g', nombre)
    nombre = re.sub(r'\b(\d+)\s*kg\b', r'\1kg', nombre)
    nombre = re.sub(r'\b(\d+)\s*lt?\b', r'\1l', nombre)
    
    # Convertir equivalencias
    nombre = re.sub(r'\b1\s*l\b', '1000ml', nombre)
    nombre = re.sub(r'\b1\s*lt\b', '1000ml', nombre)
    nombre = re.sub(r'\b1\s*kg\b', '1000g', nombre)
    
    # Limpiar caracteres especiales
    nombre = re.sub(r'[.,\-_/\\]', ' ', nombre)
    nombre = ' '.join(nombre.split())
    
    return nombre

def son_precios_similares(precio1: float, precio2: float, tolerancia: float = 0.05) -> bool:
    """Verifica si dos precios son similares (dentro del % de tolerancia)"""
    if precio1 == 0 or precio2 == 0:
        return False
    
    mayor = max(precio1, precio2)
    menor = min(precio1, precio2)
    diferencia_porcentual = (mayor - menor) / mayor
    
    return diferencia_porcentual <= tolerancia

# ==========================================
# ENDPOINT DE DEBUG
# ==========================================

@router.get("/admin/duplicados/productos/debug")
async def debug_productos():
    """Endpoint de prueba para ver estructura de datos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print("🔍 DEBUG: Verificando estructura de tablas...")
        
        # ✅ CORRECCIÓN: establecimiento está en facturas
        cursor.execute("""
            SELECT 
                p.id,
                p.nombre,
                p.codigo,
                f.establecimiento,
                p.valor,
                f.fecha_cargue
            FROM productos p
            LEFT JOIN facturas f ON p.factura_id = f.id
            WHERE p.nombre IS NOT NULL 
              AND p.nombre != ''
              AND f.establecimiento IS NOT NULL
            ORDER BY f.establecimiento, p.nombre
            LIMIT 100
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "nombre": row[1],
                "codigo": row[2] or "",
                "establecimiento": row[3],
                "precio": float(row[4]) if row[4] else 0,
                "fecha": row[5].isoformat() if row[5] else None
            })
        
        cursor.close()
        conn.close()
        
        # Agrupar por establecimiento
        por_establecimiento = {}
        for p in productos:
            est = p["establecimiento"]
            if est not in por_establecimiento:
                por_establecimiento[est] = []
            por_establecimiento[est].append(p)
        
        print(f"✅ DEBUG: {len(productos)} productos obtenidos")
        print(f"📊 Establecimientos únicos: {len(por_establecimiento)}")
        
        return {
            "success": True,
            "total_productos": len(productos),
            "productos_muestra": productos[:20],
            "por_establecimiento": {k: len(v) for k, v in por_establecimiento.items()},
            "message": "Debug exitoso - establecimiento viene de facturas"
        }
        
    except Exception as e:
        print(f"❌ Error en debug: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ENDPOINT PRINCIPAL: DETECTAR DUPLICADOS
# ==========================================

@router.get("/admin/duplicados/productos")
async def detectar_productos_duplicados(
    umbral: float = Query(90.0, ge=0, le=100, description="Umbral de similitud (%)"),
    criterio: str = Query("todos", description="Criterio de detección")
):
    """Detectar productos duplicados - VERSIÓN CORREGIDA"""
    
    conn = None
    cursor = None
    
    try:
        print(f"\n{'='*60}")
        print(f"🔍 DETECTANDO PRODUCTOS DUPLICADOS")
        print(f"   Umbral: {umbral}%")
        print(f"   Criterio: {criterio}")
        print(f"{'='*60}\n")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print("📊 Ejecutando query SQL...")
        
        # ✅ CORRECCIÓN: establecimiento desde facturas
        cursor.execute("""
            SELECT 
                p.id,
                p.nombre,
                p.codigo,
                f.establecimiento,
                p.valor,
                f.fecha_cargue
            FROM productos p
            INNER JOIN facturas f ON p.factura_id = f.id
            WHERE p.nombre IS NOT NULL 
              AND p.nombre != ''
              AND f.establecimiento IS NOT NULL
              AND f.establecimiento != ''
            ORDER BY f.establecimiento, p.nombre
            LIMIT 500
        """)
        
        productos_raw = cursor.fetchall()
        print(f"✅ Query ejecutado: {len(productos_raw)} productos obtenidos")
        
        cursor.close()
        conn.close()
        conn = None
        cursor = None
        
        if len(productos_raw) == 0:
            print("⚠️ No hay productos para analizar")
            return {
                "success": True,
                "total": 0,
                "duplicados": [],
                "mensaje": "No hay productos con establecimiento para analizar"
            }
        
        # Convertir a diccionarios
        productos = []
        for row in productos_raw:
            productos.append({
                "id": row[0],
                "nombre": row[1] or "",
                "codigo": row[2] or "",
                "establecimiento": row[3] or "",
                "precio": float(row[4]) if row[4] else 0.0,
                "fecha_cargue": row[5].isoformat() if row[5] else None,
                "ultima_actualizacion": row[5].isoformat() if row[5] else None,
                "veces_visto": 1
            })
        
        print(f"✅ Productos convertidos: {len(productos)}")
        print(f"\n📦 MUESTRA (primeros 3):")
        for i, p in enumerate(productos[:3]):
            print(f"   {i+1}. ID={p['id']} | {p['nombre'][:40]} | {p['establecimiento']}")
        print()
        
        # Detectar duplicados
        print("🔍 Buscando duplicados...")
        duplicados = []
        procesados = set()
        
        for i, prod1 in enumerate(productos):
            for j, prod2 in enumerate(productos[i+1:], start=i+1):
                if prod1["id"] == prod2["id"]:
                    continue
                
                # ⚠️ CRÍTICO: SOLO comparar si es el MISMO establecimiento
                if prod1["establecimiento"] != prod2["establecimiento"]:
                    continue
                
                par_id = tuple(sorted([prod1["id"], prod2["id"]]))
                if par_id in procesados:
                    continue
                
                es_duplicado = False
                razones = []
                similitud = 0
                
                # 1. Mismo código EAN
                if prod1["codigo"] and prod2["codigo"] and len(prod1["codigo"]) >= 8:
                    if prod1["codigo"] == prod2["codigo"]:
                        es_duplicado = True
                        razones.append("Mismo código EAN")
                        similitud = 100
                
                # 2. Nombre muy similar
                if not es_duplicado:
                    similitud = calcular_similitud(prod1["nombre"], prod2["nombre"])
                    
                    if similitud >= umbral:
                        razones.append(f"Nombre {similitud:.0f}% similar")
                        
                        # Verificar precio también
                        if son_precios_similares(prod1["precio"], prod2["precio"], 0.05):
                            es_duplicado = True
                            razones.append("Precio similar (±5%)")
                
                if es_duplicado and razones:
                    duplicados.append({
                        "id": f"dup-{len(duplicados)}",
                        "producto1": prod1,
                        "producto2": prod2,
                        "similitud": round(similitud, 1),
                        "mismo_codigo": prod1.get("codigo") == prod2.get("codigo") if prod1.get("codigo") and prod2.get("codigo") else False,
                        "mismo_establecimiento": True,
                        "nombre_similar": similitud >= umbral,
                        "razon": " + ".join(razones),
                        "seleccionado": prod2["id"]
                    })
                    
                    procesados.add(par_id)
                    
                    if len(duplicados) == 1:
                        print(f"   ✅ Primer duplicado encontrado:")
                        print(f"      - {prod1['nombre'][:40]}")
                        print(f"      - {prod2['nombre'][:40]}")
                        print(f"      Razón: {' + '.join(razones)}")
        
        print(f"\n{'='*60}")
        print(f"✅ RESULTADO: {len(duplicados)} pares de duplicados encontrados")
        print(f"{'='*60}\n")
        
        return {
            "success": True,
            "total": len(duplicados),
            "duplicados": duplicados,
            "debug_info": {
                "productos_analizados": len(productos),
                "umbral_usado": umbral,
                "criterio": criterio,
                "establecimientos_unicos": len(set(p["establecimiento"] for p in productos))
            }
        }
        
    except Exception as e:
        print(f"\n❌ ERROR EN DETECCIÓN DE DUPLICADOS:")
        print(f"   Tipo: {type(e).__name__}")
        print(f"   Mensaje: {str(e)}")
        print(f"\n📋 TRACEBACK COMPLETO:")
        traceback.print_exc()
        print(f"\n{'='*60}\n")
        
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass
        
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ==========================================
# FUSIONAR PRODUCTOS
# ==========================================

@router.post("/admin/duplicados/productos/fusionar")
async def fusionar_productos(request: FusionProductosRequest):
    """Eliminar producto duplicado"""
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"\n🗑️ ELIMINANDO PRODUCTO DUPLICADO")
        print(f"   Mantener: #{request.producto_mantener_id}")
        print(f"   Eliminar: #{request.producto_eliminar_id}")
        
        # Verificar que ambos productos existen
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
        
        # Eliminar el producto duplicado
        cursor.execute("""
            DELETE FROM productos WHERE id = %s
        """, (request.producto_eliminar_id,))
        
        print(f"   ✅ Producto eliminado")
        
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
# FACTURAS DUPLICADAS
# ==========================================

@router.get("/admin/duplicados/facturas")
async def detectar_facturas_duplicadas(
    criterio: str = Query("all", description="Criterio de detección")
):
    """Detectar facturas duplicadas"""
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
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
                
                # Criterios de duplicación
                if (fac1["establecimiento"] == fac2["establecimiento"] and
                    fac1["fecha"] == fac2["fecha"] and
                    abs(fac1["total"] - fac2["total"]) < 0.01):
                    es_duplicado = True
                    razon = "Misma fecha, establecimiento y total"
                
                elif (fac1["establecimiento"] == fac2["establecimiento"] and
                      fac1["fecha"] == fac2["fecha"] and
                      abs(fac1["num_productos"] - fac2["num_productos"]) <= 2):
                    es_duplicado = True
                    razon = "Misma fecha, establecimiento y productos similares"
                
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
                
                # Filtros por criterio
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
    """Eliminar una factura y todos sus productos"""
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"\n🗑️ ELIMINANDO FACTURA #{factura_id}")
        
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        productos_eliminados = cursor.rowcount
        
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

print("✅ duplicados_routes.py cargado correctamente")
