"""
api_listas_compras.py - Gestión de listas de compras guardadas
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from database import get_db_connection
import os

router = APIRouter(prefix="/api/listas", tags=["listas_compras"])


# ========================================
# MODELOS
# ========================================
class ItemLista(BaseModel):
    producto_busqueda: str
    producto_maestro_id: Optional[int] = None
    producto_nombre: Optional[str] = None
    producto_marca: Optional[str] = None
    establecimiento_id: Optional[int] = None
    establecimiento_nombre: Optional[str] = None
    precio_seleccionado: Optional[float] = None
    rating: Optional[float] = 0


class CrearListaRequest(BaseModel):
    usuario_id: int
    nombre: Optional[str] = "Mi lista"
    items: List[ItemLista]
    tienda_recomendada: Optional[str] = None
    tienda_recomendada_id: Optional[int] = None
    total_estimado: Optional[float] = 0
    notas: Optional[str] = None


class MarcarCompradoRequest(BaseModel):
    usuario_id: int
    comprado: bool = True


# ========================================
# 1. CREAR/GUARDAR LISTA
# ========================================
@router.post("/guardar")
async def guardar_lista(data: CrearListaRequest):
    """
    POST /api/listas/guardar
    Guarda una nueva lista de compras
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Insertar lista principal
        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO listas_compras 
                (usuario_id, nombre, total_productos, total_estimado, 
                 tienda_recomendada, tienda_recomendada_id, notas)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.usuario_id,
                data.nombre,
                len(data.items),
                data.total_estimado,
                data.tienda_recomendada,
                data.tienda_recomendada_id,
                data.notas
            ))
            lista_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO listas_compras 
                (usuario_id, nombre, total_productos, total_estimado, 
                 tienda_recomendada, tienda_recomendada_id, notas)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                data.usuario_id,
                data.nombre,
                len(data.items),
                data.total_estimado,
                data.tienda_recomendada,
                data.tienda_recomendada_id,
                data.notas
            ))
            lista_id = cursor.lastrowid
        
        # Insertar items
        for orden, item in enumerate(data.items):
            if database_type == "postgresql":
                cursor.execute("""
                    INSERT INTO items_lista_compras
                    (lista_id, producto_busqueda, producto_maestro_id, producto_nombre,
                     producto_marca, establecimiento_id, establecimiento_nombre,
                     precio_seleccionado, rating, orden)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    lista_id,
                    item.producto_busqueda,
                    item.producto_maestro_id,
                    item.producto_nombre,
                    item.producto_marca,
                    item.establecimiento_id,
                    item.establecimiento_nombre,
                    item.precio_seleccionado,
                    item.rating,
                    orden
                ))
            else:
                cursor.execute("""
                    INSERT INTO items_lista_compras
                    (lista_id, producto_busqueda, producto_maestro_id, producto_nombre,
                     producto_marca, establecimiento_id, establecimiento_nombre,
                     precio_seleccionado, rating, orden)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    lista_id,
                    item.producto_busqueda,
                    item.producto_maestro_id,
                    item.producto_nombre,
                    item.producto_marca,
                    item.establecimiento_id,
                    item.establecimiento_nombre,
                    item.precio_seleccionado,
                    item.rating,
                    orden
                ))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Lista guardada: ID {lista_id} con {len(data.items)} productos")
        
        return {
            "success": True,
            "lista_id": lista_id,
            "mensaje": f"Lista '{data.nombre}' guardada con {len(data.items)} productos"
        }
        
    except Exception as e:
        conn.rollback()
        conn.close()
        print(f"❌ Error guardando lista: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 2. OBTENER LISTAS DEL USUARIO
# ========================================
@router.get("/usuario/{usuario_id}")
async def obtener_listas_usuario(usuario_id: int, estado: str = "activa"):
    """
    GET /api/listas/usuario/{usuario_id}
    Obtiene todas las listas de un usuario
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        if database_type == "postgresql":
            cursor.execute("""
                SELECT 
                    id, nombre, fecha_creacion, total_productos, 
                    total_estimado, tienda_recomendada, estado, completada
                FROM listas_compras
                WHERE usuario_id = %s AND estado = %s
                ORDER BY fecha_creacion DESC
            """, (usuario_id, estado))
        else:
            cursor.execute("""
                SELECT 
                    id, nombre, fecha_creacion, total_productos, 
                    total_estimado, tienda_recomendada, estado, completada
                FROM listas_compras
                WHERE usuario_id = ? AND estado = ?
                ORDER BY fecha_creacion DESC
            """, (usuario_id, estado))
        
        listas = []
        for row in cursor.fetchall():
            # Contar items comprados
            lista_id = row[0]
            if database_type == "postgresql":
                cursor.execute("""
                    SELECT COUNT(*) FROM items_lista_compras 
                    WHERE lista_id = %s AND comprado = TRUE
                """, (lista_id,))
            else:
                cursor.execute("""
                    SELECT COUNT(*) FROM items_lista_compras 
                    WHERE lista_id = ? AND comprado = 1
                """, (lista_id,))
            
            comprados = cursor.fetchone()[0]
            
            listas.append({
                "id": row[0],
                "nombre": row[1],
                "fecha_creacion": str(row[2]) if row[2] else None,
                "total_productos": row[3],
                "total_estimado": float(row[4]) if row[4] else 0,
                "tienda_recomendada": row[5],
                "estado": row[6],
                "completada": bool(row[7]),
                "productos_comprados": comprados,
                "progreso": round(comprados / row[3] * 100, 0) if row[3] > 0 else 0
            })
        
        conn.close()
        
        return {
            "success": True,
            "listas": listas,
            "total": len(listas)
        }
        
    except Exception as e:
        conn.close()
        print(f"❌ Error obteniendo listas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 3. OBTENER DETALLE DE UNA LISTA
# ========================================
@router.get("/{lista_id}")
async def obtener_lista_detalle(lista_id: int):
    """
    GET /api/listas/{lista_id}
    Obtiene el detalle de una lista con todos sus items
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Obtener datos de la lista
        if database_type == "postgresql":
            cursor.execute("""
                SELECT 
                    id, usuario_id, nombre, fecha_creacion, total_productos,
                    total_estimado, tienda_recomendada, tienda_recomendada_id,
                    estado, completada, notas
                FROM listas_compras
                WHERE id = %s
            """, (lista_id,))
        else:
            cursor.execute("""
                SELECT 
                    id, usuario_id, nombre, fecha_creacion, total_productos,
                    total_estimado, tienda_recomendada, tienda_recomendada_id,
                    estado, completada, notas
                FROM listas_compras
                WHERE id = ?
            """, (lista_id,))
        
        lista_row = cursor.fetchone()
        if not lista_row:
            conn.close()
            raise HTTPException(status_code=404, detail="Lista no encontrada")
        
        # Obtener items
        if database_type == "postgresql":
            cursor.execute("""
                SELECT 
                    id, producto_busqueda, producto_maestro_id, producto_nombre,
                    producto_marca, establecimiento_id, establecimiento_nombre,
                    precio_seleccionado, rating, comprado, orden
                FROM items_lista_compras
                WHERE lista_id = %s
                ORDER BY orden
            """, (lista_id,))
        else:
            cursor.execute("""
                SELECT 
                    id, producto_busqueda, producto_maestro_id, producto_nombre,
                    producto_marca, establecimiento_id, establecimiento_nombre,
                    precio_seleccionado, rating, comprado, orden
                FROM items_lista_compras
                WHERE lista_id = ?
                ORDER BY orden
            """, (lista_id,))
        
        items = []
        for row in cursor.fetchall():
            items.append({
                "id": row[0],
                "producto_busqueda": row[1],
                "producto_maestro_id": row[2],
                "producto_nombre": row[3],
                "producto_marca": row[4],
                "establecimiento_id": row[5],
                "establecimiento_nombre": row[6],
                "precio": float(row[7]) if row[7] else 0,
                "precio_formateado": f"${row[7]:,.0f}".replace(",", ".") if row[7] else "$0",
                "rating": float(row[8]) if row[8] else 0,
                "comprado": bool(row[9]),
                "orden": row[10]
            })
        
        conn.close()
        
        # Calcular resumen
        total_items = len(items)
        comprados = len([i for i in items if i["comprado"]])
        
        return {
            "success": True,
            "lista": {
                "id": lista_row[0],
                "usuario_id": lista_row[1],
                "nombre": lista_row[2],
                "fecha_creacion": str(lista_row[3]) if lista_row[3] else None,
                "total_productos": lista_row[4],
                "total_estimado": float(lista_row[5]) if lista_row[5] else 0,
                "total_formateado": f"${lista_row[5]:,.0f}".replace(",", ".") if lista_row[5] else "$0",
                "tienda_recomendada": lista_row[6],
                "tienda_recomendada_id": lista_row[7],
                "estado": lista_row[8],
                "completada": bool(lista_row[9]),
                "notas": lista_row[10]
            },
            "items": items,
            "resumen": {
                "total_items": total_items,
                "comprados": comprados,
                "pendientes": total_items - comprados,
                "progreso": round(comprados / total_items * 100, 0) if total_items > 0 else 0
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.close()
        print(f"❌ Error obteniendo lista: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 4. MARCAR ITEM COMO COMPRADO
# ========================================
@router.put("/item/{item_id}/comprado")
async def marcar_item_comprado(item_id: int, data: MarcarCompradoRequest):
    """
    PUT /api/listas/item/{item_id}/comprado
    Marca o desmarca un item como comprado
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Verificar que el item existe y pertenece al usuario
        if database_type == "postgresql":
            cursor.execute("""
                SELECT ilc.id, lc.usuario_id
                FROM items_lista_compras ilc
                JOIN listas_compras lc ON ilc.lista_id = lc.id
                WHERE ilc.id = %s
            """, (item_id,))
        else:
            cursor.execute("""
                SELECT ilc.id, lc.usuario_id
                FROM items_lista_compras ilc
                JOIN listas_compras lc ON ilc.lista_id = lc.id
                WHERE ilc.id = ?
            """, (item_id,))
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            raise HTTPException(status_code=404, detail="Item no encontrado")
        
        if result[1] != data.usuario_id:
            conn.close()
            raise HTTPException(status_code=403, detail="No autorizado")
        
        # Actualizar item
        if database_type == "postgresql":
            cursor.execute("""
                UPDATE items_lista_compras
                SET comprado = %s,
                    fecha_comprado = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE NULL END
                WHERE id = %s
            """, (data.comprado, data.comprado, item_id))
        else:
            cursor.execute("""
                UPDATE items_lista_compras
                SET comprado = ?,
                    fecha_comprado = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END
                WHERE id = ?
            """, (data.comprado, data.comprado, item_id))
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "mensaje": "Item marcado como comprado" if data.comprado else "Item desmarcado"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 5. ELIMINAR LISTA
# ========================================
@router.delete("/{lista_id}")
async def eliminar_lista(lista_id: int, usuario_id: int):
    """
    DELETE /api/listas/{lista_id}?usuario_id=X
    Elimina (archiva) una lista
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Verificar propiedad
        if database_type == "postgresql":
            cursor.execute(
                "SELECT usuario_id FROM listas_compras WHERE id = %s",
                (lista_id,)
            )
        else:
            cursor.execute(
                "SELECT usuario_id FROM listas_compras WHERE id = ?",
                (lista_id,)
            )
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            raise HTTPException(status_code=404, detail="Lista no encontrada")
        
        if result[0] != usuario_id:
            conn.close()
            raise HTTPException(status_code=403, detail="No autorizado")
        
        # Archivar lista (no eliminar físicamente)
        if database_type == "postgresql":
            cursor.execute("""
                UPDATE listas_compras
                SET estado = 'archivada'
                WHERE id = %s
            """, (lista_id,))
        else:
            cursor.execute("""
                UPDATE listas_compras
                SET estado = 'archivada'
                WHERE id = ?
            """, (lista_id,))
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "mensaje": "Lista archivada correctamente"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API Listas de Compras cargada")
