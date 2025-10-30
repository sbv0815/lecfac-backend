from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from database import get_db_connection
from auth import get_current_user

router = APIRouter()

class ProductoBase(BaseModel):
    codigo_ean: str
    nombre: str
    marca: Optional[str] = None
    categoria: Optional[str] = None

class ProductoCreate(ProductoBase):
    pass

class ProductoUpdate(ProductoBase):
    pass

@router.get("/verificar/{codigo_ean}")
async def verificar_producto(codigo_ean: str, current_user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales FROM productos_maestros WHERE codigo_ean = %s LIMIT 1", (codigo_ean,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return {'existe': True, 'producto': {'id': row[0], 'codigo_ean': row[1], 'nombre': row[2], 'marca': row[3], 'categoria': row[4], 'auditado_manualmente': row[5], 'validaciones_manuales': row[6]}}
        else:
            return {'existe': False, 'producto': None, 'mensaje': 'Producto no encontrado'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/producto")
async def crear_producto(producto: ProductoCreate, current_user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM productos_maestros WHERE codigo_ean = %s", (producto.codigo_ean,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            raise HTTPException(status_code=400, detail="Producto ya existe")
        cursor.execute("INSERT INTO productos_maestros (codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales) VALUES (%s, %s, %s, %s, TRUE, 1) RETURNING id, codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales", (producto.codigo_ean, producto.nombre, producto.marca, producto.categoria))
        row = cursor.fetchone()
        cursor.execute("INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha) VALUES (%s, %s, 'crear', %s)", (current_user['id'], row[0], datetime.now()))
        conn.commit()
        cursor.close()
        conn.close()
        return {'mensaje': 'Producto creado', 'producto': {'id': row[0], 'codigo_ean': row[1], 'nombre': row[2], 'marca': row[3], 'categoria': row[4]}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/producto/{producto_id}")
async def actualizar_producto(producto_id: int, producto: ProductoUpdate, current_user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE productos_maestros SET nombre = %s, marca = %s, categoria = %s WHERE id = %s RETURNING id, codigo_ean, nombre, marca, categoria", (producto.nombre, producto.marca, producto.categoria, producto_id))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No encontrado")
        cursor.execute("INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha) VALUES (%s, %s, 'actualizar', %s)", (current_user['id'], producto_id, datetime.now()))
        conn.commit()
        cursor.close()
        conn.close()
        return {'mensaje': 'Actualizado', 'producto': {'id': row[0], 'codigo_ean': row[1], 'nombre': row[2], 'marca': row[3], 'categoria': row[4]}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/validar/{producto_id}")
async def validar_producto(producto_id: int, current_user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE productos_maestros SET auditado_manualmente = TRUE, validaciones_manuales = validaciones_manuales + 1, ultima_validacion = %s WHERE id = %s", (datetime.now(), producto_id))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="No encontrado")
        cursor.execute("INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha) VALUES (%s, %s, 'validar', %s)", (current_user['id'], producto_id, datetime.now()))
        conn.commit()
        cursor.close()
        conn.close()
        return {'mensaje': 'Validado'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/estadisticas")
async def obtener_estadisticas(current_user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT accion, COUNT(*) FROM auditoria_productos WHERE usuario_id = %s GROUP BY accion", (current_user['id'],))
        stats = {'productos_creados': 0, 'productos_actualizados': 0, 'productos_validados': 0, 'total_acciones': 0}
        for row in cursor.fetchall():
            if row[0] == 'crear': stats['productos_creados'] = row[1]
            elif row[0] == 'actualizar': stats['productos_actualizados'] = row[1]
            elif row[0] == 'validar': stats['productos_validados'] = row[1]
            stats['total_acciones'] += row[1]
        cursor.execute("SELECT accion, producto_maestro_id, fecha FROM auditoria_productos WHERE usuario_id = %s ORDER BY fecha DESC LIMIT 10", (current_user['id'],))
        stats['ultimas_acciones'] = [{'accion': r[0], 'producto_maestro_id': r[1], 'fecha': r[2].isoformat() if r[2] else None} for r in cursor.fetchall()]
        cursor.close()
        conn.close()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

print("API Auditoria cargada")
