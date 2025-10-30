from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from database import get_db_connection
from auth import get_current_user

router = APIRouter()

# ==========================================
# MODELOS PYDANTIC
# ==========================================

class ProductoBase(BaseModel):
    codigo_ean: str
    nombre: str
    marca: Optional[str] = None
    categoria: Optional[str] = None

class ProductoCreate(ProductoBase):
    pass

class ProductoUpdate(ProductoBase):
    pass

# ==========================================
# ENDPOINTS (sin prefijo /api/admin/auditoria)
# ==========================================

@router.get("/verificar/{codigo_ean}")
async def verificar_producto(
    codigo_ean: str,
    current_user: dict = Depends(get_current_user)
):
    """Verificar si un producto existe por codigo EAN"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, codigo_ean, nombre, marca, categoria,
                   auditado_manualmente, validaciones_manuales
            FROM productos_maestros
            WHERE codigo_ean = %s
            LIMIT 1
        """, (codigo_ean,))

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            producto = {
                'id': row[0],
                'codigo_ean': row[1],
                'nombre': row[2],
                'marca': row[3],
                'categoria': row[4],
                'auditado_manualmente': row[5],
                'validaciones_manuales': row[6]
            }
            return {'existe': True, 'producto': producto}
        else:
            return {'existe': False, 'producto': None, 'mensaje': f'Producto con EAN {codigo_ean} no encontrado'}

    except Exception as e:
        print(f"Error en verificar_producto: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al verificar producto: {str(e)}")


@router.post("/producto")
async def crear_producto(
    producto: ProductoCreate,
    current_user: dict = Depends(get_current_user)
):
    """Crear un nuevo producto"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM productos_maestros WHERE codigo_ean = %s", (producto.codigo_ean,))

        if cursor.fetchone():
            cursor.close()
            conn.close()
            raise HTTPException(status_code=400, detail="El producto ya existe")

        cursor.execute("""
            INSERT INTO productos_maestros
            (codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales)
            VALUES (%s, %s, %s, %s, TRUE, 1)
            RETURNING id, codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales
        """, (producto.codigo_ean, producto.nombre, producto.marca, producto.categoria))

        row = cursor.fetchone()
        producto_id = row[0]

        cursor.execute("""
            INSERT INTO auditoria_productos
            (usuario_id, producto_maestro_id, accion, datos_nuevos, fecha)
            VALUES (%s, %s, 'crear', %s, %s)
        """, (current_user['id'], producto_id, f'{{"nombre": "{producto.nombre}"}}', datetime.now()))

        conn.commit()

        producto_dict = {
            'id': row[0],
            'codigo_ean': row[1],
            'nombre': row[2],
            'marca': row[3],
            'categoria': row[4],
            'auditado_manualmente': row[5],
            'validaciones_manuales': row[6]
        }

        cursor.close()
        conn.close()

        return {'mensaje': 'Producto creado exitosamente', 'producto': producto_dict}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en crear_producto: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al crear producto: {str(e)}")


@router.put("/producto/{producto_id}")
async def actualizar_producto(
    producto_id: int,
    producto: ProductoUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Actualizar un producto existente"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT nombre, marca, categoria FROM productos_maestros WHERE id = %s", (producto_id,))

        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        cursor.execute("""
            UPDATE productos_maestros
            SET nombre = %s, marca = %s, categoria = %s
            WHERE id = %s
            RETURNING id, codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales
        """, (producto.nombre, producto.marca, producto.categoria, producto_id))

        row = cursor.fetchone()

        cursor.execute("""
            INSERT INTO auditoria_productos
            (usuario_id, producto_maestro_id, accion, datos_nuevos, fecha)
            VALUES (%s, %s, 'actualizar', %s, %s)
        """, (current_user['id'], producto_id, f'{{"nombre": "{producto.nombre}"}}', datetime.now()))

        conn.commit()

        producto_dict = {
            'id': row[0],
            'codigo_ean': row[1],
            'nombre': row[2],
            'marca': row[3],
            'categoria': row[4],
            'auditado_manualmente': row[5],
            'validaciones_manuales': row[6]
        }

        cursor.close()
        conn.close()

        return {'mensaje': 'Producto actualizado exitosamente', 'producto': producto_dict}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en actualizar_producto: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al actualizar producto: {str(e)}")


@router.post("/validar/{producto_id}")
async def validar_producto(
    producto_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Marcar un producto como validado manualmente"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE productos_maestros
            SET auditado_manualmente = TRUE,
                validaciones_manuales = validaciones_manuales + 1,
                ultima_validacion = %s
            WHERE id = %s
        """, (datetime.now(), producto_id))

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        cursor.execute("""
            INSERT INTO auditoria_productos
            (usuario_id, producto_maestro_id, accion, fecha)
            VALUES (%s, %s, 'validar', %s)
        """, (current_user['id'], producto_id, datetime.now()))

        conn.commit()
        cursor.close()
        conn.close()

        return {'mensaje': 'Producto validado exitosamente'}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en validar_producto: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al validar producto: {str(e)}")


@router.get("/historial")
async def obtener_historial(
    current_user: dict = Depends(get_current_user),
    limit: int = 50
):
    """Obtener historial de auditorias del usuario"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT a.id, a.accion, a.fecha, p.id, p.codigo_ean, p.nombre
            FROM auditoria_productos a
            LEFT JOIN productos_maestros p ON a.producto_maestro_id = p.id
            WHERE a.usuario_id = %s
            ORDER BY a.fecha DESC
            LIMIT %s
        """, (current_user['id'], limit))

        rows = cursor.fetchall()

        historial = []
        for row in rows:
            historial.append({
                'id': row[0],
                'accion': row[1],
                'fecha': row[2].isoformat() if row[2] else None,
                'producto': {
                    'id': row[3],
                    'codigo_ean': row[4],
                    'nombre': row[5]
                } if row[3] else None
            })

        cursor.close()
        conn.close()

        return {'historial': historial}

    except Exception as e:
        print(f"Error en obtener_historial: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener historial: {str(e)}")


@router.get("/estadisticas")
async def obtener_estadisticas(current_user: dict = Depends(get_current_user)):
    """Obtener estadisticas de auditoria del usuario"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT accion, COUNT(*) as total
            FROM auditoria_productos
            WHERE usuario_id = %s
            GROUP BY accion
        """, (current_user['id'],))

        stats = {
            'productos_creados': 0,
            'productos_actualizados': 0,
            'productos_validados': 0,
            'total_acciones': 0
        }

        for row in cursor.fetchall():
            accion, total = row[0], row[1]
            if accion == 'crear':
                stats['productos_creados'] = total
            elif accion == 'actualizar':
                stats['productos_actualizados'] = total
            elif accion == 'validar':
                stats['productos_validados'] = total
            stats['total_acciones'] += total

        cursor.execute("""
            SELECT accion, producto_maestro_id, fecha
            FROM auditoria_productos
            WHERE usuario_id = %s
            ORDER BY fecha DESC
            LIMIT 10
        """, (current_user['id'],))

        ultimas_acciones = []
        for row in cursor.fetchall():
            ultimas_acciones.append({
                'accion': row[0],
                'producto_maestro_id': row[1],
                'fecha': row[2].isoformat() if row[2] else None
            })

        stats['ultimas_acciones'] = ultimas_acciones

        cursor.close()
        conn.close()

        return stats

    except Exception as e:
        print(f"Error en obtener_estadisticas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener estadisticas: {str(e)}")


print("API Auditoria Productos cargada")
#   U p d a t e d   a t   1 0 / 3 0 / 2 0 2 5   1 4 : 4 8 : 1 1  
 