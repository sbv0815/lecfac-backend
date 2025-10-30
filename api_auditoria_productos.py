"""
API DE AUDITORÍA DE PRODUCTOS
==============================
Endpoints para app móvil de validación y corrección de productos

Autor: Santiago
Fecha: 2025-10-30
Sistema: LecFac
"""

from fastapi import APIRouter, HTTPException, Depends, File, UploadFile
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import os
import base64

from database import get_db_connection
from auth import get_current_user

router = APIRouter(prefix="/api/admin/auditoria", tags=["Auditoría Productos"])


# ============================================================================
# MODELOS PYDANTIC
# ============================================================================

class ProductoVerificacion(BaseModel):
    """Respuesta de verificación de producto"""
    existe: bool
    producto_id: Optional[int] = None
    codigo_ean: str
    nombre_normalizado: Optional[str] = None
    marca: Optional[str] = None
    categoria: Optional[str] = None
    subcategoria: Optional[str] = None
    precio_promedio: Optional[float] = None
    total_reportes: Optional[int] = None
    imagen_url: Optional[str] = None
    ultima_actualizacion: Optional[datetime] = None

    # Indicadores de calidad
    tiene_marca: bool = False
    tiene_categoria: bool = False
    requiere_revision: bool = False
    razon_revision: Optional[str] = None


class ProductoCrear(BaseModel):
    """Datos para crear un nuevo producto"""
    codigo_ean: str = Field(..., min_length=8, max_length=13)
    nombre_normalizado: str = Field(..., min_length=3, max_length=500)
    marca: Optional[str] = Field(None, max_length=100)
    categoria: str = Field(..., max_length=50)
    subcategoria: Optional[str] = Field(None, max_length=50)
    contenido: Optional[str] = Field(None, max_length=100)
    notas: Optional[str] = Field(None, max_length=500)


class ProductoActualizar(BaseModel):
    """Datos para actualizar un producto existente"""
    nombre_normalizado: Optional[str] = Field(None, min_length=3, max_length=500)
    marca: Optional[str] = Field(None, max_length=100)
    categoria: Optional[str] = Field(None, max_length=50)
    subcategoria: Optional[str] = Field(None, max_length=50)
    contenido: Optional[str] = Field(None, max_length=100)
    razon_cambio: str = Field(..., min_length=3, max_length=500)


class AuditoriaLog(BaseModel):
    """Log de auditoría"""
    id: int
    usuario_id: int
    producto_maestro_id: int
    accion: str  # "validar", "crear", "actualizar"
    cambios: dict
    fecha: datetime


# ============================================================================
# ENDPOINT 1: VERIFICAR PRODUCTO POR CÓDIGO
# ============================================================================

@router.get("/verificar/{codigo_ean}", response_model=ProductoVerificacion)
async def verificar_producto(
    codigo_ean: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Verifica si un producto existe en la BD y retorna sus datos.

    Evalúa la calidad de los datos y determina si requiere revisión.
    """

    # Validar formato de código
    if not codigo_ean.isdigit():
        raise HTTPException(status_code=400, detail="Código EAN debe ser numérico")

    if len(codigo_ean) < 8 or len(codigo_ean) > 13:
        raise HTTPException(
            status_code=400,
            detail="Código EAN debe tener entre 8 y 13 dígitos"
        )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Buscar producto
        cursor.execute("""
            SELECT
                id,
                codigo_ean,
                nombre_normalizado,
                marca,
                categoria,
                subcategoria,
                precio_promedio_global,
                total_reportes,
                imagen_url,
                ultima_actualizacion
            FROM productos_maestros
            WHERE codigo_ean = %s
            LIMIT 1
        """, (codigo_ean,))

        resultado = cursor.fetchone()

        # Producto NO existe
        if not resultado:
            return ProductoVerificacion(
                existe=False,
                codigo_ean=codigo_ean,
                requiere_revision=True,
                razon_revision="Producto no existe en la base de datos"
            )

        # Producto existe - analizar calidad
        (prod_id, ean, nombre, marca, categoria, subcategoria,
         precio, reportes, imagen, fecha_act) = resultado

        tiene_marca = marca is not None and marca.strip() != ""
        tiene_categoria = categoria is not None and categoria.strip() != ""

        # Determinar si requiere revisión
        requiere_revision = False
        razon_revision = None

        # Nombre muy corto o sospechoso
        if len(nombre or "") < 5:
            requiere_revision = True
            razon_revision = "Nombre muy corto"

        # Sin marca
        elif not tiene_marca:
            requiere_revision = True
            razon_revision = "Sin marca asignada"

        # Sin categoría
        elif not tiene_categoria:
            requiere_revision = True
            razon_revision = "Sin categoría asignada"

        # Nombre con caracteres raros (posible error OCR)
        elif any(char in (nombre or "") for char in ['|', '_', '~', '{', '}']):
            requiere_revision = True
            razon_revision = "Nombre con caracteres inusuales"

        cursor.close()
        conn.close()

        return ProductoVerificacion(
            existe=True,
            producto_id=prod_id,
            codigo_ean=ean,
            nombre_normalizado=nombre,
            marca=marca,
            categoria=categoria,
            subcategoria=subcategoria,
            precio_promedio=precio,
            total_reportes=reportes,
            imagen_url=imagen,
            ultima_actualizacion=fecha_act,
            tiene_marca=tiene_marca,
            tiene_categoria=tiene_categoria,
            requiere_revision=requiere_revision,
            razon_revision=razon_revision
        )

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# ENDPOINT 2: CREAR NUEVO PRODUCTO
# ============================================================================

@router.post("/producto", response_model=dict)
async def crear_producto(
    producto: ProductoCrear,
    current_user: dict = Depends(get_current_user)
):
    """
    Crea un nuevo producto en productos_maestros.

    Registra la auditoría en tabla auditoria_productos.
    """

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Verificar que el producto NO exista ya
        cursor.execute("""
            SELECT id FROM productos_maestros
            WHERE codigo_ean = %s
        """, (producto.codigo_ean,))

        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail=f"Producto con EAN {producto.codigo_ean} ya existe"
            )

        # Crear producto
        cursor.execute("""
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                nombre_comercial,
                marca,
                categoria,
                subcategoria,
                contenido,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion,
                auditado_manualmente
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE)
            RETURNING id
        """, (
            producto.codigo_ean,
            producto.nombre_normalizado,
            producto.nombre_normalizado,
            producto.marca,
            producto.categoria,
            producto.subcategoria,
            producto.contenido
        ))

        producto_id = cursor.fetchone()[0]

        # Registrar auditoría
        cursor.execute("""
            INSERT INTO auditoria_productos (
                usuario_id,
                producto_maestro_id,
                accion,
                datos_anteriores,
                datos_nuevos,
                razon,
                fecha
            ) VALUES (%s, %s, 'crear', %s, %s, %s, CURRENT_TIMESTAMP)
        """, (
            current_user['user_id'],
            producto_id,
            None,  # No hay datos anteriores
            producto.dict(),
            producto.notas or "Creación manual vía app de auditoría"
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": "Producto creado exitosamente",
            "producto_id": producto_id,
            "codigo_ean": producto.codigo_ean
        }

    except HTTPException:
        if conn:
            conn.rollback()
            conn.close()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# ENDPOINT 3: ACTUALIZAR PRODUCTO EXISTENTE
# ============================================================================

@router.put("/producto/{producto_id}", response_model=dict)
async def actualizar_producto(
    producto_id: int,
    actualizacion: ProductoActualizar,
    current_user: dict = Depends(get_current_user)
):
    """
    Actualiza un producto existente.

    Registra los cambios en auditoria_productos.
    """

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Obtener datos actuales
        cursor.execute("""
            SELECT
                nombre_normalizado,
                marca,
                categoria,
                subcategoria,
                contenido
            FROM productos_maestros
            WHERE id = %s
        """, (producto_id,))

        resultado = cursor.fetchone()
        if not resultado:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        datos_anteriores = {
            "nombre_normalizado": resultado[0],
            "marca": resultado[1],
            "categoria": resultado[2],
            "subcategoria": resultado[3],
            "contenido": resultado[4]
        }

        # Construir query de actualización dinámico
        campos_actualizar = []
        valores = []

        if actualizacion.nombre_normalizado:
            campos_actualizar.append("nombre_normalizado = %s")
            campos_actualizar.append("nombre_comercial = %s")
            valores.extend([actualizacion.nombre_normalizado, actualizacion.nombre_normalizado])

        if actualizacion.marca is not None:
            campos_actualizar.append("marca = %s")
            valores.append(actualizacion.marca)

        if actualizacion.categoria:
            campos_actualizar.append("categoria = %s")
            valores.append(actualizacion.categoria)

        if actualizacion.subcategoria is not None:
            campos_actualizar.append("subcategoria = %s")
            valores.append(actualizacion.subcategoria)

        if actualizacion.contenido is not None:
            campos_actualizar.append("contenido = %s")
            valores.append(actualizacion.contenido)

        if not campos_actualizar:
            raise HTTPException(
                status_code=400,
                detail="No hay campos para actualizar"
            )

        # Agregar campos de auditoría
        campos_actualizar.append("ultima_actualizacion = CURRENT_TIMESTAMP")
        campos_actualizar.append("auditado_manualmente = TRUE")
        valores.append(producto_id)

        # Ejecutar actualización
        query = f"""
            UPDATE productos_maestros
            SET {', '.join(campos_actualizar)}
            WHERE id = %s
        """

        cursor.execute(query, valores)

        # Registrar auditoría
        cursor.execute("""
            INSERT INTO auditoria_productos (
                usuario_id,
                producto_maestro_id,
                accion,
                datos_anteriores,
                datos_nuevos,
                razon,
                fecha
            ) VALUES (%s, %s, 'actualizar', %s, %s, %s, CURRENT_TIMESTAMP)
        """, (
            current_user['user_id'],
            producto_id,
            datos_anteriores,
            actualizacion.dict(exclude_unset=True),
            actualizacion.razon_cambio
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": "Producto actualizado exitosamente",
            "producto_id": producto_id,
            "campos_actualizados": len(campos_actualizar) - 2  # -2 por los campos de auditoría
        }

    except HTTPException:
        if conn:
            conn.rollback()
            conn.close()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# ENDPOINT 4: VALIDAR PRODUCTO (CONFIRMAR DATOS CORRECTOS)
# ============================================================================

@router.post("/validar/{producto_id}", response_model=dict)
async def validar_producto(
    producto_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Marca un producto como validado (datos correctos).

    Incrementa contador de validaciones.
    """

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Actualizar producto
        cursor.execute("""
            UPDATE productos_maestros
            SET auditado_manualmente = TRUE,
                validaciones_manuales = COALESCE(validaciones_manuales, 0) + 1,
                ultima_validacion = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (producto_id,))

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Registrar auditoría
        cursor.execute("""
            INSERT INTO auditoria_productos (
                usuario_id,
                producto_maestro_id,
                accion,
                datos_anteriores,
                datos_nuevos,
                razon,
                fecha
            ) VALUES (%s, %s, 'validar', NULL, NULL, %s, CURRENT_TIMESTAMP)
        """, (
            current_user['user_id'],
            producto_id,
            "Datos verificados como correctos"
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": "Producto validado exitosamente",
            "producto_id": producto_id
        }

    except HTTPException:
        if conn:
            conn.rollback()
            conn.close()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# ENDPOINT 5: HISTORIAL DE AUDITORÍAS
# ============================================================================

@router.get("/historial", response_model=List[dict])
async def obtener_historial(
    limite: int = 50,
    current_user: dict = Depends(get_current_user)
):
    """
    Obtiene el historial de auditorías del usuario actual.
    """

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                a.id,
                a.accion,
                a.fecha,
                pm.codigo_ean,
                pm.nombre_normalizado,
                a.razon
            FROM auditoria_productos a
            INNER JOIN productos_maestros pm ON a.producto_maestro_id = pm.id
            WHERE a.usuario_id = %s
            ORDER BY a.fecha DESC
            LIMIT %s
        """, (current_user['user_id'], limite))

        resultados = cursor.fetchall()

        historial = []
        for row in resultados:
            historial.append({
                "id": row[0],
                "accion": row[1],
                "fecha": row[2].isoformat() if row[2] else None,
                "codigo_ean": row[3],
                "nombre_producto": row[4],
                "razon": row[5]
            })

        cursor.close()
        conn.close()

        return historial

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# ENDPOINT 6: ESTADÍSTICAS DE AUDITORÍA
# ============================================================================

@router.get("/estadisticas", response_model=dict)
async def obtener_estadisticas(
    current_user: dict = Depends(get_current_user)
):
    """
    Obtiene estadísticas de auditoría del usuario.
    """

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Contar por tipo de acción
        cursor.execute("""
            SELECT
                accion,
                COUNT(*) as total
            FROM auditoria_productos
            WHERE usuario_id = %s
            GROUP BY accion
        """, (current_user['user_id'],))

        stats = {}
        for accion, total in cursor.fetchall():
            stats[accion] = total

        # Total productos auditados hoy
        cursor.execute("""
            SELECT COUNT(DISTINCT producto_maestro_id)
            FROM auditoria_productos
            WHERE usuario_id = %s
            AND DATE(fecha) = CURRENT_DATE
        """, (current_user['user_id'],))

        stats['auditados_hoy'] = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            "validados": stats.get('validar', 0),
            "creados": stats.get('crear', 0),
            "actualizados": stats.get('actualizar', 0),
            "auditados_hoy": stats['auditados_hoy'],
            "total": sum(v for k, v in stats.items() if k != 'auditados_hoy')
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


print("✅ API Auditoría Productos cargada")
print("   📌 Endpoints disponibles:")
print("      GET  /api/admin/auditoria/verificar/{codigo_ean}")
print("      POST /api/admin/auditoria/producto")
print("      PUT  /api/admin/auditoria/producto/{producto_id}")
print("      POST /api/admin/auditoria/validar/{producto_id}")
print("      GET  /api/admin/auditoria/historial")
print("      GET  /api/admin/auditoria/estadisticas")
