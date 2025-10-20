from fastapi import APIRouter, HTTPException, Header
from typing import Optional
from database import get_db_connection
import os

router = APIRouter()


@router.get("/api/mobile/my-stats")
async def get_user_stats(authorization: Optional[str] = Header(None)):
    """
    Obtiene estadísticas del usuario
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    # Extraer user_id del token (simplificado)
    try:
        user_id = int(authorization.replace("Bearer ", ""))
    except:
        raise HTTPException(status_code=401, detail="Token inválido")

    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        # 1. Total de facturas
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COUNT(*) FROM facturas WHERE usuario_id = %s
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) FROM facturas WHERE usuario_id = ?
            """,
                (user_id,),
            )

        total_facturas = cursor.fetchone()[0] or 0

        # 2. Total de productos en inventario
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = %s
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = ?
            """,
                (user_id,),
            )

        total_productos = cursor.fetchone()[0] or 0

        # 3. Gasto total del mes actual
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
                AND EXTRACT(MONTH FROM fecha_factura) = EXTRACT(MONTH FROM CURRENT_DATE)
                AND EXTRACT(YEAR FROM fecha_factura) = EXTRACT(YEAR FROM CURRENT_DATE)
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
                AND strftime('%m', fecha_factura) = strftime('%m', 'now')
                AND strftime('%Y', fecha_factura) = strftime('%Y', 'now')
            """,
                (user_id,),
            )

        gasto_mes = cursor.fetchone()[0] or 0

        # 4. Promedio por factura
        promedio_factura = (gasto_mes / total_facturas) if total_facturas > 0 else 0

        # 5. Última factura
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT fecha_factura, establecimiento, total_factura
                FROM facturas
                WHERE usuario_id = %s
                ORDER BY fecha_factura DESC
                LIMIT 1
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT fecha_factura, establecimiento, total_factura
                FROM facturas
                WHERE usuario_id = ?
                ORDER BY fecha_factura DESC
                LIMIT 1
            """,
                (user_id,),
            )

        ultima_factura_data = cursor.fetchone()
        ultima_factura = None
        if ultima_factura_data:
            ultima_factura = {
                "fecha": str(ultima_factura_data[0]),
                "establecimiento": ultima_factura_data[1],
                "total": ultima_factura_data[2],
            }

        # 6. Top 3 establecimientos más visitados
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT establecimiento, COUNT(*) as visitas
                FROM facturas
                WHERE usuario_id = %s AND establecimiento IS NOT NULL
                GROUP BY establecimiento
                ORDER BY visitas DESC
                LIMIT 3
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT establecimiento, COUNT(*) as visitas
                FROM facturas
                WHERE usuario_id = ? AND establecimiento IS NOT NULL
                GROUP BY establecimiento
                ORDER BY visitas DESC
                LIMIT 3
            """,
                (user_id,),
            )

        top_establecimientos = [
            {"nombre": row[0], "visitas": row[1]} for row in cursor.fetchall()
        ]

        conn.close()

        return {
            "success": True,
            "stats": {
                "total_facturas": total_facturas,
                "total_productos": total_productos,
                "gasto_mes_actual": gasto_mes,
                "promedio_por_factura": int(promedio_factura),
                "ultima_factura": ultima_factura,
                "top_establecimientos": top_establecimientos,
            },
        }

    except Exception as e:
        conn.close()
        print(f"❌ Error obteniendo estadísticas: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
