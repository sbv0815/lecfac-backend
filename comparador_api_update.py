# ============================================================
# MODIFICACIÓN PARA comparador_api.py
# Agregar campo fecha_actualizacion en las respuestas de productos
# ============================================================

# BUSCAR este fragmento en tu endpoint /api/comparador/mejor-en-tienda
# y AGREGAR el campo fecha_actualizacion en el SELECT

# ANTES (ejemplo):
"""
SELECT 
    pm.id,
    pm.nombre,
    pm.marca,
    pp.precio,
    pp.establecimiento_id,
    e.nombre as establecimiento,
    COALESCE(pm.rating_promedio, 0) as rating
FROM productos_maestros pm
JOIN precios_productos pp ON pm.id = pp.producto_maestro_id
JOIN establecimientos e ON pp.establecimiento_id = e.id
WHERE ...
"""

# DESPUÉS (agregar fecha_actualizacion):
"""
SELECT 
    pm.id,
    pm.nombre,
    pm.marca,
    pp.precio,
    pp.establecimiento_id,
    e.nombre as establecimiento,
    COALESCE(pm.rating_promedio, 0) as rating,
    pp.fecha_actualizacion,
    pp.created_at as fecha_precio
FROM productos_maestros pm
JOIN precios_productos pp ON pm.id = pp.producto_maestro_id
JOIN establecimientos e ON pp.establecimiento_id = e.id
WHERE ...
"""

# ============================================================
# CÓDIGO COMPLETO DEL ENDPOINT ACTUALIZADO
# Reemplaza tu función mejor_en_tienda con esta versión
# ============================================================

from fastapi import APIRouter, Query
from datetime import datetime, timedelta

router = APIRouter()

@router.get("/api/comparador/mejor-en-tienda")
async def mejor_en_tienda(
    busqueda: str = Query(..., description="Término de búsqueda"),
    establecimiento_id: int = Query(None, description="Filtrar por establecimiento"),
    ordenar_por: str = Query("valor", description="precio, rating, valor")
):
    """
    Busca productos y retorna con fecha de actualización del precio
    """
    print("=" * 60)
    print(f"🏪 MODO TIENDA: Buscando '{busqueda}'")
    print(f"   📍 Establecimiento ID: {establecimiento_id or 'Todos'}")
    print(f"   📊 Ordenar por: {ordenar_por}")
    print("=" * 60)
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Buscar con ILIKE y agregar fecha_actualizacion
        search_pattern = f"%{busqueda}%"
        
        query = """
            SELECT DISTINCT ON (pm.id)
                pm.id,
                pm.nombre,
                pm.marca,
                pm.categoria,
                pp.precio,
                pp.establecimiento_id,
                e.nombre as establecimiento,
                COALESCE(pm.rating_promedio, 0) as rating,
                pp.fecha_actualizacion,
                COALESCE(pp.updated_at, pp.created_at, NOW()) as fecha_precio
            FROM productos_maestros pm
            JOIN precios_productos pp ON pm.id = pp.producto_maestro_id
            JOIN establecimientos e ON pp.establecimiento_id = e.id
            WHERE (
                pm.nombre ILIKE %s
                OR pm.marca ILIKE %s
                OR pm.categoria ILIKE %s
            )
        """
        
        params = [search_pattern, search_pattern, search_pattern]
        
        if establecimiento_id:
            query += " AND pp.establecimiento_id = %s"
            params.append(establecimiento_id)
        
        # Ordenar por precio más reciente
        query += " ORDER BY pm.id, pp.fecha_actualizacion DESC NULLS LAST"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        productos = []
        for row in rows:
            # Calcular antigüedad del precio
            fecha_precio = row[9] if row[9] else datetime.now()
            dias_antiguedad = (datetime.now() - fecha_precio).days if fecha_precio else 0
            
            # Determinar frescura del precio
            if dias_antiguedad <= 7:
                frescura = "reciente"
                frescura_color = "green"
            elif dias_antiguedad <= 30:
                frescura = "semana"
                frescura_color = "orange"
            else:
                frescura = "antiguo"
                frescura_color = "red"
            
            productos.append({
                "id": row[0],
                "nombre": row[1],
                "marca": row[2],
                "categoria": row[3],
                "precio": float(row[4]) if row[4] else 0,
                "establecimiento_id": row[5],
                "establecimiento": row[6],
                "rating": float(row[7]) if row[7] else 0,
                "fecha_actualizacion": fecha_precio.isoformat() if fecha_precio else None,
                "dias_antiguedad": dias_antiguedad,
                "frescura": frescura,
                "frescura_color": frescura_color,
            })
        
        # Ordenar según criterio
        if ordenar_por == "precio":
            productos.sort(key=lambda x: x["precio"] if x["precio"] > 0 else float('inf'))
        elif ordenar_por == "rating":
            productos.sort(key=lambda x: x["rating"], reverse=True)
        else:  # valor (calidad/precio)
            productos.sort(key=lambda x: (x["rating"] / (x["precio"] / 10000)) if x["precio"] > 0 else 0, reverse=True)
        
        print(f"📊 Productos encontrados: {len(productos)}")
        
        return {
            "success": True,
            "productos": productos,
            "total": len(productos),
            "busqueda": busqueda
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"success": False, "productos": [], "error": str(e)}
    finally:
        if conn:
            conn.close()


# ============================================================
# ALTERNATIVA: Si no tienes campo fecha_actualizacion
# Puedes usar created_at de la factura donde se escaneó el precio
# ============================================================

# Si tu tabla precios_productos NO tiene fecha_actualizacion,
# puedes agregarla con:
#
# ALTER TABLE precios_productos 
# ADD COLUMN IF NOT EXISTS fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
#
# ALTER TABLE precios_productos 
# ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
#
# -- Trigger para actualizar automáticamente
# CREATE OR REPLACE FUNCTION update_updated_at()
# RETURNS TRIGGER AS $$
# BEGIN
#     NEW.updated_at = CURRENT_TIMESTAMP;
#     RETURN NEW;
# END;
# $$ LANGUAGE plpgsql;
#
# CREATE TRIGGER precios_productos_updated_at
# BEFORE UPDATE ON precios_productos
# FOR EACH ROW EXECUTE FUNCTION update_updated_at();
