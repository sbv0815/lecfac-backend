"""
Módulo para guardar productos enriquecidos del scraping web
en la tabla productos_web_enriched

Uso:
    from db_productos_enriched import guardar_producto_enriched, buscar_por_ean, buscar_por_plu

    # Guardar producto del scraper
    await guardar_producto_enriched(resultado_scraper)

    # Buscar por EAN
    producto = buscar_por_ean("7703386000000")

    # Buscar por PLU + supermercado
    producto = buscar_por_plu("237373", "Carulla")
"""

import os
import re
from typing import Optional, Dict, List
from datetime import datetime

# Intentar importar psycopg (psycopg3) primero, luego psycopg2
try:
    import psycopg

    PSYCOPG_VERSION = 3
except ImportError:
    try:
        import psycopg2 as psycopg

        PSYCOPG_VERSION = 2
    except ImportError:
        psycopg = None
        PSYCOPG_VERSION = 0


def get_connection():
    """Obtiene conexión a PostgreSQL"""
    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("❌ DATABASE_URL no configurada")
        return None

    try:
        if PSYCOPG_VERSION == 3:
            conn = psycopg.connect(database_url)
        else:
            conn = psycopg.connect(database_url)
        return conn
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        return None


def crear_tabla_enriched():
    """Crea la tabla productos_web_enriched si no existe"""
    conn = get_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS productos_web_enriched (
                id SERIAL PRIMARY KEY,
                ean VARCHAR(20),
                plu VARCHAR(20),
                sku_id VARCHAR(50),
                nombre_completo TEXT NOT NULL,
                nombre_ocr TEXT,
                marca VARCHAR(100),
                presentacion VARCHAR(200),
                supermercado VARCHAR(50) NOT NULL,
                url_producto TEXT,
                precio_web INTEGER,
                precio_lista INTEGER,
                verificado BOOLEAN DEFAULT FALSE,
                fuente VARCHAR(50) DEFAULT 'vtex_api',
                fecha_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_plu_supermercado UNIQUE (plu, supermercado)
            )
        """
        )

        # Crear índices
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_enriched_ean ON productos_web_enriched(ean)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_enriched_plu ON productos_web_enriched(plu)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_enriched_supermercado ON productos_web_enriched(supermercado)"
        )

        conn.commit()
        print("✅ Tabla productos_web_enriched creada/verificada")
        return True

    except Exception as e:
        print(f"❌ Error creando tabla: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def extraer_marca_presentacion(nombre_completo: str) -> tuple:
    """
    Extrae marca y presentación del nombre del producto
    Ej: "Arepas blancas SARY de maíz extra delgadas x10und (600 gr)"
        -> marca: "SARY", presentacion: "600 gr"
    """
    marca = None
    presentacion = None

    # Buscar presentación entre paréntesis al final
    match_presentacion = re.search(r"\(([^)]+)\)\s*$", nombre_completo)
    if match_presentacion:
        presentacion = match_presentacion.group(1).strip()

    # Lista de marcas conocidas colombianas
    marcas_conocidas = [
        "SARY",
        "ALPINA",
        "ALQUERIA",
        "COLANTA",
        "ZENÚ",
        "ZENU",
        "RICA",
        "PIETRAN",
        "RANCHERA",
        "DON MAIZ",
        "DONA PAISA",
        "DOÑA PAISA",
        "SARY CONSCIENTE",
        "LA FAZENDA",
        "TAEQ",
        "MARCA PROPIA",
        "EKONO",
        "JUMBO",
        "EXITO",
        "CARULLA",
    ]

    nombre_upper = nombre_completo.upper()
    for m in marcas_conocidas:
        if m in nombre_upper:
            marca = m.title()
            break

    return marca, presentacion


def guardar_producto_enriched(resultado: Dict) -> Optional[int]:
    """
    Guarda un producto enriquecido del scraper en la base de datos

    Args:
        resultado: Dict con los campos del scraper:
            - nombre_ocr: str
            - nombre_completo: str
            - plu_ocr: str
            - plu_web: str
            - ean_web: str
            - precio_web: int
            - precio_lista_web: int
            - supermercado: str
            - url: str
            - plu_verificado: bool
            - enriquecido: bool

    Returns:
        ID del producto insertado/actualizado o None si falló
    """
    if not resultado.get("enriquecido"):
        print("⚠️ Producto no enriquecido, no se guarda")
        return None

    conn = get_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()

        # Extraer marca y presentación del nombre
        marca, presentacion = extraer_marca_presentacion(
            resultado.get("nombre_completo", "")
        )

        # Usar PLU web si está disponible, sino el del OCR
        plu = resultado.get("plu_web") or resultado.get("plu_ocr")

        # UPSERT: Insertar o actualizar si ya existe
        if PSYCOPG_VERSION == 3:
            cursor.execute(
                """
                INSERT INTO productos_web_enriched (
                    ean, plu, sku_id, nombre_completo, nombre_ocr,
                    marca, presentacion, supermercado, url_producto,
                    precio_web, precio_lista, verificado, fuente
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (plu, supermercado)
                DO UPDATE SET
                    ean = EXCLUDED.ean,
                    nombre_completo = EXCLUDED.nombre_completo,
                    precio_web = EXCLUDED.precio_web,
                    precio_lista = EXCLUDED.precio_lista,
                    verificado = EXCLUDED.verificado,
                    url_producto = EXCLUDED.url_producto,
                    fecha_actualizacion = CURRENT_TIMESTAMP
                RETURNING id
            """,
                (
                    resultado.get("ean_web"),
                    plu,
                    resultado.get("sku_id"),
                    resultado.get("nombre_completo"),
                    resultado.get("nombre_ocr"),
                    marca,
                    presentacion,
                    resultado.get("supermercado", "Carulla"),
                    resultado.get("url"),
                    resultado.get("precio_web"),
                    resultado.get("precio_lista_web"),
                    resultado.get("plu_verificado", False),
                    "vtex_api",
                ),
            )
        else:
            # psycopg2 no soporta RETURNING en UPSERT fácilmente
            cursor.execute(
                """
                INSERT INTO productos_web_enriched (
                    ean, plu, sku_id, nombre_completo, nombre_ocr,
                    marca, presentacion, supermercado, url_producto,
                    precio_web, precio_lista, verificado, fuente
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (plu, supermercado)
                DO UPDATE SET
                    ean = EXCLUDED.ean,
                    nombre_completo = EXCLUDED.nombre_completo,
                    precio_web = EXCLUDED.precio_web,
                    precio_lista = EXCLUDED.precio_lista,
                    verificado = EXCLUDED.verificado,
                    url_producto = EXCLUDED.url_producto,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """,
                (
                    resultado.get("ean_web"),
                    plu,
                    resultado.get("sku_id"),
                    resultado.get("nombre_completo"),
                    resultado.get("nombre_ocr"),
                    marca,
                    presentacion,
                    resultado.get("supermercado", "Carulla"),
                    resultado.get("url"),
                    resultado.get("precio_web"),
                    resultado.get("precio_lista_web"),
                    resultado.get("plu_verificado", False),
                    "vtex_api",
                ),
            )

        conn.commit()

        # Obtener el ID
        if PSYCOPG_VERSION == 3:
            row = cursor.fetchone()
            producto_id = row[0] if row else None
        else:
            # Buscar el ID del producto insertado
            cursor.execute(
                """
                SELECT id FROM productos_web_enriched
                WHERE plu = %s AND supermercado = %s
            """,
                (plu, resultado.get("supermercado", "Carulla")),
            )
            row = cursor.fetchone()
            producto_id = row[0] if row else None

        print(f"✅ Producto guardado/actualizado: ID={producto_id}, PLU={plu}")
        return producto_id

    except Exception as e:
        print(f"❌ Error guardando producto: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def guardar_candidatos_enriched(
    candidatos: List[Dict], supermercado: str = "Carulla"
) -> int:
    """
    Guarda múltiples candidatos del scraper
    Útil para enriquecer la base de datos con productos similares encontrados

    Returns:
        Número de productos guardados exitosamente
    """
    guardados = 0

    for candidato in candidatos:
        # Construir resultado compatible
        resultado = {
            "enriquecido": True,
            "nombre_completo": candidato.get("nombre"),
            "plu_web": candidato.get("plu"),
            "ean_web": candidato.get("ean"),
            "precio_web": candidato.get("precio"),
            "precio_lista_web": candidato.get("precio_lista"),
            "supermercado": supermercado,
            "url": candidato.get("url"),
            "plu_verificado": False,  # Candidatos no están verificados
            "sku_id": candidato.get("sku_id"),
        }

        if guardar_producto_enriched(resultado):
            guardados += 1

    print(f"📦 Guardados {guardados}/{len(candidatos)} candidatos")
    return guardados


def buscar_por_ean(ean: str) -> Optional[Dict]:
    """Busca un producto por su código EAN"""
    conn = get_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, ean, plu, nombre_completo, marca, presentacion,
                   supermercado, precio_web, url_producto, verificado,
                   fecha_actualizacion
            FROM productos_web_enriched
            WHERE ean = %s
            ORDER BY fecha_actualizacion DESC
            LIMIT 1
        """,
            (ean,),
        )

        row = cursor.fetchone()
        if row:
            return {
                "id": row[0],
                "ean": row[1],
                "plu": row[2],
                "nombre_completo": row[3],
                "marca": row[4],
                "presentacion": row[5],
                "supermercado": row[6],
                "precio_web": row[7],
                "url": row[8],
                "verificado": row[9],
                "fecha_actualizacion": row[10],
            }
        return None

    except Exception as e:
        print(f"❌ Error buscando por EAN: {e}")
        return None
    finally:
        conn.close()


def buscar_por_plu(plu: str, supermercado: str = None) -> Optional[Dict]:
    """Busca un producto por su PLU (y opcionalmente supermercado)"""
    conn = get_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()

        if supermercado:
            cursor.execute(
                """
                SELECT id, ean, plu, nombre_completo, marca, presentacion,
                       supermercado, precio_web, url_producto, verificado,
                       fecha_actualizacion
                FROM productos_web_enriched
                WHERE plu = %s AND supermercado = %s
            """,
                (plu, supermercado),
            )
        else:
            cursor.execute(
                """
                SELECT id, ean, plu, nombre_completo, marca, presentacion,
                       supermercado, precio_web, url_producto, verificado,
                       fecha_actualizacion
                FROM productos_web_enriched
                WHERE plu = %s
                ORDER BY fecha_actualizacion DESC
                LIMIT 1
            """,
                (plu,),
            )

        row = cursor.fetchone()
        if row:
            return {
                "id": row[0],
                "ean": row[1],
                "plu": row[2],
                "nombre_completo": row[3],
                "marca": row[4],
                "presentacion": row[5],
                "supermercado": row[6],
                "precio_web": row[7],
                "url": row[8],
                "verificado": row[9],
                "fecha_actualizacion": row[10],
            }
        return None

    except Exception as e:
        print(f"❌ Error buscando por PLU: {e}")
        return None
    finally:
        conn.close()


def comparar_precios_ean(ean: str) -> List[Dict]:
    """
    Compara precios del mismo producto (por EAN) en diferentes supermercados

    Returns:
        Lista de precios ordenados de menor a mayor
    """
    conn = get_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT supermercado, nombre_completo, precio_web,
                   url_producto, fecha_actualizacion
            FROM productos_web_enriched
            WHERE ean = %s AND precio_web IS NOT NULL
            ORDER BY precio_web ASC
        """,
            (ean,),
        )

        resultados = []
        for row in cursor.fetchall():
            resultados.append(
                {
                    "supermercado": row[0],
                    "nombre": row[1],
                    "precio": row[2],
                    "url": row[3],
                    "fecha": row[4],
                }
            )

        return resultados

    except Exception as e:
        print(f"❌ Error comparando precios: {e}")
        return []
    finally:
        conn.close()


def estadisticas_enriched() -> Dict:
    """Obtiene estadísticas de la tabla de productos enriquecidos"""
    conn = get_connection()
    if not conn:
        return {}

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT ean) as eans_unicos,
                COUNT(DISTINCT plu) as plus_unicos,
                COUNT(*) FILTER (WHERE verificado = TRUE) as verificados,
                COUNT(DISTINCT supermercado) as supermercados
            FROM productos_web_enriched
        """
        )

        row = cursor.fetchone()

        return {
            "total_productos": row[0],
            "eans_unicos": row[1],
            "plus_unicos": row[2],
            "verificados": row[3],
            "supermercados": row[4],
        }

    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return {}
    finally:
        conn.close()


# ============================================
# PRUEBAS
# ============================================

if __name__ == "__main__":
    print("=" * 60)
    print("🧪 Probando módulo db_productos_enriched")
    print("=" * 60)

    # Crear tabla
    crear_tabla_enriched()

    # Simular resultado del scraper
    resultado_test = {
        "nombre_ocr": "AREPA EXTRADELGA SARY",
        "nombre_completo": "Arepas blancas SARY de maíz extra delgadas x10und (600 gr)",
        "plu_ocr": "237373",
        "plu_web": "237373",
        "ean_web": "7703386000000",
        "precio_web": 8600,
        "precio_lista_web": 9500,
        "supermercado": "Carulla",
        "url": "https://www.carulla.com/arepa-sary/p",
        "plu_verificado": True,
        "enriquecido": True,
    }

    # Guardar
    print("\n📝 Guardando producto de prueba...")
    id_producto = guardar_producto_enriched(resultado_test)

    if id_producto:
        # Buscar por PLU
        print("\n🔍 Buscando por PLU...")
        encontrado = buscar_por_plu("237373", "Carulla")
        if encontrado:
            print(f"   ✅ Encontrado: {encontrado['nombre_completo']}")
            print(f"   💰 Precio: ${encontrado['precio_web']:,}")

        # Buscar por EAN
        print("\n🔍 Buscando por EAN...")
        encontrado = buscar_por_ean("7703386000000")
        if encontrado:
            print(f"   ✅ Encontrado: {encontrado['nombre_completo']}")

    # Estadísticas
    print("\n📊 Estadísticas:")
    stats = estadisticas_enriched()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    print("\n" + "=" * 60)
