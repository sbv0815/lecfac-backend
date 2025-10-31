import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from database import get_db_connection
import Levenshtein  # pip install python-Levenshtein
import re

class ProductResolver:
    """
    Resuelve la identidad de productos usando múltiples estrategias
    """

    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()

    def normalizar_nombre(self, nombre: str) -> str:
        """
        Normaliza nombres para comparación
        """
        if not nombre:
            return ""

        nombre = nombre.lower().strip()

        # Remover caracteres especiales
        nombre = re.sub(r'[^\w\s]', ' ', nombre)

        # Normalizar espacios
        nombre = re.sub(r'\s+', ' ', nombre)

        # Remover palabras comunes que no aportan
        stop_words = {'x', 'un', 'gr', 'ml', 'kg', 'lt', 'und'}
        palabras = [p for p in nombre.split() if p not in stop_words]

        return ' '.join(palabras)

    def extraer_palabras_clave(self, nombre: str) -> List[str]:
        """
        Extrae palabras significativas
        """
        normalizado = self.normalizar_nombre(nombre)
        return [p for p in normalizado.split() if len(p) >= 3]

    def buscar_por_ean(self, ean: str) -> Optional[int]:
        """
        Estrategia 1: Match exacto por EAN
        Confianza: 100%
        """
        if not ean or len(ean) < 8:
            return None

        self.cursor.execute("""
            SELECT producto_canonico_id
            FROM productos_variantes
            WHERE codigo = %s AND tipo_codigo = 'EAN'
            LIMIT 1
        """, (ean,))

        result = self.cursor.fetchone()
        return result[0] if result else None

    def buscar_por_nombre_similar(
        self,
        nombre: str,
        establecimiento: str,
        threshold: float = 0.85
    ) -> Optional[Tuple[int, float]]:
        """
        Estrategia 2: Match por similitud de nombre
        Usa Levenshtein distance
        """
        nombre_norm = self.normalizar_nombre(nombre)

        if len(nombre_norm) < 5:
            return None

        # Buscar productos canónicos con palabras clave similares
        palabras = self.extraer_palabras_clave(nombre)

        if not palabras:
            return None

        # Construir query con búsqueda por palabras clave
        query = """
            SELECT
                pc.id,
                pc.nombre_oficial,
                pc.nombre_normalizado
            FROM productos_canonicos pc
            WHERE 1=1
        """

        # Agregar condiciones OR para cada palabra clave
        conditions = []
        for palabra in palabras[:3]:  # Máximo 3 palabras más relevantes
            conditions.append(f"pc.palabras_clave @> ARRAY['{palabra}']::TEXT[]")

        if conditions:
            query += " AND (" + " OR ".join(conditions) + ")"

        query += " LIMIT 20"

        self.cursor.execute(query)
        candidatos = self.cursor.fetchall()

        mejor_match = None
        mejor_score = 0.0

        for candidato in candidatos:
            canonico_id, nombre_oficial, nombre_normalizado = candidato

            # Calcular similitud
            score = Levenshtein.ratio(nombre_norm, nombre_normalizado)

            if score > mejor_score and score >= threshold:
                mejor_score = score
                mejor_match = canonico_id

        if mejor_match:
            return (mejor_match, mejor_score)

        return None

    def crear_producto_canonico(
        self,
        nombre: str,
        ean: Optional[str],
        marca: Optional[str],
        categoria: Optional[str]
    ) -> int:
        """
        Crea un nuevo producto canónico
        """
        nombre_norm = self.normalizar_nombre(nombre)
        palabras_clave = self.extraer_palabras_clave(nombre)

        self.cursor.execute("""
            INSERT INTO productos_canonicos (
                nombre_oficial,
                marca,
                categoria,
                ean_principal,
                nombre_normalizado,
                palabras_clave
            ) VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            nombre,
            marca,
            categoria,
            ean,
            nombre_norm,
            palabras_clave
        ))

        canonico_id = self.cursor.fetchone()[0]
        self.conn.commit()

        print(f"✅ Producto canónico creado: ID {canonico_id} - {nombre}")

        return canonico_id

    def registrar_variante(
        self,
        canonico_id: int,
        codigo: str,
        tipo_codigo: str,
        nombre_en_recibo: str,
        establecimiento: str,
        cadena: Optional[str]
    ) -> int:
        """
        Registra una variante (alias) del producto canónico
        """
        try:
            self.cursor.execute("""
                INSERT INTO productos_variantes (
                    producto_canonico_id,
                    codigo,
                    tipo_codigo,
                    nombre_en_recibo,
                    establecimiento,
                    cadena,
                    veces_reportado,
                    primera_vez_visto,
                    ultima_vez_visto
                ) VALUES (%s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (codigo, establecimiento)
                DO UPDATE SET
                    veces_reportado = productos_variantes.veces_reportado + 1,
                    ultima_vez_visto = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                canonico_id,
                codigo,
                tipo_codigo,
                nombre_en_recibo,
                establecimiento,
                cadena
            ))

            variante_id = self.cursor.fetchone()[0]
            self.conn.commit()

            return variante_id

        except Exception as e:
            self.conn.rollback()
            print(f"⚠️ Error registrando variante: {e}")
            return None

    def resolver_producto(
        self,
        codigo: str,
        nombre: str,
        establecimiento: str,
        precio: int,
        marca: Optional[str] = None,
        categoria: Optional[str] = None
    ) -> Tuple[int, int, str]:
        """
        FUNCIÓN PRINCIPAL: Resuelve la identidad de un producto

        Returns:
            (canonico_id, variante_id, accion)
            accion: 'found_ean', 'found_similar', 'created_new'
        """

        print(f"\n🔍 Resolviendo: {nombre} ({codigo}) en {establecimiento}")

        # ESTRATEGIA 1: Match por EAN
        if codigo and len(codigo) >= 8:
            canonico_id = self.buscar_por_ean(codigo)

            if canonico_id:
                print(f"   ✅ Match por EAN → Canónico #{canonico_id}")

                variante_id = self.registrar_variante(
                    canonico_id,
                    codigo,
                    'EAN',
                    nombre,
                    establecimiento,
                    None
                )

                return (canonico_id, variante_id, 'found_ean')

        # ESTRATEGIA 2: Match por nombre similar
        resultado = self.buscar_por_nombre_similar(nombre, establecimiento)

        if resultado:
            canonico_id, score = resultado
            print(f"   ✅ Match por nombre (score: {score:.2%}) → Canónico #{canonico_id}")

            tipo_codigo = 'EAN' if codigo and len(codigo) >= 8 else 'PLU'

            variante_id = self.registrar_variante(
                canonico_id,
                codigo or f"PLU_{establecimiento}_{nombre[:20]}",
                tipo_codigo,
                nombre,
                establecimiento,
                None
            )

            return (canonico_id, variante_id, 'found_similar')

        # ESTRATEGIA 3: Crear nuevo producto canónico
        print(f"   ➕ Creando nuevo producto canónico")

        canonico_id = self.crear_producto_canonico(
            nombre,
            codigo if codigo and len(codigo) >= 8 else None,
            marca,
            categoria
        )

        tipo_codigo = 'EAN' if codigo and len(codigo) >= 8 else 'PLU'

        variante_id = self.registrar_variante(
            canonico_id,
            codigo or f"PLU_{establecimiento}_{canonico_id}",
            tipo_codigo,
            nombre,
            establecimiento,
            None
        )

        return (canonico_id, variante_id, 'created_new')

    def close(self):
        """Cerrar conexiones"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# ========================================
# FUNCIÓN DE AYUDA PARA INTEGRACIÓN
# ========================================
def procesar_item_con_resolucion(
    codigo: str,
    nombre: str,
    precio: int,
    establecimiento: str,
    usuario_id: int,
    factura_id: int
) -> Dict:
    """
    Procesa un item de factura con resolución de identidad
    """
    resolver = ProductResolver()

    try:
        canonico_id, variante_id, accion = resolver.resolver_producto(
            codigo=codigo,
            nombre=nombre,
            establecimiento=establecimiento,
            precio=precio
        )

        # Guardar precio
        cursor = resolver.cursor
        cursor.execute("""
            INSERT INTO precios_productos (
                producto_canonico_id,
                variante_id,
                establecimiento,
                precio,
                fecha,
                usuario_id,
                factura_id
            ) VALUES (%s, %s, %s, %s, CURRENT_DATE, %s, %s)
        """, (canonico_id, variante_id, establecimiento, precio, usuario_id, factura_id))

        resolver.conn.commit()

        return {
            'success': True,
            'canonico_id': canonico_id,
            'variante_id': variante_id,
            'accion': accion
        }

    except Exception as e:
        print(f"❌ Error procesando item: {e}")
        resolver.conn.rollback()
        return {'success': False, 'error': str(e)}

    finally:
        resolver.close()
