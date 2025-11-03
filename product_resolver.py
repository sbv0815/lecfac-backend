"""
============================================================================
PRODUCT RESOLVER - Sistema de Productos Canónicos
============================================================================
Resuelve productos usando la arquitectura unificada:
- productos_canonicos: La verdad única del producto
- productos_variantes: Alias por establecimiento
- productos_maestros: Legacy (compatibilidad)
============================================================================
"""

import os
from typing import Tuple, Optional, Dict, Any
from datetime import datetime


class ProductResolver:
    """
    Resuelve productos usando el sistema de productos canónicos

    Flujo:
    1. Busca variante existente (código + establecimiento)
    2. Si no existe, busca en canónicos por EAN
    3. Si no existe, crea canónico nuevo + variante
    4. Actualiza/crea producto_maestro para compatibilidad
    """

    def __init__(self):
        """Inicializa el resolver con conexión a BD"""
        from database import get_db_connection
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()
        self.database_type = os.environ.get("DATABASE_TYPE", "postgresql")

    def close(self):
        """Cierra la conexión a la base de datos"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

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
        Resuelve un producto y retorna IDs + acción realizada

        Args:
            codigo: Código del producto (EAN, PLU, etc.)
            nombre: Nombre del producto
            establecimiento: Nombre del establecimiento
            precio: Precio del producto
            marca: Marca del producto (opcional)
            categoria: Categoría del producto (opcional)

        Returns:
            Tuple[canonico_id, variante_id, accion]
            - canonico_id: ID del producto canónico
            - variante_id: ID de la variante
            - accion: 'found_variant' | 'found_canonical' | 'created_new'
        """

        # Normalizar datos
        codigo = str(codigo).strip() if codigo else ""
        nombre = str(nombre).strip()
        establecimiento = str(establecimiento).strip()

        if not nombre:
            raise ValueError("El nombre del producto es requerido")

        # Determinar tipo de código
        tipo_codigo = self._determinar_tipo_codigo(codigo)

        # PASO 1: Buscar variante existente
        variante = self._buscar_variante(codigo, establecimiento)

        if variante:
            canonico_id = variante['producto_canonico_id']
            variante_id = variante['id']

            # Actualizar estadísticas de la variante
            self._actualizar_variante(variante_id)

            return (canonico_id, variante_id, 'found_variant')

        # PASO 2: Buscar canónico por EAN (si es EAN válido)
        canonico_id = None

        if tipo_codigo == 'EAN' and len(codigo) >= 8:
            canonico = self._buscar_canonico_por_ean(codigo)
            if canonico:
                canonico_id = canonico['id']

        # PASO 3: Si no existe canónico, crear uno nuevo
        if not canonico_id:
            canonico_id = self._crear_producto_canonico(
                codigo=codigo,
                nombre=nombre,
                marca=marca,
                categoria=categoria,
                precio=precio
            )

        # PASO 4: Crear variante
        variante_id = self._crear_variante(
            canonico_id=canonico_id,
            codigo=codigo,
            tipo_codigo=tipo_codigo,
            nombre_en_recibo=nombre,
            establecimiento=establecimiento
        )

        # PASO 5: Crear/actualizar producto_maestro (legacy)
        self._sincronizar_producto_maestro(canonico_id, codigo, nombre, marca, categoria)

        return (canonico_id, variante_id, 'created_new')

    def _determinar_tipo_codigo(self, codigo: str) -> str:
        """Determina el tipo de código (EAN, PLU, INTERNO)"""
        if not codigo:
            return 'INTERNO'

        # EAN: 8-14 dígitos
        if codigo.isdigit() and 8 <= len(codigo) <= 14:
            return 'EAN'

        # PLU: 3-5 dígitos
        if codigo.isdigit() and 3 <= len(codigo) <= 5:
            return 'PLU'

        # Cualquier otro caso
        return 'INTERNO'

    def _buscar_variante(self, codigo: str, establecimiento: str) -> Optional[Dict[str, Any]]:
        """Busca una variante existente por código + establecimiento"""
        try:
            if self.database_type == "postgresql":
                self.cursor.execute("""
                    SELECT id, producto_canonico_id, codigo, tipo_codigo
                    FROM productos_variantes
                    WHERE codigo = %s AND establecimiento = %s
                    LIMIT 1
                """, (codigo, establecimiento))
            else:
                self.cursor.execute("""
                    SELECT id, producto_canonico_id, codigo, tipo_codigo
                    FROM productos_variantes
                    WHERE codigo = ? AND establecimiento = ?
                    LIMIT 1
                """, (codigo, establecimiento))

            row = self.cursor.fetchone()

            if row:
                return {
                    'id': row[0],
                    'producto_canonico_id': row[1],
                    'codigo': row[2],
                    'tipo_codigo': row[3]
                }

            return None

        except Exception as e:
            print(f"⚠️ Error buscando variante: {e}")
            return None

    def _buscar_canonico_por_ean(self, ean: str) -> Optional[Dict[str, Any]]:
        """Busca un producto canónico por EAN"""
        try:
            if self.database_type == "postgresql":
                self.cursor.execute("""
                    SELECT id, nombre_oficial, marca, categoria
                    FROM productos_canonicos
                    WHERE ean_principal = %s
                    LIMIT 1
                """, (ean,))
            else:
                self.cursor.execute("""
                    SELECT id, nombre_oficial, marca, categoria
                    FROM productos_canonicos
                    WHERE ean_principal = ?
                    LIMIT 1
                """, (ean,))

            row = self.cursor.fetchone()

            if row:
                return {
                    'id': row[0],
                    'nombre_oficial': row[1],
                    'marca': row[2],
                    'categoria': row[3]
                }

            return None

        except Exception as e:
            print(f"⚠️ Error buscando canónico por EAN: {e}")
            return None

    def _crear_producto_canonico(
        self,
        codigo: str,
        nombre: str,
        marca: Optional[str],
        categoria: Optional[str],
        precio: int
    ) -> int:
        """Crea un nuevo producto canónico"""
        try:
            # Determinar EAN principal
            ean_principal = codigo if len(codigo) >= 8 and codigo.isdigit() else None

            # Normalizar nombre
            nombre_normalizado = nombre.lower().strip()

            if self.database_type == "postgresql":
                self.cursor.execute("""
                    INSERT INTO productos_canonicos (
                        nombre_oficial,
                        marca,
                        categoria,
                        ean_principal,
                        nombre_normalizado,
                        precio_promedio_global,
                        total_reportes,
                        fecha_creacion,
                        ultima_actualizacion
                    ) VALUES (%s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id
                """, (nombre, marca, categoria, ean_principal, nombre_normalizado, precio))

                canonico_id = self.cursor.fetchone()[0]
            else:
                self.cursor.execute("""
                    INSERT INTO productos_canonicos (
                        nombre_oficial,
                        marca,
                        categoria,
                        ean_principal,
                        nombre_normalizado,
                        precio_promedio_global,
                        total_reportes
                    ) VALUES (?, ?, ?, ?, ?, ?, 1)
                """, (nombre, marca, categoria, ean_principal, nombre_normalizado, precio))

                canonico_id = self.cursor.lastrowid

            self.conn.commit()

            print(f"   ✅ Producto canónico creado: {nombre} (ID: {canonico_id})")

            return canonico_id

        except Exception as e:
            print(f"❌ Error creando producto canónico: {e}")
            self.conn.rollback()
            raise e

    def _crear_variante(
        self,
        canonico_id: int,
        codigo: str,
        tipo_codigo: str,
        nombre_en_recibo: str,
        establecimiento: str
    ) -> int:
        """Crea una nueva variante del producto"""
        try:
            if self.database_type == "postgresql":
                self.cursor.execute("""
                    INSERT INTO productos_variantes (
                        producto_canonico_id,
                        codigo,
                        tipo_codigo,
                        nombre_en_recibo,
                        establecimiento,
                        veces_reportado,
                        primera_vez_visto,
                        ultima_vez_visto
                    ) VALUES (%s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id
                """, (canonico_id, codigo, tipo_codigo, nombre_en_recibo, establecimiento))

                variante_id = self.cursor.fetchone()[0]
            else:
                self.cursor.execute("""
                    INSERT INTO productos_variantes (
                        producto_canonico_id,
                        codigo,
                        tipo_codigo,
                        nombre_en_recibo,
                        establecimiento,
                        veces_reportado
                    ) VALUES (?, ?, ?, ?, ?, 1)
                """, (canonico_id, codigo, tipo_codigo, nombre_en_recibo, establecimiento))

                variante_id = self.cursor.lastrowid

            self.conn.commit()

            print(f"   ✅ Variante creada: {codigo} en {establecimiento} (ID: {variante_id})")

            return variante_id

        except Exception as e:
            print(f"❌ Error creando variante: {e}")
            self.conn.rollback()
            raise e

    def _actualizar_variante(self, variante_id: int):
        """Actualiza estadísticas de una variante existente"""
        try:
            if self.database_type == "postgresql":
                self.cursor.execute("""
                    UPDATE productos_variantes
                    SET veces_reportado = veces_reportado + 1,
                        ultima_vez_visto = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (variante_id,))
            else:
                self.cursor.execute("""
                    UPDATE productos_variantes
                    SET veces_reportado = veces_reportado + 1
                    WHERE id = ?
                """, (variante_id,))

            self.conn.commit()

        except Exception as e:
            print(f"⚠️ Error actualizando variante: {e}")
            self.conn.rollback()

    def _sincronizar_producto_maestro(
        self,
        canonico_id: int,
        codigo: str,
        nombre: str,
        marca: Optional[str],
        categoria: Optional[str]
    ):
        """Sincroniza con productos_maestros (legacy) para compatibilidad"""
        try:
            # Buscar si ya existe
            if self.database_type == "postgresql":
                self.cursor.execute("""
                    SELECT id FROM productos_maestros
                    WHERE producto_canonico_id = %s
                    LIMIT 1
                """, (canonico_id,))
            else:
                self.cursor.execute("""
                    SELECT id FROM productos_maestros
                    WHERE producto_canonico_id = ?
                    LIMIT 1
                """, (canonico_id,))

            maestro = self.cursor.fetchone()

            if maestro:
                # Actualizar existente
                maestro_id = maestro[0]

                if self.database_type == "postgresql":
                    self.cursor.execute("""
                        UPDATE productos_maestros
                        SET total_reportes = total_reportes + 1,
                            ultima_actualizacion = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (maestro_id,))
                else:
                    self.cursor.execute("""
                        UPDATE productos_maestros
                        SET total_reportes = total_reportes + 1
                        WHERE id = ?
                    """, (maestro_id,))
            else:
                # Crear nuevo
                codigo_ean = codigo if len(codigo) >= 8 and codigo.isdigit() else None

                if self.database_type == "postgresql":
                    self.cursor.execute("""
                        INSERT INTO productos_maestros (
                            producto_canonico_id,
                            codigo_ean,
                            nombre_normalizado,
                            marca,
                            categoria,
                            total_reportes,
                            primera_vez_reportado,
                            ultima_actualizacion
                        ) VALUES (%s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, (canonico_id, codigo_ean, nombre, marca, categoria))
                else:
                    self.cursor.execute("""
                        INSERT INTO productos_maestros (
                            producto_canonico_id,
                            codigo_ean,
                            nombre_normalizado,
                            marca,
                            categoria,
                            total_reportes
                        ) VALUES (?, ?, ?, ?, ?, 1)
                    """, (canonico_id, codigo_ean, nombre, marca, categoria))

            self.conn.commit()

        except Exception as e:
            print(f"⚠️ Error sincronizando producto_maestro: {e}")
            self.conn.rollback()


# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def resolver_producto_simple(
    codigo: str,
    nombre: str,
    establecimiento: str,
    precio: int,
    marca: Optional[str] = None,
    categoria: Optional[str] = None
) -> Tuple[int, int, str]:
    """
    Función de conveniencia para resolver un producto sin manejar conexión

    Returns:
        Tuple[canonico_id, variante_id, accion]
    """
    resolver = ProductResolver()
    try:
        return resolver.resolver_producto(
            codigo=codigo,
            nombre=nombre,
            establecimiento=establecimiento,
            precio=precio,
            marca=marca,
            categoria=categoria
        )
    finally:
        resolver.close()


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("🧪 TESTING PRODUCT RESOLVER")
    print("=" * 80)

    # Test 1: Crear producto nuevo
    print("\n1️⃣ Test: Crear producto nuevo")
    try:
        canonico_id, variante_id, accion = resolver_producto_simple(
            codigo="7702129001234",
            nombre="Leche Colanta Entera 1100ml",
            establecimiento="JUMBO",
            precio=4500,
            marca="Colanta",
            categoria="Lácteos"
        )
        print(f"✅ Canónico ID: {canonico_id}")
        print(f"✅ Variante ID: {variante_id}")
        print(f"✅ Acción: {accion}")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test 2: Buscar producto existente
    print("\n2️⃣ Test: Buscar mismo producto")
    try:
        canonico_id, variante_id, accion = resolver_producto_simple(
            codigo="7702129001234",
            nombre="Leche Colanta Entera 1100ml",
            establecimiento="JUMBO",
            precio=4600,
            marca="Colanta",
            categoria="Lácteos"
        )
        print(f"✅ Canónico ID: {canonico_id}")
        print(f"✅ Variante ID: {variante_id}")
        print(f"✅ Acción: {accion}")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test 3: Mismo EAN, diferente establecimiento
    print("\n3️⃣ Test: Mismo EAN, diferente establecimiento")
    try:
        canonico_id, variante_id, accion = resolver_producto_simple(
            codigo="7702129001234",
            nombre="Leche Colanta Entera 1100ml",
            establecimiento="EXITO",
            precio=4700,
            marca="Colanta",
            categoria="Lácteos"
        )
        print(f"✅ Canónico ID: {canonico_id}")
        print(f"✅ Variante ID: {variante_id}")
        print(f"✅ Acción: {accion}")
    except Exception as e:
        print(f"❌ Error: {e}")

    print("\n" + "=" * 80)
    print("✅ TESTING COMPLETADO")
    print("=" * 80)
