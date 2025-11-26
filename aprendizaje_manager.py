"""
============================================================================
APRENDIZAJE_MANAGER.PY V2.0 - ACTUALIZADO PARA ESQUEMA RAILWAY
============================================================================
Gestiona el sistema de aprendizaje automático de LecFac

ACTUALIZACIONES V2.0:
- ✅ Adaptado al esquema REAL de Railway (verificado 2024-11-11)
- ✅ Usa todas las columnas existentes correctamente
- ✅ Maneja fecha_primera_vez y fecha_ultima_confirmacion
- ✅ Soporte para activo (boolean)
- ✅ Constraint único: (ocr_normalizado, establecimiento)

ESQUEMA REAL correcciones_aprendidas:
- id (integer)
- ocr_original (text)
- ocr_normalizado (text)
- nombre_validado (text)
- establecimiento (varchar 100)
- confianza (numeric)
- veces_confirmado (integer)
- veces_rechazado (integer)
- fecha_primera_vez (timestamp)
- fecha_ultima_confirmacion (timestamp)
- activo (boolean)
- codigo_ean (varchar 13)

FUNCIONES:
- buscar_correccion_aprendida()
- guardar_correccion_aprendida()
- incrementar_confianza()
- desactivar_correccion()
- obtener_estadisticas()
============================================================================
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
import os


class AprendizajeManager:
    """
    Gestor del sistema de aprendizaje automático
    """

    def __init__(self, cursor, conn):
        """
        Inicializa el gestor de aprendizaje

        Args:
            cursor: Cursor de la base de datos
            conn: Conexión a la base de datos
        """
        self.cursor = cursor
        self.conn = conn
        self.es_postgres = os.getenv('DATABASE_TYPE', 'sqlite').lower() == 'postgresql'

        print("🧠 AprendizajeManager V2.0 inicializado")

    def buscar_correccion_aprendida(
        self,
        ocr_normalizado: str,
        establecimiento: str = None,
        codigo_ean: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Busca si ya existe una corrección aprendida para un nombre OCR

        Prioridad de búsqueda:
        1. Por código EAN (si se proporciona)
        2. Por OCR + establecimiento (específico)
        3. Por OCR solo (genérico)

        Args:
            ocr_normalizado: Nombre normalizado del OCR (uppercase, sin tildes)
            establecimiento: Establecimiento donde se vio (opcional)
            codigo_ean: Código EAN del producto (opcional, prioridad máxima)

        Returns:
            Dict con datos de la corrección o None si no existe
        """

        try:
            placeholder = "%s" if self.es_postgres else "?"

            # PRIORIDAD 1: Búsqueda por EAN (más confiable)
            if codigo_ean:
                query = f"""
                    SELECT
                        id,
                        ocr_original,
                        ocr_normalizado,
                        nombre_validado,
                        establecimiento,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado,
                        fecha_primera_vez,
                        fecha_ultima_confirmacion
                    FROM correcciones_aprendidas
                    WHERE codigo_ean = {placeholder}
                      AND activo = TRUE
                    ORDER BY confianza DESC
                    LIMIT 1
                """
                self.cursor.execute(query, (codigo_ean,))
                resultado = self.cursor.fetchone()

                if resultado:
                    return self._dict_from_row(resultado, fuente='ean')

            # PRIORIDAD 2: Búsqueda por OCR + establecimiento (específico)
            if establecimiento:
                query = f"""
                    SELECT
                        id,
                        ocr_original,
                        ocr_normalizado,
                        nombre_validado,
                        establecimiento,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado,
                        fecha_primera_vez,
                        fecha_ultima_confirmacion
                    FROM correcciones_aprendidas
                    WHERE ocr_normalizado = {placeholder}
                      AND establecimiento = {placeholder}
                      AND activo = TRUE
                    ORDER BY confianza DESC
                    LIMIT 1
                """
                self.cursor.execute(query, (ocr_normalizado, establecimiento))
                resultado = self.cursor.fetchone()

                if resultado:
                    return self._dict_from_row(resultado, fuente='especifico')

            # PRIORIDAD 3: Búsqueda genérica (cualquier establecimiento)
            query = f"""
                SELECT
                    id,
                    ocr_original,
                    ocr_normalizado,
                    nombre_validado,
                    establecimiento,
                    codigo_ean,
                    confianza,
                    veces_confirmado,
                    veces_rechazado,
                    fecha_primera_vez,
                    fecha_ultima_confirmacion
                FROM correcciones_aprendidas
                WHERE ocr_normalizado = {placeholder}
                  AND activo = TRUE
                ORDER BY confianza DESC
                LIMIT 1
            """
            self.cursor.execute(query, (ocr_normalizado,))
            resultado = self.cursor.fetchone()

            if resultado:
                return self._dict_from_row(resultado, fuente='generico')

            return None

        except Exception as e:
            print(f"      ⚠️ Error buscando aprendizaje: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return None

    def _dict_from_row(self, row: tuple, fuente: str = 'desconocido') -> Dict[str, Any]:
        """Convierte una fila de resultados en un diccionario"""
        return {
            'id': row[0],
            'ocr_original': row[1],
            'ocr_normalizado': row[2],
            'nombre_validado': row[3],
            'establecimiento': row[4],
            'codigo_ean': row[5],
            'confianza': float(row[6]) if row[6] else 0.0,
            'veces_confirmado': row[7] or 0,
            'veces_rechazado': row[8] or 0,
            'fecha_primera_vez': row[9],
            'fecha_ultima_confirmacion': row[10],
            'fuente_busqueda': fuente  # Para debugging
        }

    def guardar_correccion_aprendida(
        self,
        ocr_original: str,
        ocr_normalizado: str,
        nombre_validado: str,
        establecimiento: str = None,
        confianza_inicial: float = 0.70,
        codigo_ean: str = None
    ) -> Optional[int]:
        """
        Guarda una nueva corrección aprendida o actualiza existente

        Args:
            ocr_original: Texto original del OCR
            ocr_normalizado: Texto normalizado (uppercase, sin tildes)
            nombre_validado: Nombre correcto validado
            establecimiento: Establecimiento (opcional)
            confianza_inicial: Nivel de confianza inicial (0.0-1.0)
            codigo_ean: Código EAN del producto (opcional)

        Returns:
            ID de la corrección guardada o None si falla
        """

        try:
            placeholder = "%s" if self.es_postgres else "?"

            # Normalizar establecimiento
            if establecimiento:
                establecimiento = establecimiento.upper().strip()

            if self.es_postgres:
    # PostgreSQL con UPSERT
                query = f"""
                    INSERT INTO correcciones_aprendidas (
                    ocr_original,
                    ocr_normalizado,
                    nombre_validado,
                    establecimiento,
                    codigo_ean,
                    confianza,
                    veces_confirmado,
                    veces_rechazado,
                    fecha_primera_vez,
                    fecha_ultima_confirmacion,
                    activo
                ) VALUES (
                    {placeholder}, {placeholder}, {placeholder}, {placeholder},
                    {placeholder}, {placeholder}, 1, 0,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE
                )
                ON CONFLICT (ocr_normalizado, establecimiento)
                DO UPDATE SET
                    nombre_validado = EXCLUDED.nombre_validado,
                    codigo_ean = COALESCE(EXCLUDED.codigo_ean, correcciones_aprendidas.codigo_ean),
                    confianza = LEAST(correcciones_aprendidas.confianza + 0.05, 0.99),
                    veces_confirmado = correcciones_aprendidas.veces_confirmado + 1,
                    fecha_ultima_confirmacion = CURRENT_TIMESTAMP,
                    activo = TRUE
                RETURNING id
            """

                self.cursor.execute(query, (
                    ocr_original,
                    ocr_normalizado,
                    nombre_validado,
                    establecimiento,
                    codigo_ean,
                    confianza_inicial
                ))

                correccion_id = self.cursor.fetchone()[0]
                self.conn.commit()

                print(f"      💾 Aprendizaje guardado: ID={correccion_id}")
                return correccion_id

            else:
                # SQLite - implementación básica
                query = f"""
                    INSERT OR REPLACE INTO correcciones_aprendidas (
                        ocr_original,
                        ocr_normalizado,
                        nombre_validado,
                        establecimiento,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado,
                        fecha_primera_vez,
                        fecha_ultima_confirmacion,
                        activo
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, 0,
                             datetime('now'), datetime('now'), 1)
                """

                self.cursor.execute(query, (
                    ocr_original,
                    ocr_normalizado,
                    nombre_validado,
                    establecimiento,
                    codigo_ean,
                    confianza_inicial
                ))

                correccion_id = self.cursor.lastrowid
                self.conn.commit()
                return correccion_id

        except Exception as e:
            print(f"      ⚠️ Error guardando aprendizaje: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return None

    def incrementar_confianza(
        self,
        correccion_id: int,
        fue_confirmado: bool = True
    ) -> bool:
        """
        Incrementa o decrementa la confianza de una corrección

        Args:
            correccion_id: ID de la corrección
            fue_confirmado: True si usuario confirmó, False si rechazó

        Returns:
            True si se actualizó correctamente
        """

        try:
            placeholder = "%s" if self.es_postgres else "?"

            if fue_confirmado:
                # Incrementar confianza (máximo 0.99)
                if self.es_postgres:
                    query = f"""
                        UPDATE correcciones_aprendidas
                        SET veces_confirmado = veces_confirmado + 1,
                            confianza = LEAST(confianza + 0.05, 0.99),
                            fecha_ultima_confirmacion = CURRENT_TIMESTAMP
                        WHERE id = {placeholder}
                    """
                else:
                    query = f"""
                        UPDATE correcciones_aprendidas
                        SET veces_confirmado = veces_confirmado + 1,
                            confianza = MIN(confianza + 0.05, 0.99),
                            fecha_ultima_confirmacion = datetime('now')
                        WHERE id = ?
                    """

                print(f"      ✅ Confianza incrementada para ID={correccion_id}")
            else:
                # Decrementar confianza (mínimo 0.30)
                if self.es_postgres:
                    query = f"""
                        UPDATE correcciones_aprendidas
                        SET veces_rechazado = veces_rechazado + 1,
                            confianza = GREATEST(confianza - 0.10, 0.30)
                        WHERE id = {placeholder}
                    """
                else:
                    query = f"""
                        UPDATE correcciones_aprendidas
                        SET veces_rechazado = veces_rechazado + 1,
                            confianza = MAX(confianza - 0.10, 0.30)
                        WHERE id = ?
                    """

                print(f"      ⚠️ Confianza decrementada para ID={correccion_id}")

            self.cursor.execute(query, (correccion_id,))
            self.conn.commit()
            return True

        except Exception as e:
            print(f"      ⚠️ Error actualizando confianza: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return False

    def desactivar_correccion(self, correccion_id: int) -> bool:
        """
        Desactiva una corrección (soft delete)

        Args:
            correccion_id: ID de la corrección a desactivar

        Returns:
            True si se desactivó correctamente
        """
        try:
            placeholder = "%s" if self.es_postgres else "?"

            query = f"""
                UPDATE correcciones_aprendidas
                SET activo = FALSE
                WHERE id = {placeholder}
            """

            self.cursor.execute(query, (correccion_id,))
            self.conn.commit()

            print(f"      🚫 Corrección desactivada: ID={correccion_id}")
            return True

        except Exception as e:
            print(f"      ⚠️ Error desactivando corrección: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return False

    def obtener_estadisticas(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del sistema de aprendizaje

        Returns:
            Dict con estadísticas del sistema
        """
        try:
            # Total de correcciones activas
            self.cursor.execute("""
                SELECT COUNT(*)
                FROM correcciones_aprendidas
                WHERE activo = TRUE
            """)
            total_activas = self.cursor.fetchone()[0]

            # Correcciones por confianza
            self.cursor.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE confianza >= 0.80) as alta,
                    COUNT(*) FILTER (WHERE confianza >= 0.60 AND confianza < 0.80) as media,
                    COUNT(*) FILTER (WHERE confianza < 0.60) as baja
                FROM correcciones_aprendidas
                WHERE activo = TRUE
            """)
            por_confianza = self.cursor.fetchone()

            # Productos con EAN
            self.cursor.execute("""
                SELECT COUNT(*)
                FROM correcciones_aprendidas
                WHERE activo = TRUE AND codigo_ean IS NOT NULL
            """)
            con_ean = self.cursor.fetchone()[0]

            # Total de confirmaciones
            self.cursor.execute("""
                SELECT
                    SUM(veces_confirmado),
                    SUM(veces_rechazado)
                FROM correcciones_aprendidas
                WHERE activo = TRUE
            """)
            confirmaciones = self.cursor.fetchone()

            return {
                'total_activas': total_activas,
                'confianza_alta': por_confianza[0] if por_confianza else 0,
                'confianza_media': por_confianza[1] if por_confianza else 0,
                'confianza_baja': por_confianza[2] if por_confianza else 0,
                'con_codigo_ean': con_ean,
                'total_confirmaciones': confirmaciones[0] if confirmaciones[0] else 0,
                'total_rechazos': confirmaciones[1] if confirmaciones[1] else 0
            }

        except Exception as e:
            print(f"      ⚠️ Error obteniendo estadísticas: {e}")
            return {}


# ==============================================================================
# MENSAJE DE CARGA
# ==============================================================================

print("=" * 80)
print("✅ aprendizaje_manager.py V2.0 CARGADO")
print("=" * 80)
print("🧠 SISTEMA DE APRENDIZAJE AUTOMÁTICO")
print("   • Búsqueda inteligente (EAN → Específico → Genérico)")
print("   • Guardar/actualizar correcciones")
print("   • Gestionar confianza y feedback")
print("   • Estadísticas del sistema")
print("=" * 80)
