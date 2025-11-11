"""
============================================================================
APRENDIZAJE_MANAGER.PY V1.1 - CORREGIDO
============================================================================
Gestiona el sistema de aprendizaje automático de LecFac

CORRECCIONES V1.1:
- ✅ Usa solo columnas que existen en correcciones_aprendidas
- ✅ Removidas columnas inexistentes: fuente_validacion, requiere_revision, precio_promedio
- ✅ Manejo robusto de errores con rollback

FUNCIONES:
- buscar_correccion_aprendida()
- guardar_correccion_aprendida()
- incrementar_confianza()
- decrementar_confianza()
- crear_validacion_pendiente()
- marcar_para_revision_admin()
============================================================================
"""

from datetime import datetime
from typing import Optional, Dict, Any
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

        print("🧠 AprendizajeManager inicializado")

    def buscar_correccion_aprendida(
        self,
        ocr_normalizado: str,
        establecimiento: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Busca si ya existe una corrección aprendida para un nombre OCR

        Args:
            ocr_normalizado: Nombre normalizado del OCR (uppercase, sin tildes)
            establecimiento: Establecimiento donde se vio (opcional)

        Returns:
            Dict con datos de la corrección o None si no existe
        """

        try:
            placeholder = "%s" if self.es_postgres else "?"

            if establecimiento:
                # Buscar específico por establecimiento
                query = f"""
                    SELECT
                        id,
                        nombre_validado,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado
                    FROM correcciones_aprendidas
                    WHERE ocr_normalizado = {placeholder}
                      AND establecimiento = {placeholder}
                      AND activo = TRUE
                    ORDER BY confianza DESC
                    LIMIT 1
                """
                self.cursor.execute(query, (ocr_normalizado, establecimiento))
            else:
                # Buscar genérico (cualquier establecimiento)
                query = f"""
                    SELECT
                        id,
                        nombre_validado,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado
                    FROM correcciones_aprendidas
                    WHERE ocr_normalizado = {placeholder}
                      AND activo = TRUE
                    ORDER BY confianza DESC
                    LIMIT 1
                """
                self.cursor.execute(query, (ocr_normalizado,))

            resultado = self.cursor.fetchone()

            if resultado:
                return {
                    'id': resultado[0],
                    'nombre_validado': resultado[1],
                    'codigo_ean': resultado[2],
                    'confianza': float(resultado[3]) if resultado[3] else 0.0,
                    'veces_confirmado': resultado[4],
                    'veces_rechazado': resultado[5]
                }

            return None

        except Exception as e:
            print(f"      ⚠️ Error buscando aprendizaje: {e}")
            # Rollback para evitar abortar transacción
            try:
                self.conn.rollback()
            except:
                pass
            return None

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
        Guarda una nueva corrección aprendida

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

            if self.es_postgres:
                # PostgreSQL con RETURNING
                query = f"""
                    INSERT INTO correcciones_aprendidas (
                        ocr_original,
                        ocr_normalizado,
                        nombre_validado,
                        establecimiento,
                        codigo_ean,
                        confianza,
                        veces_confirmado
                    ) VALUES (
                        {placeholder}, {placeholder}, {placeholder}, {placeholder},
                        {placeholder}, {placeholder}, 0
                    )
                    ON CONFLICT (ocr_normalizado, establecimiento)
                    DO UPDATE SET
                        nombre_validado = EXCLUDED.nombre_validado,
                        codigo_ean = COALESCE(EXCLUDED.codigo_ean, correcciones_aprendidas.codigo_ean),
                        confianza = GREATEST(correcciones_aprendidas.confianza, EXCLUDED.confianza),
                        fecha_ultima_confirmacion = CURRENT_TIMESTAMP
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
                return correccion_id

            else:
                # SQLite
                query = f"""
                    INSERT OR REPLACE INTO correcciones_aprendidas (
                        ocr_original,
                        ocr_normalizado,
                        nombre_validado,
                        establecimiento,
                        codigo_ean,
                        confianza,
                        veces_confirmado
                    ) VALUES (?, ?, ?, ?, ?, ?, 0)
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
        Incrementa la confianza de una corrección cuando el usuario confirma

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
                            fecha_ultima_confirmacion = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """
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

    def crear_validacion_pendiente(
        self,
        factura_id: int,
        usuario_id: int,
        item_factura_id: int,
        nombre_sugerido: str,
        nivel_confianza: float,
        nombre_ocr: str = None,
        motivo_duda: str = None
    ) -> Optional[int]:
        """
        Crea una validación pendiente para que el usuario confirme

        Args:
            factura_id: ID de la factura
            usuario_id: ID del usuario
            item_factura_id: ID del item en la factura
            nombre_sugerido: Nombre que se le sugiere al usuario
            nivel_confianza: Nivel de confianza (0.0-1.0)
            nombre_ocr: Nombre original del OCR (opcional)
            motivo_duda: Razón por la que se pregunta (opcional)

        Returns:
            ID de la validación o None si falla
        """

        try:
            placeholder = "%s" if self.es_postgres else "?"

            if self.es_postgres:
                query = f"""
                    INSERT INTO validaciones_pendientes_usuario (
                        usuario_id,
                        factura_id,
                        item_factura_id,
                        nombre_ocr,
                        nombre_sugerido,
                        nivel_confianza,
                        motivo_duda
                    ) VALUES (
                        {placeholder}, {placeholder}, {placeholder}, {placeholder},
                        {placeholder}, {placeholder}, {placeholder}
                    )
                    RETURNING id
                """

                self.cursor.execute(query, (
                    usuario_id,
                    factura_id,
                    item_factura_id,
                    nombre_ocr,
                    nombre_sugerido,
                    nivel_confianza,
                    motivo_duda
                ))

                validacion_id = self.cursor.fetchone()[0]
                self.conn.commit()
                return validacion_id

            else:
                query = f"""
                    INSERT INTO validaciones_pendientes_usuario (
                        usuario_id,
                        factura_id,
                        item_factura_id,
                        nombre_ocr,
                        nombre_sugerido,
                        nivel_confianza,
                        motivo_duda
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """

                self.cursor.execute(query, (
                    usuario_id,
                    factura_id,
                    item_factura_id,
                    nombre_ocr,
                    nombre_sugerido,
                    nivel_confianza,
                    motivo_duda
                ))

                validacion_id = self.cursor.lastrowid
                self.conn.commit()
                return validacion_id

        except Exception as e:
            print(f"      ⚠️ Error creando validación pendiente: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return None

    def marcar_para_revision_admin(
        self,
        producto_maestro_id: int,
        motivo: str,
        detalles: str = None,
        prioridad: int = 5
    ) -> Optional[int]:
        """
        Marca un producto para revisión manual del administrador

        Args:
            producto_maestro_id: ID del producto
            motivo: Motivo de la revisión
            detalles: Detalles adicionales (opcional)
            prioridad: Prioridad 1-10 (1=urgente, 10=baja)

        Returns:
            ID del registro de revisión o None si falla
        """

        try:
            placeholder = "%s" if self.es_postgres else "?"

            if self.es_postgres:
                query = f"""
                    INSERT INTO productos_revision_admin (
                        producto_maestro_id,
                        motivo_revision,
                        detalles,
                        prioridad
                    ) VALUES (
                        {placeholder}, {placeholder}, {placeholder}, {placeholder}
                    )
                    RETURNING id
                """

                self.cursor.execute(query, (
                    producto_maestro_id,
                    motivo,
                    detalles,
                    prioridad
                ))

                revision_id = self.cursor.fetchone()[0]
                self.conn.commit()
                return revision_id

            else:
                query = f"""
                    INSERT INTO productos_revision_admin (
                        producto_maestro_id,
                        motivo_revision,
                        detalles,
                        prioridad
                    ) VALUES (?, ?, ?, ?)
                """

                self.cursor.execute(query, (
                    producto_maestro_id,
                    motivo,
                    detalles,
                    prioridad
                ))

                revision_id = self.cursor.lastrowid
                self.conn.commit()
                return revision_id

        except Exception as e:
            print(f"      ⚠️ Error marcando para revisión: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return None


# ==============================================================================
# MENSAJE DE CARGA
# ==============================================================================

print("=" * 80)
print("✅ aprendizaje_manager.py V1.1 CARGADO")
print("=" * 80)
print("🧠 SISTEMA DE APRENDIZAJE AUTOMÁTICO")
print("   • Buscar correcciones aprendidas")
print("   • Guardar nuevas validaciones")
print("   • Gestionar feedback de usuarios")
print("   • Marcar para revisión admin")
print("=" * 80)
