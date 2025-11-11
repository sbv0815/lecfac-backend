"""
aprendizaje_manager.py - SISTEMA DE APRENDIZAJE AUTOMÁTICO
===========================================================
Versión: 1.0
Propósito: Gestionar el aprendizaje automático del sistema de productos

FLUJO DE APRENDIZAJE:
1. Buscar si ya existe corrección aprendida (evita llamar Perplexity)
2. Si no existe, validar con Perplexity y guardar resultado
3. Incrementar confianza cuando usuarios confirman
4. Marcar para revisión cuando hay dudas

NIVELES DE CONFIANZA:
- ALTA (0.9-1.0):   Usar automáticamente, no preguntar usuario
- MEDIA (0.7-0.89): Preguntar al usuario para confirmar
- BAJA (0.0-0.69):  Marcar para revisión admin
"""

import os
from datetime import datetime
from typing import Optional, Dict, List, Tuple
import json


class AprendizajeManager:
    """
    Gestor del sistema de aprendizaje automático de productos.

    Responsabilidades:
    - Buscar correcciones aprendidas antes de llamar Perplexity
    - Guardar nuevas correcciones validadas
    - Actualizar confianza basada en feedback de usuarios
    - Gestionar validaciones pendientes de usuario
    - Marcar productos para revisión admin
    """

    def __init__(self, cursor, conn):
        """
        Inicializa el gestor de aprendizaje.

        Args:
            cursor: Cursor de base de datos
            conn: Conexión a base de datos
        """
        self.cursor = cursor
        self.conn = conn
        self.is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"

        print("🧠 AprendizajeManager inicializado")


    # =========================================================================
    # BÚSQUEDA DE CORRECCIONES APRENDIDAS
    # =========================================================================

    def buscar_correccion_aprendida(
        self,
        ocr_normalizado: str,
        establecimiento: str = None,
        codigo_ean: str = None
    ) -> Optional[Dict]:
        """
        Busca si ya existe una corrección aprendida para este producto.

        PRIORIDAD DE BÚSQUEDA:
        1. Por código EAN (si existe) - Más confiable
        2. Por OCR + establecimiento - Contexto específico
        3. Por OCR solo - Genérico

        Args:
            ocr_normalizado: Nombre normalizado del OCR
            establecimiento: Supermercado específico (opcional)
            codigo_ean: Código EAN si existe (opcional)

        Returns:
            Dict con corrección aprendida o None si no existe
            {
                'id': int,
                'nombre_validado': str,
                'codigo_ean': str,
                'confianza': float,
                'veces_confirmado': int,
                'fuente_validacion': str,
                'requiere_revision': bool
            }
        """

        # ESTRATEGIA 1: Buscar por código EAN (más confiable)
        if codigo_ean:
            if self.is_postgresql:
                self.cursor.execute("""
                    SELECT
                        id,
                        nombre_validado,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado,
                        fuente_validacion,
                        requiere_revision,
                        precio_promedio
                    FROM correcciones_aprendidas
                    WHERE codigo_ean = %s
                      AND NOT requiere_revision
                    ORDER BY confianza DESC
                    LIMIT 1
                """, (codigo_ean,))
            else:
                self.cursor.execute("""
                    SELECT
                        id,
                        nombre_validado,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado,
                        fuente_validacion,
                        requiere_revision,
                        precio_promedio
                    FROM correcciones_aprendidas
                    WHERE codigo_ean = ?
                      AND requiere_revision = 0
                    ORDER BY confianza DESC
                    LIMIT 1
                """, (codigo_ean,))

            row = self.cursor.fetchone()
            if row:
                print(f"   🎯 Corrección aprendida encontrada por EAN: {codigo_ean}")
                return self._row_to_dict(row)

        # ESTRATEGIA 2: Buscar por OCR + establecimiento
        if establecimiento:
            if self.is_postgresql:
                self.cursor.execute("""
                    SELECT
                        id,
                        nombre_validado,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado,
                        fuente_validacion,
                        requiere_revision,
                        precio_promedio
                    FROM correcciones_aprendidas
                    WHERE ocr_normalizado = %s
                      AND establecimiento = %s
                      AND NOT requiere_revision
                    ORDER BY confianza DESC
                    LIMIT 1
                """, (ocr_normalizado, establecimiento))
            else:
                self.cursor.execute("""
                    SELECT
                        id,
                        nombre_validado,
                        codigo_ean,
                        confianza,
                        veces_confirmado,
                        veces_rechazado,
                        fuente_validacion,
                        requiere_revision,
                        precio_promedio
                    FROM correcciones_aprendidas
                    WHERE ocr_normalizado = ?
                      AND establecimiento = ?
                      AND requiere_revision = 0
                    ORDER BY confianza DESC
                    LIMIT 1
                """, (ocr_normalizado, establecimiento))

            row = self.cursor.fetchone()
            if row:
                print(f"   🎯 Corrección aprendida encontrada por OCR + establecimiento")
                return self._row_to_dict(row)

        # ESTRATEGIA 3: Buscar por OCR solo (sin establecimiento)
        if self.is_postgresql:
            self.cursor.execute("""
                SELECT
                    id,
                    nombre_validado,
                    codigo_ean,
                    confianza,
                    veces_confirmado,
                    veces_rechazado,
                    fuente_validacion,
                    requiere_revision,
                    precio_promedio
                FROM correcciones_aprendidas
                WHERE ocr_normalizado = %s
                  AND NOT requiere_revision
                ORDER BY confianza DESC
                LIMIT 1
            """, (ocr_normalizado,))
        else:
            self.cursor.execute("""
                SELECT
                    id,
                    nombre_validado,
                    codigo_ean,
                    confianza,
                    veces_confirmado,
                    veces_rechazado,
                    fuente_validacion,
                    requiere_revision,
                    precio_promedio
                FROM correcciones_aprendidas
                WHERE ocr_normalizado = ?
                  AND requiere_revision = 0
                ORDER BY confianza DESC
                LIMIT 1
            """, (ocr_normalizado,))

        row = self.cursor.fetchone()
        if row:
            print(f"   🎯 Corrección aprendida encontrada por OCR genérico")
            return self._row_to_dict(row)

        print(f"   ℹ️  No hay corrección aprendida para: {ocr_normalizado}")
        return None


    def _row_to_dict(self, row) -> Dict:
        """Convierte un row de BD a diccionario."""
        return {
            'id': row[0],
            'nombre_validado': row[1],
            'codigo_ean': row[2],
            'confianza': float(row[3]),
            'veces_confirmado': row[4],
            'veces_rechazado': row[5],
            'fuente_validacion': row[6],
            'requiere_revision': bool(row[7]),
            'precio_promedio': row[8]
        }


    # =========================================================================
    # GUARDAR NUEVAS CORRECCIONES
    # =========================================================================

    def guardar_correccion_aprendida(
        self,
        ocr_original: str,
        ocr_normalizado: str,
        nombre_validado: str,
        codigo_ean: str = None,
        establecimiento: str = None,
        precio: int = None,
        confianza_inicial: float = 0.7,
        fuente_validacion: str = 'perplexity',
        fue_validado_manual: bool = False
    ) -> int:
        """
        Guarda una nueva corrección aprendida en la base de datos.

        Args:
            ocr_original: Texto original del OCR
            ocr_normalizado: Texto normalizado del OCR
            nombre_validado: Nombre final validado
            codigo_ean: Código EAN (opcional)
            establecimiento: Supermercado (opcional)
            precio: Precio del producto (opcional)
            confianza_inicial: Nivel de confianza inicial (0.0-1.0)
            fuente_validacion: Origen ('perplexity', 'usuario', 'admin')
            fue_validado_manual: Si fue confirmado manualmente

        Returns:
            int: ID de la corrección creada
        """

        print(f"\n   💾 Guardando corrección aprendida:")
        print(f"      OCR: {ocr_original} → Validado: {nombre_validado}")
        print(f"      Confianza inicial: {confianza_inicial}")
        print(f"      Fuente: {fuente_validacion}")

        try:
            if self.is_postgresql:
                self.cursor.execute("""
                    INSERT INTO correcciones_aprendidas (
                        ocr_original,
                        ocr_normalizado,
                        nombre_validado,
                        codigo_ean,
                        establecimiento,
                        precio_promedio,
                        confianza,
                        fuente_validacion,
                        fue_validado_manual,
                        veces_confirmado,
                        fecha_primera_vez,
                        fecha_ultima_vez
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, 1,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (ocr_normalizado, establecimiento)
                    DO UPDATE SET
                        nombre_validado = EXCLUDED.nombre_validado,
                        confianza = EXCLUDED.confianza,
                        veces_confirmado = correcciones_aprendidas.veces_confirmado + 1,
                        fecha_ultima_vez = CURRENT_TIMESTAMP
                    RETURNING id
                """, (
                    ocr_original,
                    ocr_normalizado,
                    nombre_validado,
                    codigo_ean,
                    establecimiento,
                    precio,
                    confianza_inicial,
                    fuente_validacion,
                    fue_validado_manual
                ))
                correccion_id = self.cursor.fetchone()[0]
            else:
                # SQLite: Verificar si existe primero
                self.cursor.execute("""
                    SELECT id FROM correcciones_aprendidas
                    WHERE ocr_normalizado = ?
                      AND (establecimiento = ? OR (establecimiento IS NULL AND ? IS NULL))
                """, (ocr_normalizado, establecimiento, establecimiento))

                existing = self.cursor.fetchone()

                if existing:
                    # Actualizar existente
                    correccion_id = existing[0]
                    self.cursor.execute("""
                        UPDATE correcciones_aprendidas
                        SET nombre_validado = ?,
                            confianza = ?,
                            veces_confirmado = veces_confirmado + 1,
                            fecha_ultima_vez = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (nombre_validado, confianza_inicial, correccion_id))
                else:
                    # Insertar nuevo
                    self.cursor.execute("""
                        INSERT INTO correcciones_aprendidas (
                            ocr_original,
                            ocr_normalizado,
                            nombre_validado,
                            codigo_ean,
                            establecimiento,
                            precio_promedio,
                            confianza,
                            fuente_validacion,
                            fue_validado_manual,
                            veces_confirmado
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """, (
                        ocr_original,
                        ocr_normalizado,
                        nombre_validado,
                        codigo_ean,
                        establecimiento,
                        precio,
                        confianza_inicial,
                        fuente_validacion,
                        fue_validado_manual
                    ))
                    correccion_id = self.cursor.lastrowid

            self.conn.commit()
            print(f"      ✅ Corrección guardada con ID: {correccion_id}")
            return correccion_id

        except Exception as e:
            print(f"      ❌ Error guardando corrección: {e}")
            self.conn.rollback()
            return None


    # =========================================================================
    # ACTUALIZAR CONFIANZA (FEEDBACK DE USUARIOS)
    # =========================================================================

    def incrementar_confianza(self, correccion_id: int, fue_confirmado: bool = True):
        """
        Actualiza la confianza de una corrección basada en feedback.

        Args:
            correccion_id: ID de la corrección aprendida
            fue_confirmado: True si usuario confirmó, False si rechazó
        """

        if fue_confirmado:
            # Usuario confirmó que es correcto
            if self.is_postgresql:
                self.cursor.execute("""
                    UPDATE correcciones_aprendidas
                    SET veces_confirmado = veces_confirmado + 1,
                        confianza = LEAST(1.0, confianza + 0.05),
                        fecha_ultima_vez = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (correccion_id,))
            else:
                self.cursor.execute("""
                    UPDATE correcciones_aprendidas
                    SET veces_confirmado = veces_confirmado + 1,
                        confianza = MIN(1.0, confianza + 0.05),
                        fecha_ultima_vez = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (correccion_id,))

            print(f"   ✅ Confianza incrementada para corrección ID {correccion_id}")
        else:
            # Usuario rechazó - bajar confianza y marcar para revisión
            if self.is_postgresql:
                self.cursor.execute("""
                    UPDATE correcciones_aprendidas
                    SET veces_rechazado = veces_rechazado + 1,
                        confianza = GREATEST(0.0, confianza - 0.1),
                        requiere_revision = TRUE,
                        fecha_ultima_vez = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (correccion_id,))
            else:
                self.cursor.execute("""
                    UPDATE correcciones_aprendidas
                    SET veces_rechazado = veces_rechazado + 1,
                        confianza = MAX(0.0, confianza - 0.1),
                        requiere_revision = 1,
                        fecha_ultima_vez = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (correccion_id,))

            print(f"   ⚠️  Confianza reducida para corrección ID {correccion_id}")

        self.conn.commit()


    # =========================================================================
    # VALIDACIONES PENDIENTES DE USUARIO
    # =========================================================================

    def crear_validacion_pendiente(
        self,
        factura_id: int,
        usuario_id: int,
        item_factura_id: int,
        ocr_original: str,
        nombre_sugerido: str,
        codigo_ean: str = None,
        precio: int = None,
        establecimiento: str = None,
        nivel_confianza: float = 0.5,
        motivo_duda: str = "Primera vez detectado",
        datos_perplexity: Dict = None,
        datos_ocr: Dict = None
    ) -> int:
        """
        Crea una validación pendiente para que el usuario confirme.

        Se usa cuando el sistema tiene dudas (confianza 0.7-0.89)

        Args:
            factura_id: ID de la factura
            usuario_id: ID del usuario
            item_factura_id: ID del item en la factura
            ocr_original: Texto original del OCR
            nombre_sugerido: Nombre que el sistema sugiere
            codigo_ean: Código EAN (opcional)
            precio: Precio del producto
            establecimiento: Supermercado
            nivel_confianza: Nivel de confianza (0.0-1.0)
            motivo_duda: Razón por la que se pregunta al usuario
            datos_perplexity: Datos completos de Perplexity (JSON)
            datos_ocr: Datos completos del OCR (JSON)

        Returns:
            int: ID de la validación pendiente creada
        """

        print(f"\n   ⚠️  Creando validación pendiente para usuario:")
        print(f"      Producto: {nombre_sugerido}")
        print(f"      Confianza: {nivel_confianza}")
        print(f"      Motivo: {motivo_duda}")

        try:
            # Convertir dicts a JSON
            datos_perplexity_json = json.dumps(datos_perplexity) if datos_perplexity else None
            datos_ocr_json = json.dumps(datos_ocr) if datos_ocr else None

            if self.is_postgresql:
                self.cursor.execute("""
                    INSERT INTO validaciones_pendientes_usuario (
                        factura_id,
                        usuario_id,
                        item_factura_id,
                        ocr_original,
                        nombre_sugerido,
                        codigo_ean,
                        precio,
                        establecimiento,
                        nivel_confianza,
                        motivo_duda,
                        estado,
                        datos_perplexity,
                        datos_ocr
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s
                    )
                    RETURNING id
                """, (
                    factura_id,
                    usuario_id,
                    item_factura_id,
                    ocr_original,
                    nombre_sugerido,
                    codigo_ean,
                    precio,
                    establecimiento,
                    nivel_confianza,
                    motivo_duda,
                    datos_perplexity_json,
                    datos_ocr_json
                ))
                validacion_id = self.cursor.fetchone()[0]
            else:
                self.cursor.execute("""
                    INSERT INTO validaciones_pendientes_usuario (
                        factura_id,
                        usuario_id,
                        item_factura_id,
                        ocr_original,
                        nombre_sugerido,
                        codigo_ean,
                        precio,
                        establecimiento,
                        nivel_confianza,
                        motivo_duda,
                        estado,
                        datos_perplexity,
                        datos_ocr
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pendiente', ?, ?
                    )
                """, (
                    factura_id,
                    usuario_id,
                    item_factura_id,
                    ocr_original,
                    nombre_sugerido,
                    codigo_ean,
                    precio,
                    establecimiento,
                    nivel_confianza,
                    motivo_duda,
                    datos_perplexity_json,
                    datos_ocr_json
                ))
                validacion_id = self.cursor.lastrowid

            self.conn.commit()
            print(f"      ✅ Validación pendiente creada con ID: {validacion_id}")
            return validacion_id

        except Exception as e:
            print(f"      ❌ Error creando validación pendiente: {e}")
            self.conn.rollback()
            return None


    def obtener_validaciones_pendientes_usuario(self, usuario_id: int) -> List[Dict]:
        """
        Obtiene todas las validaciones pendientes de un usuario.

        Args:
            usuario_id: ID del usuario

        Returns:
            List[Dict]: Lista de validaciones pendientes
        """

        if self.is_postgresql:
            self.cursor.execute("""
                SELECT
                    id,
                    factura_id,
                    item_factura_id,
                    ocr_original,
                    nombre_sugerido,
                    codigo_ean,
                    precio,
                    establecimiento,
                    nivel_confianza,
                    motivo_duda,
                    fecha_creacion
                FROM validaciones_pendientes_usuario
                WHERE usuario_id = %s
                  AND estado = 'pendiente'
                ORDER BY fecha_creacion DESC
            """, (usuario_id,))
        else:
            self.cursor.execute("""
                SELECT
                    id,
                    factura_id,
                    item_factura_id,
                    ocr_original,
                    nombre_sugerido,
                    codigo_ean,
                    precio,
                    establecimiento,
                    nivel_confianza,
                    motivo_duda,
                    fecha_creacion
                FROM validaciones_pendientes_usuario
                WHERE usuario_id = ?
                  AND estado = 'pendiente'
                ORDER BY fecha_creacion DESC
            """, (usuario_id,))

        rows = self.cursor.fetchall()

        validaciones = []
        for row in rows:
            validaciones.append({
                'id': row[0],
                'factura_id': row[1],
                'item_factura_id': row[2],
                'ocr_original': row[3],
                'nombre_sugerido': row[4],
                'codigo_ean': row[5],
                'precio': row[6],
                'establecimiento': row[7],
                'nivel_confianza': float(row[8]),
                'motivo_duda': row[9],
                'fecha_creacion': row[10]
            })

        return validaciones


    def procesar_respuesta_usuario(
        self,
        validacion_id: int,
        usuario_confirmo: bool,
        nombre_corregido: str = None,
        codigo_corregido: str = None
    ) -> bool:
        """
        Procesa la respuesta del usuario a una validación pendiente.

        Args:
            validacion_id: ID de la validación pendiente
            usuario_confirmo: True si confirmó, False si corrigió
            nombre_corregido: Nombre corregido por usuario (si lo corrigió)
            codigo_corregido: Código corregido por usuario (si lo corrigió)

        Returns:
            bool: True si se procesó correctamente
        """

        try:
            if usuario_confirmo:
                # Usuario confirmó que el nombre sugerido es correcto
                estado = 'confirmado'
                print(f"   ✅ Usuario confirmó validación ID {validacion_id}")
            else:
                # Usuario corrigió el producto
                estado = 'corregido'
                print(f"   ✏️  Usuario corrigió validación ID {validacion_id}")
                print(f"      Nuevo nombre: {nombre_corregido}")

            if self.is_postgresql:
                self.cursor.execute("""
                    UPDATE validaciones_pendientes_usuario
                    SET estado = %s,
                        nombre_corregido_usuario = %s,
                        codigo_corregido_usuario = %s,
                        fecha_respuesta = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (estado, nombre_corregido, codigo_corregido, validacion_id))
            else:
                self.cursor.execute("""
                    UPDATE validaciones_pendientes_usuario
                    SET estado = ?,
                        nombre_corregido_usuario = ?,
                        codigo_corregido_usuario = ?,
                        fecha_respuesta = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (estado, nombre_corregido, codigo_corregido, validacion_id))

            self.conn.commit()
            return True

        except Exception as e:
            print(f"   ❌ Error procesando respuesta usuario: {e}")
            self.conn.rollback()
            return False


    # =========================================================================
    # PRODUCTOS PARA REVISIÓN ADMIN
    # =========================================================================

    def marcar_para_revision_admin(
        self,
        producto_maestro_id: int,
        nombre_actual: str,
        codigo_ean: str = None,
        motivo_revision: str = 'confianza_baja',
        prioridad: int = 5,
        detalles: Dict = None
    ) -> int:
        """
        Marca un producto para que el admin lo revise manualmente.

        Args:
            producto_maestro_id: ID del producto maestro
            nombre_actual: Nombre actual del producto
            codigo_ean: Código EAN (opcional)
            motivo_revision: Razón de la revisión
            prioridad: 1 (urgente) a 10 (puede esperar)
            detalles: Información adicional (JSON)

        Returns:
            int: ID del registro de revisión
        """

        print(f"\n   🚨 Marcando para revisión admin:")
        print(f"      Producto ID: {producto_maestro_id}")
        print(f"      Nombre: {nombre_actual}")
        print(f"      Motivo: {motivo_revision}")
        print(f"      Prioridad: {prioridad}/10")

        try:
            detalles_json = json.dumps(detalles) if detalles else None

            if self.is_postgresql:
                self.cursor.execute("""
                    INSERT INTO productos_revision_admin (
                        producto_maestro_id,
                        nombre_actual,
                        codigo_ean,
                        motivo_revision,
                        prioridad,
                        detalles_json,
                        estado
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'pendiente')
                    RETURNING id
                """, (
                    producto_maestro_id,
                    nombre_actual,
                    codigo_ean,
                    motivo_revision,
                    prioridad,
                    detalles_json
                ))
                revision_id = self.cursor.fetchone()[0]
            else:
                self.cursor.execute("""
                    INSERT INTO productos_revision_admin (
                        producto_maestro_id,
                        nombre_actual,
                        codigo_ean,
                        motivo_revision,
                        prioridad,
                        detalles_json,
                        estado
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pendiente')
                """, (
                    producto_maestro_id,
                    nombre_actual,
                    codigo_ean,
                    motivo_revision,
                    prioridad,
                    detalles_json
                ))
                revision_id = self.cursor.lastrowid

            self.conn.commit()
            print(f"      ✅ Marcado para revisión con ID: {revision_id}")
            return revision_id

        except Exception as e:
            print(f"      ❌ Error marcando para revisión: {e}")
            self.conn.rollback()
            return None


    # =========================================================================
    # HISTORIAL DE VALIDACIONES (PARA ANÁLISIS)
    # =========================================================================

    def registrar_en_historial(
        self,
        factura_id: int,
        usuario_id: int,
        producto_maestro_id: int,
        ocr_original: str,
        nombre_python: str,
        nombre_perplexity: str,
        nombre_final: str,
        tuvo_correccion_python: bool,
        fue_validado_perplexity: bool,
        fue_validado_usuario: bool,
        confianza_final: float,
        fuente_final: str,
        datos_completos: Dict = None
    ):
        """
        Registra el proceso completo de validación en el historial.

        Útil para análisis posteriores y mejora del sistema.
        """

        try:
            datos_json = json.dumps(datos_completos) if datos_completos else None

            if self.is_postgresql:
                self.cursor.execute("""
                    INSERT INTO historial_validaciones (
                        factura_id,
                        usuario_id,
                        producto_maestro_id,
                        ocr_original,
                        nombre_python,
                        nombre_perplexity,
                        nombre_final,
                        tuvo_correccion_python,
                        fue_validado_perplexity,
                        fue_validado_usuario,
                        confianza_final,
                        fuente_final,
                        datos_completos
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    factura_id,
                    usuario_id,
                    producto_maestro_id,
                    ocr_original,
                    nombre_python,
                    nombre_perplexity,
                    nombre_final,
                    tuvo_correccion_python,
                    fue_validado_perplexity,
                    fue_validado_usuario,
                    confianza_final,
                    fuente_final,
                    datos_json
                ))
            else:
                self.cursor.execute("""
                    INSERT INTO historial_validaciones (
                        factura_id,
                        usuario_id,
                        producto_maestro_id,
                        ocr_original,
                        nombre_python,
                        nombre_perplexity,
                        nombre_final,
                        tuvo_correccion_python,
                        fue_validado_perplexity,
                        fue_validado_usuario,
                        confianza_final,
                        fuente_final,
                        datos_completos
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, (
                    factura_id,
                    usuario_id,
                    producto_maestro_id,
                    ocr_original,
                    nombre_python,
                    nombre_perplexity,
                    nombre_final,
                    tuvo_correccion_python,
                    fue_validado_perplexity,
                    fue_validado_usuario,
                    confianza_final,
                    fuente_final,
                    datos_json
                ))

            self.conn.commit()

        except Exception as e:
            print(f"   ⚠️  Error registrando en historial: {e}")
            # No hacer rollback, esto es solo logging


# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def calcular_nivel_confianza(
    fue_validado_perplexity: bool,
    precio_similar: bool,
    tiene_codigo_ean: bool,
    veces_visto_antes: int = 0
) -> Tuple[float, str]:
    """
    Calcula el nivel de confianza de una validación.

    Returns:
        Tuple[float, str]: (confianza, categoria)
        donde categoria es 'alta', 'media' o 'baja'
    """
    confianza = 0.5  # Base

    # Factores que aumentan confianza
    if fue_validado_perplexity:
        confianza += 0.2
    if precio_similar:
        confianza += 0.1
    if tiene_codigo_ean:
        confianza += 0.1
    if veces_visto_antes > 0:
        confianza += min(0.1, veces_visto_antes * 0.02)

    # Limitar entre 0 y 1
    confianza = max(0.0, min(1.0, confianza))

    # Categorizar
    if confianza >= 0.9:
        categoria = 'alta'
    elif confianza >= 0.7:
        categoria = 'media'
    else:
        categoria = 'baja'

    return confianza, categoria


print("=" * 80)
print("✅ aprendizaje_manager.py CARGADO")
print("=" * 80)
print("🧠 SISTEMA DE APRENDIZAJE AUTOMÁTICO")
print("   • Buscar correcciones aprendidas")
print("   • Guardar nuevas validaciones")
print("   • Gestionar feedback de usuarios")
print("   • Marcar para revisión admin")
print("=" * 80)
