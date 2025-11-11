"""
============================================================================
MATCHING_INTELIGENTE.PY V6.1 - PRODUCTOS_REFERENCIA COMO FUENTE PRIORITARIA
============================================================================
Sistema que integra TODAS las fuentes de datos con productos_referencia
como VERDAD ABSOLUTA para validación y enriquecimiento

JERARQUÍA DE CONFIABILIDAD (ACTUALIZADA):
1. 🥇 productos_referencia (app escaneo EAN) - MÁXIMA CONFIANZA (100%)
   └─→ VERDAD ABSOLUTA: Ignora nombre OCR, usa datos de referencia
   └─→ Enriquece automáticamente productos_maestros
   └─→ Vincula códigos PLU si existen en la factura

2. 🥈 productos_maestros + validación referencia - ALTA CONFIANZA
   └─→ Si existe en maestros: Validar contra productos_referencia
   └─→ CONFLICTO detectado: Priorizar productos_referencia
   └─→ Actualizar automáticamente con datos correctos

3. 🥉 codigos_establecimiento (PLU) + enriquecimiento - MEDIA-ALTA
   └─→ Buscar producto vinculado
   └─→ Si tiene EAN: Enriquecer con productos_referencia
   └─→ Actualizar datos faltantes

4. 🧠 correcciones_aprendidas (validadas) - MEDIA
5. 🔍 Perplexity (primera vez) - BAJA

FLUJO COMPLETO ACTUALIZADO:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📄 FACTURA PROCESADA (OCR)
   ├─ Código EAN o PLU detectado
   ├─ Nombre OCR (puede estar mal escrito)
   ├─ Precio
   └─ Establecimiento

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CASO 1: FACTURA CON EAN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣ Buscar en productos_referencia (PRIMERO)
   └─→ ✅ ENCONTRADO:
       ├─ Usar TODOS los datos de referencia (NO el OCR)
       ├─ Sincronizar a productos_maestros
       ├─ Si factura tiene PLU: Vincular en codigos_establecimiento
       ├─ Log: Validación cruzada OCR vs Referencia
       └─ RETORNAR datos oficiales ✅

   └─→ ❌ NO ENCONTRADO:
       ├─ Buscar en productos_maestros
       ├─ Si existe: Usar pero marcar para revisión
       └─ Si no existe: Validar con Perplexity

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CASO 2: FACTURA SOLO CON PLU
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
2️⃣ Buscar en codigos_establecimiento
   └─→ ✅ ENCONTRADO:
       ├─ Obtener producto_maestro_id
       ├─ Si tiene EAN: Buscar en productos_referencia
       ├─ Enriquecer con datos faltantes
       └─ RETORNAR datos enriquecidos ✅

   └─→ ❌ NO ENCONTRADO:
       ├─ Validar con Perplexity
       └─ Crear nuevo + Guardar PLU

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CASO 3: PRODUCTO NUEVO (NO EXISTE EN NINGUNA FUENTE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
3️⃣ Validar con Perplexity
   └─→ Crear en productos_maestros
   └─→ Si tiene EAN: Sugerir agregar a productos_referencia (app)
   └─→ Si tiene PLU: Guardar en codigos_establecimiento

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
============================================================================
"""

import os
import re
from typing import Optional, Dict, Any, Tuple
from datetime import datetime


class MatchingInteligente:
    """
    Sistema inteligente de matching con productos_referencia prioritario
    """

    def __init__(self, cursor, conn, aprendizaje_mgr=None):
        """
        Inicializa el sistema de matching

        Args:
            cursor: Cursor de la base de datos
            conn: Conexión a la base de datos
            aprendizaje_mgr: Manager de aprendizaje (opcional)
        """
        self.cursor = cursor
        self.conn = conn
        self.aprendizaje_mgr = aprendizaje_mgr
        self.es_postgres = os.getenv('DATABASE_TYPE', 'sqlite').lower() == 'postgresql'

        # Estadísticas de matching
        self.stats = {
            'fuente_referencia': 0,
            'fuente_referencia_enriquecimiento': 0,
            'fuente_maestros': 0,
            'fuente_maestros_validados': 0,
            'fuente_codigos_est': 0,
            'fuente_aprendizaje': 0,
            'fuente_perplexity': 0,
            'productos_creados': 0,
            'productos_actualizados': 0,
            'plu_vinculados': 0,
            'conflictos_resueltos': 0,
            'errores': 0
        }

    def clasificar_codigo(self, codigo: str, cadena: str = None) -> Dict[str, Any]:
        """
        Clasifica un código y determina su tipo

        Args:
            codigo: Código del producto
            cadena: Cadena comercial (opcional)

        Returns:
            Dict con tipo, validez y confiabilidad
        """
        if not codigo or len(codigo) < 4:
            return {
                'tipo': 'invalido',
                'valido': False,
                'confiabilidad': 0.0
            }

        codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))
        longitud = len(codigo_limpio)

        # EAN (códigos de barras estándar)
        if longitud in (8, 13, 14):
            return {
                'tipo': 'EAN',
                'codigo_limpio': codigo_limpio,
                'valido': True,
                'confiabilidad': 1.0,
                'descripcion': f'Código EAN-{longitud} estándar internacional'
            }

        # PLU estándar (frutas/verduras internacional)
        if longitud in (4, 5):
            try:
                if 3000 <= int(codigo_limpio) <= 4999:
                    return {
                        'tipo': 'PLU_ESTANDAR',
                        'codigo_limpio': codigo_limpio,
                        'valido': True,
                        'confiabilidad': 0.8,
                        'descripcion': 'PLU estándar internacional (frutas/verduras)'
                    }
            except:
                pass

        # PLU local (específico del establecimiento)
        if longitud in (4, 5, 6):
            return {
                'tipo': 'PLU_LOCAL',
                'codigo_limpio': codigo_limpio,
                'valido': True,
                'confiabilidad': 0.7 if cadena else 0.5,
                'descripcion': f'PLU local del establecimiento ({cadena or "desconocido"})'
            }

        # UPC (usado en algunos productos)
        if longitud == 12:
            return {
                'tipo': 'UPC',
                'codigo_limpio': codigo_limpio,
                'valido': True,
                'confiabilidad': 0.9,
                'descripcion': 'Código UPC-12'
            }

        # Código interno/otro
        return {
            'tipo': 'OTRO',
            'codigo_limpio': codigo_limpio,
            'valido': False,
            'confiabilidad': 0.3,
            'descripcion': 'Código no reconocido'
        }

    def buscar_en_productos_referencia(self, codigo_ean: str) -> Optional[Dict[str, Any]]:
        """
        🥇 FUENTE 1: Busca en productos_referencia (app de escaneo)
        MÁXIMA CONFIABILIDAD - VERDAD ABSOLUTA

        Args:
            codigo_ean: Código EAN del producto

        Returns:
            Dict con datos del producto o None
        """
        try:
            placeholder = "%s" if self.es_postgres else "?"

            query = f"""
                SELECT
                    id,
                    codigo_ean,
                    nombre,
                    marca,
                    categoria,
                    presentacion,
                    unidad_medida,
                    created_at
                FROM productos_referencia
                WHERE codigo_ean = {placeholder}
                LIMIT 1
            """

            self.cursor.execute(query, (codigo_ean,))
            resultado = self.cursor.fetchone()

            if resultado:
                print(f"   🥇 ENCONTRADO en productos_referencia (VERDAD ABSOLUTA)")
                self.stats['fuente_referencia'] += 1

                return {
                    'encontrado': True,
                    'fuente': 'productos_referencia',
                    'confianza': 1.0,
                    'id_referencia': resultado[0],
                    'codigo_ean': resultado[1],
                    'nombre': resultado[2],
                    'marca': resultado[3],
                    'categoria': resultado[4],
                    'presentacion': resultado[5],
                    'unidad_medida': resultado[6],
                    'fecha_registro': resultado[7]
                }

            return None

        except Exception as e:
            print(f"   ⚠️ Error buscando en productos_referencia: {e}")
            self.stats['errores'] += 1
            return None

    def buscar_en_productos_maestros(self, codigo_ean: str) -> Optional[Dict[str, Any]]:
        """
        🥈 FUENTE 2: Busca en productos_maestros

        Args:
            codigo_ean: Código EAN del producto

        Returns:
            Dict con datos del producto o None
        """
        try:
            placeholder = "%s" if self.es_postgres else "?"

            query = f"""
                SELECT
                    id,
                    codigo_ean,
                    nombre_normalizado,
                    marca,
                    categoria,
                    presentacion,
                    precio_promedio_global,
                    total_reportes,
                    auditado_manualmente,
                    validaciones_manuales
                FROM productos_maestros
                WHERE codigo_ean = {placeholder}
                LIMIT 1
            """

            self.cursor.execute(query, (codigo_ean,))
            resultado = self.cursor.fetchone()

            if resultado:
                print(f"   🥈 ENCONTRADO en productos_maestros")
                self.stats['fuente_maestros'] += 1

                validaciones = resultado[9] or 0
                auditado = resultado[8] or False

                if auditado and validaciones >= 3:
                    confianza = 0.95
                elif validaciones >= 2:
                    confianza = 0.85
                elif validaciones >= 1:
                    confianza = 0.75
                else:
                    confianza = 0.65

                return {
                    'encontrado': True,
                    'fuente': 'productos_maestros',
                    'confianza': confianza,
                    'id': resultado[0],
                    'codigo_ean': resultado[1],
                    'nombre': resultado[2],
                    'marca': resultado[3],
                    'categoria': resultado[4],
                    'presentacion': resultado[5],
                    'precio_promedio': resultado[6],
                    'total_reportes': resultado[7],
                    'auditado': auditado,
                    'validaciones': validaciones
                }

            return None

        except Exception as e:
            print(f"   ⚠️ Error buscando en productos_maestros: {e}")
            self.stats['errores'] += 1
            return None

    def buscar_en_codigos_establecimiento(
        self,
        codigo_plu: str,
        establecimiento_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        🥉 FUENTE 3: Busca PLU en codigos_establecimiento

        Args:
            codigo_plu: Código PLU local
            establecimiento_id: ID del establecimiento

        Returns:
            Dict con datos del producto o None
        """
        try:
            placeholder = "%s" if self.es_postgres else "?"

            query = f"""
                SELECT
                    ce.id,
                    ce.producto_maestro_id,
                    ce.codigo_local,
                    ce.tipo_codigo,
                    ce.veces_visto,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    pm.presentacion
                FROM codigos_establecimiento ce
                INNER JOIN productos_maestros pm ON ce.producto_maestro_id = pm.id
                WHERE ce.codigo_local = {placeholder}
                  AND ce.establecimiento_id = {placeholder}
                  AND ce.activo = TRUE
                ORDER BY ce.veces_visto DESC
                LIMIT 1
            """

            self.cursor.execute(query, (codigo_plu, establecimiento_id))
            resultado = self.cursor.fetchone()

            if resultado:
                veces_visto = resultado[4]

                if veces_visto >= 10:
                    confianza = 0.90
                elif veces_visto >= 5:
                    confianza = 0.80
                elif veces_visto >= 2:
                    confianza = 0.70
                else:
                    confianza = 0.60

                print(f"   🥉 ENCONTRADO en codigos_establecimiento (visto {veces_visto} veces)")
                self.stats['fuente_codigos_est'] += 1

                return {
                    'encontrado': True,
                    'fuente': 'codigos_establecimiento',
                    'confianza': confianza,
                    'id_codigo': resultado[0],
                    'producto_maestro_id': resultado[1],
                    'codigo_plu': resultado[2],
                    'tipo_codigo': resultado[3],
                    'veces_visto': veces_visto,
                    'codigo_ean': resultado[5],
                    'nombre': resultado[6],
                    'marca': resultado[7],
                    'categoria': resultado[8],
                    'presentacion': resultado[9]
                }

            return None

        except Exception as e:
            print(f"   ⚠️ Error buscando en codigos_establecimiento: {e}")
            self.stats['errores'] += 1
            return None

    def validar_nombre_ocr_vs_referencia(
        self,
        nombre_ocr: str,
        nombre_referencia: str
    ) -> Dict[str, Any]:
        """
        Valida el nombre OCR contra el nombre de referencia

        Args:
            nombre_ocr: Nombre detectado por OCR
            nombre_referencia: Nombre oficial de productos_referencia

        Returns:
            Dict con análisis de la validación
        """
        # Normalizar para comparación
        ocr_norm = nombre_ocr.lower().strip()
        ref_norm = nombre_referencia.lower().strip()

        # Calcular similitud básica
        palabras_ocr = set(ocr_norm.split())
        palabras_ref = set(ref_norm.split())

        if palabras_ref:
            palabras_comunes = palabras_ocr.intersection(palabras_ref)
            similitud = len(palabras_comunes) / len(palabras_ref)
        else:
            similitud = 0.0

        # Determinar calidad del OCR
        if similitud >= 0.8:
            calidad = "EXCELENTE"
            emoji = "✅"
        elif similitud >= 0.6:
            calidad = "BUENA"
            emoji = "✓"
        elif similitud >= 0.4:
            calidad = "REGULAR"
            emoji = "⚠️"
        else:
            calidad = "MALA"
            emoji = "❌"

        return {
            'similitud': similitud,
            'calidad': calidad,
            'emoji': emoji,
            'nombre_ocr': nombre_ocr,
            'nombre_referencia': nombre_referencia,
            'usar_referencia': True  # SIEMPRE usar referencia
        }

    def sincronizar_referencia_a_maestros(
        self,
        datos_referencia: Dict[str, Any],
        precio: int = None,
        nombre_ocr: str = None
    ) -> int:
        """
        Sincroniza un producto de referencia a productos_maestros
        Actualiza datos si hay conflictos

        Args:
            datos_referencia: Datos del producto de referencia
            precio: Precio actual (opcional)
            nombre_ocr: Nombre OCR para validación cruzada (opcional)

        Returns:
            ID del producto en productos_maestros
        """
        try:
            # Validación cruzada OCR vs Referencia
            if nombre_ocr:
                validacion = self.validar_nombre_ocr_vs_referencia(
                    nombre_ocr,
                    datos_referencia['nombre']
                )
                print(f"\n   🔍 VALIDACIÓN CRUZADA:")
                print(f"      OCR:        {validacion['nombre_ocr']}")
                print(f"      REFERENCIA: {validacion['nombre_referencia']}")
                print(f"      Similitud:  {validacion['similitud']:.2%} - {validacion['emoji']} {validacion['calidad']}")
                print(f"      → Usando datos de REFERENCIA (VERDAD ABSOLUTA)")

            # Verificar si ya existe en productos_maestros
            resultado_maestros = self.buscar_en_productos_maestros(datos_referencia['codigo_ean'])

            if resultado_maestros:
                producto_id = resultado_maestros['id']

                # Detectar conflictos
                conflictos = []
                if datos_referencia['nombre'] != resultado_maestros['nombre']:
                    conflictos.append(f"nombre: '{resultado_maestros['nombre']}' → '{datos_referencia['nombre']}'")
                if datos_referencia.get('marca') and datos_referencia['marca'] != resultado_maestros.get('marca'):
                    conflictos.append(f"marca: '{resultado_maestros.get('marca')}' → '{datos_referencia['marca']}'")
                if datos_referencia.get('categoria') and datos_referencia['categoria'] != resultado_maestros.get('categoria'):
                    conflictos.append(f"categoría: '{resultado_maestros.get('categoria')}' → '{datos_referencia['categoria']}'")

                if conflictos:
                    print(f"\n   ⚠️ CONFLICTOS DETECTADOS en productos_maestros:")
                    for conflicto in conflictos:
                        print(f"      • {conflicto}")
                    print(f"   🔄 RESOLVIENDO: Priorizando datos de productos_referencia...")

                    placeholder = "%s" if self.es_postgres else "?"

                    self.cursor.execute(f"""
                        UPDATE productos_maestros
                        SET nombre_normalizado = {placeholder},
                            marca = {placeholder},
                            categoria = {placeholder},
                            presentacion = {placeholder},
                            auditado_manualmente = TRUE,
                            validaciones_manuales = COALESCE(validaciones_manuales, 0) + 1,
                            ultima_validacion = CURRENT_TIMESTAMP
                        WHERE id = {placeholder}
                    """, (
                        datos_referencia['nombre'],
                        datos_referencia.get('marca'),
                        datos_referencia.get('categoria'),
                        datos_referencia.get('presentacion'),
                        producto_id
                    ))

                    self.conn.commit()
                    self.stats['conflictos_resueltos'] += 1
                    self.stats['productos_actualizados'] += 1
                    print(f"      ✅ Producto actualizado con datos de referencia")

                return producto_id

            else:
                # No existe, crear nuevo
                print(f"\n   ➕ Creando en productos_maestros desde productos_referencia...")

                placeholder = "%s" if self.es_postgres else "?"

                if self.es_postgres:
                    self.cursor.execute(f"""
                        INSERT INTO productos_maestros (
                            codigo_ean,
                            nombre_normalizado,
                            marca,
                            categoria,
                            presentacion,
                            precio_promedio_global,
                            total_reportes,
                            auditado_manualmente,
                            validaciones_manuales
                        ) VALUES (
                            {placeholder}, {placeholder}, {placeholder}, {placeholder},
                            {placeholder}, {placeholder}, 1, TRUE, 1
                        )
                        RETURNING id
                    """, (
                        datos_referencia['codigo_ean'],
                        datos_referencia['nombre'],
                        datos_referencia.get('marca'),
                        datos_referencia.get('categoria'),
                        datos_referencia.get('presentacion'),
                        precio
                    ))

                    producto_id = self.cursor.fetchone()[0]
                else:
                    self.cursor.execute(f"""
                        INSERT INTO productos_maestros (
                            codigo_ean,
                            nombre_normalizado,
                            marca,
                            categoria,
                            presentacion,
                            precio_promedio_global,
                            total_reportes,
                            auditado_manualmente,
                            validaciones_manuales
                        ) VALUES (?, ?, ?, ?, ?, ?, 1, 1, 1)
                    """, (
                        datos_referencia['codigo_ean'],
                        datos_referencia['nombre'],
                        datos_referencia.get('marca'),
                        datos_referencia.get('categoria'),
                        datos_referencia.get('presentacion'),
                        precio
                    ))

                    producto_id = self.cursor.lastrowid

                self.conn.commit()
                print(f"      ✅ Producto creado ID: {producto_id}")
                self.stats['productos_creados'] += 1

                return producto_id

        except Exception as e:
            print(f"   ❌ Error sincronizando: {e}")
            self.stats['errores'] += 1
            try:
                self.conn.rollback()
            except:
                pass
            return None

    def enriquecer_con_referencia(
        self,
        producto_maestro_id: int,
        codigo_ean: str
    ) -> Optional[Dict[str, Any]]:
        """
        Enriquece un producto con datos de productos_referencia

        Args:
            producto_maestro_id: ID del producto en maestros
            codigo_ean: Código EAN del producto

        Returns:
            Dict con datos enriquecidos o None
        """
        try:
            if not codigo_ean:
                return None

            # Buscar en productos_referencia
            resultado_ref = self.buscar_en_productos_referencia(codigo_ean)

            if resultado_ref:
                print(f"\n   🔄 ENRIQUECIENDO con productos_referencia...")
                self.stats['fuente_referencia_enriquecimiento'] += 1

                placeholder = "%s" if self.es_postgres else "?"

                # Actualizar productos_maestros con datos de referencia
                self.cursor.execute(f"""
                    UPDATE productos_maestros
                    SET nombre_normalizado = {placeholder},
                        marca = COALESCE({placeholder}, marca),
                        categoria = COALESCE({placeholder}, categoria),
                        presentacion = COALESCE({placeholder}, presentacion),
                        auditado_manualmente = TRUE,
                        validaciones_manuales = COALESCE(validaciones_manuales, 0) + 1,
                        ultima_validacion = CURRENT_TIMESTAMP
                    WHERE id = {placeholder}
                """, (
                    resultado_ref['nombre'],
                    resultado_ref.get('marca'),
                    resultado_ref.get('categoria'),
                    resultado_ref.get('presentacion'),
                    producto_maestro_id
                ))

                self.conn.commit()
                print(f"      ✅ Producto enriquecido con datos de referencia")
                self.stats['productos_actualizados'] += 1

                return resultado_ref

            return None

        except Exception as e:
            print(f"   ⚠️ Error enriqueciendo: {e}")
            self.stats['errores'] += 1
            return None

    def vincular_plu_a_producto(
        self,
        producto_maestro_id: int,
        establecimiento_id: int,
        codigo_plu: str,
        tipo_codigo: str
    ) -> bool:
        """
        Vincula un código PLU de la factura al producto encontrado

        Args:
            producto_maestro_id: ID del producto maestro
            establecimiento_id: ID del establecimiento
            codigo_plu: Código PLU de la factura
            tipo_codigo: Tipo de código

        Returns:
            True si se vinculó correctamente
        """
        try:
            if not codigo_plu or not producto_maestro_id:
                return False

            placeholder = "%s" if self.es_postgres else "?"

            if self.es_postgres:
                self.cursor.execute(f"""
                    INSERT INTO codigos_establecimiento (
                        producto_maestro_id,
                        establecimiento_id,
                        codigo_local,
                        tipo_codigo,
                        veces_visto,
                        activo
                    ) VALUES (
                        {placeholder}, {placeholder}, {placeholder}, {placeholder}, 1, TRUE
                    )
                    ON CONFLICT (producto_maestro_id, establecimiento_id, codigo_local)
                    DO UPDATE SET
                        veces_visto = codigos_establecimiento.veces_visto + 1,
                        ultima_vez_visto = CURRENT_TIMESTAMP,
                        activo = TRUE
                """, (producto_maestro_id, establecimiento_id, codigo_plu, tipo_codigo))
            else:
                # SQLite
                self.cursor.execute(f"""
                    INSERT OR REPLACE INTO codigos_establecimiento (
                        producto_maestro_id,
                        establecimiento_id,
                        codigo_local,
                        tipo_codigo,
                        veces_visto,
                        activo
                    ) VALUES (?, ?, ?, ?, 1, 1)
                """, (producto_maestro_id, establecimiento_id, codigo_plu, tipo_codigo))

            self.conn.commit()
            print(f"\n   🔗 PLU VINCULADO: {codigo_plu} → Producto {producto_maestro_id}")
            self.stats['plu_vinculados'] += 1
            return True

        except Exception as e:
            print(f"   ⚠️ Error vinculando PLU: {e}")
            self.stats['errores'] += 1
            try:
                self.conn.rollback()
            except:
                pass
            return False

    def buscar_producto_completo(
        self,
        codigo: str,
        nombre_ocr: str,
        precio: int,
        establecimiento_id: int,
        cadena: str = None,
        codigo_plu_factura: str = None
    ) -> Dict[str, Any]:
        """
        🎯 FUNCIÓN PRINCIPAL: Busca un producto usando TODAS las fuentes
        con productos_referencia como PRIORIDAD ABSOLUTA

        Args:
            codigo: Código del producto (EAN o PLU)
            nombre_ocr: Nombre detectado por OCR
            precio: Precio del producto
            establecimiento_id: ID del establecimiento
            cadena: Cadena comercial
            codigo_plu_factura: Código PLU adicional en la factura (opcional)

        Returns:
            Dict con resultado del matching
        """
        print(f"\n{'='*70}")
        print(f"🔍 MATCHING INTELIGENTE V6.1 - PRODUCTOS_REFERENCIA PRIORITARIO")
        print(f"{'='*70}")
        print(f"Código: {codigo} | Precio: ${precio:,} | {cadena or 'N/A'}")
        print(f"OCR: {nombre_ocr}")
        if codigo_plu_factura:
            print(f"PLU adicional: {codigo_plu_factura}")

        # Clasificar código
        clasificacion = self.clasificar_codigo(codigo, cadena)
        print(f"\n📋 Código: {clasificacion['tipo']} - {clasificacion.get('descripcion', 'N/A')}")

        # ================================================================
        # CASO 1: CÓDIGO EAN - BUSCAR EN PRODUCTOS_REFERENCIA PRIMERO
        # ================================================================

        if clasificacion['tipo'] == 'EAN':
            print(f"\n🔍 Buscando EAN {codigo} en productos_referencia...")

            # 1️⃣ PRODUCTOS_REFERENCIA (MÁXIMA PRIORIDAD)
            resultado_ref = self.buscar_en_productos_referencia(codigo)

            if resultado_ref:
                # ✅ ENCONTRADO en productos_referencia
                print(f"\n   ✅ Usando datos de PRODUCTOS_REFERENCIA (VERDAD ABSOLUTA)")

                # Sincronizar a productos_maestros
                producto_id = self.sincronizar_referencia_a_maestros(
                    resultado_ref,
                    precio,
                    nombre_ocr
                )

                # Si hay PLU en la factura, vincularlo
                if codigo_plu_factura and producto_id:
                    tipo_plu = 'PLU_LOCAL' if len(codigo_plu_factura) <= 6 else 'OTRO'
                    self.vincular_plu_a_producto(
                        producto_id,
                        establecimiento_id,
                        codigo_plu_factura,
                        tipo_plu
                    )

                return {
                    'producto_maestro_id': producto_id,
                    'nombre_final': resultado_ref['nombre'],
                    'marca': resultado_ref.get('marca'),
                    'categoria': resultado_ref.get('categoria'),
                    'presentacion': resultado_ref.get('presentacion'),
                    'fuente': 'productos_referencia',
                    'confianza': 1.0,
                    'es_nuevo': False,
                    'requiere_validacion': False,
                    'validado_con_referencia': True
                }

            # 2️⃣ NO encontrado en referencia, buscar en productos_maestros
            print(f"\n   ℹ️ No encontrado en productos_referencia")
            print(f"   🔍 Buscando en productos_maestros...")

            resultado_maestros = self.buscar_en_productos_maestros(codigo)

            if resultado_maestros:
                # Existe en maestros pero no en referencia
                print(f"\n   ⚠️ Producto existe en maestros pero NO en productos_referencia")
                print(f"   💡 SUGERENCIA: Escanear en la app para agregar a productos_referencia")

                self.stats['fuente_maestros_validados'] += 1

                # Si hay PLU, vincularlo
                if codigo_plu_factura:
                    tipo_plu = 'PLU_LOCAL' if len(codigo_plu_factura) <= 6 else 'OTRO'
                    self.vincular_plu_a_producto(
                        resultado_maestros['id'],
                        establecimiento_id,
                        codigo_plu_factura,
                        tipo_plu
                    )

                return {
                    'producto_maestro_id': resultado_maestros['id'],
                    'nombre_final': resultado_maestros['nombre'],
                    'marca': resultado_maestros.get('marca'),
                    'categoria': resultado_maestros.get('categoria'),
                    'presentacion': resultado_maestros.get('presentacion'),
                    'fuente': 'productos_maestros',
                    'confianza': resultado_maestros['confianza'],
                    'es_nuevo': False,
                    'requiere_validacion': True,
                    'validado_con_referencia': False,
                    'sugerencia': 'Escanear en app para validar con productos_referencia'
                }

        # ================================================================
        # CASO 2: CÓDIGO PLU - BUSCAR EN CODIGOS_ESTABLECIMIENTO
        # ================================================================

        elif clasificacion['tipo'] in ('PLU_LOCAL', 'PLU_ESTANDAR'):
            print(f"\n🔍 Buscando PLU {codigo} en {cadena}...")

            resultado_plu = self.buscar_en_codigos_establecimiento(codigo, establecimiento_id)

            if resultado_plu:
                # PLU encontrado, verificar si tiene EAN para enriquecer
                print(f"\n   ✅ PLU encontrado")

                if resultado_plu.get('codigo_ean'):
                    print(f"   🔍 Producto tiene EAN: {resultado_plu['codigo_ean']}")
                    print(f"   🔍 Buscando en productos_referencia para enriquecer...")

                    # Intentar enriquecer con productos_referencia
                    datos_ref = self.enriquecer_con_referencia(
                        resultado_plu['producto_maestro_id'],
                        resultado_plu['codigo_ean']
                    )

                    if datos_ref:
                        # Enriquecido exitosamente
                        return {
                            'producto_maestro_id': resultado_plu['producto_maestro_id'],
                            'nombre_final': datos_ref['nombre'],
                            'marca': datos_ref.get('marca'),
                            'categoria': datos_ref.get('categoria'),
                            'presentacion': datos_ref.get('presentacion'),
                            'fuente': 'codigos_establecimiento + productos_referencia',
                            'confianza': 1.0,
                            'es_nuevo': False,
                            'requiere_validacion': False,
                            'enriquecido_con_referencia': True,
                            'codigo_plu': codigo
                        }

                # No se pudo enriquecer o no tiene EAN
                return {
                    'producto_maestro_id': resultado_plu['producto_maestro_id'],
                    'nombre_final': resultado_plu['nombre'],
                    'marca': resultado_plu.get('marca'),
                    'categoria': resultado_plu.get('categoria'),
                    'presentacion': resultado_plu.get('presentacion'),
                    'fuente': 'codigos_establecimiento',
                    'confianza': resultado_plu['confianza'],
                    'es_nuevo': False,
                    'requiere_validacion': resultado_plu['veces_visto'] < 3,
                    'enriquecido_con_referencia': False,
                    'codigo_plu': codigo
                }

        # ================================================================
        # CASO 3: PRODUCTO NO ENCONTRADO - REQUIERE VALIDACIÓN
        # ================================================================

        print(f"\n⚠️ Producto NO encontrado en ninguna fuente")
        print(f"   → Requiere validación con Perplexity")
        print(f"   → Crear nuevo producto")
        print(f"   💡 Si tiene EAN: Sugerir escanear en app")

        return {
            'producto_maestro_id': None,
            'nombre_final': nombre_ocr,
            'fuente': 'no_encontrado',
            'confianza': 0.0,
            'es_nuevo': True,
            'requiere_validacion': True,
            'validado_con_referencia': False,
            'codigo': codigo,
            'tipo_codigo': clasificacion['tipo'],
            'sugerencia': 'Escanear en app si tiene código EAN' if clasificacion['tipo'] == 'EAN' else None
        }

    def imprimir_estadisticas(self):
        """Imprime estadísticas detalladas del matching"""
        print(f"\n{'='*70}")
        print(f"📊 ESTADÍSTICAS DE MATCHING V6.1")
        print(f"{'='*70}")
        print(f"🥇 Productos Referencia: {self.stats['fuente_referencia']}")
        print(f"   └─ Enriquecimientos: {self.stats['fuente_referencia_enriquecimiento']}")
        print(f"🥈 Productos Maestros: {self.stats['fuente_maestros']}")
        print(f"   └─ Validados con referencia: {self.stats['fuente_maestros_validados']}")
        print(f"🥉 Códigos Establecimiento: {self.stats['fuente_codigos_est']}")
        print(f"🧠 Aprendizaje: {self.stats['fuente_aprendizaje']}")
        print(f"🔍 Perplexity: {self.stats['fuente_perplexity']}")
        print(f"\n📝 ACCIONES:")
        print(f"   ➕ Productos Creados: {self.stats['productos_creados']}")
        print(f"   🔄 Productos Actualizados: {self.stats['productos_actualizados']}")
        print(f"   🔗 PLU Vinculados: {self.stats['plu_vinculados']}")
        print(f"   ⚠️ Conflictos Resueltos: {self.stats['conflictos_resueltos']}")
        print(f"   ❌ Errores: {self.stats['errores']}")
        print(f"{'='*70}\n")


# ==============================================================================
# MENSAJE DE CARGA
# ==============================================================================

print("="*80)
print("✅ matching_inteligente.py V6.1 CARGADO")
print("="*80)
print("🎯 PRODUCTOS_REFERENCIA COMO FUENTE PRIORITARIA:")
print("   🥇 productos_referencia (VERDAD ABSOLUTA - 100% confianza)")
print("      └─ Ignora nombre OCR, usa datos oficiales")
print("      └─ Sincroniza automáticamente a productos_maestros")
print("      └─ Vincula códigos PLU de la factura")
print("   🥈 productos_maestros (validación con referencia)")
print("      └─ Detecta y resuelve conflictos automáticamente")
print("   🥉 codigos_establecimiento (enriquecimiento con referencia)")
print("      └─ Si tiene EAN: busca en productos_referencia")
print("   🧠 correcciones_aprendidas (validadas)")
print("   🔍 Perplexity (cuando sea necesario)")
print("="*80)
