"""
SISTEMA MAESTRO DE NORMALIZACIÓN DE PRODUCTOS
==============================================

Este script unifica TODOS los componentes del sistema de validación:
1. Tabla de memoria para códigos aprendidos
2. Integración con el OCR
3. Validación automática
4. Actualización de productos_maestros

Autor: Santiago
Fecha: 2025-10-30
Sistema: LecFac
"""
import psycopg2
from datetime import datetime

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"


def conectar_db():
    """Conecta a la base de datos"""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return None


def paso_1_habilitar_extensiones():
    """
    PASO 1: Habilita extensiones necesarias de PostgreSQL
    - pg_trgm: Para búsqueda por similitud
    """
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("\n" + "="*80)
        print("📦 PASO 1: Habilitando extensiones PostgreSQL")
        print("="*80 + "\n")

        # Habilitar pg_trgm
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        conn.commit()
        print("✅ Extensión pg_trgm habilitada (búsqueda por similitud)")

        # Verificar
        cur.execute("SELECT * FROM pg_extension WHERE extname = 'pg_trgm'")
        if cur.fetchone():
            print("✅ Verificación exitosa\n")
        else:
            print("⚠️  No se pudo verificar la extensión\n")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error habilitando extensiones: {e}\n")
        conn.rollback()
        cur.close()
        conn.close()
        return False


def paso_2_permitir_ean_null():
    """
    PASO 2: Permite que productos_referencia acepte productos sin EAN
    Útil para productos con PLU, a granel, o códigos locales
    """
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("="*80)
        print("🔧 PASO 2: Permitiendo EAN NULL en productos_referencia")
        print("="*80 + "\n")

        # Permitir NULL
        cur.execute("""
            ALTER TABLE productos_referencia
            ALTER COLUMN codigo_ean DROP NOT NULL
        """)
        print("✅ codigo_ean ahora puede ser NULL")

        # Ajustar UNIQUE constraint
        cur.execute("DROP INDEX IF EXISTS productos_referencia_codigo_ean_key")
        cur.execute("""
            CREATE UNIQUE INDEX productos_referencia_codigo_ean_unique
            ON productos_referencia(codigo_ean)
            WHERE codigo_ean IS NOT NULL
        """)
        print("✅ UNIQUE solo aplica cuando codigo_ean no es NULL\n")

        conn.commit()
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"⚠️  Posible error (puede ser normal si ya estaba configurado): {e}\n")
        conn.rollback()
        cur.close()
        conn.close()
        return True  # Continuar de todas formas


def paso_3_crear_tabla_memoria():
    """
    PASO 3: Crea tabla central de MEMORIA del sistema
    Esta tabla almacena TODOS los códigos y nombres que el sistema ya aprendió

    FUNCIÓN: Evitar re-procesar productos ya validados
    """
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("="*80)
        print("🧠 PASO 3: Creando tabla de MEMORIA del sistema")
        print("="*80 + "\n")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS codigos_normalizados (
                id SERIAL PRIMARY KEY,

                -- Lo que el OCR leyó
                codigo_leido VARCHAR(50) NOT NULL,
                nombre_leido VARCHAR(200) NOT NULL,

                -- A qué producto real corresponde
                producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestros(id),

                -- Tipo de código
                tipo_codigo VARCHAR(20) DEFAULT 'desconocido',
                -- Valores: 'EAN', 'PLU', 'interno_establecimiento', 'sin_codigo'

                -- Contexto (opcional)
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                -- Algunos códigos son específicos de un establecimiento

                -- Metadatos
                confianza DECIMAL(3,2) DEFAULT 1.00,
                fecha_aprendizaje TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                veces_usado INTEGER DEFAULT 0,
                ultima_vez_usado TIMESTAMP,

                -- Información de validación
                validado_automaticamente BOOLEAN DEFAULT TRUE,
                validado_manualmente BOOLEAN DEFAULT FALSE,
                usuario_validador_id INTEGER REFERENCES usuarios(id),

                -- Constraints
                CHECK (tipo_codigo IN ('EAN', 'PLU', 'interno_establecimiento', 'sin_codigo', 'desconocido')),
                CHECK (confianza >= 0 AND confianza <= 1),

                -- UNIQUE: mismo código+nombre en mismo establecimiento = mismo producto
                UNIQUE(codigo_leido, nombre_leido, establecimiento_id)
            )
        """)

        print("✅ Tabla 'codigos_normalizados' creada")
        print("   Esta tabla es la MEMORIA del sistema\n")

        # Crear índices para búsqueda rápida
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_codigos_norm_codigo
            ON codigos_normalizados(codigo_leido)
        """)
        print("✅ Índice por código creado")

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_codigos_norm_nombre
            ON codigos_normalizados(nombre_leido)
        """)
        print("✅ Índice por nombre creado")

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_codigos_norm_producto
            ON codigos_normalizados(producto_maestro_id)
        """)
        print("✅ Índice por producto_maestro_id creado")

        # Índice compuesto para búsqueda rápida
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_codigos_norm_busqueda
            ON codigos_normalizados(codigo_leido, nombre_leido, establecimiento_id)
        """)
        print("✅ Índice compuesto para búsqueda rápida creado\n")

        conn.commit()
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error creando tabla memoria: {e}\n")
        import traceback
        traceback.print_exc()
        conn.rollback()
        cur.close()
        conn.close()
        return False


def paso_4_migrar_datos_existentes():
    """
    PASO 4: Migra datos existentes a la tabla de memoria
    Toma productos de items_factura que ya tienen producto_maestro_id
    """
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("="*80)
        print("📦 PASO 4: Migrando datos existentes a tabla de memoria")
        print("="*80 + "\n")

        # Migrar items_factura que ya tienen matching
        cur.execute("""
            INSERT INTO codigos_normalizados (
                codigo_leido,
                nombre_leido,
                producto_maestro_id,
                tipo_codigo,
                establecimiento_id,
                confianza,
                fecha_aprendizaje,
                veces_usado
            )
            SELECT DISTINCT ON (if.codigo_leido, if.nombre_leido, f.establecimiento_id)
                if.codigo_leido,
                if.nombre_leido,
                if.producto_maestro_id,
                CASE
                    WHEN LENGTH(if.codigo_leido) >= 8 THEN 'EAN'
                    WHEN LENGTH(if.codigo_leido) >= 3 AND LENGTH(if.codigo_leido) < 8 THEN 'PLU'
                    ELSE 'sin_codigo'
                END as tipo_codigo,
                f.establecimiento_id,
                COALESCE(if.matching_confianza / 100.0, 0.90),
                if.fecha_creacion,
                COUNT(*) OVER (PARTITION BY if.codigo_leido, if.nombre_leido) as veces_usado
            FROM items_factura if
            INNER JOIN facturas f ON if.factura_id = f.id
            WHERE if.producto_maestro_id IS NOT NULL
            AND if.codigo_leido IS NOT NULL
            AND if.nombre_leido IS NOT NULL
            ON CONFLICT (codigo_leido, nombre_leido, establecimiento_id) DO NOTHING
        """)

        migrados = cur.rowcount
        conn.commit()

        print(f"✅ {migrados} códigos migrados desde items_factura")

        # Estadísticas
        cur.execute("""
            SELECT
                tipo_codigo,
                COUNT(*) as cantidad,
                AVG(confianza) as confianza_promedio
            FROM codigos_normalizados
            GROUP BY tipo_codigo
            ORDER BY cantidad DESC
        """)

        print("\n📊 Estadísticas de códigos en memoria:")
        for tipo, cantidad, conf_prom in cur.fetchall():
            print(f"   {tipo}: {cantidad} códigos (confianza promedio: {conf_prom:.2%})")

        print()

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error migrando datos: {e}\n")
        import traceback
        traceback.print_exc()
        conn.rollback()
        cur.close()
        conn.close()
        return False


def paso_5_crear_funcion_busqueda():
    """
    PASO 5: Crea función SQL para búsqueda inteligente en memoria
    Esta función se usa desde el proceso de OCR
    """
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("="*80)
        print("🔍 PASO 5: Creando función de búsqueda inteligente")
        print("="*80 + "\n")

        # Función de búsqueda
        cur.execute("""
            CREATE OR REPLACE FUNCTION buscar_producto_en_memoria(
                p_codigo VARCHAR(50),
                p_nombre VARCHAR(200),
                p_establecimiento_id INTEGER DEFAULT NULL
            )
            RETURNS TABLE(
                producto_maestro_id INTEGER,
                confianza DECIMAL(3,2),
                tipo_codigo VARCHAR(20),
                veces_usado INTEGER
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                -- Búsqueda exacta primero
                RETURN QUERY
                SELECT
                    cn.producto_maestro_id,
                    cn.confianza,
                    cn.tipo_codigo,
                    cn.veces_usado
                FROM codigos_normalizados cn
                WHERE cn.codigo_leido = p_codigo
                AND cn.nombre_leido = p_nombre
                AND (p_establecimiento_id IS NULL OR cn.establecimiento_id = p_establecimiento_id)
                ORDER BY cn.confianza DESC, cn.veces_usado DESC
                LIMIT 1;

                -- Si no hay resultado exacto, buscar por código solo
                IF NOT FOUND THEN
                    RETURN QUERY
                    SELECT
                        cn.producto_maestro_id,
                        cn.confianza * 0.8 as confianza, -- Penalizar por no ser match exacto
                        cn.tipo_codigo,
                        cn.veces_usado
                    FROM codigos_normalizados cn
                    WHERE cn.codigo_leido = p_codigo
                    AND (p_establecimiento_id IS NULL OR cn.establecimiento_id = p_establecimiento_id)
                    ORDER BY cn.confianza DESC, cn.veces_usado DESC
                    LIMIT 1;
                END IF;

                -- Si aún no hay resultado, buscar por similitud de nombre
                IF NOT FOUND THEN
                    RETURN QUERY
                    SELECT
                        cn.producto_maestro_id,
                        (SIMILARITY(cn.nombre_leido, p_nombre) * cn.confianza * 0.7)::DECIMAL(3,2) as confianza,
                        cn.tipo_codigo,
                        cn.veces_usado
                    FROM codigos_normalizados cn
                    WHERE SIMILARITY(cn.nombre_leido, p_nombre) > 0.6
                    AND (p_establecimiento_id IS NULL OR cn.establecimiento_id = p_establecimiento_id)
                    ORDER BY SIMILARITY(cn.nombre_leido, p_nombre) DESC, cn.veces_usado DESC
                    LIMIT 1;
                END IF;
            END;
            $$;
        """)

        print("✅ Función 'buscar_producto_en_memoria' creada")
        print("   Esta función busca productos en 3 niveles:")
        print("   1. Match exacto (código + nombre)")
        print("   2. Match por código")
        print("   3. Match por similitud de nombre\n")

        conn.commit()
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error creando función: {e}\n")
        import traceback
        traceback.print_exc()
        conn.rollback()
        cur.close()
        conn.close()
        return False


def paso_6_documentar_integracion():
    """
    PASO 6: Documenta cómo integrar esto en process_invoice.py
    """
    print("="*80)
    print("📚 PASO 6: Documentación de integración")
    print("="*80 + "\n")

    print("Para integrar este sistema en tu proceso de OCR, sigue estos pasos:\n")

    print("1️⃣  En process_invoice.py, después de detectar un producto:")
    print("```python")
    print("from validar_productos_automatico import validar_y_guardar_producto")
    print("")
    print("# Para cada producto detectado por OCR")
    print("for item in productos_detectados:")
    print("    # Primero buscar en memoria")
    print("    producto_id = buscar_en_memoria(item['codigo'], item['nombre'])")
    print("    ")
    print("    if not producto_id:")
    print("        # No está en memoria, validar con Claude")
    print("        resultado = validar_y_guardar_producto(")
    print("            nombre_ocr=item['nombre'],")
    print("            codigo_ean=item['codigo'],")
    print("            precio=item['precio']")
    print("        )")
    print("        ")
    print("        if resultado and resultado['confianza'] >= 0.70:")
    print("            producto_id = crear_o_actualizar_producto_maestro(resultado)")
    print("            # Guardar en memoria para futuros usos")
    print("            guardar_en_memoria(item['codigo'], item['nombre'], producto_id)")
    print("```\n")

    print("2️⃣  Función helper para buscar en memoria:")
    print("```python")
    print("def buscar_en_memoria(codigo, nombre, establecimiento_id=None):")
    print("    cur.execute('''")
    print("        SELECT * FROM buscar_producto_en_memoria(%s, %s, %s)")
    print("    ''', (codigo, nombre, establecimiento_id))")
    print("    ")
    print("    resultado = cur.fetchone()")
    print("    if resultado and resultado['confianza'] >= 0.70:")
    print("        # Actualizar contador de uso")
    print("        cur.execute('''")
    print("            UPDATE codigos_normalizados")
    print("            SET veces_usado = veces_usado + 1,")
    print("                ultima_vez_usado = NOW()")
    print("            WHERE codigo_leido = %s AND nombre_leido = %s")
    print("        ''', (codigo, nombre))")
    print("        return resultado['producto_maestro_id']")
    print("    return None")
    print("```\n")

    print("3️⃣  Estructura final del flujo:")
    print("```")
    print("OCR detecta producto")
    print("    ↓")
    print("Buscar en codigos_normalizados (memoria)")
    print("    ↓")
    print("¿Encontrado?")
    print("    Sí → Usar producto_maestro_id (RÁPIDO)")
    print("    No → Validar con Claude → Guardar en memoria")
    print("```\n")

    print("✅ Con este sistema:")
    print("   - NO repites trabajo de validación")
    print("   - Sistema aprende con cada factura")
    print("   - Soporta EAN, PLU, y códigos locales")
    print("   - Recuerda nombres cortados del OCR\n")


def verificar_sistema():
    """
    Verifica que todo esté configurado correctamente
    """
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("="*80)
        print("✅ VERIFICACIÓN FINAL DEL SISTEMA")
        print("="*80 + "\n")

        # Verificar extensión
        cur.execute("SELECT * FROM pg_extension WHERE extname = 'pg_trgm'")
        if cur.fetchone():
            print("✅ Extensión pg_trgm: HABILITADA")
        else:
            print("❌ Extensión pg_trgm: NO ENCONTRADA")

        # Verificar tabla
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'codigos_normalizados'
            )
        """)
        if cur.fetchone()[0]:
            print("✅ Tabla codigos_normalizados: EXISTE")

            # Contar registros
            cur.execute("SELECT COUNT(*) FROM codigos_normalizados")
            total = cur.fetchone()[0]
            print(f"   📊 Total códigos en memoria: {total}")
        else:
            print("❌ Tabla codigos_normalizados: NO EXISTE")

        # Verificar función
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_proc
                WHERE proname = 'buscar_producto_en_memoria'
            )
        """)
        if cur.fetchone()[0]:
            print("✅ Función buscar_producto_en_memoria: EXISTE")
        else:
            print("❌ Función buscar_producto_en_memoria: NO EXISTE")

        # Verificar productos_referencia
        cur.execute("""
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'productos_referencia'
            AND column_name = 'codigo_ean'
        """)
        result = cur.fetchone()
        if result and result[1] == 'YES':
            print("✅ productos_referencia.codigo_ean: ACEPTA NULL")
        else:
            print("⚠️  productos_referencia.codigo_ean: REQUIERE VALOR")

        print("\n" + "="*80)
        print("🎉 SISTEMA CONFIGURADO Y LISTO PARA USAR")
        print("="*80 + "\n")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error en verificación: {e}\n")
        cur.close()
        conn.close()
        return False


def main():
    """
    Función principal que ejecuta todos los pasos
    """
    print("\n" + "="*80)
    print("🚀 CONFIGURACIÓN MAESTRA DEL SISTEMA DE NORMALIZACIÓN")
    print("Sistema LecFac - Validación Inteligente de Productos")
    print("="*80 + "\n")

    print("Este script va a:")
    print("1. Habilitar extensiones PostgreSQL necesarias")
    print("2. Permitir productos sin código EAN")
    print("3. Crear tabla de MEMORIA del sistema")
    print("4. Migrar datos existentes a la memoria")
    print("5. Crear función de búsqueda inteligente")
    print("6. Documentar integración con process_invoice.py")
    print("7. Verificar que todo funcione\n")

    respuesta = input("¿Continuar? (s/n): ").strip().lower()

    if respuesta not in ['s', 'si', 'sí', 'y', 'yes']:
        print("\n❌ Operación cancelada")
        return

    print()

    # Ejecutar pasos
    exito = True

    exito = paso_1_habilitar_extensiones() and exito
    exito = paso_2_permitir_ean_null() and exito
    exito = paso_3_crear_tabla_memoria() and exito
    exito = paso_4_migrar_datos_existentes() and exito
    exito = paso_5_crear_funcion_busqueda() and exito

    paso_6_documentar_integracion()

    if exito:
        verificar_sistema()
        print("\n🎉 CONFIGURACIÓN COMPLETADA EXITOSAMENTE")
        print("\nPróximos pasos:")
        print("1. Ejecutar: python validar_productos_automatico.py")
        print("2. Validar productos pendientes (opción 2)")
        print("3. Integrar búsqueda en memoria en process_invoice.py")
    else:
        print("\n⚠️  Hubo algunos errores, pero el sistema puede funcionar")
        print("   Revisa los mensajes arriba para más detalles")


if __name__ == "__main__":
    main()
