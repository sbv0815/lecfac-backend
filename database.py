# Reemplaza la sección de "ARREGLAR TABLA precios_productos" (línea ~297)
# con esta versión corregida:

# ============================================
# 🔧 ARREGLAR TABLA precios_productos
# ============================================
print("🔧 Verificando/arreglando tabla precios_productos...")

# Verificar si la tabla existe
cursor.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'precios_productos'
    )
""")
tabla_existe = cursor.fetchone()[0]

if tabla_existe:
    print("⚠️ Tabla precios_productos existe, verificando estructura...")
    
    # Verificar columnas existentes
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_name = 'precios_productos'
        ORDER BY ordinal_position
    """)
    columnas_existentes = {row[0]: row[1] for row in cursor.fetchall()}
    print(f"   📋 Columnas existentes: {list(columnas_existentes.keys())}")
    
    # 🔥 PROBLEMA: Si tiene "establecimiento" (texto), eliminarlo
    if 'establecimiento' in columnas_existentes:
        print("   ⚠️ PROBLEMA: Columna 'establecimiento' (texto) existe")
        print("   🗑️ Eliminando columna 'establecimiento'...")
        try:
            cursor.execute("ALTER TABLE precios_productos DROP COLUMN establecimiento")
            conn.commit()
            print("   ✅ Columna 'establecimiento' eliminada")
        except Exception as e:
            print(f"   ❌ Error eliminando columna: {e}")
            conn.rollback()
    
    # Agregar establecimiento_id si no existe
    if 'establecimiento_id' not in columnas_existentes:
        print("   ➕ Agregando columna establecimiento_id...")
        try:
            cursor.execute("""
                ALTER TABLE precios_productos 
                ADD COLUMN establecimiento_id INTEGER
            """)
            conn.commit()
            print("   ✅ Columna establecimiento_id agregada")
        except Exception as e:
            print(f"   ❌ Error: {e}")
            conn.rollback()
    
    # Agregar producto_maestro_id si no existe (en lugar de producto_id)
    if 'producto_maestro_id' not in columnas_existentes and 'producto_id' not in columnas_existentes:
        print("   ➕ Agregando columna producto_maestro_id...")
        try:
            cursor.execute("""
                ALTER TABLE precios_productos 
                ADD COLUMN producto_maestro_id INTEGER
            """)
            conn.commit()
            print("   ✅ Columna producto_maestro_id agregada")
        except Exception as e:
            print(f"   ❌ Error: {e}")
            conn.rollback()
    
    # Si tiene producto_id, renombrarlo a producto_maestro_id
    if 'producto_id' in columnas_existentes and 'producto_maestro_id' not in columnas_existentes:
        print("   🔄 Renombrando producto_id → producto_maestro_id...")
        try:
            cursor.execute("""
                ALTER TABLE precios_productos 
                RENAME COLUMN producto_id TO producto_maestro_id
            """)
            conn.commit()
            print("   ✅ Columna renombrada")
        except Exception as e:
            print(f"   ❌ Error: {e}")
            conn.rollback()
    
    # Agregar otras columnas faltantes
    columnas_requeridas = {
        'precio': 'INTEGER',
        'fecha_registro': 'DATE',
        'usuario_id': 'INTEGER',
        'factura_id': 'INTEGER',
        'verificado': 'BOOLEAN DEFAULT FALSE',
        'es_outlier': 'BOOLEAN DEFAULT FALSE',
        'votos_confianza': 'INTEGER DEFAULT 0',
        'fecha_actualizacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        'fecha_creacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    }
    
    for columna, tipo in columnas_requeridas.items():
        if columna not in columnas_existentes:
            print(f"   ➕ Agregando columna {columna}...")
            try:
                cursor.execute(f"""
                    ALTER TABLE precios_productos 
                    ADD COLUMN {columna} {tipo}
                """)
                conn.commit()
            except Exception as e:
                print(f"   ⚠️ {columna}: {e}")
                conn.rollback()
    
    # Eliminar constraints viejos
    print("   🔧 Limpiando constraints viejos...")
    cursor.execute("""
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        WHERE rel.relname = 'precios_productos'
        AND con.contype = 'f'
    """)
    constraints = cursor.fetchall()
    
    for constraint in constraints:
        constraint_name = constraint[0]
        try:
            cursor.execute(f"ALTER TABLE precios_productos DROP CONSTRAINT IF EXISTS {constraint_name}")
            conn.commit()
        except:
            conn.rollback()
    
    # Agregar constraints correctos
    print("   ✅ Agregando constraints correctos...")
    
    try:
        cursor.execute("""
            ALTER TABLE precios_productos 
            ADD CONSTRAINT precios_productos_producto_maestro_fkey 
            FOREIGN KEY (producto_maestro_id) REFERENCES productos_maestros(id)
        """)
        conn.commit()
        print("   ✅ FK producto_maestro_id → productos_maestros")
    except Exception as e:
        print(f"   ⚠️ FK producto_maestro: {e}")
        conn.rollback()
    
    try:
        cursor.execute("""
            ALTER TABLE precios_productos 
            ADD CONSTRAINT precios_productos_establecimiento_fkey 
            FOREIGN KEY (establecimiento_id) REFERENCES establecimientos(id)
        """)
        conn.commit()
        print("   ✅ FK establecimiento_id → establecimientos")
    except Exception as e:
        print(f"   ⚠️ FK establecimiento: {e}")
        conn.rollback()
    
    try:
        cursor.execute("""
            ALTER TABLE precios_productos 
            ADD CONSTRAINT precios_productos_usuario_fkey 
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        """)
        conn.commit()
        print("   ✅ FK usuario_id → usuarios")
    except Exception as e:
        print(f"   ⚠️ FK usuario: {e}")
        conn.rollback()
    
    # Verificar estructura final
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_name = 'precios_productos'
        ORDER BY ordinal_position
    """)
    columnas_finales = cursor.fetchall()
    print("   📋 Estructura final:")
    for col in columnas_finales:
        print(f"      - {col[0]}: {col[1]}")

else:
    # Crear tabla desde cero con estructura correcta
    print("✨ Creando tabla precios_productos desde cero...")
    cursor.execute('''
    CREATE TABLE precios_productos (
        id SERIAL PRIMARY KEY,
        producto_maestro_id INTEGER NOT NULL,
        establecimiento_id INTEGER NOT NULL,
        precio INTEGER NOT NULL,
        fecha_registro DATE NOT NULL,
        usuario_id INTEGER,
        factura_id INTEGER,
        verificado BOOLEAN DEFAULT FALSE,
        es_outlier BOOLEAN DEFAULT FALSE,
        votos_confianza INTEGER DEFAULT 0,
        fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        CONSTRAINT precios_productos_producto_maestro_fkey 
            FOREIGN KEY (producto_maestro_id) REFERENCES productos_maestros(id),
        CONSTRAINT precios_productos_establecimiento_fkey 
            FOREIGN KEY (establecimiento_id) REFERENCES establecimientos(id),
        CONSTRAINT precios_productos_usuario_fkey 
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
        
        CHECK (precio > 0)
    )
    ''')
    conn.commit()
    print("   ✅ Tabla creada con estructura correcta")

print("✅ Tabla 'precios_productos' configurada correctamente")
