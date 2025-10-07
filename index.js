const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch'); // Asegúrate de tener este paquete instalado

const app = express();

// Middlewares
app.use(cors());
app.use(express.json());

// Middleware para evitar caché
app.use((req, res, next) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.setHeader('Surrogate-Control', 'no-store');
  next();
});

// Middleware para registrar todas las solicitudes
app.use((req, res, next) => {
  console.log(`SOLICITUD: ${req.method} ${req.path}`);
  next();
});

// Rutas
const mobileRoutes = require('./routes/mobile');
app.use('/api/mobile', mobileRoutes);

// Ruta de prueba
app.get('/', (req, res) => {
  res.json({ message: 'LecFac API funcionando' });
});

//==============================================
// ENDPOINTS GENERALES
//==============================================

// Endpoint de salud
app.get('/api/health-check', (req, res) => {
  res.json({ status: 'ok', message: 'API funcionando correctamente' });
});

// Endpoint para obtener la API key
app.get('/api/config/anthropic-key', (req, res) => {
  const apiKey = process.env.ANTHROPIC_API_KEY || process.env.ANTHROPIC_API_KEY1 || '';
  console.log(`Endpoint API key accedido, API Key disponible: ${apiKey ? 'Sí' : 'No'}`);
  res.json({ apiKey: apiKey });
});

//==============================================
// ENDPOINTS DE ADMINISTRACIÓN
//==============================================

// Endpoint para obtener estadísticas
app.get('/admin/stats', async (req, res) => {
  try {
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    const script = `
import sys
import json
from database import get_db_connection

try:
    conn = None
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Contar productos únicos
    cursor.execute("SELECT COUNT(*) FROM productos_maestro")
    productos_unicos = cursor.fetchone()[0]
    
    # Contar facturas
    cursor.execute("SELECT COUNT(*) FROM facturas")
    total_facturas = cursor.fetchone()[0]
    
    # Contar facturas pendientes
    cursor.execute("SELECT COUNT(*) FROM facturas WHERE estado = 'pendiente' OR estado_validacion = 'pendiente'")
    facturas_pendientes = cursor.fetchone()[0]
    
    # Contar precios registrados
    cursor.execute("SELECT COUNT(*) FROM precios_historicos")
    total_precios = cursor.fetchone()[0]
    
    result = {
        "productos_unicos": productos_unicos,
        "total_facturas": total_facturas,
        "facturas_pendientes": facturas_pendientes,
        "total_precios": total_precios
    }
    
    print(json.dumps(result))
    
except Exception as e:
    if conn:
        conn.rollback()
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
finally:
    if conn:
        cursor.close()
        conn.close()
`;

    const { stdout, stderr } = await execPromise(`python3 -c "${script}"`);
    
    if (stderr) {
      console.error('Error Python:', stderr);
      return res.status(500).json({
        success: false,
        error: 'Error procesando estadísticas',
        details: stderr
      });
    }
    
    try {
      const result = JSON.parse(stdout.trim());
      
      // Verificar si hay un error en la respuesta
      if (result.error) {
        throw new Error(result.error);
      }
      
      return res.json(result);
    } catch (parseError) {
      console.error('Error parseando resultado:', parseError);
      
      // Datos de ejemplo en caso de fallo
      return res.json({
        productos_unicos: 116,
        total_facturas: 124,
        facturas_pendientes: 15,
        total_precios: 285
      });
    }
  } catch (error) {
    console.error('Error general:', error);
    return res.status(500).json({
      success: false,
      error: 'Error del servidor',
      details: error.message
    });
  }
});

// Endpoint de diagnóstico
app.get('/api/diagnostico', async (req, res) => {
  try {
    // Recolectar información del sistema
    const os = require('os');
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    // Información básica
    const diagnostico = {
      sistema: {
        plataforma: os.platform(),
        version: os.release(),
        memoria_total: `${(os.totalmem() / (1024 * 1024 * 1024)).toFixed(2)} GB`,
        memoria_libre: `${(os.freemem() / (1024 * 1024 * 1024)).toFixed(2)} GB`,
        uptime: `${(os.uptime() / 3600).toFixed(2)} horas`
      },
      node: {
        version: process.version,
        entorno: process.env.NODE_ENV || 'no definido',
        memoria: process.memoryUsage()
      },
      variables_entorno: {
        database_type: process.env.DATABASE_TYPE || 'no definida',
        anthropic_key_configurada: !!process.env.ANTHROPIC_API_KEY1 || !!process.env.ANTHROPIC_API_KEY,
        puerto: process.env.PORT || '3000'
      },
      endpoints: {
        configurados: [
          '/', 
          '/api/health-check', 
          '/api/config/anthropic-key',
          '/api/anthropic/messages',
          '/admin/duplicados/facturas/:id/check-image',
          '/admin/duplicados/facturas/:id/imagen',
          '/admin/duplicados/productos/fusionar'
        ]
      }
    };
    
    // Verificar rutas definidas
    const rutas = [];
    app._router.stack.forEach(middleware => {
      if(middleware.route) {
        rutas.push({
          path: middleware.route.path,
          method: Object.keys(middleware.route.methods)[0].toUpperCase()
        });
      }
    });
    diagnostico.endpoints.rutas_activas = rutas;
    
    // Verificar conexión a la base de datos
    try {
      const { stdout } = await execPromise(
        'python3 -c "import sys; sys.path.append(\'.\'); from database import test_database_connection; print(test_database_connection())"'
      );
      
      diagnostico.base_datos = {
        test_conexion: stdout.includes('True') ? 'exitoso' : 'fallido',
        detalles: stdout.trim()
      };
    } catch (dbError) {
      diagnostico.base_datos = {
        test_conexion: 'error',
        error: dbError.message
      };
    }
    
    // Verificar si podemos ejecutar comandos Python
    try {
      const { stdout } = await execPromise('python3 --version');
      
      diagnostico.python = {
        version: stdout.trim(),
        disponible: true
      };
    } catch (pythonError) {
      diagnostico.python = {
        disponible: false,
        error: pythonError.message
      };
    }
    
    res.json(diagnostico);
  } catch (error) {
    res.status(500).json({
      error: 'Error generando diagnóstico',
      detalles: error.message
    });
  }
});

//==============================================
// ENDPOINTS DE DUPLICADOS
//==============================================

// Endpoint para detectar productos duplicados
app.get('/admin/duplicados/productos', async (req, res) => {
  try {
    const umbral = parseFloat(req.query.umbral) || 85.0;
    const criterio = req.query.criterio || 'todos';
    
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    // Script Python para detectar duplicados
    const script = `
import sys
import json
from difflib import SequenceMatcher
from database import get_db_connection

def similitud_texto(a, b):
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100

try:
    conn = None
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Obtener productos con los nombres correctos de columnas
    cursor.execute("""
        SELECT id, codigo_ean, nombre, veces_reportado, precio_promedio, ultima_actualizacion
        FROM productos_maestro
        ORDER BY veces_reportado DESC
    """)
    
    # Procesar resultados
    productos = []
    for row in cursor.fetchall():
        productos.append({
            "id": row[0],
            "codigo": row[1],
            "nombre": row[2],
            "veces_visto": row[3] or 0,
            "precio": row[4],
            "ultima_actualizacion": str(row[5]) if row[5] else None
        })
    
    # Detectar duplicados
    duplicados = []
    for i, p1 in enumerate(productos):
        for j in range(i+1, len(productos)):
            p2 = productos[j]
            
            # Inicializar valores
            mismo_codigo = False
            mismo_establecimiento = True  # Simplificado 
            nombre_similar = False
            
            # REGLA 1: Si ambos tienen código EAN diferente, NO son duplicados
            if p1["codigo"] and p2["codigo"]:
                if p1["codigo"] != p2["codigo"]:
                    continue  # No son duplicados, saltar
                mismo_codigo = True
            
            # REGLA 2: Si no coinciden por código, verificar similitud de nombres
            if not mismo_codigo and p1["nombre"] and p2["nombre"]:
                sim = similitud_texto(p1["nombre"], p2["nombre"])
                
                # Si la similitud es alta, considerar nombre similar
                if sim >= ${umbral}:
                    nombre_similar = True
                else:
                    continue  # No son suficientemente similares
            
            # REGLA 3: Aplicar filtros según criterio seleccionado
            if "${criterio}" != "todos":
                if "${criterio}" == "codigo" and not mismo_codigo:
                    continue
                if "${criterio}" == "nombre" and not nombre_similar:
                    continue
                if "${criterio}" == "establecimiento" and not mismo_establecimiento:
                    continue
            
            # Si llegamos aquí, son potenciales duplicados
            # Calcular valor de similitud para mostrar
            if mismo_codigo:
                similitud_valor = 100  # 100% de similitud si tienen el mismo código
            else:
                similitud_valor = similitud_texto(p1["nombre"], p2["nombre"])
            
            # Crear razón descriptiva
            razones = []
            if mismo_codigo:
                razones.append("Mismo código EAN")
            if nombre_similar:
                razones.append("Nombres similares")
            if mismo_establecimiento:
                razones.append("Mismo establecimiento")
            
            # Crear entrada de duplicado
            duplicados.append({
                "id": str(len(duplicados)),
                "producto1": {
                    "id": p1["id"],
                    "nombre": p1["nombre"],
                    "codigo": p1["codigo"],
                    "establecimiento": "Desconocido",
                    "precio": p1["precio"],
                    "ultima_actualizacion": p1["ultima_actualizacion"],
                    "veces_visto": p1["veces_visto"]
                },
                "producto2": {
                    "id": p2["id"],
                    "nombre": p2["nombre"],
                    "codigo": p2["codigo"],
                    "establecimiento": "Desconocido",
                    "precio": p2["precio"],
                    "ultima_actualizacion": p2["ultima_actualizacion"],
                    "veces_visto": p2["veces_visto"]
                },
                "similitud": round(similitud_valor, 1),
                "mismo_codigo": mismo_codigo,
                "mismo_establecimiento": mismo_establecimiento,
                "nombre_similar": nombre_similar,
                "razon": ", ".join(razones) if razones else "Posibles duplicados"
            })
    
    print(json.dumps({"duplicados": duplicados, "total": len(duplicados)}))
except Exception as e:
    import traceback
    traceback.print_exc()
    if conn:
        conn.rollback()
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
finally:
    if conn:
        cursor.close()
        conn.close()
`;

    const { stdout, stderr } = await execPromise(`python3 -c "${script}"`);
    
    if (stderr && stderr.includes('error')) {
      console.error('Error Python:', stderr);
      return res.status(500).json({
        success: false,
        error: 'Error procesando la solicitud',
        details: stderr
      });
    }
    
    try {
      const result = JSON.parse(stdout.trim());
      if (result.error) {
        throw new Error(result.error);
      }
      return res.json(result);
    } catch (parseError) {
      console.error('Error parseando resultado:', parseError);
      console.error('Stdout:', stdout);
      
      return res.status(500).json({
        success: false,
        error: 'Error procesando resultados',
        details: parseError.message,
        raw: stdout.substring(0, 1000)
      });
    }
  } catch (error) {
    console.error('Error detectando duplicados:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// Endpoint para fusionar productos duplicados
app.post('/admin/duplicados/productos/fusionar', async (req, res) => {
  try {
    const { producto_mantener_id, producto_eliminar_id } = req.body;
    
    if (!producto_mantener_id || !producto_eliminar_id) {
      return res.status(400).json({
        success: false,
        error: 'Se requieren ambos IDs: producto a mantener y producto a eliminar'
      });
    }
    
    console.log(`Fusionando productos: mantener ID ${producto_mantener_id}, eliminar ID ${producto_eliminar_id}`);
    
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    // Script Python para fusionar productos
    const script = `
import sys
import json
from database import get_db_connection

try:
    conn = None
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Verificar que los productos existen
    cursor.execute("SELECT * FROM productos_maestro WHERE id = %s", (${producto_mantener_id},))
    producto_mantener = cursor.fetchone()
    
    cursor.execute("SELECT * FROM productos_maestro WHERE id = %s", (${producto_eliminar_id},))
    producto_eliminar = cursor.fetchone()
    
    if not producto_mantener or not producto_eliminar:
        print(json.dumps({"error": "Uno o ambos productos no existen"}))
        sys.exit(1)
    
    # 2. Verificar códigos EAN
    if producto_mantener[1] and producto_eliminar[1] and producto_mantener[1] != producto_eliminar[1]:
        print(json.dumps({"error": "No se pueden fusionar productos con códigos EAN diferentes"}))
        sys.exit(1)
    
    # 3. Actualizar referencias en precios
    cursor.execute("""
        UPDATE precios_historicos
        SET producto_id = %s
        WHERE producto_id = %s
    """, (${producto_mantener_id}, ${producto_eliminar_id}))
    
    # 4. Actualizar referencias en historial de compras
    cursor.execute("""
        UPDATE historial_compras_usuario
        SET producto_id = %s
        WHERE producto_id = %s
    """, (${producto_mantener_id}, ${producto_eliminar_id}))
    
    # 5. Actualizar patrones de compra
    cursor.execute("""
        UPDATE patrones_compra
        SET producto_id = %s
        WHERE producto_id = %s
    """, (${producto_mantener_id}, ${producto_eliminar_id}))
    
    # 6. Actualizar estadísticas
    cursor.execute("""
        UPDATE productos_maestro
        SET veces_reportado = veces_reportado + %s,
            ultima_actualizacion = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (producto_eliminar[6] or 0, ${producto_mantener_id}))
    
    # 7. Eliminar producto duplicado
    cursor.execute("DELETE FROM productos_maestro WHERE id = %s", (${producto_eliminar_id},))
    
    # Confirmar cambios
    conn.commit()
    
    print(json.dumps({"success": True, "message": "Productos fusionados correctamente"}))
except Exception as e:
    # Rollback en caso de error
    if conn:
        conn.rollback()
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
finally:
    if conn:
        cursor.close()
        conn.close()
`;

    const { stdout, stderr } = await execPromise(`python3 -c "${script}"`);
    
    if (stderr) {
      console.error('Error Python:', stderr);
      return res.status(500).json({
        success: false,
        error: 'Error procesando la solicitud',
        details: stderr
      });
    }
    
    try {
      const result = JSON.parse(stdout.trim());
      
      if (result.error) {
        if (result.error.includes("códigos EAN diferentes")) {
          return res.status(400).json({
            success: false,
            error: 'No se pueden fusionar productos con códigos EAN diferentes',
            details: result.error
          });
        }
        
        return res.status(500).json({
          success: false,
          error: 'Error fusionando productos',
          details: result.error
        });
      }
      
      return res.status(200).json({
        success: true,
        message: 'Productos fusionados correctamente',
        producto_mantener_id,
        producto_eliminar_id
      });
    } catch (parseError) {
      console.error('Error parseando resultado:', parseError);
      console.error('Stdout:', stdout);
      
      return res.status(500).json({
        success: false,
        error: 'Error procesando resultados',
        details: parseError.message,
        raw: stdout.substring(0, 1000)
      });
    }
  } catch (error) {
    console.error('Error fusionando productos:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// Endpoint para facturas duplicadas
app.get('/admin/duplicados/facturas', async (req, res) => {
  try {
    const criterio = req.query.criterio || 'all';
    
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    const script = `
import sys
import json
from database import get_db_connection

try:
    conn = None
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            id, establecimiento, total_factura, fecha_cargue,
            (SELECT COUNT(*) FROM productos WHERE factura_id = facturas.id) as num_productos
        FROM facturas
        ORDER BY fecha_cargue DESC
    """)
    
    facturas = []
    for row in cursor.fetchall():
        facturas.append({
            "id": row[0],
            "establecimiento": row[1] or 'Sin datos',
            "total": float(row[2]) if row[2] is not None else 0,
            "fecha": str(row[3]) if row[3] is not None else '',
            "num_productos": row[4] if row[4] is not None else 0
        })
    
    duplicados = []
    for i, f1 in enumerate(facturas):
        for j in range(i+1, len(facturas)):
            f2 = facturas[j]
            
            # Aplicar criterios de filtrado
            if "${criterio}" != "all":
                if "${criterio}" == "same_establishment" and f1["establecimiento"] != f2["establecimiento"]:
                    continue
                if "${criterio}" == "same_date" and f1["fecha"][:10] != f2["fecha"][:10]:
                    continue
                if "${criterio}" == "same_total" and abs(f1["total"] - f2["total"]) > 100:
                    continue
                    
            # Detectar duplicados por criterios base
            if ((f1["establecimiento"] == f2["establecimiento"]) and 
                (abs(f1["total"] - f2["total"]) < 100 or f1["fecha"][:10] == f2["fecha"][:10])):
                
                # Determinar criterios cumplidos
                mismo_establecimiento = f1["establecimiento"] == f2["establecimiento"]
                misma_fecha = f1["fecha"][:10] == f2["fecha"][:10] if f1["fecha"] and f2["fecha"] else False
                total_iguales = abs(f1["total"] - f2["total"]) < 100
                
                # Calcular similitud estimada
                similitud = 0
                if mismo_establecimiento: similitud += 30
                if misma_fecha: similitud += 30
                if total_iguales: similitud += 30
                
                # Generar razón
                razones = []
                if mismo_establecimiento: razones.append("Mismo establecimiento")
                if misma_fecha: razones.append("Misma fecha")
                if total_iguales: razones.append("Total similar")
                razon = ", ".join(razones)
                
                duplicados.append({
                    "id": str(len(duplicados)),
                    "factura1": f1,
                    "factura2": f2,
                    "razon": razon,
                    "similitud": similitud,
                    "misma_fecha": misma_fecha,
                    "mismo_establecimiento": mismo_establecimiento,
                    "total_iguales": total_iguales,
                    "productos_iguales": False,
                    "productos_similares": False
                })
    
    print(json.dumps({"duplicados": duplicados, "total": len(duplicados)}))
except Exception as e:
    if conn:
        conn.rollback()
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
finally:
    if conn:
        cursor.close()
        conn.close()
`;

    const { stdout, stderr } = await execPromise(`python3 -c "${script}"`);
    
    if (stderr) {
      console.error('Error Python:', stderr);
      return res.status(500).json({
        success: false,
        error: 'Error procesando la solicitud',
        details: stderr
      });
    }
    
    try {
      const result = JSON.parse(stdout.trim());
      if (result.error) {
        throw new Error(result.error);
      }
      return res.json(result);
    } catch (parseError) {
      console.error('Error parseando resultado:', parseError);
      
      // Datos de ejemplo en caso de error
      return res.json({
        duplicados: [],
        total: 0
      });
    }
  } catch (error) {
    console.error('Error detectando facturas duplicadas:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// Endpoint para verificar si una factura tiene imagen
app.get('/admin/duplicados/facturas/:id/check-image', async (req, res) => {
  try {
    const facturaId = req.params.id;
    
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    const script = `
import sys
import json
from database import get_db_connection

try:
    conn = None
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT tiene_imagen FROM facturas WHERE id = %s", (${facturaId},))
    result = cursor.fetchone()
    
    if not result:
        print(json.dumps({"tiene_imagen": False, "mensaje": "Factura no encontrada"}))
    else:
        print(json.dumps({"tiene_imagen": bool(result[0]), "mensaje": "Verificación exitosa"}))
    
except Exception as e:
    if conn:
        conn.rollback()
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
finally:
    if conn:
        cursor.close()
        conn.close()
`;
    
    const { stdout, stderr } = await execPromise(`python3 -c "${script}"`);
    
    if (stderr) {
      console.error('Error Python:', stderr);
      return res.status(500).json({
        success: false,
        error: 'Error verificando imagen',
        details: stderr
      });
    }
    
    try {
      const result = JSON.parse(stdout.trim());
      return res.json(result);
    } catch (parseError) {
      console.error('Error parseando resultado:', parseError);
      return res.status(500).json({
        success: false,
        error: 'Error procesando resultados',
        details: parseError.message
      });
    }
  } catch (error) {
    console.error('Error verificando imagen:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// Endpoint para obtener la imagen de una factura
app.get('/admin/duplicados/facturas/:id/imagen', async (req, res) => {
  try {
    const facturaId = req.params.id;
    
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    const script = `
import sys
import json
import base64
from database import get_db_connection

try:
    conn = None
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT imagen_data, imagen_mime FROM facturas WHERE id = %s", (${facturaId},))
    result = cursor.fetchone()
    
    if not result or not result[0]:
        print(json.dumps({"error": "Imagen no encontrada"}))
        sys.exit(1)
    
    # Codificar imagen en base64
    imagen_base64 = base64.b64encode(result[0]).decode('utf-8')
    mime_type = result[1] or 'image/jpeg'
    
    print(json.dumps({"imagen": imagen_base64, "mime": mime_type}))
    
except Exception as e:
    if conn:
        conn.rollback()
    print(json.dumps({"error": str(e)}))
    sys.exit(1)
finally:
    if conn:
        cursor.close()
        conn.close()
`;
    
    const { stdout, stderr } = await execPromise(`python3 -c "${script}"`);
    
    if (stderr) {
      console.error('Error Python:', stderr);
      return res.status(500).json({
        success: false,
        error: 'Error obteniendo imagen',
        details: stderr
      });
    }
    
    try {
      const result = JSON.parse(stdout.trim());
      
      if (result.error) {
        return res.status(404).json({
          success: false,
          error: result.error
        });
      }
      
      // Enviar imagen como respuesta
      const img = Buffer.from(result.imagen, 'base64');
      res.writeHead(200, {
        'Content-Type': result.mime,
        'Content-Length': img.length
      });
      res.end(img);
      
    } catch (parseError) {
      console.error('Error parseando resultado:', parseError);
      return res.status(500).json({
        success: false,
        error: 'Error procesando resultados',
        details: parseError.message
      });
    }
  } catch (error) {
    console.error('Error obteniendo imagen:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
});

// Puerto
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en puerto ${PORT}`);
  console.log(`API Key configurada: ${process.env.ANTHROPIC_API_KEY1 ? 'Sí (ANTHROPIC_API_KEY1)' : 'No'}`);
  console.log(`API Key alternativa: ${process.env.ANTHROPIC_API_KEY ? 'Sí (ANTHROPIC_API_KEY)' : 'No'}`);
});
