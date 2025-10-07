const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch'); // Asegúrate de tener este paquete instalado

const app = express();

// Middlewares
app.use(cors());
app.use(express.json());

// Rutas
const mobileRoutes = require('./routes/mobile');
app.use('/api/mobile', mobileRoutes);

// Ruta de prueba
app.get('/', (req, res) => {
  res.json({ message: 'LecFac API funcionando' });
});

// Middleware para registrar todas las solicitudes
app.use((req, res, next) => {
  console.log(`SOLICITUD: ${req.method} ${req.path}`);
  next();
});

// Endpoint de salud para verificar que el servidor está funcionando
app.get('/api/health-check', (req, res) => {
  res.json({ status: 'ok' });
});

// Endpoint para obtener la API key
// Endpoint para obtener la API key
app.get('/api/config/anthropic-key', (req, res) => {
  // Recuperar la API key de las variables de entorno
  // Intentar primero ANTHROPIC_API_KEY1, luego ANTHROPIC_API_KEY como respaldo
  const apiKey = process.env.ANTHROPIC_API_KEY1 || process.env.ANTHROPIC_API_KEY || '';
  res.json({ apiKey: apiKey });
});
// Endpoint para comunicarse con la API de Anthropic
app.post('/api/anthropic/messages', async (req, res) => {
  try {
    // Verificar si tenemos la API key (intentar ambas variables de entorno)
    const apiKey = process.env.ANTHROPIC_API_KEY1 || process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'API key de Anthropic no configurada en el servidor' });
    }
    
    const anthropicResponse = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify(req.body)
    });
    
    const data = await anthropicResponse.json();
    res.json(data);
  } catch (error) {
    console.error('Error llamando a la API de Anthropic:', error);
    res.status(500).json({ error: 'Error procesando la petición: ' + error.message });
  }
});

// Endpoint para verificar si una factura tiene imagen
app.get('/admin/duplicados/facturas/:id/check-image', async (req, res) => {
  try {
    const facturaId = req.params.id;
    
    // Implementación temporal hasta que tengas verificación real de imágenes
    // Devuelve siempre false para evitar errores en el frontend
    res.json({ 
      tiene_imagen: false,
      mensaje: 'Funcionalidad de verificación de imágenes aún no implementada'
    });
  } catch (error) {
    console.error('Error verificando imagen:', error);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint para servir imágenes de facturas
app.get('/admin/duplicados/facturas/:id/imagen', async (req, res) => {
  try {
    const facturaId = req.params.id;
    
    // Implementación temporal - devuelve un mensaje informativo
    // Esto evitará errores 404 en el frontend
    res.status(404).json({
      error: 'Imagen no encontrada',
      mensaje: 'La funcionalidad de imágenes no está implementada todavía'
    });
    
    // Cuando implementes esta funcionalidad, sería algo como:
    // const imagePath = `./storage/facturas/${facturaId}.jpg`;
    // if (fs.existsSync(imagePath)) {
    //   res.sendFile(imagePath, { root: __dirname });
    // } else {
    //   res.status(404).send('Imagen no encontrada');
    // }
  } catch (error) {
    console.error('Error sirviendo imagen:', error);
    res.status(500).json({ error: error.message });
  }
});
// Endpoint para fusionar productos duplicados
// Endpoint para fusionar productos duplicados
app.post('/admin/duplicados/productos/fusionar', async (req, res) => {
  try {
    const { producto_mantener_id, producto_eliminar_id } = req.body;
    
    // Validación
    if (!producto_mantener_id || !producto_eliminar_id) {
      return res.status(400).json({
        success: false,
        error: 'Se requieren ambos IDs: producto a mantener y producto a eliminar'
      });
    }
    
    console.log(`Fusionando productos: mantener ID ${producto_mantener_id}, eliminar ID ${producto_eliminar_id}`);
    
    // Crear conexión a la base de datos
    const { exec } = require('child_process');
    const util = require('util');
    const execPromise = util.promisify(exec);
    
    // Ejecutar el script Python para fusionar productos
    const scriptCommand = `python3 -c "
import sys
sys.path.append('.')
from database import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

try:
    # 1. Obtener productos
    if '${process.env.DATABASE_TYPE}' == 'postgresql':
        cursor.execute('SELECT * FROM productos_maestro WHERE id = %s', (${producto_mantener_id},))
        producto_mantener = cursor.fetchone()
        cursor.execute('SELECT * FROM productos_maestro WHERE id = %s', (${producto_eliminar_id},))
        producto_eliminar = cursor.fetchone()
    else:
        cursor.execute('SELECT * FROM productos_maestro WHERE id = ?', (${producto_mantener_id},))
        producto_mantener = cursor.fetchone()
        cursor.execute('SELECT * FROM productos_maestro WHERE id = ?', (${producto_eliminar_id},))
        producto_eliminar = cursor.fetchone()
    
    if not producto_mantener or not producto_eliminar:
        print('ERROR: Uno o ambos productos no existen')
        sys.exit(1)
    
    # 2. Fusionar histórico de precios
    if '${process.env.DATABASE_TYPE}' == 'postgresql':
        # Actualizar referencias en precios_historicos
        cursor.execute('''
            UPDATE precios_historicos 
            SET producto_id = %s
            WHERE producto_id = %s
        ''', (${producto_mantener_id}, ${producto_eliminar_id}))
        
        # Actualizar referencias en historial_compras_usuario
        cursor.execute('''
            UPDATE historial_compras_usuario 
            SET producto_id = %s
            WHERE producto_id = %s
        ''', (${producto_mantener_id}, ${producto_eliminar_id}))
        
        # Actualizar patrones_compra
        cursor.execute('''
            UPDATE patrones_compra
            SET producto_id = %s
            WHERE producto_id = %s
        ''', (${producto_mantener_id}, ${producto_eliminar_id}))
    else:
        # SQLite
        cursor.execute('''
            UPDATE precios_historicos 
            SET producto_id = ?
            WHERE producto_id = ?
        ''', (${producto_mantener_id}, ${producto_eliminar_id}))
        
        cursor.execute('''
            UPDATE historial_compras_usuario 
            SET producto_id = ?
            WHERE producto_id = ?
        ''', (${producto_mantener_id}, ${producto_eliminar_id}))
        
        cursor.execute('''
            UPDATE patrones_compra
            SET producto_id = ?
            WHERE producto_id = ?
        ''', (${producto_mantener_id}, ${producto_eliminar_id}))
    
    # 3. Actualizar estadísticas del producto mantenido
    if '${process.env.DATABASE_TYPE}' == 'postgresql':
        cursor.execute('''
            UPDATE productos_maestro 
            SET veces_reportado = veces_reportado + %s,
                ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
        ''', (producto_eliminar[6] or 0, ${producto_mantener_id}))
    else:
        cursor.execute('''
            UPDATE productos_maestro 
            SET veces_reportado = veces_reportado + ?,
                ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (producto_eliminar[6] or 0, ${producto_mantener_id}))
    
    # 4. Eliminar el producto duplicado
    if '${process.env.DATABASE_TYPE}' == 'postgresql':
        cursor.execute('DELETE FROM productos_maestro WHERE id = %s', (${producto_eliminar_id},))
    else:
        cursor.execute('DELETE FROM productos_maestro WHERE id = ?', (${producto_eliminar_id},))
    
    # Confirmar cambios
    conn.commit()
    print('SUCCESS: Productos fusionados correctamente')
except Exception as e:
    conn.rollback()
    print(f'ERROR: {str(e)}')
    sys.exit(1)
finally:
    conn.close()
"`;

    try {
      const { stdout, stderr } = await execPromise(scriptCommand);
      
      if (stderr && stderr.includes('ERROR')) {
        console.error('Error en la fusión:', stderr);
        return res.status(500).json({
          success: false,
          error: 'Error al fusionar productos',
          details: stderr
        });
      }
      
      if (stdout.includes('SUCCESS')) {
        return res.status(200).json({
          success: true,
          message: 'Productos fusionados correctamente',
          producto_mantener_id,
          producto_eliminar_id
        });
      } else {
        return res.status(500).json({
          success: false,
          error: 'Resultado inesperado',
          details: stdout
        });
      }
    } catch (pythonError) {
      console.error('Error ejecutando script Python:', pythonError);
      return res.status(500).json({
        success: false,
        error: 'Error al ejecutar la fusión',
        details: pythonError.message
      });
    }
    
  } catch (error) {
    console.error('Error general al fusionar productos:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor al fusionar productos',
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
      const { stdout } = await new Promise((resolve, reject) => {
        exec('python3 -c "import sys; sys.path.append(\'.\'); from database import test_database_connection; test_database_connection()"', 
          (error, stdout, stderr) => {
            if (error) {
              reject(error);
            }
            resolve({ stdout, stderr });
          });
      });
      
      diagnostico.base_datos = {
        test_conexion: stdout.includes('✅') ? 'exitoso' : 'fallido',
        detalles: stdout
      };
    } catch (dbError) {
      diagnostico.base_datos = {
        test_conexion: 'error',
        error: dbError.message
      };
    }
    
    // Verificar si podemos ejecutar comandos Python
    try {
      const { stdout } = await new Promise((resolve, reject) => {
        exec('python3 --version', (error, stdout, stderr) => {
          if (error) {
            reject(error);
          }
          resolve({ stdout, stderr });
        });
      });
      
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

// Puerto
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en puerto ${PORT}`);
  console.log(`API Key configurada: ${process.env.ANTHROPIC_API_KEY1 ? 'Sí (ANTHROPIC_API_KEY1)' : 'No'}`);
  console.log(`API Key alternativa: ${process.env.ANTHROPIC_API_KEY ? 'Sí (ANTHROPIC_API_KEY)' : 'No'}`);
});
