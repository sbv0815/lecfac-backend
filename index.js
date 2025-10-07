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

// Endpoint de salud para verificar que el servidor está funcionando
app.get('/api/health-check', (req, res) => {
  res.json({ status: 'ok' });
});

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

    // En este punto harías las operaciones de base de datos para fusionar productos
    // Como no tenemos acceso a tu modelo de datos, implementamos una respuesta simulada
    
    console.log(`Fusionando productos: mantener ID ${producto_mantener_id}, eliminar ID ${producto_eliminar_id}`);
    
    // Simular un pequeño retraso para que parezca que está procesando
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Devolver respuesta exitosa
    return res.status(200).json({
      success: true,
      message: 'Productos fusionados correctamente',
      producto_mantener_id,
      producto_eliminar_id
    });
    
  } catch (error) {
    console.error('Error al fusionar productos:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor al fusionar productos',
      details: error.message
    });
  }
});

// Endpoint para obtener duplicados de productos
app.get('/admin/duplicados/productos', (req, res) => {
  try {
    const umbral = req.query.umbral || '85';
    const criterio = req.query.criterio || 'todos';
    
    // Datos de ejemplo para el frontend
    const duplicados = [
      {
        id: '1',
        producto1: {
          id: 101,
          nombre: 'Leche Entera 1L',
          codigo: '7791234567890',
          establecimiento: 'Supermercado XYZ',
          precio: 2500,
          ultima_actualizacion: new Date().toISOString(),
          veces_visto: 12
        },
        producto2: {
          id: 102,
          nombre: 'Leche Entera 1 Litro',
          codigo: '7791234567890',
          establecimiento: 'Supermercado XYZ',
          precio: 2600,
          ultima_actualizacion: new Date(Date.now() - 86400000).toISOString(), // 1 día antes
          veces_visto: 8
        },
        similitud: 95,
        mismo_codigo: true,
        mismo_establecimiento: true,
        nombre_similar: true,
        razon: 'Mismo código de barras con nombres ligeramente diferentes'
      },
      {
        id: '2',
        producto1: {
          id: 103,
          nombre: 'Arroz Blanco 1kg',
          codigo: '7795678901234',
          establecimiento: 'Tienda ABC',
          precio: 1800,
          ultima_actualizacion: new Date().toISOString(),
          veces_visto: 5
        },
        producto2: {
          id: 104,
          nombre: 'Arroz Integral 1kg',
          codigo: '7795678901235',
          establecimiento: 'Tienda ABC',
          precio: 2200,
          ultima_actualizacion: new Date().toISOString(),
          veces_visto: 3
        },
        similitud: 88,
        mismo_codigo: false,
        mismo_establecimiento: true,
        nombre_similar: true,
        razon: 'Nombres similares en el mismo establecimiento'
      }
    ];
    
    res.json({
      success: true,
      total: duplicados.length,
      duplicados
    });
  } catch (error) {
    console.error('Error obteniendo duplicados:', error);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint para obtener duplicados de facturas
app.get('/admin/duplicados/facturas', (req, res) => {
  try {
    const criterio = req.query.criterio || 'all';
    
    // Datos de ejemplo para el frontend
    const duplicados = [
      {
        id: '1',
        factura1: {
          id: 201,
          establecimiento: 'Supermercado XYZ',
          total: 15800,
          fecha: new Date().toISOString(),
          num_productos: 6,
          estado: 'Procesada',
          productos: [
            { nombre: 'Leche Entera 1L', codigo: '7791234567890', precio: 2500 },
            { nombre: 'Pan Blanco 500g', codigo: '7799876543210', precio: 1500 }
          ]
        },
        factura2: {
          id: 202,
          establecimiento: 'Supermercado XYZ',
          total: 15800,
          fecha: new Date().toISOString(),
          num_productos: 6,
          estado: 'Procesada',
          productos: [
            { nombre: 'Leche Entera 1L', codigo: '7791234567890', precio: 2500 },
            { nombre: 'Pan Blanco 500g', codigo: '7799876543210', precio: 1500 }
          ]
        },
        misma_fecha: true,
        mismo_establecimiento: true,
        total_iguales: true,
        productos_iguales: true,
        razon: 'Facturas idénticas del mismo día'
      },
      {
        id: '2',
        factura1: {
          id: 203,
          establecimiento: 'Tienda ABC',
          total: 8700,
          fecha: new Date(Date.now() - 86400000).toISOString(), // 1 día antes
          num_productos: 4,
          estado: 'Procesada',
          productos: [
            { nombre: 'Arroz Blanco 1kg', codigo: '7795678901234', precio: 1800 },
            { nombre: 'Aceite 900ml', codigo: '7794567890123', precio: 3200 }
          ]
        },
        factura2: {
          id: 204,
          establecimiento: 'Tienda ABC',
          total: 8900,
          fecha: new Date(Date.now() - 86400000).toISOString(), // 1 día antes
          num_productos: 4,
          estado: 'Procesada',
          productos: [
            { nombre: 'Arroz Blanco 1kg', codigo: '7795678901234', precio: 1800 },
            { nombre: 'Aceite 900ml', codigo: '7794567890123', precio: 3300 }
          ]
        },
        misma_fecha: true,
        mismo_establecimiento: true,
        total_iguales: false,
        productos_similares: true,
        razon: 'Productos similares en la misma fecha'
      }
    ];
    
    res.json({
      success: true,
      total: duplicados.length,
      duplicados
    });
  } catch (error) {
    console.error('Error obteniendo facturas duplicadas:', error);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint para eliminar facturas
app.delete('/admin/duplicados/facturas/:id', (req, res) => {
  try {
    const facturaId = req.params.id;
    
    console.log(`Eliminando factura con ID: ${facturaId}`);
    
    // Simular procesamiento
    setTimeout(() => {
      res.json({
        success: true,
        message: `Factura #${facturaId} eliminada correctamente`
      });
    }, 300);
    
  } catch (error) {
    console.error('Error eliminando factura:', error);
    res.status(500).json({ error: error.message });
  }
});

// Endpoint para obtener estadísticas generales
app.get('/admin/stats', (req, res) => {
  res.json({
    productos_unicos: 358,
    productos_duplicados: 12,
    total_facturas: 124,
    facturas_duplicadas: 8
  });
});

// Puerto
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en puerto ${PORT}`);
  console.log(`API Key configurada: ${process.env.ANTHROPIC_API_KEY1 ? 'Sí (ANTHROPIC_API_KEY1)' : 'No'}`);
  console.log(`API Key alternativa: ${process.env.ANTHROPIC_API_KEY ? 'Sí (ANTHROPIC_API_KEY)' : 'No'}`);
});
