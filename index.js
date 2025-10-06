const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch'); // Asegúrate de tener este paquete instalado

const app = express();

// Middlewares
app.use(cors());
app.use(express.json());

// Ruta para la API de Anthropic
app.post('/api/anthropic/messages', async (req, res) => {
  try {
    const anthropicResponse = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY, // Usa la variable de entorno en el servidor
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify(req.body)
    });
    
    const data = await anthropicResponse.json();
    res.json(data);
  } catch (error) {
    console.error('Error calling Anthropic API:', error);
    res.status(500).json({ error: 'Error processing request' });
  }
});

// Rutas
const mobileRoutes = require('./routes/mobile');
app.use('/api/mobile', mobileRoutes);

// Ruta de prueba
app.get('/', (req, res) => {
  res.json({ message: 'LecFac API funcionando' });
});

// Puerto
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en puerto ${PORT}`);
});
// Endpoint para obtener la API key
app.get('/api/config/anthropic-key', (req, res) => {
  // Recuperar la API key de las variables de entorno
  const apiKey = process.env.ANTHROPIC_API_KEY || '';
  res.json({ apiKey: apiKey });
});

// Endpoint para comunicarse con la API de Anthropic
app.post('/api/anthropic/messages', async (req, res) => {
  try {
    // Verificar si tenemos la API key
    const apiKey = process.env.ANTHROPIC_API_KEY;
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

// Endpoint de prueba para verificar que el servidor está funcionando
app.get('/api/health-check', (req, res) => {
  res.json({ status: 'ok' });
});
// Add these endpoints to your existing Express app:

// Endpoint to securely provide the Anthropic API key to the frontend
app.get('/api/config/anthropic-key', (req, res) => {
  // Use the environment variable name you have configured in Render
  const apiKey = process.env.ANTHROPIC_API_KEY1 || '';
  res.json({ apiKey: apiKey });
});

// Proxy endpoint for Anthropic API to avoid CORS issues
app.post('/api/anthropic/messages', async (req, res) => {
  try {
    // Use the correct environment variable name
    const apiKey = process.env.ANTHROPIC_API_KEY1;
    if (!apiKey) {
      return res.status(500).json({ error: 'API key no configurada' });
    }
    
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify(req.body)
    });
    
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error API Anthropic:', error);
    res.status(500).json({ error: error.message });
  }
});

// For handling invoice image checks
app.get('/admin/duplicados/facturas/:id/check-image', async (req, res) => {
  try {
    const facturaId = req.params.id;
    
    // This is a temporary implementation since you haven't set up actual image checking yet
    // You would replace this with actual logic to check if images exist
    res.json({ 
      tiene_imagen: false,
      mensaje: 'Funcionalidad de verificación de imágenes aún no implementada'
    });
  } catch (error) {
    console.error('Error verificando imagen:', error);
    res.status(500).json({ error: error.message });
  }
});

// If you need an endpoint to serve the actual images
app.get('/admin/duplicados/facturas/:id/imagen', async (req, res) => {
  try {
    const facturaId = req.params.id;
    
    // This is a placeholder - you would replace with actual image serving code
    // For now, we'll just return a 404 with a helpful message
    res.status(404).json({
      error: 'Imagen no encontrada',
      mensaje: 'La funcionalidad de imágenes no está implementada todavía'
    });
    
    // When you implement this properly, it would look more like:
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
