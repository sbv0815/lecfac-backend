const express = require('express');
const cors = require('cors');
const path = require('path');

const app = express();

// ==========================================
// MIDDLEWARES
// ==========================================
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

// Middleware para logging
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
  next();
});

// ==========================================
// SERVIR ARCHIVOS ESTÁTICOS (HTML)
// ==========================================
app.use(express.static('public')); // Si tienes una carpeta public
app.use(express.static(__dirname)); // Para servir archivos del directorio raíz

// ==========================================
// RUTAS DE PÁGINAS HTML
// ==========================================
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'admin_dashboard.html'));
});

app.get('/gestor-duplicados', (req, res) => {
  res.sendFile(path.join(__dirname, 'gestor_duplicados.html'));
});

app.get('/editor', (req, res) => {
  res.sendFile(path.join(__dirname, 'editor.html'));
});

// ==========================================
// HEALTH CHECK
// ==========================================
app.get('/api/health-check', (req, res) => {
  res.json({ 
    status: 'ok', 
    message: 'Node.js API funcionando correctamente',
    timestamp: new Date().toISOString()
  });
});

// ==========================================
// ENDPOINT PARA API KEY (IMPORTANTE)
// ==========================================
app.get('/api/config/anthropic-key', (req, res) => {
  // Priorizar ANTHROPIC_API_KEY1, luego ANTHROPIC_API_KEY
  const apiKey = process.env.ANTHROPIC_API_KEY1 || process.env.ANTHROPIC_API_KEY || '';
  
  console.log(`[API KEY] Solicitada - Disponible: ${apiKey ? 'Sí' : 'No'}`);
  
  res.json({ 
    apiKey: apiKey,
    configured: !!apiKey
  });
});

// ==========================================
// PROXY A ANTHROPIC (Para llamadas desde frontend)
// ==========================================
app.post('/api/anthropic/messages', async (req, res) => {
  try {
    const apiKey = process.env.ANTHROPIC_API_KEY1 || process.env.ANTHROPIC_API_KEY;
    
    if (!apiKey) {
      return res.status(500).json({ 
        error: 'API key no configurada en el servidor',
        details: 'Configura ANTHROPIC_API_KEY o ANTHROPIC_API_KEY1 en las variables de entorno'
      });
    }

    const fetch = (await import('node-fetch')).default;
    
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
    
    if (!response.ok) {
      console.error('[ANTHROPIC ERROR]', data);
      return res.status(response.status).json(data);
    }

    res.json(data);
  } catch (error) {
    console.error('[ANTHROPIC PROXY ERROR]', error);
    res.status(500).json({ 
      error: 'Error en proxy de Anthropic',
      details: error.message 
    });
  }
});

// ==========================================
// RUTAS MÓVILES (si las necesitas)
// ==========================================
const mobileRoutes = require('./routes/mobile');
app.use('/api/mobile', mobileRoutes);

// ==========================================
// ⚠️ ELIMINAR ENDPOINTS DUPLICADOS
// ==========================================
// Los siguientes endpoints YA ESTÁN en admin_dashboard.py
// NO los incluyas aquí para evitar conflictos:
//
// ❌ /admin/stats
// ❌ /admin/productos/catalogo
// ❌ /admin/facturas
// ❌ /admin/duplicados/productos
// ❌ /admin/duplicados/productos/fusionar
// ❌ /admin/duplicados/facturas
// ❌ /admin/duplicados/facturas/:id/imagen
// ❌ /admin/duplicados/facturas/:id/check-image
// ❌ /admin/alertas/cambios-precio
// ❌ /admin/productos/:id/comparar-establecimientos
//
// Todos esos endpoints deben ser manejados por FastAPI (Python)

// ==========================================
// ENDPOINT DE DIAGNÓSTICO (Node.js específico)
// ==========================================
app.get('/api/diagnostico/nodejs', (req, res) => {
  const os = require('os');
  
  res.json({
    estado: 'ok',
    servidor: 'Node.js',
    version_node: process.version,
    plataforma: os.platform(),
    memoria_libre: `${(os.freemem() / (1024 * 1024 * 1024)).toFixed(2)} GB`,
    memoria_total: `${(os.totalmem() / (1024 * 1024 * 1024)).toFixed(2)} GB`,
    uptime: `${(os.uptime() / 3600).toFixed(2)} horas`,
    variables_entorno: {
      NODE_ENV: process.env.NODE_ENV || 'no definido',
      PORT: process.env.PORT || '3000',
      DATABASE_TYPE: process.env.DATABASE_TYPE || 'no definido',
      API_KEY_CONFIGURADA: !!(process.env.ANTHROPIC_API_KEY1 || process.env.ANTHROPIC_API_KEY)
    },
    rutas_nodejs: [
      'GET /',
      'GET /gestor-duplicados',
      'GET /editor',
      'GET /api/health-check',
      'GET /api/config/anthropic-key',
      'POST /api/anthropic/messages',
      'GET /api/diagnostico/nodejs',
      '/api/mobile/* (rutas móviles)'
    ],
    nota: 'Las rutas /admin/* son manejadas por FastAPI (Python)'
  });
});

// ==========================================
// MANEJO DE ERRORES 404
// ==========================================
app.use((req, res) => {
  res.status(404).json({ 
    error: 'Ruta no encontrada',
    path: req.path,
    method: req.method,
    sugerencia: 'Verifica que la ruta exista. Las rutas /admin/* son manejadas por FastAPI.'
  });
});

// ==========================================
// MANEJO DE ERRORES GENERALES
// ==========================================
app.use((err, req, res, next) => {
  console.error('[ERROR]', err);
  res.status(500).json({ 
    error: 'Error interno del servidor',
    details: err.message,
    timestamp: new Date().toISOString()
  });
});

// ==========================================
// INICIAR SERVIDOR
// ==========================================
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log('═══════════════════════════════════════════════');
  console.log(`🚀 Servidor Node.js iniciado en puerto ${PORT}`);
  console.log('═══════════════════════════════════════════════');
  console.log(`📅 Fecha: ${new Date().toISOString()}`);
  console.log(`🌍 Entorno: ${process.env.NODE_ENV || 'development'}`);
  console.log(`🔑 API Key Anthropic: ${process.env.ANTHROPIC_API_KEY1 || process.env.ANTHROPIC_API_KEY ? '✅ Configurada' : '❌ No configurada'}`);
  console.log(`💾 Base de datos: ${process.env.DATABASE_TYPE || 'No definida'}`);
  console.log('═══════════════════════════════════════════════');
  console.log('📄 Rutas HTML disponibles:');
  console.log('   - http://localhost:' + PORT + '/');
  console.log('   - http://localhost:' + PORT + '/gestor-duplicados');
  console.log('   - http://localhost:' + PORT + '/editor');
  console.log('═══════════════════════════════════════════════');
  console.log('🔌 Endpoints API disponibles:');
  console.log('   - GET  /api/health-check');
  console.log('   - GET  /api/config/anthropic-key');
  console.log('   - POST /api/anthropic/messages');
  console.log('   - GET  /api/diagnostico/nodejs');
  console.log('   - *    /api/mobile/* (rutas móviles)');
  console.log('═══════════════════════════════════════════════');
  console.log('⚠️  NOTA: Las rutas /admin/* son manejadas por FastAPI');
  console.log('═══════════════════════════════════════════════');
});

module.exports = app;
const { spawn } = require('child_process');

// Iniciar FastAPI como subproceso
const fastapi = spawn('uvicorn', ['main:app', '--port', '8000'], {
  stdio: 'inherit'
});

// Proxy a FastAPI
app.use('/admin', async (req, res) => {
  const fetch = (await import('node-fetch')).default;
  const response = await fetch(`http://localhost:8000${req.url}`);
  const data = await response.json();
  res.json(data);
});
