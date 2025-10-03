const express = require('express');
const router = express.Router();
const multer = require('multer');
const upload = multer({ storage: multer.memoryStorage() });

// Endpoint para procesar factura con OCR
router.post('/process-invoice', upload.single('image'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No se recibió imagen' });
    }
    // Datos de prueba por ahora
    const ocrResult = {
      establecimiento: 'Supermercado Ejemplo',
      fecha: new Date().toISOString().split('T')[0],
      total: 150.50,
      items: [
        { descripcion: 'Leche', cantidad: 2, precio: 25.00 },
        { descripcion: 'Pan', cantidad: 3, precio: 15.00 }
      ]
    };
    res.json(ocrResult);
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Error procesando factura' });
  }
});

// Endpoint para guardar factura
router.post('/save-invoice', async (req, res) => {
  try {
    res.json({ 
      success: true, 
      message: 'Factura guardada',
      data: req.body 
    });
  } catch (error) {
    res.status(500).json({ error: 'Error guardando factura' });
  }
});

module.exports = router;  
