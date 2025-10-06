// Configuración global
const API_URL = window.location.origin;
let productosDuplicados = [];
let facturasDuplicadas = [];
let paginaActualProductos = 1;
let paginaActualFacturas = 1;
const elementosPorPagina = 5;
let totalFusiones = 0;
let totalEliminaciones = 0;
let procesandoIA = false;
let procesoIACancelado = false;

// Resultados de IA
let resultadosProductosIA = [];
let resultadosFacturasIA = [];

// Configuración de IA
let configuracionIA = {
    apiKey: "", // Inicialmente vacío
    modelo: "claude-3-haiku-20240307",
    tamanoLote: 10,
    intervaloLote: 5, // segundos
    criteriosProductos: "",
    criteriosFacturas: ""
};

// MODO_DEMO desactivado - Trabajamos con datos reales
const MODO_DEMO = false;

// Inicialización
document.addEventListener('DOMContentLoaded', function() {
    verificarConexionAPI();
    cargarEstadisticas();
    cargarConfiguracionIA();
    obtenerAPIKey(); // Nueva función para obtener la API key del servidor
});

// Obtener API Key del servidor
async function obtenerAPIKey() {
    try {
        const response = await fetch(`${API_URL}/api/config/anthropic-key`);
        if (response.ok) {
            const data = await response.json();
            configuracionIA.apiKey = data.apiKey || "";
            // Actualizar el campo del formulario con asteriscos por seguridad
            document.getElementById('apiKey').value = configuracionIA.apiKey ? "********" : "";
        }
    } catch (error) {
        console.error('Error obteniendo API key:', error);
    }
}

// Verificar si la API está activa
async function verificarConexionAPI() {
    try {
        const response = await fetch(`${API_URL}/api/health-check`);
        if (!response.ok) {
            mostrarMensajeError('No se pudo conectar con la API. Algunas funciones pueden no estar disponibles.');
        }
    } catch (error) {
        console.error('Error verificando API:', error);
        mostrarMensajeError('Error de conexión con el servidor. Algunas funciones pueden no estar disponibles.');
    }
}

function mostrarMensajeError(mensaje) {
    const container = document.querySelector('.container');
    const alerta = document.createElement('div');
    alerta.style.background = '#fff3e0';
    alerta.style.border = '1px solid #ffb74d';
    alerta.style.borderRadius = '8px';
    alerta.style.padding = '15px';
    alerta.style.marginBottom = '20px';
    
    alerta.innerHTML = `
        <h3 style="margin-bottom: 10px; color: #e65100;">⚠️ Advertencia</h3>
        <p>${mensaje}</p>
    `;
    
    container.insertBefore(alerta, container.firstChild);
}

// Función para cambiar pestañas
function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    
    document.querySelector(`.tab[onclick="switchTab('${tabName}')"]`).classList.add('active');
    document.getElementById(`tab-${tabName}`).classList.add('active');
    
    if (tabName === 'productos') {
        detectarProductosDuplicados();
    } else if (tabName === 'facturas') {
        detectarFacturasDuplicadas();
    }
}

// Cargar estadísticas generales
async function cargarEstadisticas() {
    try {
        const response = await fetch(`${API_URL}/admin/stats`);
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const data = await response.json();
        
        document.getElementById('total-productos').textContent = data.productos_unicos || 0;
        document.getElementById('total-facturas').textContent = data.total_facturas || 0;
        
    } catch (error) {
        console.error('Error cargando estadísticas:', error);
        mostrarToast('Error al cargar estadísticas: ' + error.message, 'error');
        
        // Usar datos de ejemplo si falla
        document.getElementById('total-productos').textContent = '358';
        document.getElementById('total-facturas').textContent = '124';
    }
}

// GESTIÓN DE PRODUCTOS DUPLICADOS
async function detectarProductosDuplicados() {
    const container = document.getElementById('productos-duplicados-container');
    container.innerHTML = '<div class="loading"><div class="spinner"></div><p>Analizando duplicados de productos...</p></div>';
    
    try {
        const umbral = document.getElementById('umbralSimilitud').value;
        const criterio = document.getElementById('criteriosProductos').value;
        
        const url = `${API_URL}/admin/duplicados/productos?umbral=${umbral}&criterio=${criterio}`;
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const data = await response.json();
        
        productosDuplicados = data.duplicados || [];
        
        // Actualizar estadísticas
        document.getElementById('total-duplicados').textContent = data.total || 0;
        
        if (productosDuplicados.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No se encontraron productos duplicados</h3>
                    <p>Prueba con un umbral de similitud más bajo o diferentes criterios</p>
                </div>
            `;
            document.getElementById('productos-pagination').innerHTML = '';
            document.getElementById('productos-ia-panel').style.display = 'none';
            return;
        }
        
        // Mostrar panel de IA
        document.getElementById('productos-ia-panel').style.display = 'block';
        
        // Renderizar la primera página
        renderizarPaginaProductos(1);
        
    } catch (error) {
        console.error('Error detectando duplicados de productos:', error);
        
        container.innerHTML = `
            <div style="padding: 20px; background: #ffebee; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="color: #c62828; margin-bottom: 10px;">Error al buscar duplicados</h3>
                <p>${error.message || 'Error desconocido'}</p>
                <button class="btn btn-primary" onclick="detectarProductosDuplicados()" style="margin-top: 15px;">
                    Intentar nuevamente
                </button>
            </div>
        `;
        document.getElementById('productos-ia-panel').style.display = 'none';
    }
}

function renderizarPaginaProductos(pagina) {
    const container = document.getElementById('productos-duplicados-container');
    const paginacion = document.getElementById('productos-pagination');
    
    // Guardar la página actual
    paginaActualProductos = pagina;
    
    // Calcular índices para la paginación
    const inicio = (pagina - 1) * elementosPorPagina;
    const fin = Math.min(inicio + elementosPorPagina, productosDuplicados.length);
    const duplicadosPagina = productosDuplicados.slice(inicio, fin);
    
    let html = '';
    
    duplicadosPagina.forEach((dup, index) => {
        const similitudClase = dup.similitud >= 85 ? 'similarity-high' : 
                              dup.similitud >= 70 ? 'similarity-medium' : 'similarity-low';
        
        // Determinar criterios de similitud para mostrar etiquetas
        const criterios = [];
        if (dup.mismo_codigo) criterios.push('Mismo código');
        if (dup.mismo_establecimiento) criterios.push('Mismo establecimiento');
        if (dup.nombre_similar) criterios.push('Nombre similar');
        
        // Verificar si este duplicado ha sido procesado por IA
        const resultadoIA = resultadosProductosIA.find(r => r.dupId === dup.id || r.dupId === index.toString());
        const iaTag = resultadoIA ? 
            `<span class="criteria-tag" style="background: #e8f5e9; color: #2e7d32;">IA: ${resultadoIA.decision === 1 ? 'Mantener #1' : 'Mantener #2'} (${resultadoIA.confianza}%)</span>` : '';
        
        html += `
            <div id="duplicado-${dup.id || index}" class="duplicate-container">
                <div class="duplicate-header">
                    <div>
                        <span class="similarity-tag ${similitudClase}">Similitud: ${dup.similitud}%</span>
                        ${criterios.map(c => `<span class="criteria-tag">${c}</span>`).join('')}
                        ${iaTag}
                    </div>
                    <button class="btn btn-success btn-sm" onclick="fusionarProductos('${dup.id || index}')">
                        Fusionar Seleccionado
                    </button>
                </div>
                
                <div class="duplicate-grid">
                    <div id="producto-${dup.producto1.id}" class="duplicate-item" data-id="${dup.producto1.id}">
                        <div class="selection-mark ${resultadoIA && resultadoIA.decision === 1 ? 'selected' : (dup.seleccionado === dup.producto2.id ? 'not-selected' : 'selected')}">1</div>
                        
                        <div class="duplicate-metadata">
                            <h3>Producto #${dup.producto1.id}</h3>
                            <p style="margin-top: 5px; color: #666;">
                                <strong>Nombre:</strong> ${dup.producto1.nombre}<br>
                                <strong>Código:</strong> ${dup.producto1.codigo || 'Sin código'}<br>
                                <strong>Establecimiento:</strong> ${dup.producto1.establecimiento || 'Desconocido'}<br>
                                <strong>Precio actual:</strong> $${typeof dup.producto1.precio === 'number' ? dup.producto1.precio.toLocaleString() : dup.producto1.precio || 'N/A'}<br>
                                <strong>Última actualización:</strong> ${dup.producto1.ultima_actualizacion ? new Date(dup.producto1.ultima_actualizacion).toLocaleDateString() : 'N/A'}<br>
                                <strong>Visto:</strong> ${dup.producto1.veces_visto || 0} veces
                            </p>
                        </div>
                        
                        <button class="btn ${resultadoIA && resultadoIA.decision === 1 ? 'btn-primary' : (dup.seleccionado === dup.producto2.id ? 'btn-secondary' : 'btn-primary')} select-button" onclick="seleccionarProducto('${dup.id || index}', ${dup.producto1.id})">
                            ${resultadoIA && resultadoIA.decision === 1 ? '✓ Seleccionado por IA' : (dup.seleccionado === dup.producto2.id ? 'Seleccionar Este' : '✓ Seleccionar Este')}
                        </button>
                    </div>
                    
                    <div id="producto-${dup.producto2.id}" class="duplicate-item" data-id="${dup.producto2.id}">
                        <div class="selection-mark ${resultadoIA && resultadoIA.decision === 2 ? 'selected' : (dup.seleccionado === dup.producto2.id ? 'selected' : 'not-selected')}">2</div>
                        
                        <div class="duplicate-metadata">
                            <h3>Producto #${dup.producto2.id}</h3>
                            <p style="margin-top: 5px; color: #666;">
                                <strong>Nombre:</strong> ${dup.producto2.nombre}<br>
                                <strong>Código:</strong> ${dup.producto2.codigo || 'Sin código'}<br>
                                <strong>Establecimiento:</strong> ${dup.producto2.establecimiento || 'Desconocido'}<br>
                                <strong>Precio actual:</strong> ${typeof dup.producto2.precio === 'number' ? dup.producto2.precio.toLocaleString() : dup.producto2.precio || 'N/A'}<br>
                                <strong>Última actualización:</strong> ${dup.producto2.ultima_actualizacion ? new Date(dup.producto2.ultima_actualizacion).toLocaleDateString() : 'N/A'}<br>
                                <strong>Visto:</strong> ${dup.producto2.veces_visto || 0} veces
}
