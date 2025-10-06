
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
                                <strong>Precio actual:</strong> $${typeof dup.producto2.precio === 'number' ? dup.producto2.precio.toLocaleString() : dup.producto2.precio || 'N/A'}<br>
                                <strong>Última actualización:</strong> ${dup.producto2.ultima_actualizacion ? new Date(dup.producto2.ultima_actualizacion).toLocaleDateString() : 'N/A'}<br>
                                <strong>Visto:</strong> ${dup.producto2.veces_visto || 0} veces
                            </p>
                        </div>
                        
                        <button class="btn ${resultadoIA && resultadoIA.decision === 2 ? 'btn-primary' : (dup.seleccionado === dup.producto2.id ? 'btn-primary' : 'btn-secondary')} select-button" onclick="seleccionarProducto('${dup.id || index}', ${dup.producto2.id})">
                            ${resultadoIA && resultadoIA.decision === 2 ? '✓ Seleccionado por IA' : (dup.seleccionado === dup.producto2.id ? '✓ Seleccionar Este' : 'Seleccionar Este')}
                        </button>
                    </div>
                </div>
                
                <div style="padding: 15px; background: #f5f7fa; border-top: 1px solid #e0e0e0;">
                    <h4 style="margin-bottom: 10px;">Razón de duplicidad</h4>
                    <p>${dup.razon || 'Productos con características similares'}</p>
                    ${resultadoIA ? `<p style="margin-top: 10px; color: #2e7d32;"><strong>Análisis IA:</strong> ${resultadoIA.explicacion || 'Sin explicación disponible'}</p>` : ''}
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
    
    // Generar paginación
    const totalPaginas = Math.ceil(productosDuplicados.length / elementosPorPagina);
    let paginacionHTML = '';
    
    if (totalPaginas > 1) {
        paginacionHTML += `
            <div class="pagination-item" onclick="renderizarPaginaProductos(1)">«</div>
            <div class="pagination-item" onclick="renderizarPaginaProductos(${Math.max(pagina - 1, 1)})">‹</div>
        `;
        
        for (let i = 1; i <= totalPaginas; i++) {
            if (i === 1 || i === totalPaginas || (i >= pagina - 1 && i <= pagina + 1)) {
                paginacionHTML += `
                    <div class="pagination-item ${i === pagina ? 'active' : ''}" onclick="renderizarPaginaProductos(${i})">
                        ${i}
                    </div>
                `;
            } else if (i === pagina - 2 || i === pagina + 2) {
                paginacionHTML += '<div class="pagination-item">...</div>';
            }
        }
        
        paginacionHTML += `
            <div class="pagination-item" onclick="renderizarPaginaProductos(${Math.min(pagina + 1, totalPaginas)})">›</div>
            <div class="pagination-item" onclick="renderizarPaginaProductos(${totalPaginas})">»</div>
        `;
    }
    
    paginacion.innerHTML = paginacionHTML;
}

function seleccionarProducto(dupId, productoId) {
    const duplicado = productosDuplicados.find(d => d.id === dupId || productosDuplicados.indexOf(d) === parseInt(dupId));
    if (!duplicado) return;
    
    // Determinar cuál es el producto seleccionado y cuál no
    let productoSeleccionado, productoNoSeleccionado;
    if (duplicado.producto1.id === productoId) {
        productoSeleccionado = duplicado.producto1;
        productoNoSeleccionado = duplicado.producto2;
        
        document.querySelector(`#producto-${duplicado.producto1.id} .selection-mark`).className = 'selection-mark selected';
        document.querySelector(`#producto-${duplicado.producto2.id} .selection-mark`).className = 'selection-mark not-selected';
        
        document.querySelector(`#producto-${duplicado.producto1.id} .select-button`).className = 'btn btn-primary select-button';
        document.querySelector(`#producto-${duplicado.producto1.id} .select-button`).innerHTML = '✓ Seleccionar Este';
        
        document.querySelector(`#producto-${duplicado.producto2.id} .select-button`).className = 'btn btn-secondary select-button';
        document.querySelector(`#producto-${duplicado.producto2.id} .select-button`).innerHTML = 'Seleccionar Este';
        
    } else {
        productoSeleccionado = duplicado.producto2;
        productoNoSeleccionado = duplicado.producto1;
        
        document.querySelector(`#producto-${duplicado.producto1.id} .selection-mark`).className = 'selection-mark not-selected';
        document.querySelector(`#producto-${duplicado.producto2.id} .selection-mark`).className = 'selection-mark selected';
        
        document.querySelector(`#producto-${duplicado.producto1.id} .select-button`).className = 'btn btn-secondary select-button';
        document.querySelector(`#producto-${duplicado.producto1.id} .select-button`).innerHTML = 'Seleccionar Este';
        
        document.querySelector(`#producto-${duplicado.producto2.id} .select-button`).className = 'btn btn-primary select-button';
        document.querySelector(`#producto-${duplicado.producto2.id} .select-button`).innerHTML = '✓ Seleccionar Este';
    }
    
    // Actualizar el duplicado con la selección
    duplicado.seleccionado = productoId;

    // Si hay un resultado de IA para este duplicado, actualizar la decisión manual
    const resultadoIndex = resultadosProductosIA.findIndex(r => r.dupId === dupId || r.dupId === parseInt(dupId));
    if (resultadoIndex !== -1) {
        resultadosProductosIA[resultadoIndex].decisionManual = duplicado.producto1.id === productoId ? 1 : 2;
    }
}

async function fusionarProductos(dupId) {
    const duplicado = productosDuplicados.find(d => d.id === dupId || productosDuplicados.indexOf(d) === parseInt(dupId));
    if (!duplicado) return;
    
    // Si no hay seleccionado, por defecto es el producto1
    const productoMantener = duplicado.seleccionado === duplicado.producto2.id ? 
                            duplicado.producto2 : duplicado.producto1;
    const productoEliminar = productoMantener === duplicado.producto1 ? 
                            duplicado.producto2 : duplicado.producto1;
    
    if (!confirm(`¿Fusionar estos productos? Se mantendrá "${productoMantener.nombre}" (#${productoMantener.id}) y se eliminará "${productoEliminar.nombre}" (#${productoEliminar.id})`)) {
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/admin/duplicados/productos/fusionar`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                producto_mantener_id: productoMantener.id,
                producto_eliminar_id: productoEliminar.id
            })
        });
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const result = await response.json();
        
        // Actualizar contador de fusiones
        totalFusiones++;
        document.getElementById('total-fusiones').textContent = totalFusiones;
        
        // Eliminar el duplicado fusionado
        productosDuplicados = productosDuplicados.filter(d => d.id !== dupId && productosDuplicados.indexOf(d) !== parseInt(dupId));
        
        // Eliminar de los resultados de IA si existe
        resultadosProductosIA = resultadosProductosIA.filter(r => r.dupId !== dupId && r.dupId !== parseInt(dupId));
        
        // Actualizar estadísticas
        document.getElementById('total-duplicados').textContent = productosDuplicados.length;
        
        // Eliminar visualmente
        document.getElementById(`duplicado-${dupId}`).style.animation = 'fadeOut 0.5s forwards';
        setTimeout(() => {
            // Actualizar la vista actual
            renderizarPaginaProductos(paginaActualProductos);
        }, 500);
        
        mostrarToast('Productos fusionados correctamente');
        
    } catch (error) {
        console.error('Error fusionando productos:', error);
        mostrarToast('Error al fusionar productos: ' + error.message, 'error');
    }
}

function mostrarAyudaProductos() {
    document.getElementById('popup-ayuda-productos').style.display = 'flex';
}

// GESTIÓN DE FACTURAS DUPLICADAS
async function detectarFacturasDuplicadas() {
    const container = document.getElementById('facturas-duplicadas-container');
    container.innerHTML = '<div class="loading"><div class="spinner"></div><p>Analizando duplicados de facturas...</p></div>';
    
    try {
        const criterio = document.getElementById('criterioFacturas').value;
        
        const url = `${API_URL}/admin/duplicados/facturas?criterio=${criterio}`;
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const data = await response.json();
        
        facturasDuplicadas = data.duplicados || [];
        
        // Actualizar estadísticas
        document.getElementById('total-facturas-duplicadas').textContent = data.total || 0;
        
        if (facturasDuplicadas.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No se encontraron facturas duplicadas</h3>
                    <p>Prueba con diferentes criterios de búsqueda</p>
                </div>
            `;
            document.getElementById('facturas-pagination').innerHTML = '';
            document.getElementById('facturas-ia-panel').style.display = 'none';
            return;
        }
        
        // Mostrar panel de IA
        document.getElementById('facturas-ia-panel').style.display = 'block';
        
        // Pre-verificar si las facturas tienen imágenes
        const mostrarImagenes = document.getElementById('mostrarImagenes').checked;
        if (mostrarImagenes) {
            for (const dup of facturasDuplicadas) {
                try {
                    const resp1 = await fetch(`${API_URL}/admin/duplicados/facturas/${dup.factura1.id}/check-image`);
                    const data1 = await resp1.json();
                    dup.factura1.tieneImagen = data1.tiene_imagen;
                    
                    const resp2 = await fetch(`${API_URL}/admin/duplicados/facturas/${dup.factura2.id}/check-image`);
                    const data2 = await resp2.json();
                    dup.factura2.tieneImagen = data2.tiene_imagen;
                } catch (error) {
                    console.error('Error verificando imágenes:', error);
                    dup.factura1.tieneImagen = false;
                    dup.factura2.tieneImagen = false;
                }
            }
        }
        
        // Renderizar la primera página
        renderizarPaginaFacturas(1);
        
    } catch (error) {
        console.error('Error detectando duplicados de facturas:', error);
        
        // Mostrar mensaje de error
        container.innerHTML = `
            <div style="padding: 20px; background: #ffebee; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="color: #c62828; margin-bottom: 10px;">Error al buscar duplicados</h3>
                <p>${error.message || 'Error desconocido'}</p>
                <button class="btn btn-primary" onclick="detectarFacturasDuplicadas()" style="margin-top: 15px;">
                    Intentar nuevamente
                </button>
            </div>
        `;
        document.getElementById('facturas-ia-panel').style.display = 'none';
    }
}

// Renderizar página de facturas
function renderizarPaginaFacturas(pagina) {
    // Código de la función...
    // Por brevedad no la incluyo completa, pero deberías copiarla del HTML original
}

// Eliminar factura
async function eliminarFactura(facturaId, dupId) {
    // Código de la función...
}

function mostrarImagenAmpliada(facturaId, titulo) {
    document.getElementById('popup-imagen-titulo').textContent = titulo;
    document.getElementById('popup-imagen-contenido').src = `${API_URL}/admin/duplicados/facturas/${facturaId}/imagen`;
    document.getElementById('popup-imagen').style.display = 'flex';
}

function compararFacturas(factura1Id, factura2Id) {
    window.open(`/comparador-facturas?id1=${factura1Id}&id2=${factura2Id}`, '_blank');
}

function verFactura(facturaId) {
    window.open(`/editor?id=${facturaId}`, '_blank');
}

function mostrarAyudaFacturas() {
    document.getElementById('popup-ayuda-facturas').style.display = 'flex';
}

function cerrarPopup(id) {
    document.getElementById(id).style.display = 'none';
}

function mostrarToast(mensaje, tipo = 'success') {
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.textContent = mensaje;
    
    if (tipo === 'error') {
        toast.style.background = '#ea4335';
    } else if (tipo === 'info') {
        toast.style.background = '#4285f4';
    }
    
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.remove();
    }, 3000);
}

// INTEGRACIÓN CON API DE ANTHROPIC

// Guardar la configuración de IA
function guardarConfiguracionIA() {
    // Obtener el valor del API key del campo (o mantener el actual si está oculto con asteriscos)
    const apiKeyInput = document.getElementById('apiKey').value;
    const apiKey = apiKeyInput === "********" ? configuracionIA.apiKey : apiKeyInput;
    
    configuracionIA = {
        apiKey: apiKey,
        modelo: document.getElementById('modeloIA').value,
        tamanoLote: parseInt(document.getElementById('tamanoLote').value),
        intervaloLote: parseInt(document.getElementById('intervaloLote').value),
        criteriosProductos: document.getElementById('criteriosProductosIA').value.trim(),
        criteriosFacturas: document.getElementById('criteriosFacturasIA').value.trim()
    };
    
    // Guardar en localStorage (excepto la API key por seguridad)
    localStorage.setItem('lecfac_ia_config', JSON.stringify({
        modelo: configuracionIA.modelo,
        tamanoLote: configuracionIA.tamanoLote,
        intervaloLote: configuracionIA.intervaloLote,
        criteriosProductos: configuracionIA.criteriosProductos,
        criteriosFacturas: configuracionIA.criteriosFacturas
    }));
    
    // Si se actualizó la API key, enviarla al servidor para guardarla
    if (apiKeyInput !== "********" && apiKeyInput.trim() !== "") {
        guardarAPIKey(apiKeyInput);
    }
    
    mostrarToast('Configuración guardada correctamente', 'success');
}

// Guardar API key en el servidor
async function guardarAPIKey(apiKey) {
    try {
        const response = await fetch(`${API_URL}/api/config/anthropic-key`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ apiKey })
        });
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
    } catch (error) {
        console.error('Error guardando API key:', error);
        mostrarToast('Error al guardar la API key en el servidor', 'error');
    }
}

// Cargar la configuración de IA desde localStorage
function cargarConfiguracionIA() {
    const configuracionGuardada = localStorage.getItem('lecfac_ia_config');
    if (configuracionGuardada) {
        try {
            const config = JSON.parse(configuracionGuardada);
            
            if (config.modelo) document.getElementById('modeloIA').value = config.modelo;
            if (config.tamanoLote) document.getElementById('tamanoLote').value = config.tamanoLote;
            if (config.intervaloLote) document.getElementById('intervaloLote').value = config.intervaloLote;
            if (config.criteriosProductos) document.getElementById('criteriosProductosIA').value = config.criteriosProductos;
            if (config.criteriosFacturas) document.getElementById('criteriosFacturasIA').value = config.criteriosFacturas;
            
            // Actualizar configuración actual
            configuracionIA.modelo = config.modelo || configuracionIA.modelo;
            configuracionIA.tamanoLote = config.tamanoLote || configuracionIA.tamanoLote;
            configuracionIA.intervaloLote = config.intervaloLote || configuracionIA.intervaloLote;
            configuracionIA.criteriosProductos = config.criteriosProductos || configuracionIA.criteriosProductos;
            configuracionIA.criteriosFacturas = config.criteriosFacturas || configuracionIA.criteriosFacturas;
            
        } catch (error) {
            console.error('Error cargando configuración de IA:', error);
        }
    }
    
    // Asegurarse de que los criterios tengan valores por defecto
    if (!configuracionIA.criteriosProductos) {
        configuracionIA.criteriosProductos = document.getElementById('criteriosProductosIA').value.trim();
    }
    
    if (!configuracionIA.criteriosFacturas) {
        configuracionIA.criteriosFacturas = document.getElementById('criteriosFacturasIA').value.trim();
    }
}

// Función para procesar duplicados con IA
async function procesarDuplicadosIA(tipo) {
    if (procesandoIA) {
        mostrarToast('Ya hay un proceso de IA en ejecución. Espera a que termine.', 'error');
        return;
    }
    
    let duplicados;
    let confianzaMinima;
    
    if (tipo === 'productos') {
        duplicados = [...productosDuplicados];
        confianzaMinima = parseInt(document.getElementById('confianzaProductos').value);
    } else { // facturas
        duplicados = [...facturasDuplicadas];
        confianzaMinima = parseInt(document.getElementById('confianzaFacturas').value);
    }
    
    if (duplicados.length === 0) {
        mostrarToast(`No se encontraron ${tipo} duplicados para procesar`, 'error');
        return;
    }
    
    // Verificar si tenemos la API key
    if (!configuracionIA.apiKey) {
        mostrarToast("No se ha configurado la API key de Anthropic", 'error');
        return;
    }
    
    // Dividir en lotes
    const tamanoLote = configuracionIA.tamanoLote;
    const lotes = [];
    
    for (let i = 0; i < duplicados.length; i += tamanoLote) {
        lotes.push(duplicados.slice(i, i + tamanoLote));
    }
    
    // Configurar panel de progreso
    document.getElementById(`${tipo}-batch-status`).style.display = 'flex';
    document.getElementById(`${tipo}-progreso`).textContent = `0/${duplicados.length}`;
    document.getElementById(`${tipo}-progress-bar`).style.width = '0%';
    
    // Iniciar procesamiento
    procesandoIA = true;
    procesoIACancelado = false;
    
    // Limpiar resultados anteriores
    if (tipo === 'productos') {
        resultadosProductosIA = [];
    } else {
        resultadosFacturasIA = [];
    }
    
    let procesados = 0;
    
    // Procesar lotes secuencialmente
    for (let i = 0; i < lotes.length; i++) {
        if (procesoIACancelado) {
            break;
        }
        
        const lote = lotes[i];
        const resultadosLote = await procesarLoteIA(tipo, lote, confianzaMinima);
        
        // Almacenar resultados
        if (tipo === 'productos') {
            resultadosProductosIA = [...resultadosProductosIA, ...resultadosLote];
        } else {
            resultadosFacturasIA = [...resultadosFacturasIA, ...resultadosLote];
        }
        
        // Actualizar progreso
        procesados += lote.length;
        const porcentaje = (procesados / duplicados.length) * 100;
        document.getElementById(`${tipo}-progreso`).textContent = `${procesados}/${duplicados.length}`;
        document.getElementById(`${tipo}-progress-bar`).style.width = `${porcentaje}%`;
        
        // Actualizar interfaz con las decisiones de IA
        if (tipo === 'productos') {
            renderizarPaginaProductos(paginaActualProductos);
        } else {
            renderizarPaginaFacturas(paginaActualFacturas);
        }
        
        // Actualizar contador
        document.getElementById(`${tipo}-procesados-ia`).textContent = procesados;
        
        // Esperar antes del siguiente lote (a menos que sea el último)
        if (i < lotes.length - 1 && !procesoIACancelado) {
            await new Promise(resolve => setTimeout(resolve, configuracionIA.intervaloLote * 1000));
        }
    }
    
    // Finalizar procesamiento
    procesandoIA = false;
    
    // Ocultar panel de progreso
    setTimeout(() => {
        document.getElementById(`${tipo}-batch-status`).style.display = 'none';
    }, 1000);
    
    // Mostrar resumen
    if (!procesoIACancelado) {
        mostrarResumenIA(tipo);
    }
}

// Procesar un lote de duplicados con IA
async function procesarLoteIA(tipo, lote, confianzaMinima) {
    try {
        // Preparar prompt para IA según tipo
        let promptBase;
        let datosAnalisis;
        
        if (tipo === 'productos') {
            // Código para productos...
        } else { // facturas
            // Código para facturas...
        }
        
        // Prompt completo
        const promptCompleto = `${promptBase}\n\nLos datos a analizar son:\n${JSON.stringify(datosAnalisis, null, 2)}\n\nDa tu respuesta en formato JSON, con la siguiente estructura para cada elemento analizado:\n{\n  "dupId": "id_del_duplicado",\n  "decision": 1 o 2 (1 para mantener el primero, 2 para mantener el segundo),\n  "confianza": porcentaje de confianza (0-100),\n  "explicacion": "explicación detallada de la decisión"\n}\n\nDevuelve un array de estos objetos, uno para cada duplicado analizado.`;
        
        // Llamar a la API de Anthropic
        const response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': configuracionIA.apiKey,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: configuracionIA.modelo,
                max_tokens: 4000,
                temperature: 0.1,
                messages: [
                    { role: "user", content: promptCompleto }
                ]
            })
        });
        
        // Procesar respuesta...
        
        return resultadosFiltrados;
        
    } catch (error) {
        console.error(`Error en procesamiento IA de ${tipo}:`, error);
        mostrarToast(`Error en procesamiento IA: ${error.message}`, 'error');
        return [];
    }
}

// Función para mostrar resumen de resultados IA
function mostrarResumenIA(tipo) {
    // Código de la función...
}

// Función para mostrar resultados y aplicar decisiones
function guardarResultadosIA(tipo) {
    mostrarResumenIA(tipo);
}

// Función para aplicar decisiones de IA
async function aplicarDecisionesIA() {
    // Código de la función...
}

function cancelarProcesamientoIA(tipo) {
    procesoIACancelado = true;
    mostrarToast(`Procesamiento de ${tipo} cancelado`, 'info');
}
