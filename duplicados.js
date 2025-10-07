// ============================================
// DUPLICADOS.JS - SISTEMA DE GESTIÓN DE DUPLICADOS
// ============================================

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
    apiKey: "",
    modelo: "claude-3-5-sonnet-20241022",
    tamanoLote: 10,
    intervaloLote: 5,
    criteriosProductos: "",
    criteriosFacturas: ""
};

// ============================================
// INICIALIZACIÓN
// ============================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Inicializando gestor de duplicados...');
    verificarConexionAPI();
    cargarEstadisticas();
    cargarConfiguracionIA();
    obtenerAPIKey();
    
    // Event listeners
    const umbralElement = document.getElementById('umbralSimilitud');
    if (umbralElement) {
        umbralElement.addEventListener('change', detectarProductosDuplicados);
    }
    
    const criteriosProductosElement = document.getElementById('criteriosProductos');
    if (criteriosProductosElement) {
        criteriosProductosElement.addEventListener('change', detectarProductosDuplicados);
    }
    
    const criterioFacturasElement = document.getElementById('criterioFacturas');
    if (criterioFacturasElement) {
        criterioFacturasElement.addEventListener('change', detectarFacturasDuplicadas);
    }
});

// ============================================
// FUNCIONES DE VERIFICACIÓN Y CONFIGURACIÓN
// ============================================

async function verificarConexionAPI() {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        
        const response = await fetch(`${API_URL}/api/health-check`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (!response.ok) {
            console.warn('API no respondió correctamente');
        } else {
            console.log('✅ Conexión con API establecida');
        }
    } catch (error) {
        console.error('❌ Error conectando con API:', error);
        mostrarMensajeError('No se pudo conectar con el servidor. Algunas funciones pueden no estar disponibles.');
    }
}

async function obtenerAPIKey() {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        
        const response = await fetch(`${API_URL}/api/config/anthropic-key`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (response.ok) {
            const data = await response.json();
            configuracionIA.apiKey = data.apiKey || "";
            
            const apiKeyElement = document.getElementById('apiKey');
            if (apiKeyElement) {
                apiKeyElement.value = configuracionIA.apiKey ? "********" : "";
            }
            
            console.log('✅ API Key obtenida del servidor');
        }
    } catch (error) {
        console.warn('⚠️ No se pudo obtener API key del servidor:', error.message);
    }
}

async function cargarEstadisticas() {
    try {
        const response = await fetch(`${API_URL}/admin/stats`);
        if (!response.ok) throw new Error(`Error HTTP: ${response.status}`);
        
        const data = await response.json();
        
        document.getElementById('total-productos').textContent = data.productos_unicos || 0;
        document.getElementById('total-facturas').textContent = data.total_facturas || 0;
        
        console.log('✅ Estadísticas cargadas');
    } catch (error) {
        console.error('❌ Error cargando estadísticas:', error);
        // Usar valores por defecto
        document.getElementById('total-productos').textContent = '-';
        document.getElementById('total-facturas').textContent = '-';
    }
}

function cargarConfiguracionIA() {
    const criteriosProductosDefault = `1. Si dos productos tienen el mismo código EAN y están en el mismo establecimiento, son duplicados.
2. Si dos productos tienen nombres muy similares (>85% similitud) y están en el mismo establecimiento, son duplicados.
3. Si los productos tienen la misma marca, mismo tamaño pero descritos de forma diferente, son duplicados.
4. Para productos sin código EAN, evaluar si las descripciones se refieren al mismo producto con diferente formato.
5. El producto con información más completa y actualizada debe ser conservado.
6. En caso de duda, conservar el producto que tiene precio más actualizado o veces visto más alto.
7. Considerar variaciones de formato como "500g" vs "1/2 kg" como el mismo producto.`;

    const criteriosFacturasDefault = `1. Si dos facturas tienen la misma fecha, mismo establecimiento y mismo total, son duplicadas.
2. Si dos facturas tienen la misma fecha, mismo establecimiento y productos idénticos o muy similares, son duplicadas.
3. Si dos facturas del mismo establecimiento tienen el mismo número de referencia, son duplicadas.
4. Para facturas con imagen, la imagen debe ser revisada para confirmar que es la misma factura.
5. En caso de duplicados, conservar la factura con imagen sobre la que no tiene imagen.
6. Si ambas tienen imagen, conservar la factura con mejor calidad de imagen o con procesamiento más completo.
7. Si difieren ligeramente en productos, la factura más completa debe ser conservada.`;

    const criteriosProductosElement = document.getElementById('criteriosProductosIA');
    const criteriosFacturasElement = document.getElementById('criteriosFacturasIA');
    
    if (criteriosProductosElement && !criteriosProductosElement.value) {
        criteriosProductosElement.value = criteriosProductosDefault;
        configuracionIA.criteriosProductos = criteriosProductosDefault;
    }
    
    if (criteriosFacturasElement && !criteriosFacturasElement.value) {
        criteriosFacturasElement.value = criteriosFacturasDefault;
        configuracionIA.criteriosFacturas = criteriosFacturasDefault;
    }
}

async function guardarConfiguracionIA() {
    const apiKeyElement = document.getElementById('apiKey');
    const modeloElement = document.getElementById('modeloIA');
    const criteriosProductosElement = document.getElementById('criteriosProductosIA');
    const criteriosFacturasElement = document.getElementById('criteriosFacturasIA');
    const tamanoLoteElement = document.getElementById('tamanoLote');
    const intervaloLoteElement = document.getElementById('intervaloLote');
    
    // Solo actualizar API key si no son asteriscos
    if (apiKeyElement && apiKeyElement.value && apiKeyElement.value !== "********") {
        configuracionIA.apiKey = apiKeyElement.value;
    }
    
    if (modeloElement) configuracionIA.modelo = modeloElement.value;
    if (criteriosProductosElement) configuracionIA.criteriosProductos = criteriosProductosElement.value;
    if (criteriosFacturasElement) configuracionIA.criteriosFacturas = criteriosFacturasElement.value;
    if (tamanoLoteElement) configuracionIA.tamanoLote = parseInt(tamanoLoteElement.value);
    if (intervaloLoteElement) configuracionIA.intervaloLote = parseInt(intervaloLoteElement.value);
    
    mostrarToast('✅ Configuración guardada exitosamente');
}

// ============================================
// GESTIÓN DE PESTAÑAS
// ============================================

function switchTab(tabName) {
    // Remover clases activas
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    
    // Agregar clases activas
    const tabButton = document.querySelector(`.tab[onclick*="${tabName}"]`);
    if (tabButton) tabButton.classList.add('active');
    
    const tabContent = document.getElementById(`tab-${tabName}`);
    if (tabContent) tabContent.classList.add('active');
    
    // Acciones específicas por pestaña
    if (tabName === 'productos') {
        console.log('📦 Cargando pestaña de productos duplicados');
    } else if (tabName === 'facturas') {
        console.log('🧾 Cargando pestaña de facturas duplicadas');
    } else if (tabName === 'configuracion') {
        console.log('⚙️ Cargando configuración de IA');
    }
}

// ============================================
// DETECCIÓN DE PRODUCTOS DUPLICADOS
// ============================================

async function detectarProductosDuplicados() {
    const container = document.getElementById('productos-duplicados-container');
    container.innerHTML = '<div class="loading"><div class="spinner"></div><p>Analizando duplicados de productos...</p></div>';
    
    try {
        const umbral = document.getElementById('umbralSimilitud')?.value || '85';
        const criterio = document.getElementById('criteriosProductos')?.value || 'todos';
        
        console.log(`🔍 Detectando productos duplicados: umbral=${umbral}, criterio=${criterio}`);
        
        const response = await fetch(`${API_URL}/admin/duplicados/productos?umbral=${umbral}&criterio=${criterio}`);
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        productosDuplicados = data.duplicados || [];
        
        // Actualizar estadísticas
        document.getElementById('total-duplicados').textContent = data.total || 0;
        
        if (productosDuplicados.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>✅ No se encontraron productos duplicados</h3>
                    <p>Prueba con un umbral de similitud más bajo o diferentes criterios</p>
                </div>
            `;
            document.getElementById('productos-pagination').innerHTML = '';
            document.getElementById('productos-ia-panel').style.display = 'none';
            return;
        }
        
        console.log(`✅ Encontrados ${productosDuplicados.length} productos duplicados`);
        
        // Mostrar panel de IA
        document.getElementById('productos-ia-panel').style.display = 'block';
        
        // Renderizar primera página
        renderizarPaginaProductos(1);
        
    } catch (error) {
        console.error('❌ Error detectando duplicados de productos:', error);
        
        container.innerHTML = `
            <div style="padding: 20px; background: #ffebee; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="color: #c62828; margin-bottom: 10px;">❌ Error al buscar duplicados</h3>
                <p>${error.message || 'Error desconocido'}</p>
                <button class="btn btn-primary" onclick="detectarProductosDuplicados()" style="margin-top: 15px;">
                    🔄 Intentar nuevamente
                </button>
            </div>
        `;
        document.getElementById('productos-ia-panel').style.display = 'none';
    }
}

function renderizarPaginaProductos(pagina) {
    paginaActualProductos = pagina;
    
    const inicio = (pagina - 1) * elementosPorPagina;
    const fin = Math.min(inicio + elementosPorPagina, productosDuplicados.length);
    const duplicadosPagina = productosDuplicados.slice(inicio, fin);
    
    const container = document.getElementById('productos-duplicados-container');
    
    if (duplicadosPagina.length === 0) {
        container.innerHTML = '<div class="empty-state"><h3>No hay más duplicados</h3></div>';
        return;
    }
    
    let html = '';
    
    duplicadosPagina.forEach((dup, localIndex) => {
        const globalIndex = inicio + localIndex;
        const dupId = dup.id || `dup-${globalIndex}`;
        
        // Determinar cuál está seleccionado (por defecto producto1)
        const seleccionado = dup.seleccionado || dup.producto1.id;
        
        // Verificar si hay resultado de IA
        const resultadoIA = resultadosProductosIA.find(r => r.dupId === dupId);
        
        html += `
            <div id="duplicado-${dupId}" class="duplicate-container">
                <div class="duplicate-header">
                    <div>
                        <span class="similarity-tag similarity-${dup.similitud >= 85 ? 'high' : dup.similitud >= 70 ? 'medium' : 'low'}">
                            Similitud: ${dup.similitud}%
                        </span>
                        ${dup.mismo_codigo ? '<span class="criteria-tag">Mismo código</span>' : ''}
                        ${dup.mismo_establecimiento ? '<span class="criteria-tag">Mismo establecimiento</span>' : ''}
                        ${dup.nombre_similar ? '<span class="criteria-tag">Nombre similar</span>' : ''}
                        ${resultadoIA ? `<span class="criteria-tag" style="background: #e8f5e9; color: #2e7d32;">IA: Mantener #${resultadoIA.decision} (${resultadoIA.confianza}%)</span>` : ''}
                    </div>
                    <button class="btn btn-success btn-sm" onclick="fusionarProductos('${dupId}')">
                        Fusionar Seleccionado
                    </button>
                </div>
                
                <div class="duplicate-grid">
                    ${renderizarProductoItem(dup.producto1, dupId, 1, seleccionado, resultadoIA)}
                    ${renderizarProductoItem(dup.producto2, dupId, 2, seleccionado, resultadoIA)}
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
    
    // Generar paginación
    generarPaginacion('productos', productosDuplicados.length);
}

function renderizarProductoItem(producto, dupId, numero, seleccionado, resultadoIA) {
    const esSeleccionado = seleccionado === producto.id || 
                          (resultadoIA && resultadoIA.decision === numero);
    
    return `
        <div id="producto-${producto.id}" class="duplicate-item ${esSeleccionado ? 'selected' : ''}" data-id="${producto.id}">
            <div class="selection-mark ${esSeleccionado ? 'selected' : 'not-selected'}">${numero}</div>
            
            <div class="duplicate-metadata">
                <h3>Producto #${producto.id}</h3>
                <p style="margin-top: 5px; color: #666;">
                    <strong>Nombre:</strong> ${producto.nombre}<br>
                    <strong>Código:</strong> ${producto.codigo || 'Sin código'}<br>
                    <strong>Establecimiento:</strong> ${producto.establecimiento || 'Desconocido'}<br>
                    <strong>Precio actual:</strong> $${typeof producto.precio === 'number' ? producto.precio.toLocaleString() : (producto.precio || 'N/A')}<br>
                    <strong>Última actualización:</strong> ${producto.ultima_actualizacion ? new Date(producto.ultima_actualizacion).toLocaleDateString() : 'N/A'}<br>
                    <strong>Visto:</strong> ${producto.veces_visto || 0} veces
                </p>
            </div>
            
            <button class="btn ${esSeleccionado ? 'btn-primary' : 'btn-secondary'} select-button" 
                    onclick="seleccionarProducto('${dupId}', ${producto.id})">
                ${esSeleccionado ? '✓ Seleccionado' : 'Seleccionar Este'}
            </button>
        </div>
    `;
}

function seleccionarProducto(dupId, productoId) {
    // Encontrar el duplicado
    const dup = productosDuplicados.find(d => (d.id || `dup-${productosDuplicados.indexOf(d)}`) === dupId);
    
    if (dup) {
        dup.seleccionado = productoId;
        renderizarPaginaProductos(paginaActualProductos);
        console.log(`✅ Producto ${productoId} seleccionado para mantener`);
    }
}

async function fusionarProductos(dupId) {
    const dup = productosDuplicados.find(d => (d.id || `dup-${productosDuplicados.indexOf(d)}`) === dupId);
    
    if (!dup) {
        mostrarToast('❌ No se encontró el duplicado', 'error');
        return;
    }
    
    const productoMantener = dup.seleccionado === dup.producto2.id ? dup.producto2 : dup.producto1;
    const productoEliminar = productoMantener === dup.producto1 ? dup.producto2 : dup.producto1;
    
    if (!confirm(`¿Fusionar estos productos?\n\nSe mantendrá: "${productoMantener.nombre}" (#${productoMantener.id})\nSe eliminará: "${productoEliminar.nombre}" (#${productoEliminar.id})`)) {
        return;
    }
    
    try {
        const elemento = document.getElementById(`duplicado-${dupId}`);
        if (elemento) {
            elemento.innerHTML = '<div class="loading"><div class="spinner"></div><p>Fusionando productos...</p></div>';
        }
        
        const response = await fetch(`${API_URL}/admin/duplicados/productos/fusionar`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                producto_mantener_id: productoMantener.id,
                producto_eliminar_id: productoEliminar.id
            })
        });
        
        const data = await response.json();
        
        if (!response.ok || !data.success) {
            throw new Error(data.error || 'Error al fusionar productos');
        }
        
        // Actualizar contadores
        totalFusiones++;
        document.getElementById('total-fusiones').textContent = totalFusiones;
        
        // Eliminar de la lista
        productosDuplicados = productosDuplicados.filter(d => 
            (d.id || `dup-${productosDuplicados.indexOf(d)}`) !== dupId
        );
        
        document.getElementById('total-duplicados').textContent = productosDuplicados.length;
        
        mostrarToast('✅ Productos fusionados correctamente');
        
        // Recargar página actual
        renderizarPaginaProductos(paginaActualProductos);
        
    } catch (error) {
        console.error('❌ Error fusionando productos:', error);
        mostrarToast('❌ Error al fusionar: ' + error.message, 'error');
        renderizarPaginaProductos(paginaActualProductos);
    }
}

// ============================================
// DETECCIÓN DE FACTURAS DUPLICADAS
// ============================================

async function detectarFacturasDuplicadas() {
    const container = document.getElementById('facturas-duplicadas-container');
    container.innerHTML = '<div class="loading"><div class="spinner"></div><p>Analizando duplicados de facturas...</p></div>';
    
    try {
        const criterio = document.getElementById('criterioFacturas')?.value || 'all';
        
        console.log(`🔍 Detectando facturas duplicadas: criterio=${criterio}`);
        
        const response = await fetch(`${API_URL}/admin/duplicados/facturas?criterio=${criterio}`);
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        facturasDuplicadas = data.duplicados || [];
        
        // Actualizar estadísticas
        document.getElementById('total-facturas-duplicadas').textContent = data.total || 0;
        
        if (facturasDuplicadas.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>✅ No se encontraron facturas duplicadas</h3>
                    <p>Prueba con diferentes criterios</p>
                </div>
            `;
            document.getElementById('facturas-pagination').innerHTML = '';
            document.getElementById('facturas-ia-panel').style.display = 'none';
            return;
        }
        
        console.log(`✅ Encontradas ${facturasDuplicadas.length} facturas duplicadas`);
        
        // Mostrar panel de IA
        document.getElementById('facturas-ia-panel').style.display = 'block';
        
        // Renderizar primera página
        renderizarPaginaFacturas(1);
        
    } catch (error) {
        console.error('❌ Error detectando duplicados de facturas:', error);
        
        container.innerHTML = `
            <div style="padding: 20px; background: #ffebee; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="color: #c62828; margin-bottom: 10px;">❌ Error al buscar duplicados</h3>
                <p>${error.message || 'Error desconocido'}</p>
                <button class="btn btn-primary" onclick="detectarFacturasDuplicadas()" style="margin-top: 15px;">
                    🔄 Intentar nuevamente
                </button>
            </div>
        `;
        document.getElementById('facturas-ia-panel').style.display = 'none';
    }
}

function renderizarPaginaFacturas(pagina) {
    paginaActualFacturas = pagina;
    
    const inicio = (pagina - 1) * elementosPorPagina;
    const fin = Math.min(inicio + elementosPorPagina, facturasDuplicadas.length);
    const duplicadosPagina = facturasDuplicadas.slice(inicio, fin);
    
    const container = document.getElementById('facturas-duplicadas-container');
    const mostrarImagenes = document.getElementById('mostrarImagenes')?.checked ?? true;
    
    if (duplicadosPagina.length === 0) {
        container.innerHTML = '<div class="empty-state"><h3>No hay más duplicados</h3></div>';
        return;
    }
    
    let html = '';
    
    duplicadosPagina.forEach((dup, localIndex) => {
        const globalIndex = inicio + localIndex;
        const dupId = dup.id || `facdup-${globalIndex}`;
        
        // Verificar si hay resultado de IA
        const resultadoIA = resultadosFacturasIA.find(r => r.dupId === dupId);
        
        html += `
            <div id="factura-dup-${dupId}" class="duplicate-container">
                <div class="duplicate-header">
                    <div>
                        <span class="similarity-tag similarity-medium">${dup.razon}</span>
                        ${resultadoIA ? `<span class="criteria-tag" style="background: #e8f5e9; color: #2e7d32;">IA: Mantener #${resultadoIA.decision} (${resultadoIA.confianza}%)</span>` : ''}
                    </div>
                    <div class="btn-group">
                        <button class="btn btn-secondary btn-sm" onclick="compararFacturas(${dup.factura1.id}, ${dup.factura2.id})">
                            🔍 Comparar
                        </button>
                    </div>
                </div>
                
                <div class="duplicate-grid">
                    ${renderizarFacturaItem(dup.factura1, dupId, 1, mostrarImagenes, resultadoIA)}
                    ${renderizarFacturaItem(dup.factura2, dupId, 2, mostrarImagenes, resultadoIA)}
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
    
    // Generar paginación
    generarPaginacion('facturas', facturasDuplicadas.length);
}

function renderizarFacturaItem(factura, dupId, numero, mostrarImagenes, resultadoIA) {
    const esRecomendadaEliminar = resultadoIA && 
        ((resultadoIA.decision === 1 && numero === 2) || 
         (resultadoIA.decision === 2 && numero === 1));
    
    return `
        <div class="duplicate-item">
            <div class="selection-mark ${esRecomendadaEliminar ? 'not-selected' : 'selected'}">${numero}</div>
            
            <div class="duplicate-metadata">
                <h3>Factura #${factura.id}</h3>
                <p style="margin-top: 5px; color: #666;">
                    <strong>Establecimiento:</strong> ${factura.establecimiento}<br>
                    <strong>Total:</strong> $${factura.total.toLocaleString()}<br>
                    <strong>Productos:</strong> ${factura.num_productos}<br>
                    <strong>Fecha:</strong> ${new Date(factura.fecha).toLocaleDateString()}<br>
                    <strong>Tiene imagen:</strong> ${factura.tiene_imagen ? '✅ Sí' : '❌ No'}
                </p>
            </div>
            
            ${mostrarImagenes && factura.tiene_imagen ? `
                <div class="duplicate-image" onclick="mostrarImagenAmpliada(${factura.id}, 'Factura #${factura.id}')" style="cursor: pointer;">
                    <img src="${API_URL}/admin/facturas/${factura.id}/imagen" 
                         onerror="this.src='data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjAwIiBoZWlnaHQ9IjIwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMjAwIiBoZWlnaHQ9IjIwMCIgZmlsbD0iI2YwZjBmMCIvPjx0ZXh0IHg9IjUwJSIgeT0iNTAlIiBmb250LWZhbWlseT0iQXJpYWwiIGZvbnQtc2l6ZT0iMTQiIHRleHQtYW5jaG9yPSJtaWRkbGUiIGR5PSIuM2VtIiBmaWxsPSIjOTk5Ij5TaW4gaW1hZ2VuPC90ZXh0Pjwvc3ZnPg=='">
                </div>
            ` : ''}
            
            <div class="btn-group">
                <button class="btn btn-secondary btn-sm" onclick="verFactura(${factura.id})">👁️ Ver</button>
                <button class="btn btn-danger btn-sm" onclick="eliminarFactura(${factura.id}, '${dupId}')">
                    🗑️ Eliminar ${esRecomendadaEliminar ? '(Recomendado por IA)' : ''}
                </button>
            </div>
        </div>
    `;
}

async function eliminarFactura(facturaId, dupId) {
    const resultadoIA = resultadosFacturasIA.find(r => r.dupId === dupId);
    
    let mensaje = `¿Eliminar la factura #${facturaId} y todos sus productos?\n\nEsta acción es PERMANENTE.`;
    
    if (resultadoIA) {
        const esRecomendada = (resultadoIA.decision === 1 && dupId.includes('2')) || 
                             (resultadoIA.decision === 2 && dupId.includes('1'));
        
        if (!esRecomendada) {
            mensaje = `⚠️ ADVERTENCIA: La IA recomienda MANTENER esta factura.\n\n${mensaje}`;
        } else {
            mensaje = `IA recomienda eliminar esta factura.\n\n${mensaje}`;
        }
    }
    
    if (!confirm(mensaje)) return;
    
    try {
        const response = await fetch(`${API_URL}/admin/facturas/${facturaId}`, {
            method: 'DELETE'
        });
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        // Actualizar contadores
        totalEliminaciones++;
        document.getElementById('total-eliminaciones').textContent = totalEliminaciones;
        
        // Eliminar de la lista
        facturasDuplicadas = facturasDuplicadas.filter(d => 
            (d.id || `facdup-${facturasDuplicadas.indexOf(d)}`) !== dupId
        );
        
        document.getElementById('total-facturas-duplicadas').textContent = facturasDuplicadas.length;
        
        mostrarToast('✅ Factura eliminada correctamente');
        
        // Recargar página actual
        setTimeout(() => {
            renderizarPaginaFacturas(paginaActualFacturas);
        }, 500);
        
    } catch (error) {
        console.error('❌ Error eliminando factura:', error);
        mostrarToast('❌ Error al eliminar: ' + error.message, 'error');
    }
}

// ============================================
// PROCESAMIENTO CON IA
// ============================================

async function procesarDuplicadosIA(tipo) {
    if (procesandoIA) {
        mostrarToast('⚠️ Ya hay un proceso de IA en ejecución', 'error');
        return;
    }
    
    if (!configuracionIA.apiKey) {
        mostrarToast('❌ Configure la API Key de Anthropic primero', 'error');
        document.querySelector('.tab[onclick*="configuracion"]').click();
        return;
    }
    
    const duplicados = tipo === 'productos' ? productosDuplicados : facturasDuplicadas;
    
    if (duplicados.length === 0) {
        mostrarToast(`❌ No hay ${tipo} duplicados para procesar`, 'error');
        return;
    }
    
    const confianzaMinima = parseInt(document.getElementById(`confianza${tipo === 'productos' ? 'Productos' : 'Facturas'}`)?.value || '85');
    
    // Configurar UI
    const batchStatusElement = document.getElementById(`${tipo}-batch-status`);
    const progresoElement = document.getElementById(`${tipo}-progreso`);
    const progressBarElement = document.getElementById(`${tipo}-progress-bar`);
    
    if (batchStatusElement) batchStatusElement.style.display = 'flex';
    if (progresoElement) progresoElement.textContent = `0/${duplicados.length}`;
    if (progressBarElement) progressBarElement.style.width = '0%';
    
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
    
    // Procesar en lotes
    const tamanoLote = configuracionIA.tamanoLote;
    
    for (let i = 0; i < duplicados.length; i += tamanoLote) {
        if (procesoIACancelado) break;
        
        const lote = duplicados.slice(i, Math.min(i + tamanoLote, duplicados.length));
        
        try {
            const resultadosLote = await procesarLoteIA(tipo, lote, confianzaMinima);
            
            // Almacenar resultados
            if (tipo === 'productos') {
                resultadosProductosIA.push(...resultadosLote);
            } else {
                resultadosFacturasIA.push(...resultadosLote);
            }
            
            // Actualizar UI
            procesados += lote.length;
            const porcentaje = (procesados / duplicados.length) * 100;
            
            if (progresoElement) progresoElement.textContent = `${procesados}/${duplicados.length}`;
            if (progressBarElement) progressBarElement.style.width = `${porcentaje}%`;
            
            document.getElementById(`${tipo}-procesados-ia`).textContent = procesados;
            
            // Actualizar vista
            if (tipo === 'productos') {
                renderizarPaginaProductos(paginaActualProductos);
            } else {
                renderizarPaginaFacturas(paginaActualFacturas);
            }
            
        } catch (error) {
            console.error(`❌ Error procesando lote ${i / tamanoLote + 1}:`, error);
            mostrarToast(`❌ Error en lote ${i / tamanoLote + 1}: ${error.message}`, 'error');
        }
        
        // Esperar entre lotes
        if (i + tamanoLote < duplicados.length && !procesoIACancelado) {
            await new Promise(resolve => setTimeout(resolve, configuracionIA.intervaloLote * 1000));
        }
    }
    
    // Finalizar
    procesandoIA = false;
    
    setTimeout(() => {
        if (batchStatusElement) batchStatusElement.style.display = 'none';
    }, 1000);
    
    if (!procesoIACancelado) {
        mostrarToast(`✅ Procesamiento completado: ${procesados} duplicados analizados`);
        mostrarResumenIA(tipo);
    }
}

async function procesarLoteIA(tipo, lote, confianzaMinima) {
    const criterios = tipo === 'productos' ? 
        configuracionIA.criteriosProductos : 
        configuracionIA.criteriosFacturas;
    
    const prompt = construirPromptIA(tipo, lote, criterios, confianzaMinima);
    
    try {
        const response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': configuracionIA.apiKey,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: configuracionIA.modelo,
                max_tokens: 4096,
                messages: [{
                    role: 'user',
                    content: prompt
                }]
            })
        });
        
        if (!response.ok) {
            throw new Error(`API Error: ${response.status}`);
        }
        
        const data = await response.json();
        const contenido = data.content[0].text;
        
        // Parsear respuesta JSON
        return parsearRespuestaIA(contenido, lote, tipo);
        
    } catch (error) {
        console.error('❌ Error llamando a API de Anthropic:', error);
        throw error;
    }
}

function construirPromptIA(tipo, lote, criterios, confianzaMinima) {
    if (tipo === 'productos') {
        return `Eres un experto en análisis de duplicados de productos en bases de datos. Analiza los siguientes pares de productos y determina cuál mantener.

CRITERIOS:
${criterios}

PRODUCTOS A ANALIZAR:
${JSON.stringify(lote, null, 2)}

Para cada par, responde en formato JSON:
{
  "analisis": [
    {
      "dupId": "id del duplicado",
      "decision": 1 o 2 (cuál producto mantener),
      "confianza": número entre 0-100,
      "razon": "explicación breve"
    }
  ]
}

Confianza mínima requerida: ${confianzaMinima}%`;
    } else {
        return `Eres un experto en análisis de duplicados de facturas. Analiza los siguientes pares de facturas y determina cuál mantener.

CRITERIOS:
${criterios}

FACTURAS A ANALIZAR:
${JSON.stringify(lote, null, 2)}

Para cada par, responde en formato JSON:
{
  "analisis": [
    {
      "dupId": "id del duplicado",
      "decision": 1 o 2 (cuál factura mantener),
      "confianza": número entre 0-100,
      "razon": "explicación breve"
    }
  ]
}

Confianza mínima requerida: ${confianzaMinima}%`;
    }
}

function parsearRespuestaIA(contenido, lote, tipo) {
    try {
        // Intentar parsear JSON directamente
        let json;
        
        // Buscar JSON en el contenido
        const jsonMatch = contenido.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
            json = JSON.parse(jsonMatch[0]);
        } else {
            throw new Error('No se encontró JSON en la respuesta');
        }
        
        return json.analisis.map((item, index) => {
            const dup = lote[index];
            const dupId = dup.id || (tipo === 'productos' ? `dup-${index}` : `facdup-${index}`);
            
            return {
                dupId: dupId,
                decision: item.decision,
                confianza: item.confianza,
                razon: item.razon
            };
        });
        
    } catch (error) {
        console.error('❌ Error parseando respuesta IA:', error);
        console.log('Contenido recibido:', contenido);
        
        // Fallback: devolver decisiones por defecto
        return lote.map((dup, index) => ({
            dupId: dup.id || (tipo === 'productos' ? `dup-${index}` : `facdup-${index}`),
            decision: 1,
            confianza: 0,
            razon: 'Error parseando respuesta IA'
        }));
    }
}

function cancelarProcesamientoIA(tipo) {
    procesoIACancelado = true;
    procesandoIA = false;
    mostrarToast('⚠️ Procesamiento cancelado por el usuario', 'info');
}

function mostrarResumenIA(tipo) {
    const resultados = tipo === 'productos' ? resultadosProductosIA : resultadosFacturasIA;
    
    // Calcular estadísticas
    const total = resultados.length;
    const confianzaPromedio = total > 0 ? 
        Math.round(resultados.reduce((sum, r) => sum + r.confianza, 0) / total) : 0;
    
    const decision1 = resultados.filter(r => r.decision === 1).length;
    const decision2 = resultados.filter(r => r.decision === 2).length;
    
    alert(`📊 Resumen del Procesamiento IA

Total procesados: ${total}
Confianza promedio: ${confianzaPromedio}%

Decisiones:
- Mantener primera opción: ${decision1}
- Mantener segunda opción: ${decision2}

Los resultados están marcados en la interfaz. Revisa las decisiones antes de aplicar cambios permanentes.`);
}

function guardarResultadosIA(tipo) {
    const resultados = tipo === 'productos' ? resultadosProductosIA : resultadosFacturasIA;
    
    if (resultados.length === 0) {
        mostrarToast('❌ No hay resultados para guardar', 'error');
        return;
    }
    
    // Crear CSV para descarga
    const csv = generarCSVResultados(resultados, tipo);
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = `resultados_ia_${tipo}_${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    mostrarToast('✅ Resultados guardados como CSV');
}

function generarCSVResultados(resultados, tipo) {
    let csv = 'DupID,Decision,Confianza,Razon\n';
    
    resultados.forEach(r => {
        csv += `"${r.dupId}",${r.decision},${r.confianza},"${r.razon.replace(/"/g, '""')}"\n`;
    });
    
    return csv;
}

function aplicarDecisionesIA() {
    alert('Función en desarrollo: Aplicar decisiones automáticamente');
}

// ============================================
// UTILIDADES
// ============================================

function generarPaginacion(tipo, totalElementos) {
    const totalPaginas = Math.ceil(totalElementos / elementosPorPagina);
    const paginaActual = tipo === 'productos' ? paginaActualProductos : paginaActualFacturas;
    const container = document.getElementById(`${tipo}-pagination`);
    
    if (totalPaginas <= 1) {
        container.innerHTML = '';
        return;
    }
    
    let html = '';
    
    // Botón anterior
    if (paginaActual > 1) {
        html += `<div class="pagination-item" onclick="cambiarPagina('${tipo}', ${paginaActual - 1})">‹</div>`;
    }
    
    // Números de página
    for (let i = 1; i <= totalPaginas; i++) {
        if (i === 1 || i === totalPaginas || (i >= paginaActual - 1 && i <= paginaActual + 1)) {
            html += `<div class="pagination-item ${i === paginaActual ? 'active' : ''}" 
                          onclick="cambiarPagina('${tipo}', ${i})">${i}</div>`;
        } else if (i === paginaActual - 2 || i === paginaActual + 2) {
            html += `<div class="pagination-item">...</div>`;
        }
    }
    
    // Botón siguiente
    if (paginaActual < totalPaginas) {
        html += `<div class="pagination-item" onclick="cambiarPagina('${tipo}', ${paginaActual + 1})">›</div>`;
    }
    
    container.innerHTML = html;
}

function cambiarPagina(tipo, pagina) {
    if (tipo === 'productos') {
        renderizarPaginaProductos(pagina);
    } else {
        renderizarPaginaFacturas(pagina);
    }
}

function mostrarImagenAmpliada(facturaId, titulo) {
    const popup = document.getElementById('popup-imagen');
    const tituloElement = document.getElementById('popup-imagen-titulo');
    const imgElement = document.getElementById('popup-imagen-contenido');
    
    if (!popup || !tituloElement || !imgElement) return;
    
    tituloElement.textContent = titulo;
    imgElement.src = `${API_URL}/admin/facturas/${facturaId}/imagen`;
    popup.style.display = 'flex';
}

function compararFacturas(id1, id2) {
    window.open(`/comparador-facturas?id1=${id1}&id2=${id2}`, '_blank');
}

function verFactura(id) {
    window.open(`/editor?id=${id}`, '_blank');
}

function cerrarPopup(id) {
    const popup = document.getElementById(id);
    if (popup) {
        popup.style.display = 'none';
    }
}

function mostrarAyudaProductos() {
    const popup = document.getElementById('popup-ayuda-productos');
    if (popup) {
        popup.style.display = 'flex';
    }
}

function mostrarAyudaFacturas() {
    const popup = document.getElementById('popup-ayuda-facturas');
    if (popup) {
        popup.style.display = 'flex';
    }
}

function mostrarToast(mensaje, tipo = 'success') {
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.textContent = mensaje;
    
    if (tipo === 'error') {
        toast.style.background = '#ea4335';
    } else if (tipo === 'info') {
        toast.style.background = '#4285f4';
    } else {
        toast.style.background = '#34a853';
    }
    
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.remove();
    }, 3000);
}

function mostrarMensajeError(mensaje) {
    const container = document.querySelector('.container');
    if (!container) return;
    
    const alerta = document.createElement('div');
    alerta.style.cssText = 'background: #fff3e0; border: 1px solid #ffb74d; border-radius: 8px; padding: 15px; margin-bottom: 20px;';
    alerta.innerHTML = `
        <h3 style="margin-bottom: 10px; color: #e65100;">⚠️ Advertencia</h3>
        <p>${mensaje}</p>
    `;
    
    container.insertBefore(alerta, container.firstChild);
}

// ============================================
// LOGGING
// ============================================

console.log('✅ duplicados.js cargado correctamente');
console.log('📦 Versión: 3.0.0');
console.log('🔧 Funcionalidades: Detección de duplicados + IA + Fusión/Eliminación');
