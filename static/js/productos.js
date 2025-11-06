// ============================================================================
// productos.js - VERSIÓN COMPLETA CON TODAS LAS FUNCIONES
// ============================================================================

console.log("✅ productos.js v2.1 cargado - Versión completa");

// ============================================================================
// VARIABLES GLOBALES
// ============================================================================
let coloresCache = null;
let productosSeleccionados = [];
let paginaActual = 1;

// ============================================================================
// CACHE DE COLORES
// ============================================================================
async function cargarColoresEstablecimientos() {
    try {
        const cacheData = localStorage.getItem('colores_establecimientos');
        const cacheTime = localStorage.getItem('colores_timestamp');
        const ahora = Date.now();
        const CACHE_DURATION = 24 * 60 * 60 * 1000;

        if (cacheData && cacheTime && (ahora - parseInt(cacheTime)) < CACHE_DURATION) {
            console.log("✅ Usando colores desde cache");
            coloresCache = JSON.parse(cacheData);
            return coloresCache;
        }

        console.log("🔄 Cargando colores desde API...");
        const response = await fetch('/api/establecimientos/colores');

        if (!response.ok) {
            console.warn("⚠️ API colores no disponible, usando defaults");
            return obtenerColoresDefault();
        }

        coloresCache = await response.json();
        localStorage.setItem('colores_establecimientos', JSON.stringify(coloresCache));
        localStorage.setItem('colores_timestamp', ahora.toString());

        console.log("✅ Colores cargados:", Object.keys(coloresCache).length);
        return coloresCache;

    } catch (error) {
        console.error("❌ Error cargando colores:", error);
        return obtenerColoresDefault();
    }
}

function obtenerColoresDefault() {
    return {
        'ÉXITO': { bg: '#e3f2fd', text: '#1565c0' },
        'JUMBO': { bg: '#fff3e0', text: '#e65100' },
        'CARULLA': { bg: '#f3e5f5', text: '#7b1fa2' },
        'OLÍMPICA': { bg: '#e8f5e9', text: '#2e7d32' },
        'D1': { bg: '#fff9c4', text: '#f57f17' },
        'ARA': { bg: '#ffe0b2', text: '#ef6c00' },
    };
}

function getColorTienda(nombreTienda) {
    if (!coloresCache) {
        coloresCache = obtenerColoresDefault();
    }

    const nombreNorm = nombreTienda.toUpperCase().trim();

    for (const [tienda, colores] of Object.entries(coloresCache)) {
        if (nombreNorm.includes(tienda) || tienda.includes(nombreNorm)) {
            return colores;
        }
    }

    return { bg: '#e9ecef', text: '#495057' };
}

// ============================================================================
// RENDERIZAR PLUs CON BADGES DE COLORES
// ============================================================================
function renderPLU(pluString) {
    if (!pluString || pluString === '-' || pluString === 'null' || pluString === '') {
        return '<span style="color: #999;">-</span>';
    }

    const plus = pluString.split(',').map(p => p.trim());
    let html = '';

    for (const plu of plus) {
        const match = plu.match(/^(.+?)\s*\((.+?)\)$/);

        if (match) {
            const codigo = match[1].trim();
            const establecimiento = match[2].trim();
            const colores = getColorTienda(establecimiento);

            html += `<span class="badge" style="background-color: ${colores.bg}; color: ${colores.text}; margin: 2px; padding: 5px 10px; border-radius: 4px; font-size: 12px; display: inline-block; font-weight: 600;">
                ${codigo} <span style="opacity: 0.8;">${establecimiento}</span>
            </span>`;
        } else {
            html += `<span class="badge" style="background: #e9ecef; color: #495057; margin: 2px; padding: 5px 10px;">${plu}</span>`;
        }
    }

    return html || '<span style="color: #999;">-</span>';
}

// ============================================================================
// CARGAR PRODUCTOS
// ============================================================================
async function cargarProductos(pagina = 1) {
    try {
        console.log(`📦 Cargando productos - Página ${pagina}`);

        const busqueda = document.getElementById('busqueda')?.value || '';
        const filtro = document.getElementById('filtro')?.value || 'todos';

        let url = `/api/productos?pagina=${pagina}&limite=50`;

        if (busqueda) {
            url += `&busqueda=${encodeURIComponent(busqueda)}`;
        }

        if (filtro !== 'todos') {
            url += `&filtro=${filtro}`;
        }

        console.log('🌐 URL:', url);

        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        console.log('📊 Respuesta API:', data);

        if (!data.productos || !Array.isArray(data.productos)) {
            throw new Error('Formato de respuesta inválido');
        }

        console.log(`✅ ${data.productos.length} productos recibidos`);

        // Debug primer producto
        if (data.productos.length > 0) {
            console.log('🔍 Primer producto:', data.productos[0]);
        }

        renderizarTabla(data.productos);
        renderizarPaginacion(data.paginacion || { pagina, paginas: 1, total: data.productos.length });
        actualizarStats(data.productos);

        paginaActual = pagina;

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarError('Error al cargar productos: ' + error.message);
        document.getElementById('productos-body').innerHTML = `
            <tr><td colspan="11" style="text-align: center; padding: 40px; color: #dc2626;">
                ❌ Error: ${error.message}
            </td></tr>
        `;
    }
}

// ============================================================================
// RENDERIZAR TABLA
// ============================================================================
function renderizarTabla(productos) {
    const tbody = document.getElementById('productos-body');

    if (!tbody) {
        console.error('❌ No existe #productos-body');
        return;
    }

    if (productos.length === 0) {
        tbody.innerHTML = '<tr><td colspan="11" style="text-align: center; padding: 40px;">No hay productos</td></tr>';
        return;
    }

    tbody.innerHTML = productos.map(p => {
        const problemas = [];
        if (!p.codigo_ean) problemas.push('Sin EAN');
        if (!p.marca || p.marca === 'Sin marca') problemas.push('Sin marca');
        if (!p.categoria || p.categoria === 'Sin categoría') problemas.push('Sin categoría');

        let estadoBadge = '';
        if (problemas.length === 0) {
            estadoBadge = '<span class="badge badge-success">✅ Completo</span>';
        } else {
            estadoBadge = problemas.map(prob =>
                `<span class="badge badge-warning" style="display: block; margin: 2px 0;">⚠️ ${prob}</span>`
            ).join('');
        }

        const pluHtml = renderPLU(p.codigo_plu || p.plus_con_tienda);

        return `
            <tr data-id="${p.id}" class="${productosSeleccionados.includes(p.id) ? 'selected' : ''}">
                <td class="checkbox-cell">
                    <input type="checkbox" class="producto-checkbox" value="${p.id}"
                           ${productosSeleccionados.includes(p.id) ? 'checked' : ''}
                           onchange="toggleProducto(${p.id})">
                </td>
                <td><strong>${p.id}</strong></td>
                <td><code style="font-size: 12px;">${p.codigo_ean || '-'}</code></td>
                <td class="plu-column">${pluHtml}</td>
                <td><strong>${p.nombre || p.nombre_normalizado || p.nombre_comercial || '-'}</strong></td>
                <td>${p.marca || '<span style="color: #999;">Sin marca</span>'}</td>
                <td>${p.categoria || '<span style="color: #999;">Sin categoría</span>'}</td>
                <td style="text-align: right;"><strong>$${(p.precio_promedio || 0).toLocaleString('es-CO')}</strong></td>
                <td style="text-align: center;">${p.veces_comprado || p.total_reportes || 0}</td>
                <td>${estadoBadge}</td>
                <td style="white-space: nowrap;">
                    <button class="btn-primary btn-small" onclick="editarProducto(${p.id})" title="Editar">✏️</button>
                    <button class="btn-secondary btn-small" onclick="verHistorial(${p.id})" title="Historial">📜</button>
                </td>
            </tr>
        `;
    }).join('');
}

// ============================================================================
// PAGINACIÓN
// ============================================================================
function renderizarPaginacion(paginacion) {
    const container = document.getElementById('pagination');
    if (!container) return;

    const { pagina = 1, paginas = 1, total = 0 } = paginacion;

    if (paginas <= 1) {
        container.innerHTML = '';
        return;
    }

    let html = '';

    // Botón Anterior
    if (pagina > 1) {
        html += `<button class="btn-secondary" onclick="cambiarPagina(${pagina - 1})">← Anterior</button>`;
    }

    // Páginas
    const maxBotones = 5;
    let inicio = Math.max(1, pagina - Math.floor(maxBotones / 2));
    let fin = Math.min(paginas, inicio + maxBotones - 1);

    for (let i = inicio; i <= fin; i++) {
        const clase = i === pagina ? 'btn-primary' : 'btn-secondary';
        html += `<button class="${clase}" onclick="cambiarPagina(${i})">${i}</button>`;
    }

    // Botón Siguiente
    if (pagina < paginas) {
        html += `<button class="btn-secondary" onclick="cambiarPagina(${pagina + 1})">Siguiente →</button>`;
    }

    container.innerHTML = html;
}

// ============================================================================
// ESTADÍSTICAS
// ============================================================================
function actualizarStats(productos) {
    const statsContainer = document.getElementById('stats');
    if (!statsContainer) return;

    const total = productos.length;
    const conEAN = productos.filter(p => p.codigo_ean && p.codigo_ean !== '-').length;
    const sinMarca = productos.filter(p => !p.marca || p.marca === 'Sin marca').length;
    const conProblemas = productos.filter(p => {
        return !p.codigo_ean || !p.marca || !p.categoria;
    }).length;

    statsContainer.innerHTML = `
        <div class="stat-card">
            <div class="stat-value">${total}</div>
            <div class="stat-label">Total Productos</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">${conEAN}</div>
            <div class="stat-label">Con EAN</div>
        </div>
        <div class="stat-card warning">
            <div class="stat-value">${sinMarca}</div>
            <div class="stat-label">Sin Marca</div>
        </div>
        <div class="stat-card danger">
            <div class="stat-value">${conProblemas}</div>
            <div class="stat-label">Con Problemas</div>
        </div>
    `;
}

// ============================================================================
// SELECCIÓN DE PRODUCTOS
// ============================================================================
function toggleProducto(id) {
    const index = productosSeleccionados.indexOf(id);
    if (index > -1) {
        productosSeleccionados.splice(index, 1);
    } else {
        productosSeleccionados.push(id);
    }
    actualizarSeleccion();
}

function toggleSelectAll() {
    const checkboxes = document.querySelectorAll('.producto-checkbox');
    const selectAll = document.getElementById('select-all');

    checkboxes.forEach(cb => {
        cb.checked = selectAll.checked;
        const id = parseInt(cb.value);
        if (selectAll.checked && !productosSeleccionados.includes(id)) {
            productosSeleccionados.push(id);
        } else if (!selectAll.checked) {
            const index = productosSeleccionados.indexOf(id);
            if (index > -1) productosSeleccionados.splice(index, 1);
        }
    });

    actualizarSeleccion();
}

function actualizarSeleccion() {
    const count = productosSeleccionados.length;
    document.getElementById('selected-count').textContent = `${count} seleccionados`;

    const btnFusionar = document.getElementById('btn-fusionar');
    const btnDeseleccionar = document.getElementById('btn-deseleccionar');

    if (btnFusionar) btnFusionar.disabled = count < 2;
    if (btnDeseleccionar) btnDeseleccionar.disabled = count === 0;

    // Actualizar filas seleccionadas
    document.querySelectorAll('tr[data-id]').forEach(tr => {
        const id = parseInt(tr.dataset.id);
        if (productosSeleccionados.includes(id)) {
            tr.classList.add('selected');
        } else {
            tr.classList.remove('selected');
        }
    });
}

function deseleccionarTodos() {
    productosSeleccionados = [];
    document.querySelectorAll('.producto-checkbox').forEach(cb => cb.checked = false);
    document.getElementById('select-all').checked = false;
    actualizarSeleccion();
}

// ============================================================================
// DUPLICADOS
// ============================================================================
async function detectarDuplicados() {
    try {
        console.log('🔍 Detectando duplicados...');
        mostrarMensaje('Analizando productos...', 'info');

        const response = await fetch('/api/productos/duplicados/detectar', {
            method: 'POST'
        });

        if (!response.ok) {
            throw new Error('Error al detectar duplicados');
        }

        const data = await response.json();

        console.log('✅ Duplicados detectados:', data);

        if (data.total_duplicados > 0) {
            mostrarMensaje(`Se encontraron ${data.total_duplicados} posibles duplicados. Cambia a la pestaña "Duplicados" para revisarlos.`, 'warning');
        } else {
            mostrarMensaje('No se encontraron duplicados', 'success');
        }

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarError('Error al detectar duplicados: ' + error.message);
    }
}

async function cargarDuplicados() {
    try {
        console.log('📋 Cargando duplicados...');

        const container = document.getElementById('duplicados-container');
        container.innerHTML = '<div style="text-align: center; padding: 40px;"><div class="loading"></div><p>Analizando productos...</p></div>';

        const response = await fetch('/api/productos/duplicados');

        if (!response.ok) {
            throw new Error('Error al cargar duplicados');
        }

        const data = await response.json();

        console.log('✅ Duplicados cargados:', data);

        if (!data.duplicados || data.duplicados.length === 0) {
            container.innerHTML = `
                <div class="alert alert-success">
                    <strong>✅ ¡Excelente!</strong><br>
                    No se encontraron productos duplicados en el catálogo.
                </div>
            `;
            return;
        }

        // Renderizar duplicados
        container.innerHTML = data.duplicados.map(grupo => {
            const tipoClase = grupo.tipo === 'ean' ? 'duplicado-item' :
                grupo.tipo === 'plu' ? 'duplicado-item plu' :
                    'duplicado-item nombre';

            const tipoLabel = grupo.tipo === 'ean' ? '🔴 Mismo EAN' :
                grupo.tipo === 'plu' ? '🟠 Mismo PLU' :
                    '🟡 Nombres Similares';

            return `
                <div class="${tipoClase}">
                    <div class="duplicado-header">
                        <div>
                            <strong>${tipoLabel}</strong>
                            <span class="badge badge-error">${grupo.productos.length} productos</span>
                        </div>
                        <button class="btn-success btn-small" onclick="fusionarGrupo([${grupo.productos.map(p => p.id).join(',')}])">
                            🔗 Fusionar Todos
                        </button>
                    </div>
                    <div class="productos-duplicados">
                        ${grupo.productos.map((p, i) => `
                            <div class="producto-dup-card ${i === 0 ? 'principal' : ''}">
                                <div style="display: flex; justify-content: space-between; align-items: start;">
                                    <div>
                                        <strong style="font-size: 14px;">${p.nombre || p.nombre_normalizado}</strong><br>
                                        <small style="color: #666;">
                                            ID: ${p.id} |
                                            ${p.codigo_ean ? `EAN: ${p.codigo_ean}` : 'Sin EAN'} |
                                            ${p.codigo_plu ? `PLU: ${p.codigo_plu}` : 'Sin PLU'}
                                        </small><br>
                                        <small>Comprado ${p.total_reportes || 0} veces</small>
                                    </div>
                                    ${i === 0 ? '<span class="badge badge-info">Principal</span>' : ''}
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }).join('');

    } catch (error) {
        console.error('❌ Error:', error);
        document.getElementById('duplicados-container').innerHTML = `
            <div class="alert alert-error">
                <strong>Error:</strong> ${error.message}
            </div>
        `;
    }
}

// ============================================================================
// FUSIÓN DE PRODUCTOS
// ============================================================================
async function fusionarSeleccionados() {
    if (productosSeleccionados.length < 2) {
        alert('Selecciona al menos 2 productos para fusionar');
        return;
    }

    if (!confirm(`¿Estás seguro de fusionar ${productosSeleccionados.length} productos? Esta acción NO se puede deshacer.`)) {
        return;
    }

    try {
        console.log('🔗 Fusionando productos:', productosSeleccionados);

        const response = await fetch('/api/productos/fusionar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                productos_ids: productosSeleccionados,
                estrategia: 'mas_completo'
            })
        });

        if (!response.ok) {
            throw new Error('Error al fusionar productos');
        }

        const data = await response.json();

        console.log('✅ Fusión exitosa:', data);

        mostrarMensaje('✅ Productos fusionados correctamente', 'success');

        // Limpiar selección y recargar
        deseleccionarTodos();
        cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarError('Error al fusionar: ' + error.message);
    }
}

async function fusionarGrupo(ids) {
    productosSeleccionados = ids;
    await fusionarSeleccionados();
}

// ============================================================================
// EDICIÓN DE PRODUCTO
// ============================================================================
async function editarProducto(id) {
    try {
        console.log('✏️ Editando producto:', id);

        // Cargar datos del producto
        const response = await fetch(`/api/productos/${id}`);

        if (!response.ok) {
            throw new Error('Producto no encontrado');
        }

        const producto = await response.json();

        // Llenar formulario
        document.getElementById('edit-id').value = producto.id;
        document.getElementById('edit-ean').value = producto.codigo_ean || '';
        document.getElementById('edit-nombre-norm').value = producto.nombre_normalizado || '';
        document.getElementById('edit-nombre-com').value = producto.nombre_comercial || '';
        document.getElementById('edit-marca').value = producto.marca || '';
        document.getElementById('edit-categoria').value = producto.categoria || '';
        document.getElementById('edit-subcategoria').value = producto.subcategoria || '';
        document.getElementById('edit-presentacion').value = producto.presentacion || '';

        // Mostrar modal
        document.getElementById('modal-editar').classList.add('active');

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarError('Error al cargar producto: ' + error.message);
    }
}

async function guardarEdicion(event) {
    event.preventDefault();

    try {
        const id = document.getElementById('edit-id').value;

        const datos = {
            codigo_ean: document.getElementById('edit-ean').value || null,
            nombre_normalizado: document.getElementById('edit-nombre-norm').value,
            nombre_comercial: document.getElementById('edit-nombre-com').value || null,
            marca: document.getElementById('edit-marca').value || null,
            categoria: document.getElementById('edit-categoria').value || null,
            subcategoria: document.getElementById('edit-subcategoria').value || null,
            presentacion: document.getElementById('edit-presentacion').value || null
        };

        console.log('💾 Guardando cambios:', datos);

        const response = await fetch(`/api/productos/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(datos)
        });

        if (!response.ok) {
            throw new Error('Error al guardar cambios');
        }

        const resultado = await response.json();

        console.log('✅ Producto actualizado:', resultado);

        cerrarModal('modal-editar');
        mostrarMensaje('✅ Producto actualizado correctamente', 'success');
        cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarError('Error al guardar: ' + error.message);
    }
}

async function verHistorial(id) {
    try {
        console.log('📜 Viendo historial de producto:', id);

        const response = await fetch(`/api/productos/${id}/historial`);

        if (!response.ok) {
            throw new Error('Error al cargar historial');
        }

        const data = await response.json();

        const container = document.getElementById('historial-container');

        if (!data.compras || data.compras.length === 0) {
            container.innerHTML = '<p>No hay historial de compras para este producto</p>';
        } else {
            container.innerHTML = `
                <table style="width: 100%;">
                    <thead>
                        <tr>
                            <th>Fecha</th>
                            <th>Establecimiento</th>
                            <th>Precio</th>
                            <th>Cantidad</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.compras.map(c => `
                            <tr>
                                <td>${new Date(c.fecha).toLocaleDateString('es-CO')}</td>
                                <td>${c.establecimiento}</td>
                                <td>$${c.precio.toLocaleString('es-CO')}</td>
                                <td>${c.cantidad}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            `;
        }

        document.getElementById('modal-historial').classList.add('active');

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarError('Error al cargar historial: ' + error.message);
    }
}

// ============================================================================
// UTILIDADES
// ============================================================================
function cambiarPagina(pagina) {
    cargarProductos(pagina);
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function limpiarFiltros() {
    document.getElementById('busqueda').value = '';
    document.getElementById('filtro').value = 'todos';
    cargarProductos(1);
}

function switchTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));

    document.getElementById(`tab-${tabName}`).classList.add('active');
    event.target.classList.add('active');

    if (tabName === 'duplicados') {
        cargarDuplicados();
    }
}

function cerrarModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

function mostrarError(mensaje) {
    mostrarMensaje(mensaje, 'error');
}

function mostrarMensaje(mensaje, tipo = 'info') {
    const container = document.querySelector('.container');
    const alert = document.createElement('div');
    alert.className = `alert alert-${tipo}`;
    alert.innerHTML = mensaje;
    alert.style.position = 'fixed';
    alert.style.top = '20px';
    alert.style.right = '20px';
    alert.style.zIndex = '9999';
    alert.style.minWidth = '300px';
    alert.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';

    document.body.appendChild(alert);

    setTimeout(() => alert.remove(), 5000);
}

async function recargarColores() {
    localStorage.removeItem('colores_establecimientos');
    localStorage.removeItem('colores_timestamp');
    coloresCache = null;
    await cargarColoresEstablecimientos();
    cargarProductos(paginaActual);
    mostrarMensaje('✅ Colores actualizados', 'success');
}

// ============================================================================
// EXPONER FUNCIONES GLOBALES
// ============================================================================
window.cargarProductos = cargarProductos;
window.cambiarPagina = cambiarPagina;
window.limpiarFiltros = limpiarFiltros;
window.switchTab = switchTab;
window.toggleProducto = toggleProducto;
window.toggleSelectAll = toggleSelectAll;
window.deseleccionarTodos = deseleccionarTodos;
window.detectarDuplicados = detectarDuplicados;
window.cargarDuplicados = cargarDuplicados;
window.fusionarSeleccionados = fusionarSeleccionados;
window.fusionarGrupo = fusionarGrupo;
window.editarProducto = editarProducto;
window.guardarEdicion = guardarEdicion;
window.verHistorial = verHistorial;
window.cerrarModal = cerrarModal;
window.recargarColores = recargarColores;

// ============================================================================
// INICIALIZACIÓN
// ============================================================================
document.addEventListener('DOMContentLoaded', async function () {
    console.log('🚀 Inicializando Gestión de Productos v2.1');

    await cargarColoresEstablecimientos();
    await cargarProductos(1);

    // Event listeners
    const busqueda = document.getElementById('busqueda');
    if (busqueda) {
        busqueda.addEventListener('keyup', (e) => {
            if (e.key === 'Enter') cargarProductos(1);
        });
    }

    // Click fuera del modal para cerrar
    document.querySelectorAll('.modal').forEach(modal => {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.remove('active');
            }
        });
    });

    console.log('✅ Sistema inicializado correctamente');
});
