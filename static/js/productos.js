// ============================================================================
// PRODUCTOS.JS - Sistema de Gestión de Productos con Colores Dinámicos
// ============================================================================

// Variables globales
let paginaActual = 1;
const limite = 50;
let productosSeleccionados = new Set();
let COLORES_ESTABLECIMIENTOS = {};
let COLORES_CARGADOS = false;

// ============================================================================
// SISTEMA DE COLORES DINÁMICOS
// ============================================================================

/**
 * Cargar colores de establecimientos desde la API
 */
async function cargarColoresEstablecimientos() {
    if (COLORES_CARGADOS) {
        return COLORES_ESTABLECIMIENTOS;
    }

    try {
        console.log('🎨 Cargando colores de establecimientos...');

        const response = await fetch('/api/establecimientos/colores');
        const data = await response.json();

        if (data.success && data.colores) {
            COLORES_ESTABLECIMIENTOS = data.colores;
            COLORES_CARGADOS = true;

            console.log(`✅ ${Object.keys(data.colores).length} colores cargados`);

            // Cachear en localStorage por 1 hora
            localStorage.setItem('colores_establecimientos', JSON.stringify({
                colores: data.colores,
                timestamp: Date.now()
            }));

            return COLORES_ESTABLECIMIENTOS;
        } else {
            console.warn('⚠️ No se pudieron cargar colores, usando defaults');
            return getColoresDefault();
        }
    } catch (error) {
        console.error('❌ Error cargando colores:', error);

        // Intentar recuperar del cache local
        const cached = localStorage.getItem('colores_establecimientos');
        if (cached) {
            const parsed = JSON.parse(cached);
            const edad = Date.now() - parsed.timestamp;

            // Si el cache tiene menos de 24 horas, usarlo
            if (edad < 24 * 60 * 60 * 1000) {
                console.log('💾 Usando colores del cache local');
                COLORES_ESTABLECIMIENTOS = parsed.colores;
                COLORES_CARGADOS = true;
                return COLORES_ESTABLECIMIENTOS;
            }
        }

        // Fallback a colores por defecto
        return getColoresDefault();
    }
}

/**
 * Colores por defecto (fallback)
 */
function getColoresDefault() {
    return {
        'EXITO': { bg: '#e3f2fd', text: '#1565c0' },
        'ÉXITO': { bg: '#e3f2fd', text: '#1565c0' },
        'Exito': { bg: '#e3f2fd', text: '#1565c0' },
        'Éxito': { bg: '#e3f2fd', text: '#1565c0' },
        'JUMBO': { bg: '#fff3e0', text: '#e65100' },
        'Jumbo': { bg: '#fff3e0', text: '#e65100' },
        'CARULLA': { bg: '#f3e5f5', text: '#7b1fa2' },
        'Carulla': { bg: '#f3e5f5', text: '#7b1fa2' },
        'OLIMPICA': { bg: '#e8f5e9', text: '#2e7d32' },
        'OLÍMPICA': { bg: '#e8f5e9', text: '#2e7d32' },
        'Olimpica': { bg: '#e8f5e9', text: '#2e7d32' },
        'D1': { bg: '#fff9c4', text: '#f57f17' },
        'ARA': { bg: '#ffe0b2', text: '#ef6c00' },
        'Ara': { bg: '#ffe0b2', text: '#ef6c00' }
    };
}

/**
 * Obtener color para un establecimiento
 */
function getColorTienda(tienda) {
    if (!tienda) {
        return { bg: '#e9ecef', text: '#495057' };
    }

    // Buscar coincidencia exacta
    if (COLORES_ESTABLECIMIENTOS[tienda]) {
        return COLORES_ESTABLECIMIENTOS[tienda];
    }

    // Buscar coincidencia parcial
    const tiendaUpper = tienda.toUpperCase();
    for (const [nombre, colores] of Object.entries(COLORES_ESTABLECIMIENTOS)) {
        if (nombre.toUpperCase().includes(tiendaUpper) ||
            tiendaUpper.includes(nombre.toUpperCase())) {
            return colores;
        }
    }

    // Fallback
    return { bg: '#e9ecef', text: '#495057' };
}

/**
 * Renderizar PLU con colores por establecimiento
 */
function renderPLU(producto) {
    if (!producto.codigo_plu || producto.codigo_plu.trim() === '') {
        return '<span style="color: #999; font-style: italic;">-</span>';
    }

    const plus = producto.codigo_plu.split(',').map(p => p.trim());

    return plus.map(pluConTienda => {
        const match = pluConTienda.match(/^(\d+)\s*\(([^)]+)\)$/);

        if (match) {
            const codigo = match[1];
            const tienda = match[2];
            const color = getColorTienda(tienda);

            return `
                <span style="
                    display: inline-block;
                    padding: 2px 8px;
                    margin: 2px;
                    background: ${color.bg};
                    color: ${color.text};
                    border-radius: 4px;
                    font-size: 11px;
                    font-weight: 500;
                    white-space: nowrap;
                    cursor: help;
                " title="PLU ${codigo} en ${tienda}">
                    ${codigo} <small style="opacity: 0.8;">${tienda}</small>
                </span>
            `;
        } else {
            return `<span style="font-family: monospace; font-size: 11px; margin: 0 4px; color: #666;">${pluConTienda}</span>`;
        }
    }).join('');
}

/**
 * Recargar colores desde la API
 */
async function recargarColores() {
    console.log('🔄 Recargando colores...');

    COLORES_CARGADOS = false;
    localStorage.removeItem('colores_establecimientos');

    await cargarColoresEstablecimientos();
    await cargarProductos(paginaActual);

    console.log('✅ Colores recargados');
    mostrarAlerta('✅ Colores actualizados correctamente', 'success', 2000);
}

// ============================================================================
// TABS
// ============================================================================
function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

    event.target.classList.add('active');
    document.getElementById(`tab-${tabName}`).classList.add('active');

    if (tabName === 'lista') {
        cargarProductos(paginaActual);
    } else if (tabName === 'calidad') {
        cargarCalidadDatos();
    }
}

// ============================================================================
// CARGAR PRODUCTOS
// ============================================================================
async function cargarProductos(pagina = 1) {
    paginaActual = pagina;
    const busqueda = document.getElementById('busqueda').value;
    const filtro = document.getElementById('filtro').value;

    const params = new URLSearchParams({
        pagina: pagina,
        limite: limite,
        ...(busqueda && { busqueda }),
        ...(filtro !== 'todos' && { filtro })
    });

    try {
        const response = await fetch(`/api/productos?${params}`);
        const data = await response.json();

        renderProductos(data.productos);
        renderPaginacion(data.paginacion);
        cargarStats();
    } catch (error) {
        console.error('Error:', error);
        mostrarAlerta('Error cargando productos', 'error');
    }
}

function renderProductos(productos) {
    const tbody = document.getElementById('productos-body');

    if (productos.length === 0) {
        tbody.innerHTML = '<tr><td colspan="11" style="text-align: center; padding: 40px;">No se encontraron productos</td></tr>';
        return;
    }

    tbody.innerHTML = productos.map(p => {
        const isSelected = productosSeleccionados.has(p.id);
        return `
        <tr class="${isSelected ? 'selected' : ''}">
            <td class="checkbox-cell">
                <input type="checkbox" ${isSelected ? 'checked' : ''} onchange="toggleProducto(${p.id})">
            </td>
            <td>${p.id}</td>
            <td class="editable" onclick="editarCampo(${p.id}, 'codigo_ean', this)">
                ${p.codigo_ean || '<span style="color: #999;">-</span>'}
            </td>
            <td>${renderPLU(p)}</td>
            <td class="editable" onclick="editarCampo(${p.id}, 'nombre_normalizado', this)">
                <strong>${p.nombre_normalizado || p.nombre_comercial || 'Sin nombre'}</strong>
            </td>
            <td class="editable" onclick="editarCampo(${p.id}, 'marca', this)">
                ${p.marca || '<span style="color: #999;">-</span>'}
            </td>
            <td class="editable" onclick="editarCampo(${p.id}, 'categoria', this)">
                ${p.categoria || '<span style="color: #999;">-</span>'}
            </td>
            <td>$${(p.precio_promedio || 0).toLocaleString()}</td>
            <td>${p.total_reportes || 0}</td>
            <td>
                ${renderBadges(p.problemas)}
            </td>
            <td>
                <button class="btn-primary btn-small" onclick="editarProducto(${p.id})">✏️</button>
                <button class="btn-secondary btn-small" onclick="verHistorial(${p.id})">📜</button>
            </td>
        </tr>
    `}).join('');
}

function renderBadges(problemas) {
    if (!problemas || problemas.length === 0) {
        return '<span class="badge badge-success">OK</span>';
    }

    const labels = {
        'sin_ean': '⚠️ Sin EAN',
        'sin_marca': 'Sin marca',
        'sin_categoria': 'Sin categoría'
    };

    return problemas.map(prob => {
        const cssClass = prob.includes('sin_ean') ? 'badge-error' : 'badge-warning';
        return `<span class="badge ${cssClass}">${labels[prob] || prob}</span>`;
    }).join(' ');
}

function renderPaginacion(paginacion) {
    const div = document.getElementById('pagination');
    const { pagina, paginas } = paginacion;

    let html = '';

    if (pagina > 1) {
        html += `<button class="btn-secondary" onclick="cargarProductos(${pagina - 1})">← Anterior</button>`;
    }

    html += `<span style="padding: 8px 16px;">Página ${pagina} de ${paginas}</span>`;

    if (pagina < paginas) {
        html += `<button class="btn-secondary" onclick="cargarProductos(${pagina + 1})">Siguiente →</button>`;
    }

    div.innerHTML = html;
}

// ============================================================================
// ESTADÍSTICAS
// ============================================================================
async function cargarStats() {
    try {
        const response = await fetch('/api/productos/estadisticas/calidad');
        const data = await response.json();

        document.getElementById('stats').innerHTML = `
            <div class="stat-card">
                <div class="stat-value">${data.total_productos}</div>
                <div class="stat-label">Total Productos</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${data.porcentaje_ean}%</div>
                <div class="stat-label">Con EAN (${data.con_ean})</div>
            </div>
            <div class="stat-card warning">
                <div class="stat-value">${data.sin_marca}</div>
                <div class="stat-label">Sin Marca</div>
            </div>
            <div class="stat-card danger">
                <div class="stat-value">${data.duplicados_potenciales}</div>
                <div class="stat-label">Duplicados Detectados</div>
            </div>
        `;
    } catch (error) {
        console.error('Error cargando stats:', error);
    }
}

// ============================================================================
// SELECCIÓN DE PRODUCTOS
// ============================================================================
function toggleProducto(id) {
    if (productosSeleccionados.has(id)) {
        productosSeleccionados.delete(id);
    } else {
        productosSeleccionados.add(id);
    }
    actualizarInterfazSeleccion();
}

function toggleSelectAll() {
    const checkbox = document.getElementById('select-all');
    const tbody = document.getElementById('productos-body');
    const rows = tbody.querySelectorAll('tr');

    if (checkbox.checked) {
        rows.forEach(row => {
            const rowCheckbox = row.querySelector('input[type="checkbox"]');
            if (rowCheckbox) {
                const id = parseInt(rowCheckbox.getAttribute('onchange').match(/\d+/)[0]);
                productosSeleccionados.add(id);
            }
        });
    } else {
        productosSeleccionados.clear();
    }

    actualizarInterfazSeleccion();
    cargarProductos(paginaActual);
}

function deseleccionarTodos() {
    productosSeleccionados.clear();
    document.getElementById('select-all').checked = false;
    actualizarInterfazSeleccion();
    cargarProductos(paginaActual);
}

function actualizarInterfazSeleccion() {
    const count = productosSeleccionados.size;
    document.getElementById('selected-count').textContent = `${count} seleccionados`;
    document.getElementById('btn-fusionar').disabled = count < 2;
    document.getElementById('btn-deseleccionar').disabled = count === 0;
}

// ============================================================================
// EDICIÓN INLINE
// ============================================================================
function editarCampo(productoId, campo, td) {
    if (td.classList.contains('editing')) return;

    const valorActual = td.textContent.trim();
    td.classList.add('editing');
    td.innerHTML = `<input type="text" value="${valorActual === '-' ? '' : valorActual}" onblur="guardarCampo(${productoId}, '${campo}', this)" onkeypress="if(event.key==='Enter') this.blur()">`;
    td.querySelector('input').focus();
}

async function guardarCampo(productoId, campo, input) {
    const nuevoValor = input.value.trim();
    const td = input.parentElement;

    try {
        const response = await fetch(`/api/productos/${productoId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ [campo]: nuevoValor || null })
        });

        if (response.ok) {
            td.classList.remove('editing');
            td.innerHTML = nuevoValor || '<span style="color: #999;">-</span>';
            mostrarAlerta('✅ Campo actualizado', 'success', 2000);
        } else {
            throw new Error('Error actualizando');
        }
    } catch (error) {
        console.error('Error:', error);
        td.classList.remove('editing');
        td.innerHTML = nuevoValor || '<span style="color: #999;">-</span>';
        mostrarAlerta('❌ Error actualizando campo', 'error');
    }
}

// ============================================================================
// MODAL DE EDICIÓN
// ============================================================================
async function editarProducto(id) {
    try {
        const response = await fetch(`/api/productos/${id}`);
        const producto = await response.json();

        document.getElementById('edit-id').value = producto.id;
        document.getElementById('edit-ean').value = producto.codigo_ean || '';
        document.getElementById('edit-nombre-norm').value = producto.nombre_normalizado || '';
        document.getElementById('edit-nombre-com').value = producto.nombre_comercial || '';
        document.getElementById('edit-marca').value = producto.marca || '';
        document.getElementById('edit-categoria').value = producto.categoria || '';
        document.getElementById('edit-subcategoria').value = producto.subcategoria || '';
        document.getElementById('edit-presentacion').value = producto.presentacion || '';

        document.getElementById('modal-editar').classList.add('active');
    } catch (error) {
        console.error('Error:', error);
        mostrarAlerta('Error cargando producto', 'error');
    }
}

async function guardarEdicion(event) {
    event.preventDefault();

    const id = document.getElementById('edit-id').value;
    const data = {
        codigo_ean: document.getElementById('edit-ean').value || null,
        nombre_normalizado: document.getElementById('edit-nombre-norm').value,
        nombre_comercial: document.getElementById('edit-nombre-com').value || null,
        marca: document.getElementById('edit-marca').value || null,
        categoria: document.getElementById('edit-categoria').value || null,
        subcategoria: document.getElementById('edit-subcategoria').value || null,
        presentacion: document.getElementById('edit-presentacion').value || null
    };

    try {
        const response = await fetch(`/api/productos/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (response.ok) {
            mostrarAlerta('✅ Producto actualizado', 'success');
            cerrarModal('modal-editar');
            cargarProductos(paginaActual);
        } else {
            throw new Error('Error actualizando');
        }
    } catch (error) {
        console.error('Error:', error);
        mostrarAlerta('Error actualizando producto', 'error');
    }
}

// ============================================================================
// FUSIÓN Y DUPLICADOS
// ============================================================================
function fusionarSeleccionados() {
    if (productosSeleccionados.size < 2) {
        mostrarAlerta('Selecciona al menos 2 productos para fusionar', 'warning');
        return;
    }

    const ids = Array.from(productosSeleccionados);

    document.getElementById('fusion-preview').innerHTML = `
        <div style="margin: 20px 0;">
            <p><strong>Productos a fusionar:</strong></p>
            <ul style="margin-left: 20px; margin-top: 10px;">
                ${ids.map(id => `<li>Producto ID: ${id}</li>`).join('')}
            </ul>
            <p style="margin-top: 15px;">
                <strong>Producto principal:</strong> ID ${ids[0]}<br>
                <small style="color: #64748b;">Los demás productos serán fusionados en este</small>
            </p>
        </div>
    `;

    document.getElementById('modal-fusionar').classList.add('active');
}

async function confirmarFusion() {
    const ids = Array.from(productosSeleccionados);
    const estrategia = document.getElementById('estrategia-fusion').value;

    const data = {
        producto_principal_id: ids[0],
        productos_duplicados_ids: ids.slice(1),
        mantener_datos_de: estrategia
    };

    try {
        const response = await fetch('/api/productos/fusionar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        const result = await response.json();

        if (response.ok) {
            mostrarAlerta(`✅ ${result.mensaje}. ${result.productos_eliminados} productos fusionados`, 'success');
            cerrarModal('modal-fusionar');
            productosSeleccionados.clear();
            actualizarInterfazSeleccion();
            cargarProductos(paginaActual);
            cargarStats();
        } else {
            throw new Error(result.detail || 'Error en fusión');
        }
    } catch (error) {
        console.error('Error:', error);
        mostrarAlerta('❌ Error fusionando productos: ' + error.message, 'error');
    }
}

async function detectarDuplicados() {
    switchTab('duplicados');
    await cargarDuplicados();
}

async function cargarDuplicados() {
    const container = document.getElementById('duplicados-container');
    container.innerHTML = '<div style="text-align: center; padding: 40px;"><div class="loading"></div><p style="margin-top: 10px;">Analizando productos...</p></div>';

    try {
        const [eanData, pluData, nombresData] = await Promise.all([
            fetch('/api/productos/duplicados/ean').then(r => r.json()),
            fetch('/api/productos/duplicados/plu-establecimiento').then(r => r.json()),
            fetch('/api/productos/duplicados/nombres-similares?umbral_similitud=0.9').then(r => r.json())
        ]);

        let html = '';

        if (eanData.total === 0 && pluData.total === 0 && nombresData.total === 0) {
            html = '<div class="alert alert-success"><strong>✅ ¡Excelente!</strong><br>No se detectaron duplicados en tu catálogo</div>';
        } else {
            if (eanData.total > 0) {
                html += `<h3 style="margin: 20px 0;">🔴 Duplicados por EAN (${eanData.total})</h3>`;
                html += eanData.duplicados.map(dup => renderDuplicadoItem(dup, 'ean')).join('');
            }

            if (pluData.total > 0) {
                html += `<h3 style="margin: 20px 0;">🟠 Duplicados por PLU+Establecimiento (${pluData.total})</h3>`;
                html += pluData.duplicados.map(dup => renderDuplicadoItem(dup, 'plu')).join('');
            }

            if (nombresData.total > 0) {
                html += `<h3 style="margin: 20px 0;">🟡 Nombres Similares (${nombresData.total})</h3>`;
                html += nombresData.duplicados.map(dup => renderDuplicadoItem(dup, 'nombre')).join('');
            }
        }

        container.innerHTML = html;
    } catch (error) {
        console.error('Error:', error);
        container.innerHTML = '<div class="alert alert-error">Error cargando duplicados</div>';
    }
}

function renderDuplicadoItem(dup, tipo) {
    const tipoClass = tipo === 'ean' ? '' : tipo === 'plu' ? 'plu' : 'nombre';
    const icon = tipo === 'ean' ? '🔴' : tipo === 'plu' ? '🟠' : '🟡';

    return `
        <div class="duplicado-item ${tipoClass}">
            <div class="duplicado-header">
                <div>
                    <strong>${icon} ${dup.razon}</strong>
                    <span class="badge badge-error" style="margin-left: 10px;">Severidad: ${dup.severidad}</span>
                </div>
                <button class="btn-success btn-small" onclick="fusionarDuplicado(${JSON.stringify(dup.productos.map(p => p.id))})">
                    🔗 Fusionar Todo
                </button>
            </div>
            <div class="productos-duplicados">
                ${dup.productos.map((p, idx) => `
                    <div class="producto-dup-card ${idx === 0 ? 'principal' : ''}">
                        <div style="display: flex; justify-content: space-between; align-items: start;">
                            <div>
                                <strong>ID ${p.id}</strong> ${idx === 0 ? '<span class="badge badge-info">PRINCIPAL</span>' : ''}<br>
                                <strong>${p.nombre_normalizado || p.nombre_comercial}</strong><br>
                                <small style="color: #64748b;">
                                    ${p.codigo_ean ? `EAN: ${p.codigo_ean}` : ''}
                                    ${p.marca ? `| Marca: ${p.marca}` : ''}
                                </small><br>
                                <small style="color: #64748b;">Compras: ${p.total_compras || 0}</small>
                            </div>
                            <button class="btn-secondary btn-small" onclick="editarProducto(${p.id})">✏️</button>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function fusionarDuplicado(ids) {
    productosSeleccionados.clear();
    ids.forEach(id => productosSeleccionados.add(id));
    actualizarInterfazSeleccion();
    fusionarSeleccionados();
}

// ============================================================================
// CALIDAD DE DATOS
// ============================================================================
async function cargarCalidadDatos() {
    try {
        const response = await fetch('/api/productos/estadisticas/calidad');
        const data = await response.json();

        document.getElementById('calidad-stats').innerHTML = `
            <div class="stat-card">
                <div class="stat-value">${data.total_productos}</div>
                <div class="stat-label">Total Productos</div>
            </div>
            <div class="stat-card ${data.porcentaje_ean < 50 ? 'danger' : data.porcentaje_ean < 80 ? 'warning' : ''}">
                <div class="stat-value">${data.porcentaje_ean}%</div>
                <div class="stat-label">Con EAN (${data.con_ean}/${data.total_productos})</div>
            </div>
            <div class="stat-card ${data.porcentaje_marca < 50 ? 'danger' : data.porcentaje_marca < 80 ? 'warning' : ''}">
                <div class="stat-value">${data.porcentaje_marca}%</div>
                <div class="stat-label">Con Marca (${data.con_marca}/${data.total_productos})</div>
            </div>
            <div class="stat-card ${data.porcentaje_categoria < 50 ? 'danger' : data.porcentaje_categoria < 80 ? 'warning' : ''}">
                <div class="stat-value">${data.porcentaje_categoria}%</div>
                <div class="stat-label">Con Categoría (${data.con_categoria}/${data.total_productos})</div>
            </div>
        `;

        // Generar recomendaciones
        const recomendaciones = [];

        if (data.porcentaje_ean < 80) {
            recomendaciones.push({
                icon: '⚠️',
                texto: `${data.sin_ean} productos sin código EAN. Agrega códigos de barras para mejorar el matching.`,
                accion: 'Ver productos sin EAN',
                filtro: 'sin_ean'
            });
        }

        if (data.porcentaje_marca < 80) {
            recomendaciones.push({
                icon: '🏷️',
                texto: `${data.sin_marca} productos sin marca. Completa esta información para mejor categorización.`,
                accion: 'Ver productos sin marca',
                filtro: 'sin_marca'
            });
        }

        if (data.duplicados_potenciales > 0) {
            recomendaciones.push({
                icon: '🔴',
                texto: `${data.duplicados_potenciales} productos duplicados detectados. ¡Fusiónalos para limpiar tu catálogo!`,
                accion: 'Ver duplicados',
                filtro: 'duplicados'
            });
        }

        if (recomendaciones.length === 0) {
            document.getElementById('recomendaciones').innerHTML = '<div class="alert alert-success">✅ ¡Tu catálogo está en excelente estado!</div>';
        } else {
            document.getElementById('recomendaciones').innerHTML = recomendaciones.map(r => `
                <div class="alert alert-warning" style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong>${r.icon} ${r.texto}</strong>
                    </div>
                    <button class="btn-primary btn-small" onclick="aplicarFiltro('${r.filtro}')">
                        ${r.accion}
                    </button>
                </div>
            `).join('');
        }
    } catch (error) {
        console.error('Error:', error);
        mostrarAlerta('Error cargando calidad de datos', 'error');
    }
}

function aplicarFiltro(filtro) {
    switchTab('lista');
    if (filtro === 'duplicados') {
        detectarDuplicados();
    } else {
        document.getElementById('filtro').value = filtro;
        cargarProductos(1);
    }
}

// ============================================================================
// HISTORIAL DE COMPRAS
// ============================================================================
async function verHistorial(productoId) {
    try {
        const response = await fetch(`/api/productos/${productoId}/historial-compras`);
        const data = await response.json();

        let html = `
            <div class="alert alert-info">
                <strong>Producto ID: ${productoId}</strong><br>
                Total de compras: ${data.total_compras}
            </div>
        `;

        if (data.compras.length === 0) {
            html += '<p style="text-align: center; color: #64748b;">No hay compras registradas</p>';
        } else {
            html += '<table style="width: 100%; margin-top: 20px;"><thead><tr><th>Fecha</th><th>Establecimiento</th><th>Cantidad</th><th>Precio</th></tr></thead><tbody>';
            html += data.compras.map(c => `
                <tr>
                    <td>${c.fecha || '-'}</td>
                    <td>${c.establecimiento || '-'}</td>
                    <td>${c.cantidad}</td>
                    <td>$${(c.precio || 0).toLocaleString()}</td>
                </tr>
            `).join('');
            html += '</tbody></table>';
        }

        document.getElementById('historial-container').innerHTML = html;
        document.getElementById('modal-historial').classList.add('active');
    } catch (error) {
        console.error('Error:', error);
        mostrarAlerta('Error cargando historial', 'error');
    }
}

// ============================================================================
// UTILIDADES
// ============================================================================
function cerrarModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

function limpiarFiltros() {
    document.getElementById('busqueda').value = '';
    document.getElementById('filtro').value = 'todos';
    cargarProductos(1);
}

function mostrarAlerta(mensaje, tipo, duracion = 5000) {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${tipo}`;
    alertDiv.textContent = mensaje;
    alertDiv.style.position = 'fixed';
    alertDiv.style.top = '20px';
    alertDiv.style.right = '20px';
    alertDiv.style.zIndex = '9999';
    alertDiv.style.minWidth = '300px';
    alertDiv.style.boxShadow = '0 4px 6px rgba(0, 0, 0, 0.1)';

    document.body.appendChild(alertDiv);

    setTimeout(() => {
        alertDiv.remove();
    }, duracion);
}

// ============================================================================
// INICIALIZACIÓN
// ============================================================================
document.addEventListener('DOMContentLoaded', async () => {
    console.log('🚀 Inicializando sistema de productos...');

    // Cargar colores primero
    await cargarColoresEstablecimientos();

    // Luego cargar productos
    await cargarProductos();

    console.log('✅ Sistema inicializado correctamente');
});

// Cerrar modales al hacer clic fuera
document.querySelectorAll('.modal').forEach(modal => {
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            modal.classList.remove('active');
        }
    });
});

console.log('📄 productos.js cargado (versión con colores dinámicos)');
