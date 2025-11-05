// ============================================================================
// productos.js - VERSIÓN CORREGIDA PARA MOSTRAR PLUs
// ============================================================================

console.log("✅ productos.js cargado - Versión con PLUs");

// Cache de colores de establecimientos
let coloresCache = null;
let cacheTimestamp = null;
const CACHE_DURATION = 24 * 60 * 60 * 1000; // 24 horas

// ============================================================================
// FUNCIONES DE COLORES
// ============================================================================

async function cargarColoresEstablecimientos() {
    try {
        // Verificar cache
        const ahora = Date.now();
        const cacheData = localStorage.getItem('colores_establecimientos');
        const cacheTime = localStorage.getItem('colores_timestamp');

        if (cacheData && cacheTime && (ahora - parseInt(cacheTime)) < CACHE_DURATION) {
            console.log("✅ Usando colores desde cache");
            coloresCache = JSON.parse(cacheData);
            return coloresCache;
        }

        // Cargar desde API
        console.log("🔄 Cargando colores desde API...");
        const response = await fetch('/api/establecimientos/colores');

        if (!response.ok) {
            console.warn("⚠️ No se pudieron cargar colores, usando defaults");
            return obtenerColoresDefault();
        }

        coloresCache = await response.json();

        // Guardar en cache
        localStorage.setItem('colores_establecimientos', JSON.stringify(coloresCache));
        localStorage.setItem('colores_timestamp', ahora.toString());

        console.log("✅ Colores cargados:", Object.keys(coloresCache).length, "establecimientos");
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

    // Normalizar nombre
    const nombreNorm = nombreTienda.toUpperCase().trim();

    // Buscar coincidencia exacta o parcial
    for (const [tienda, colores] of Object.entries(coloresCache)) {
        if (nombreNorm.includes(tienda.toUpperCase()) || tienda.toUpperCase().includes(nombreNorm)) {
            return colores;
        }
    }

    // Default gris
    return { bg: '#e9ecef', text: '#495057' };
}

// ============================================================================
// RENDERIZAR PLUs CON BADGES
// ============================================================================

function renderPLU(pluString) {
    if (!pluString || pluString === '-' || pluString === 'null' || pluString === '') {
        return '-';
    }

    // El formato viene como: "253026 (OLÍMPICA), 284239 (ÉXITO)"
    const plus = pluString.split(',').map(p => p.trim());

    let html = '';
    for (const plu of plus) {
        // Extraer código y establecimiento
        const match = plu.match(/^(.+?)\s*\((.+?)\)$/);

        if (match) {
            const codigo = match[1].trim();
            const establecimiento = match[2].trim();
            const colores = getColorTienda(establecimiento);

            html += `<span class="badge" style="background-color: ${colores.bg}; color: ${colores.text}; margin-right: 5px; margin-bottom: 3px; display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px;">
                ${codigo} ${establecimiento}
            </span>`;
        } else {
            // Si no tiene formato, mostrar simple
            html += `<span class="badge badge-secondary" style="margin-right: 5px;">${plu}</span>`;
        }
    }

    return html || '-';
}

// ============================================================================
// CARGAR PRODUCTOS
// ============================================================================

async function cargarProductos(pagina = 1, busqueda = '', filtro = '') {
    try {
        console.log(`📦 Cargando productos - Página ${pagina}`);

        // Construir URL
        let url = `/api/productos?pagina=${pagina}&limite=50`;

        if (busqueda) {
            url += `&busqueda=${encodeURIComponent(busqueda)}`;
        }

        if (filtro) {
            url += `&filtro=${filtro}`;
        }

        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`Error ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();

        if (!data.success) {
            throw new Error(data.message || 'Error desconocido');
        }

        console.log(`✅ ${data.productos.length} productos cargados`);

        // Renderizar tabla
        renderizarTabla(data.productos);

        // Renderizar paginación
        renderizarPaginacion(data.paginacion);

        // Actualizar estadísticas
        actualizarEstadisticas(data.paginacion);

    } catch (error) {
        console.error('❌ Error cargando productos:', error);
        mostrarError('Error al cargar productos: ' + error.message);
    }
}

function renderizarTabla(productos) {
    const tbody = document.querySelector('#tabla-productos tbody');

    if (!tbody) {
        console.error('❌ No se encontró tbody de tabla-productos');
        return;
    }

    if (productos.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" class="text-center">No se encontraron productos</td></tr>';
        return;
    }

    tbody.innerHTML = productos.map(p => {
        // Calcular problemas
        const problemas = [];
        if (!p.codigo_ean) problemas.push('Sin EAN');
        if (!p.marca) problemas.push('Sin marca');
        if (!p.categoria) problemas.push('Sin categoría');

        const estadoBadge = problemas.length === 0
            ? '<span class="badge badge-success">OK</span>'
            : `<span class="badge badge-warning">⚠️ ${problemas.join(' ')}</span>`;

        // Renderizar PLU con badges de colores
        const pluHtml = renderPLU(p.codigo_plu);

        return `
            <tr data-id="${p.id}">
                <td><input type="checkbox" class="producto-checkbox" value="${p.id}"></td>
                <td>${p.id}</td>
                <td>${p.codigo_ean || '-'}</td>
                <td class="plu-column">${pluHtml}</td>
                <td>${p.nombre_normalizado || p.nombre_comercial || '-'}</td>
                <td>${p.marca || '-'}</td>
                <td>${p.categoria || '-'}</td>
                <td>$${(p.precio_promedio || 0).toLocaleString('es-CO')}</td>
                <td>${p.total_reportes || 0}</td>
                <td>${estadoBadge}</td>
                <td>
                    <button class="btn btn-sm btn-primary" onclick="editarProducto(${p.id})">✏️</button>
                    <button class="btn btn-sm btn-info" onclick="verHistorial(${p.id})">📜</button>
                </td>
            </tr>
        `;
    }).join('');
}

function renderizarPaginacion(paginacion) {
    const container = document.getElementById('paginacion-container');

    if (!container || !paginacion) return;

    const { pagina, paginas } = paginacion;

    let html = '<nav><ul class="pagination">';

    // Anterior
    html += `<li class="page-item ${pagina <= 1 ? 'disabled' : ''}">
        <a class="page-link" href="#" onclick="cambiarPagina(${pagina - 1}); return false;">Anterior</a>
    </li>`;

    // Páginas
    const maxBotones = 5;
    let inicio = Math.max(1, pagina - Math.floor(maxBotones / 2));
    let fin = Math.min(paginas, inicio + maxBotones - 1);

    if (fin - inicio < maxBotones - 1) {
        inicio = Math.max(1, fin - maxBotones + 1);
    }

    for (let i = inicio; i <= fin; i++) {
        html += `<li class="page-item ${i === pagina ? 'active' : ''}">
            <a class="page-link" href="#" onclick="cambiarPagina(${i}); return false;">${i}</a>
        </li>`;
    }

    // Siguiente
    html += `<li class="page-item ${pagina >= paginas ? 'disabled' : ''}">
        <a class="page-link" href="#" onclick="cambiarPagina(${pagina + 1}); return false;">Siguiente</a>
    </li>`;

    html += '</ul></nav>';

    container.innerHTML = html;
}

function actualizarEstadisticas(paginacion) {
    const statsDiv = document.getElementById('productos-stats');
    if (!statsDiv || !paginacion) return;

    statsDiv.innerHTML = `
        <div class="alert alert-info">
            📊 Mostrando ${paginacion.limite} de ${paginacion.total} productos (Página ${paginacion.pagina} de ${paginacion.paginas})
        </div>
    `;
}

function mostrarError(mensaje) {
    const container = document.querySelector('.container');
    if (!container) return;

    const alertDiv = document.createElement('div');
    alertDiv.className = 'alert alert-danger alert-dismissible fade show';
    alertDiv.innerHTML = `
        <strong>Error:</strong> ${mensaje}
        <button type="button" class="close" data-dismiss="alert">&times;</button>
    `;

    container.insertBefore(alertDiv, container.firstChild);

    // Auto-cerrar después de 5 segundos
    setTimeout(() => alertDiv.remove(), 5000);
}

// ============================================================================
// FUNCIONES GLOBALES
// ============================================================================

window.cambiarPagina = function (pagina) {
    const busqueda = document.getElementById('busqueda-input')?.value || '';
    const filtro = document.getElementById('filtro-select')?.value || '';
    cargarProductos(pagina, busqueda, filtro);
};

window.buscarProductos = function () {
    const busqueda = document.getElementById('busqueda-input')?.value || '';
    const filtro = document.getElementById('filtro-select')?.value || '';
    cargarProductos(1, busqueda, filtro);
};

window.aplicarFiltro = function () {
    buscarProductos();
};

window.editarProducto = function (id) {
    console.log('Editar producto:', id);
    // TODO: Implementar modal de edición
};

window.verHistorial = function (id) {
    console.log('Ver historial:', id);
    // TODO: Implementar modal de historial
};

window.recargarColores = async function () {
    console.log('🔄 Recargando colores...');
    localStorage.removeItem('colores_establecimientos');
    localStorage.removeItem('colores_timestamp');
    coloresCache = null;

    await cargarColoresEstablecimientos();

    // Recargar productos para actualizar badges
    cambiarPagina(1);

    alert('✅ Colores actualizados');
};

// ============================================================================
// INICIALIZACIÓN
// ============================================================================

document.addEventListener('DOMContentLoaded', async function () {
    console.log('🚀 Inicializando productos.js');

    // Cargar colores primero
    await cargarColoresEstablecimientos();

    // Cargar productos
    cargarProductos(1);

    // Event listeners
    const busquedaInput = document.getElementById('busqueda-input');
    if (busquedaInput) {
        busquedaInput.addEventListener('keyup', function (e) {
            if (e.key === 'Enter') {
                buscarProductos();
            }
        });
    }

    const filtroSelect = document.getElementById('filtro-select');
    if (filtroSelect) {
        filtroSelect.addEventListener('change', aplicarFiltro);
    }

    console.log('✅ productos.js inicializado');
});
