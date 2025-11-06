// funciones_plu_modal.js
// Funciones para manejar PLUs en el modal de edición

let pluCounter = 0;
let establecimientosCache = [];

// =============================================================
// 🌐 Base de API (forzar HTTPS)
// =============================================================
function getApiBase() {
    try {
        let origin = window.location.origin;
        if (origin.startsWith('http://')) origin = origin.replace('http://', 'https://');
        if (!origin.startsWith('https://')) origin = 'https://' + window.location.host;
        origin = origin.replace(/\/+$/, '');
        return origin.replace(/^http:/, 'https:');
    } catch (e) {
        console.error('⚠️ Error determinando API base:', e);
        return 'https://lecfac-backend-production.up.railway.app';
    }
}

// =============================================================
// 🧰 fetchSeguro: evita Mixed Content si el backend redirige a http
// - Añade “/” final en la URL base de recursos
// - Si recibe 307 a http, rehace la URL en https y reintenta
// =============================================================
async function fetchSeguro(url, options = {}) {
    const resp = await fetch(url, options);
    if (resp.status === 307) {
        const loc = resp.headers.get('location') || '';
        if (loc.startsWith('http://')) {
            const httpsUrl = loc.replace('http://', 'https://');
            return fetch(httpsUrl, options);
        }
    }
    return resp;
}

// =============================================================
// Cargar establecimientos al iniciar
// =============================================================
async function cargarEstablecimientos() {
    try {
        const apiBase = getApiBase();
        // IMPORTANTE: “/” final para evitar 307 → http
        const url = `${apiBase}/api/establecimientos/`;
        const response = await fetchSeguro(url);

        if (response.ok) {
            establecimientosCache = await response.json();
            console.log('✅ Establecimientos cargados:', establecimientosCache.length);
        } else {
            console.warn('⚠️ No se pudieron cargar establecimientos (status:', response.status, ')');
        }
    } catch (error) {
        console.error('❌ Error cargando establecimientos:', error);
        // Fallback para que el modal funcione
        establecimientosCache = [
            { id: 1, nombre_normalizado: 'Éxito' },
            { id: 2, nombre_normalizado: 'Carulla' },
            { id: 3, nombre_normalizado: 'Jumbo' },
            { id: 4, nombre_normalizado: 'Olímpica' },
            { id: 5, nombre_normalizado: 'D1' },
            { id: 6, nombre_normalizado: 'Ara' },
            { id: 7, nombre_normalizado: 'Justo y Bueno' },
            { id: 8, nombre_normalizado: 'Alkosto' },
            { id: 9, nombre_normalizado: 'OLÍMPICA' }
        ];
    }
}

// =============================================================
// Agregar un PLU vacío al formulario
// =============================================================
function agregarPLU() {
    pluCounter++;
    const contenedor = document.getElementById('contenedorPLUs');

    const pluHTML = `
        <div class="plu-item" id="plu-${pluCounter}">
            <div class="row">
                <div class="col-md-4">
                    <label class="form-label">Establecimiento</label>
                    <select class="form-select plu-establecimiento" data-plu-id="${pluCounter}">
                        <option value="">Seleccionar...</option>
                        ${establecimientosCache.map(e =>
        `<option value="${e.id}">${e.nombre_normalizado || 'Est. ' + e.id}</option>`
    ).join('')}
                    </select>
                </div>
                <div class="col-md-3">
                    <label class="form-label">Código PLU</label>
                    <input type="text" class="form-control plu-codigo"
                           data-plu-id="${pluCounter}" placeholder="Ej: 967509">
                </div>
                <div class="col-md-3">
                    <label class="form-label">Precio Unitario</label>
                    <input type="number" class="form-control plu-precio"
                           data-plu-id="${pluCounter}" placeholder="Ej: 5000">
                </div>
                <div class="col-md-2">
                    <label class="form-label">&nbsp;</label>
                    <button type="button" class="btn btn-danger btn-sm d-block"
                            onclick="eliminarPLU(${pluCounter})">
                        <i class="bi bi-trash"></i> Eliminar
                    </button>
                </div>
            </div>
        </div>
    `;
    contenedor.insertAdjacentHTML('beforeend', pluHTML);
}

// =============================================================
// Eliminar un PLU del formulario
// =============================================================
function eliminarPLU(id) {
    const elemento = document.getElementById(`plu-${id}`);
    if (elemento) elemento.remove();
}

// =============================================================
// Cargar PLUs existentes al editar producto
// =============================================================
async function cargarPLUsProducto(productoId) {
    try {
        const apiBase = getApiBase();
        const url = `${apiBase}/api/productos/${productoId}/plus/`; // “/” final
        const response = await fetchSeguro(url);
        if (response.ok) {
            const data = await response.json();

            document.getElementById('contenedorPLUs').innerHTML = '';
            pluCounter = 0;

            if (data.plus && data.plus.length > 0) {
                data.plus.forEach(plu => agregarPLUExistente(plu));
            } else {
                agregarPLU(); // uno vacío
            }

            console.log(`✅ ${data.plus?.length || 0} PLUs cargados`);
        }
    } catch (error) {
        console.error('❌ Error cargando PLUs:', error);
    }
}

// =============================================================
// Agregar un PLU existente (con datos)
// =============================================================
function agregarPLUExistente(plu) {
    pluCounter++;
    const contenedor = document.getElementById('contenedorPLUs');

    const pluHTML = `
        <div class="plu-item" id="plu-${pluCounter}">
            <div class="row">
                <div class="col-md-4">
                    <label class="form-label">Establecimiento</label>
                    <select class="form-select plu-establecimiento" data-plu-id="${pluCounter}">
                        <option value="">Seleccionar...</option>
                        ${establecimientosCache.map(e =>
        `<option value="${e.id}" ${e.id === plu.establecimiento_id ? 'selected' : ''}>
                                ${e.nombre_normalizado || 'Est. ' + e.id}
                            </option>`
    ).join('')}
                    </select>
                </div>
                <div class="col-md-3">
                    <label class="form-label">Código PLU</label>
                    <input type="text" class="form-control plu-codigo"
                           data-plu-id="${pluCounter}"
                           value="${plu.codigo_plu || ''}"
                           placeholder="Ej: 967509">
                </div>
                <div class="col-md-3">
                    <label class="form-label">Precio Unitario</label>
                    <input type="number" class="form-control plu-precio"
                           data-plu-id="${pluCounter}"
                           value="${plu.precio_unitario || ''}"
                           placeholder="Ej: 5000">
                </div>
                <div class="col-md-2">
                    <label class="form-label">&nbsp;</label>
                    <button type="button" class="btn btn-danger btn-sm d-block"
                            onclick="eliminarPLU(${pluCounter})">
                        <i class="bi bi-trash"></i> Eliminar
                    </button>
                </div>
            </div>
        </div>
    `;
    contenedor.insertAdjacentHTML('beforeend', pluHTML);
}

// =============================================================
// Recopilar PLUs del formulario
// =============================================================
function recopilarPLUs() {
    const plus = [];
    document.querySelectorAll('.plu-item').forEach(item => {
        const establecimientoSelect = item.querySelector('.plu-establecimiento');
        const codigoInput = item.querySelector('.plu-codigo');
        const precioInput = item.querySelector('.plu-precio');

        if (establecimientoSelect?.value && codigoInput?.value) {
            plus.push({
                establecimiento_id: parseInt(establecimientoSelect.value),
                codigo_plu: codigoInput.value.trim(),
                precio_unitario: precioInput.value ? parseInt(precioInput.value) : null
            });
        }
    });
    return plus;
}

// =============================================================
// Guardar edición completa (producto + PLUs)
// =============================================================
async function guardarEdicionPLUs() {
    const productoId = document.getElementById('productoId').value;
    const apiBase = getApiBase();

    const datosProducto = {
        codigo_ean: document.getElementById('codigoEan').value || null,
        nombre_normalizado: document.getElementById('nombreNormalizado').value,
        nombre_comercial: document.getElementById('nombreComercial').value || null,
        marca: document.getElementById('marca').value || null,
        categoria: document.getElementById('categoria').value || null,
        subcategoria: document.getElementById('subcategoria').value || null,
        presentacion: document.getElementById('presentacion').value || null
    };

    try {
        // Producto
        const respProducto = await fetchSeguro(`${apiBase}/api/productos/${productoId}/`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(datosProducto)
        });
        if (!respProducto.ok) throw new Error('Error actualizando producto');

        // PLUs
        const plus = recopilarPLUs();
        const respPLUs = await fetchSeguro(`${apiBase}/api/productos/${productoId}/plus/`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(plus)
        });
        if (!respPLUs.ok) throw new Error('Error actualizando PLUs');

        // Cerrar modal (si usas Bootstrap)
        const modalEl = document.getElementById('modal-editar');
        const modal = modalEl ? bootstrap.Modal.getInstance(modalEl) : null;
        if (modal) modal.hide();
        else modalEl?.classList.remove('active');

        // Recargar
        if (typeof cargarProductos === 'function') cargarProductos();

        alert('✅ Producto actualizado correctamente');
    } catch (error) {
        console.error('❌ Error guardando:', error);
        alert('Error al guardar: ' + error.message);
    }
}

// =============================================================
// Duplicados
// =============================================================
async function detectarDuplicados() {
    console.log('🔍 Detectando duplicados...');
    try {
        const apiBase = getApiBase();
        const resp = await fetchSeguro(`${apiBase}/api/productos/duplicados/?umbral_similitud=0.8&limite=50`);
        if (!resp.ok) throw new Error('Error al detectar duplicados');
        const data = await resp.json();
        if (data.duplicados?.length > 0) mostrarDuplicados(data.duplicados);
        else alert('No se encontraron productos duplicados');
    } catch (error) {
        console.error('❌ Error:', error);
        alert('Error detectando duplicados. Verifica la consola.');
    }
}

function mostrarDuplicados(duplicados) {
    let html = '<h5>Posibles Duplicados Encontrados:</h5><ul>';
    duplicados.forEach(dup => {
        html += `
            <li>
                <strong>${dup.nombre1}</strong> (ID: ${dup.id1})
                <br>↔️<br>
                <strong>${dup.nombre2}</strong> (ID: ${dup.id2})
                <br>Similitud: ${(dup.similitud * 100).toFixed(1)}%
            </li><hr>`;
    });
    html += '</ul>';

    const modalHTML = `
        <div class="modal fade" id="modalDuplicados" tabindex="-1">
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">Productos Duplicados</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">${html}</div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cerrar</button>
                    </div>
                </div>
            </div>
        </div>`;
    if (!document.getElementById('modalDuplicados')) {
        document.body.insertAdjacentHTML('beforeend', modalHTML);
    } else {
        document.querySelector('#modalDuplicados .modal-body').innerHTML = html;
    }
    const modal = new bootstrap.Modal(document.getElementById('modalDuplicados'));
    modal.show();
}

// =============================================================
// Inicialización
// =============================================================
document.addEventListener('DOMContentLoaded', () => {
    cargarEstablecimientos();
});

// =============================================================
// Exportar
// =============================================================
window.agregarPLU = agregarPLU;
window.eliminarPLU = eliminarPLU;
window.cargarPLUsProducto = cargarPLUsProducto;
window.detectarDuplicados = detectarDuplicados;
window.recopilarPLUs = recopilarPLUs;
