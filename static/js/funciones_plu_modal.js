// funciones_plu_modal.js
// Funciones para manejar PLUs en el modal de edición

let pluCounter = 0;
let establecimientosCache = [];

// =============================================================
// 🌐 Función auxiliar global para obtener la URL base de la API
// Corrige Mixed Content y redirecciones HTTP→HTTPS
// =============================================================
function getApiBase() {
    try {
        let origin = window.location.origin;

        // Si el origen empieza con http:// → forzamos https://
        if (origin.startsWith('http://')) {
            origin = origin.replace('http://', 'https://');
        }

        // Si el navegador o proxy devuelve algo sin protocolo, agregamos https
        if (!origin.startsWith('https://')) {
            origin = 'https://' + window.location.host;
        }

        // Limpieza de barras dobles finales
        origin = origin.replace(/\/+$/, '');

        // Fuerza HTTPS explícitamente (Railway a veces responde con http)
        return origin.replace(/^http:/, 'https:');
    } catch (e) {
        console.error('⚠️ Error determinando API base:', e);
        return 'https://lecfac-backend-production.up.railway.app';
    }
}

// =============================================================
// Cargar establecimientos al iniciar
// =============================================================
async function cargarEstablecimientos() {
    try {
        const apiBase = getApiBase();
        const response = await fetch(`${apiBase}/api/establecimientos`);

        if (response.ok) {
            establecimientosCache = await response.json();
            console.log('✅ Establecimientos cargados:', establecimientosCache.length);
        } else {
            console.warn('⚠️ No se pudieron cargar establecimientos (status:', response.status, ')');
        }
    } catch (error) {
        console.error('❌ Error cargando establecimientos:', error);
        // Usar establecimientos por defecto como fallback
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
        const response = await fetch(`${apiBase}/api/productos/${productoId}/plus`);
        if (response.ok) {
            const data = await response.json();

            document.getElementById('contenedorPLUs').innerHTML = '';
            pluCounter = 0;

            if (data.plus && data.plus.length > 0) {
                data.plus.forEach(plu => agregarPLUExistente(plu));
            } else {
                agregarPLU(); // agregar uno vacío si no hay PLUs
            }

            console.log(`✅ ${data.plus.length} PLUs cargados`);
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
// Recopilar PLUs del formulario antes de guardar
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
        const responseProducto = await fetch(`${apiBase}/api/productos/${productoId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(datosProducto)
        });

        if (!responseProducto.ok) throw new Error('Error actualizando producto');

        const plus = recopilarPLUs();
        console.log('PLUs a guardar:', plus);

        const responsePLUs = await fetch(`${apiBase}/api/productos/${productoId}/plus`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(plus)
        });

        if (!responsePLUs.ok) throw new Error('Error actualizando PLUs');

        const modal = bootstrap.Modal.getInstance(document.getElementById('modal-editar'));
        modal.hide();

        cargarProductos();
        alert('✅ Producto actualizado correctamente');

    } catch (error) {
        console.error('❌ Error guardando:', error);
        alert('Error al guardar: ' + error.message);
    }
}

// =============================================================
// Detección de duplicados
// =============================================================
async function detectarDuplicados() {
    console.log('🔍 Detectando duplicados...');
    try {
        const apiBase = getApiBase();
        const response = await fetch(`${apiBase}/api/productos/duplicados?umbral_similitud=0.8&limite=50`);
        if (response.ok) {
            const data = await response.json();
            if (data.duplicados?.length > 0) mostrarDuplicados(data.duplicados);
            else alert('No se encontraron productos duplicados');
        } else {
            throw new Error('Error al detectar duplicados');
        }
    } catch (error) {
        console.error('❌ Error:', error);
        alert('Error detectando duplicados. Verifica la consola.');
    }
}

// =============================================================
// Mostrar duplicados en modal
// =============================================================
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
// Inicialización automática al cargar
// =============================================================
document.addEventListener('DOMContentLoaded', () => {
    cargarEstablecimientos();
});

// =============================================================
// Exportar funciones globales
// =============================================================
window.agregarPLU = agregarPLU;
window.eliminarPLU = eliminarPLU;
window.cargarPLUsProducto = cargarPLUsProducto;
window.detectarDuplicados = detectarDuplicados;
window.recopilarPLUs = recopilarPLUs;
