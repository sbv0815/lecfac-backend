// funciones_plu_modal.js
// Funciones para manejar PLUs en el modal de edición (v2.0 - Fix)

let pluCounter = 0;
let establecimientosCache = [];

// =============================================================
// 🌐 Base de API (forzar HTTPS)
// =============================================================
function getApiBase() {
    let origin = window.location.origin;
    if (origin.startsWith('http://')) {
        origin = origin.replace('http://', 'https://');
    }
    return origin;
}

// =============================================================
// Cargar establecimientos al iniciar
// =============================================================
async function cargarEstablecimientos() {
    try {
        const apiBase = getApiBase();
        // Sin slash final para evitar redirect 307
        const urlEstablecimientos = `${apiBase}/api/establecimientos`;
        console.log('🏪 Cargando establecimientos desde:', urlEstablecimientos);

        const response = await fetch(urlEstablecimientos);

        if (response.ok) {
            establecimientosCache = await response.json();
            console.log('✅ Establecimientos cargados:', establecimientosCache.length);
        } else {
            console.warn('⚠️ No se pudieron cargar establecimientos (status:', response.status, ')');
            usarEstablecimientosFallback();
        }
    } catch (error) {
        console.error('❌ Error cargando establecimientos:', error);
        usarEstablecimientosFallback();
    }
}

function usarEstablecimientosFallback() {
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
    console.log('📦 Usando establecimientos fallback:', establecimientosCache.length);
}

// =============================================================
// Agregar un PLU vacío al formulario
// =============================================================
function agregarPLU() {
    pluCounter++;
    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) {
        console.error('❌ No se encontró el contenedor de PLUs');
        return;
    }

    const pluHTML = `
        <div class="plu-item" id="plu-${pluCounter}">
            <div class="plu-row">
                <div class="form-group">
                    <label>Establecimiento</label>
                    <select class="form-control plu-establecimiento" data-plu-id="${pluCounter}">
                        <option value="">Seleccionar...</option>
                        ${establecimientosCache.map(e =>
        `<option value="${e.id}">${e.nombre_normalizado || 'Est. ' + e.id}</option>`
    ).join('')}
                    </select>
                </div>
                <div class="form-group">
                    <label>Código PLU</label>
                    <input type="text" class="form-control plu-codigo"
                           data-plu-id="${pluCounter}" placeholder="Ej: 967509">
                </div>
                <div class="form-group">
                    <label>Precio Unitario</label>
                    <input type="number" class="form-control plu-precio"
                           data-plu-id="${pluCounter}" placeholder="Ej: 5000">
                </div>
                <button type="button" class="btn-remove-plu"
                        onclick="eliminarPLU(${pluCounter})">
                    Eliminar
                </button>
            </div>
        </div>
    `;
    contenedor.insertAdjacentHTML('beforeend', pluHTML);
    console.log('➕ PLU agregado:', pluCounter);
}

// =============================================================
// Eliminar un PLU del formulario
// =============================================================
function eliminarPLU(id) {
    const elemento = document.getElementById(`plu-${id}`);
    if (elemento) {
        elemento.remove();
        console.log('🗑️ PLU eliminado:', id);
    }
}

// =============================================================
// Cargar PLUs existentes al editar producto
// =============================================================
async function cargarPLUsProducto(productoId) {
    try {
        const apiBase = getApiBase();
        // Sin slash final
        const urlPLUs = `${apiBase}/api/productos/${productoId}/plus`;
        console.log('📋 Cargando PLUs del producto:', urlPLUs);

        const response = await fetch(urlPLUs);

        if (response.ok) {
            const data = await response.json();
            const contenedor = document.getElementById('contenedorPLUs');

            if (contenedor) {
                contenedor.innerHTML = '';
                pluCounter = 0;

                if (data.plus && data.plus.length > 0) {
                    data.plus.forEach(plu => agregarPLUExistente(plu));
                } else {
                    // Agregar un PLU vacío por defecto
                    agregarPLU();
                }

                console.log(`✅ ${data.plus?.length || 0} PLUs cargados para producto ${productoId}`);
            }
        } else {
            console.warn(`⚠️ No se pudieron cargar PLUs (status: ${response.status})`);
            // Agregar un PLU vacío
            const contenedor = document.getElementById('contenedorPLUs');
            if (contenedor) {
                contenedor.innerHTML = '';
                pluCounter = 0;
                agregarPLU();
            }
        }
    } catch (error) {
        console.error('❌ Error cargando PLUs:', error);
        // Agregar un PLU vacío en caso de error
        const contenedor = document.getElementById('contenedorPLUs');
        if (contenedor) {
            contenedor.innerHTML = '';
            pluCounter = 0;
            agregarPLU();
        }
    }
}

// =============================================================
// Agregar un PLU existente (con datos)
// =============================================================
function agregarPLUExistente(plu) {
    pluCounter++;
    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) return;

    const pluHTML = `
        <div class="plu-item" id="plu-${pluCounter}">
            <div class="plu-row">
                <div class="form-group">
                    <label>Establecimiento</label>
                    <select class="form-control plu-establecimiento" data-plu-id="${pluCounter}">
                        <option value="">Seleccionar...</option>
                        ${establecimientosCache.map(e =>
        `<option value="${e.id}" ${e.id === plu.establecimiento_id ? 'selected' : ''}>
                                ${e.nombre_normalizado || 'Est. ' + e.id}
                            </option>`
    ).join('')}
                    </select>
                </div>
                <div class="form-group">
                    <label>Código PLU</label>
                    <input type="text" class="form-control plu-codigo"
                           data-plu-id="${pluCounter}"
                           value="${plu.codigo_plu || ''}"
                           placeholder="Ej: 967509">
                </div>
                <div class="form-group">
                    <label>Precio Unitario</label>
                    <input type="number" class="form-control plu-precio"
                           data-plu-id="${pluCounter}"
                           value="${plu.precio_unitario || ''}"
                           placeholder="Ej: 5000">
                </div>
                <button type="button" class="btn-remove-plu"
                        onclick="eliminarPLU(${pluCounter})">
                    Eliminar
                </button>
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

    console.log('📦 PLUs recopilados:', plus);
    return plus;
}

// =============================================================
// Detectar duplicados
// =============================================================
async function detectarDuplicados() {
    console.log('🔍 Detectando duplicados...');
    try {
        const apiBase = getApiBase();
        // Sin slash final
        const urlDuplicados = `${apiBase}/api/productos/duplicados?umbral_similitud=0.8&limite=50`;

        const response = await fetch(urlDuplicados);
        if (!response.ok) {
            throw new Error('Error al detectar duplicados');
        }

        const data = await response.json();

        if (data.duplicados && data.duplicados.length > 0) {
            mostrarDuplicadosSimple(data.duplicados);
        } else {
            alert('✅ No se encontraron productos duplicados');
        }
    } catch (error) {
        console.error('❌ Error:', error);
        alert('Error detectando duplicados. Verifica la consola para más detalles.');
    }
}

// =============================================================
// Mostrar duplicados (sin Bootstrap)
// =============================================================
function mostrarDuplicadosSimple(duplicados) {
    const container = document.getElementById('duplicados-container');
    if (!container) {
        console.error('❌ No se encontró el contenedor de duplicados');
        return;
    }

    let html = '<h3>🔍 Posibles Duplicados Encontrados:</h3>';

    duplicados.forEach(dup => {
        html += `
            <div class="duplicado-item">
                <div class="duplicado-header">
                    <strong>${dup.nombre1}</strong> (ID: ${dup.id1})
                </div>
                <div style="text-align: center; padding: 10px 0;">↔️</div>
                <div class="duplicado-header">
                    <strong>${dup.nombre2}</strong> (ID: ${dup.id2})
                </div>
                <div style="margin-top: 10px; color: #666;">
                    Similitud: ${(dup.similitud * 100).toFixed(1)}%
                </div>
            </div>
        `;
    });

    container.innerHTML = html;
    console.log(`✅ Mostrando ${duplicados.length} duplicados`);
}

// =============================================================
// Cargar duplicados (función alternativa)
// =============================================================
async function cargarDuplicados() {
    console.log('📋 Cargando duplicados...');

    try {
        const apiBase = getApiBase();
        const urlDuplicados = `${apiBase}/api/productos/duplicados`;

        const response = await fetch(urlDuplicados);
        if (!response.ok) {
            throw new Error(`Error ${response.status}`);
        }

        const data = await response.json();
        console.log('✅ Duplicados cargados:', data);

        if (data.duplicados && data.duplicados.length > 0) {
            mostrarDuplicadosSimple(data.duplicados);
        } else {
            const container = document.getElementById('duplicados-container');
            if (container) {
                container.innerHTML = '<p>No se encontraron duplicados.</p>';
            }
        }

    } catch (error) {
        console.error('❌ Error:', error);
        const container = document.getElementById('duplicados-container');
        if (container) {
            container.innerHTML = `<p style="color: #dc2626;">Error cargando duplicados: ${error.message}</p>`;
        }
    }
}

// =============================================================
// Inicialización
// =============================================================
document.addEventListener('DOMContentLoaded', () => {
    cargarEstablecimientos();
    console.log('✅ Sistema de PLUs inicializado');
});

// =============================================================
// Exportar funciones globales
// =============================================================
window.agregarPLU = agregarPLU;
window.eliminarPLU = eliminarPLU;
window.cargarPLUsProducto = cargarPLUsProducto;
window.detectarDuplicados = detectarDuplicados;
window.recopilarPLUs = recopilarPLUs;
window.cargarDuplicados = cargarDuplicados;
