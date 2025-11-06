// funciones_plu_modal.js - Gestión de PLUs (Versión 4.0 HTTPS Fix)
console.log("🏪 Inicializando funciones PLU Modal - v4.0");

// Variable global para almacenar establecimientos
let establecimientosCache = [];

// =============================================================
// Función para obtener la base de la API (SIEMPRE HTTPS)
// =============================================================
function getApiBase() {
    // SIEMPRE usar HTTPS
    return 'https://lecfac-backend-production.up.railway.app';
}

// =============================================================
// Cargar establecimientos una sola vez
// =============================================================
async function cargarEstablecimientosCache() {
    if (establecimientosCache.length > 0) {
        return establecimientosCache; // Ya están cargados
    }

    console.log("📍 Cargando establecimientos desde la API...");

    try {
        const apiBase = getApiBase();
        const url = `${apiBase}/api/establecimientos`;

        console.log("🔗 URL establecimientos:", url);

        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`Error ${response.status}`);
        }

        establecimientosCache = await response.json();
        console.log("✅ Establecimientos cargados:", establecimientosCache);

        return establecimientosCache;

    } catch (error) {
        console.error("❌ Error cargando establecimientos:", error);

        // Fallback con establecimientos conocidos de Colombia
        establecimientosCache = [
            { id: 1, nombre: "Carulla", nombre_normalizado: "CARULLA" },
            { id: 2, nombre: "Éxito", nombre_normalizado: "ÉXITO" },
            { id: 4, nombre: "Jumbo", nombre_normalizado: "JUMBO" },
            { id: 6, nombre: "Cencosud", nombre_normalizado: "CENCOSUD COLOMBIA" },
            { id: 8, nombre: "Farmatodo", nombre_normalizado: "FARMATODO" },
            { id: 9, nombre: "Olímpica", nombre_normalizado: "OLÍMPICA" },
            { id: 10, nombre: "Ara", nombre_normalizado: "JERONIMO MARTINS COLOMBIA SAS" },
            { id: 11, nombre: "Ara", nombre_normalizado: "JERONIMO MARTINS COLOMBIA" },
            { id: 14, nombre: "Ara", nombre_normalizado: "JERONIMO MARTINS" }
        ];
        console.log("⚠️ Usando establecimientos de fallback");
        return establecimientosCache;
    }
}

// =============================================================
// Poblar un select con establecimientos
// =============================================================
function poblarSelectEstablecimientos(selectElement, valorSeleccionado = null) {
    if (!selectElement) return;

    // Limpiar select
    selectElement.innerHTML = '<option value="">Selecciona establecimiento</option>';

    // Agregar establecimientos
    establecimientosCache.forEach(est => {
        const option = document.createElement('option');
        option.value = est.id;
        option.textContent = est.nombre_normalizado || est.nombre || `Establecimiento ${est.id}`;

        // Seleccionar si corresponde
        if (valorSeleccionado && parseInt(est.id) === parseInt(valorSeleccionado)) {
            option.selected = true;
        }

        selectElement.appendChild(option);
    });

    // Log para debugging
    if (valorSeleccionado) {
        console.log(`📌 Select ${selectElement.id} - Valor seleccionado: ${valorSeleccionado} - Texto: ${selectElement.options[selectElement.selectedIndex]?.text || 'No encontrado'}`);
    }
}

// =============================================================
// Agregar un nuevo PLU
// =============================================================
async function agregarPLU() {
    console.log("➕ Agregando nuevo PLU");

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) {
        console.error("❌ No se encontró el contenedor de PLUs");
        return;
    }

    // Asegurar que los establecimientos estén cargados
    await cargarEstablecimientosCache();

    const timestamp = Date.now();
    const nuevoPLU = document.createElement('div');
    nuevoPLU.className = 'plu-item';
    nuevoPLU.setAttribute('data-plu-id', timestamp);

    nuevoPLU.innerHTML = `
        <div class="plu-row">
            <div class="form-group">
                <label>Establecimiento</label>
                <select id="plu-establecimiento-${timestamp}" class="plu-establecimiento-select" required>
                    <option value="">Selecciona establecimiento</option>
                </select>
            </div>
            <div class="form-group">
                <label>Código PLU</label>
                <input type="text"
                       id="plu-codigo-${timestamp}"
                       class="plu-codigo-input"
                       placeholder="Ej: 02000000013657"
                       required>
            </div>
            <div class="form-group">
                <label>Precio Unitario</label>
                <input type="number"
                       id="plu-precio-${timestamp}"
                       class="plu-precio-input"
                       placeholder="5000"
                       min="0"
                       step="1">
            </div>
            <button type="button" class="btn-remove-plu" onclick="eliminarPLU(this)">❌</button>
        </div>
    `;

    contenedor.appendChild(nuevoPLU);

    // Poblar el select con establecimientos
    const selectElement = document.getElementById(`plu-establecimiento-${timestamp}`);
    poblarSelectEstablecimientos(selectElement);

    // Agregar listener para debugging
    selectElement.addEventListener('change', function () {
        console.log(`🔄 Cambio en establecimiento: ${this.value} - ${this.options[this.selectedIndex]?.text}`);
    });
}

// =============================================================
// Cargar PLUs existentes de un producto
// =============================================================
async function cargarPLUsProducto(productoId) {
    console.log(`📋 Cargando PLUs del producto ${productoId}`);

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) {
        console.error("❌ No se encontró el contenedor de PLUs");
        return;
    }

    // Limpiar contenedor
    contenedor.innerHTML = '';

    // Asegurar que los establecimientos estén cargados
    await cargarEstablecimientosCache();

    try {
        const apiBase = getApiBase();
        const response = await fetch(`${apiBase}/api/productos/${productoId}/plus`);

        if (!response.ok) {
            throw new Error("Error cargando PLUs");
        }

        const data = await response.json();
        console.log("✅ PLUs recibidos de la API:", data);

        if (data.plus && data.plus.length > 0) {
            // Cargar cada PLU existente
            for (const plu of data.plus) {
                const timestamp = Date.now() + Math.random() * 1000;
                const pluDiv = document.createElement('div');
                pluDiv.className = 'plu-item';
                pluDiv.setAttribute('data-plu-id', timestamp);

                pluDiv.innerHTML = `
                    <div class="plu-row">
                        <div class="form-group">
                            <label>Establecimiento</label>
                            <select id="plu-establecimiento-${timestamp}" class="plu-establecimiento-select" required>
                                <option value="">Selecciona establecimiento</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label>Código PLU</label>
                            <input type="text"
                                   id="plu-codigo-${timestamp}"
                                   class="plu-codigo-input"
                                   value="${plu.codigo_plu || ''}"
                                   required>
                        </div>
                        <div class="form-group">
                            <label>Precio Unitario</label>
                            <input type="number"
                                   id="plu-precio-${timestamp}"
                                   class="plu-precio-input"
                                   value="${plu.precio_unitario || ''}"
                                   min="0"
                                   step="1">
                        </div>
                        <button type="button" class="btn-remove-plu" onclick="eliminarPLU(this)">❌</button>
                    </div>
                `;

                contenedor.appendChild(pluDiv);

                // Poblar el select y seleccionar el establecimiento correcto
                const selectElement = document.getElementById(`plu-establecimiento-${timestamp}`);
                poblarSelectEstablecimientos(selectElement, plu.establecimiento_id);

                // Log detallado para debugging
                console.log(`📦 PLU cargado:`, {
                    codigo: plu.codigo_plu,
                    establecimiento_id: plu.establecimiento_id,
                    establecimiento_nombre: plu.establecimiento_nombre,
                    precio: plu.precio_unitario
                });

                // Agregar listener para debugging
                selectElement.addEventListener('change', function () {
                    console.log(`🔄 Cambio en establecimiento: ${this.value} - ${this.options[this.selectedIndex]?.text}`);
                });
            }
        } else {
            console.log("ℹ️ No hay PLUs para este producto");
            // Agregar un PLU vacío por defecto
            await agregarPLU();
        }

    } catch (error) {
        console.error("❌ Error cargando PLUs:", error);
        // Agregar un PLU vacío en caso de error
        await agregarPLU();
    }
}

// =============================================================
// Recopilar PLUs del formulario para guardar
// =============================================================
function recopilarPLUs() {
    console.log("📦 Recopilando PLUs del formulario...");

    const plus = [];
    const items = document.querySelectorAll('.plu-item');

    items.forEach((item, index) => {
        // Buscar elementos por clase en lugar de por ID parcial
        const establecimientoSelect = item.querySelector('.plu-establecimiento-select');
        const codigoInput = item.querySelector('.plu-codigo-input');
        const precioInput = item.querySelector('.plu-precio-input');

        if (establecimientoSelect && codigoInput) {
            const establecimientoId = establecimientoSelect.value;
            const codigo = codigoInput.value;
            const precio = precioInput ? parseFloat(precioInput.value) || null : null;

            // Log detallado
            console.log(`📌 PLU ${index + 1}:`, {
                select_id: establecimientoSelect.id,
                establecimiento_id: establecimientoId,
                establecimiento_texto: establecimientoSelect.options[establecimientoSelect.selectedIndex]?.text || 'No seleccionado',
                codigo: codigo,
                precio: precio
            });

            // Solo agregar si hay establecimiento y código
            if (establecimientoId && codigo && codigo.trim()) {
                plus.push({
                    establecimiento_id: parseInt(establecimientoId),
                    codigo_plu: codigo.trim(),
                    precio_unitario: precio
                });
            } else {
                console.warn(`⚠️ PLU ${index + 1} incompleto - No se incluirá`);
            }
        }
    });

    console.log("✅ PLUs finales a guardar:", plus);
    return plus;
}

// =============================================================
// Eliminar un PLU
// =============================================================
function eliminarPLU(button) {
    const pluItem = button.closest('.plu-item');
    if (pluItem) {
        const pluId = pluItem.getAttribute('data-plu-id');
        console.log(`🗑️ Eliminando PLU ${pluId}`);
        pluItem.remove();
    }
}

// =============================================================
// Inicialización al cargar el DOM
// =============================================================
document.addEventListener('DOMContentLoaded', async function () {
    console.log("🚀 Inicializando sistema de PLUs");

    // Precargar establecimientos al iniciar
    await cargarEstablecimientosCache();

    console.log("✅ Sistema de PLUs listo");
});

// =============================================================
// Exportar funciones globales
// =============================================================
window.agregarPLU = agregarPLU;
window.cargarPLUsProducto = cargarPLUsProducto;
window.recopilarPLUs = recopilarPLUs;
window.eliminarPLU = eliminarPLU;

console.log("✅ Funciones PLU Modal exportadas - Versión 4.0 HTTPS");
