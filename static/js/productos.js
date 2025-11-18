// productos.js - Gestión de productos (v2.5 - Con establecimientos por PLU)

console.log("🚀 Inicializando Gestión de Productos v2.5 - Con Establecimientos");

// =============================================================
// Variables globales
// =============================================================
let paginaActual = 1;
let limite = 50;
let totalPaginas = 1;
let productosCache = [];
let coloresCache = null;
let timeoutBusqueda = null;

// =============================================================
// 🌐 Base API
// =============================================================
function getApiBase() {
    return 'https://lecfac-backend-production.up.railway.app';
}

// =============================================================
// Cargar productos (⭐ ACTUALIZADO CON NUEVO ENDPOINT)
// =============================================================
// =============================================================
// 🔧 FIX PARA productos.js - LÍNEA 25-35 APROXIMADAMENTE
// Reemplazar la función cargarProductos() con esta versión corregida
// =============================================================

async function cargarProductos(pagina = 1) {
    try {
        const apiBase = getApiBase();

        // Obtener valores de búsqueda y filtro
        const busqueda = document.getElementById("busqueda")?.value || "";
        const filtro = document.getElementById("filtro")?.value || "todos";

        // ✅ FIX: Aumentar límite a 500 y cambiar orden a DESC (más recientes primero)
        // Los productos más viejos se verán en páginas siguientes
        let url = `${apiBase}/api/v2/productos?limite=500`;

        // Agregar parámetro de búsqueda si existe
        if (busqueda.trim()) {
            url += `&busqueda=${encodeURIComponent(busqueda.trim())}`;
        }

        // Agregar filtros
        if (filtro !== "todos") {
            url += `&filtro=${filtro}`;
        }

        console.log(`📦 Cargando productos - Página ${pagina}`);
        if (busqueda) console.log(`🔍 Búsqueda: "${busqueda}"`);
        if (filtro !== "todos") console.log(`🏷️ Filtro: ${filtro}`);
        console.log("🌐 URL:", url);

        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Error ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        console.log("📊 Respuesta API:", data);

        // ✅ FIX: Guardar TODOS los productos sin límite artificial
        productosCache = data.productos || [];

        // ✅ FIX: Calcular paginación REAL del lado del cliente
        const productosPorPagina = limite; // 50 por defecto
        const totalProductos = productosCache.length;
        totalPaginas = Math.ceil(totalProductos / productosPorPagina) || 1;

        // ✅ FIX: Calcular índices para la paginación del lado del cliente
        const inicio = (pagina - 1) * productosPorPagina;
        const fin = inicio + productosPorPagina;
        const productosPagina = productosCache.slice(inicio, fin);

        paginaActual = pagina;

        console.log(`✅ ${totalProductos} productos totales, mostrando ${productosPagina.length} en página ${pagina}`);

        // Mostrar mensaje especial si no hay resultados
        if (productosCache.length === 0 && busqueda) {
            mostrarSinResultados(busqueda);
        } else {
            // ✅ FIX: Mostrar solo los productos de la página actual
            mostrarProductos(productosPagina);
        }

        actualizarPaginacion();
        actualizarEstadisticas(data);

    } catch (error) {
        console.error("❌ Error cargando productos:", error);
        mostrarError(error);
    }
}

// =============================================================
// ✅ TAMBIÉN AGREGAR ESTA FUNCIÓN AUXILIAR AL FINAL DEL ARCHIVO
// =============================================================

// Función auxiliar para mantener sincronización al cambiar página
function cargarPagina(num) {
    if (num < 1 || num > totalPaginas) return;

    // ✅ FIX: No volver a llamar la API, solo cambiar la vista de los productos en caché
    paginaActual = num;

    const productosPorPagina = limite;
    const inicio = (num - 1) * productosPorPagina;
    const fin = inicio + productosPorPagina;
    const productosPagina = productosCache.slice(inicio, fin);

    mostrarProductos(productosPagina);
    actualizarPaginacion();

    // Scroll al inicio de la tabla
    window.scrollTo({ top: 0, behavior: 'smooth' });
}


// =============================================================
// Configurar búsqueda en tiempo real
// =============================================================
function configurarBuscadorTiempoReal() {
    const inputBusqueda = document.getElementById('busqueda');

    if (!inputBusqueda) {
        console.error('No se encontró el input de búsqueda');
        return;
    }

    // Búsqueda en tiempo real con debounce
    inputBusqueda.addEventListener('input', function (e) {
        if (timeoutBusqueda) {
            clearTimeout(timeoutBusqueda);
        }

        mostrarBuscando();

        timeoutBusqueda = setTimeout(() => {
            console.log('🔍 Búsqueda en tiempo real:', e.target.value);
            cargarProductos(1);
        }, 500);
    });

    // También permitir búsqueda con Enter
    inputBusqueda.addEventListener('keypress', function (e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            if (timeoutBusqueda) clearTimeout(timeoutBusqueda);
            console.log('🔍 Búsqueda con Enter:', e.target.value);
            cargarProductos(1);
        }
    });

    console.log('✅ Buscador en tiempo real configurado');
}

// =============================================================
// Mostrar estados
// =============================================================
function mostrarBuscando() {
    const tbody = document.getElementById("productos-body");
    if (tbody && tbody.children.length === 1) {
        tbody.innerHTML = `
            <tr>
                <td colspan="12" style="text-align: center; padding: 40px;">
                    <div class="loading"></div>
                    <p style="margin-top: 10px;">Buscando productos...</p>
                </td>
            </tr>
        `;
    }
}

function mostrarSinResultados(busqueda) {
    const tbody = document.getElementById("productos-body");
    if (tbody) {
        tbody.innerHTML = `
            <tr>
                <td colspan="12" style="text-align: center; padding: 40px;">
                    <p style="font-size: 18px; margin-bottom: 10px;">
                        No se encontraron productos para: <strong>"${busqueda}"</strong>
                    </p>
                    <p style="color: #666; margin-bottom: 20px;">
                        Intenta con otros términos de búsqueda
                    </p>
                    <button class="btn-secondary" onclick="limpiarFiltros()">
                        🔄 Limpiar búsqueda
                    </button>
                </td>
            </tr>
        `;
    }
}

function mostrarError(error) {
    const tbody = document.getElementById("productos-body");
    if (tbody) {
        tbody.innerHTML = `
            <tr>
                <td colspan="12" style="text-align: center; padding: 40px; color: #dc2626;">
                    <p>❌ Error cargando productos</p>
                    <p style="font-size: 14px; color: #666;">${error.message}</p>
                    <button class="btn-primary" onclick="cargarProductos(${paginaActual})" style="margin-top: 10px;">
                        Reintentar
                    </button>
                </td>
            </tr>
        `;
    }
}

// =============================================================
// ⭐ MOSTRAR PRODUCTOS (ACTUALIZADO CON ESTABLECIMIENTOS)
// =============================================================
// =============================================================
// ⭐ MOSTRAR PRODUCTOS (VERSIÓN FINAL CORREGIDA)
// =============================================================
function mostrarProductos(productos) {
    const tbody = document.getElementById("productos-body");
    if (!tbody) return;

    tbody.innerHTML = "";

    if (!productos || productos.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="12" style="text-align: center; padding: 40px;">
                    No hay productos para mostrar
                </td>
            </tr>
        `;
        return;
    }

    productos.forEach((p) => {
        // Parsear plus si es string JSON
        let plusArray = p.plus;
        if (typeof p.plus === 'string') {
            try {
                plusArray = JSON.parse(p.plus);
            } catch (e) {
                plusArray = [];
            }
        }
        if (!Array.isArray(plusArray)) {
            plusArray = [];
        }

        // Mostrar PLUs como badges
        let plusHTML = '<span style="color: #999;">Sin PLUs</span>';
        let establecimientosHTML = '<span style="color: #999;">-</span>';

        if (plusArray && plusArray.length > 0) {
            plusHTML = plusArray.map(plu =>
                `<span class="badge badge-info">${plu.codigo || 'N/A'}</span>`
            ).join(' ');

            establecimientosHTML = plusArray.map(plu =>
                `<span class="badge badge-success">🏪 ${plu.establecimiento || 'Desconocido'}</span>`
            ).join(' ');
        }

        // Precio - usar el precio del primer PLU si existe, o 0
        let precioHTML = '<span style="color: #999;">-</span>';
        if (plusArray && plusArray.length > 0 && plusArray[0].precio > 0) {
            precioHTML = `$${parseInt(plusArray[0].precio).toLocaleString('es-CO')}`;
        }

        // Marca
        const marcaHTML = p.marca || '<span style="color: #999;">Sin marca</span>';

        // Categoría (ya viene como texto desde la API)
        const categoriaHTML = p.categoria || '<span style="color: #999;">Sin categoría</span>';

        // Estado badges
        const estadoBadges = [];
        if (!p.codigo_ean) estadoBadges.push('<span class="badge badge-warning">Sin EAN</span>');
        if (!p.marca) estadoBadges.push('<span class="badge badge-warning">Sin Marca</span>');
        if (p.categoria === 'Sin categoría' || !p.categoria) estadoBadges.push('<span class="badge badge-warning">Sin Categoría</span>');
        const estadoHTML = estadoBadges.length > 0 ?
            estadoBadges.join(' ') :
            '<span class="badge badge-success">Completo</span>';

        const row = `
            <tr>
                <td class="checkbox-cell">
                    <input type="checkbox" value="${p.id}" onchange="toggleProductSelection(${p.id})">
                </td>
                <td>${p.id}</td>
                <td>${p.codigo_ean || '<span style="color: #999;">-</span>'}</td>
                <td>${plusHTML}</td>
                <td>${establecimientosHTML}</td>
                <td><strong>${p.nombre || '-'}</strong></td>
                <td>${marcaHTML}</td>
                <td>${categoriaHTML}</td>
                <td>${precioHTML}</td>
                <td>${p.num_establecimientos || 0}</td>
                <td>${estadoHTML}</td>
                <td>
                    <button class="btn-small btn-primary" onclick="editarProducto(${p.id})" title="Editar">
                        ✏️
                    </button>
                    <button class="btn-small btn-danger" onclick="eliminarProducto(${p.id}, '${(p.nombre || '').replace(/'/g, "\\'")}');" title="Eliminar">
                        🗑️
                    </button>
                </td>
            </tr>
        `;
        tbody.insertAdjacentHTML("beforeend", row);
    });
}


// =============================================================
// Actualizar estadísticas y paginación
// =============================================================
function actualizarEstadisticas(data) {
    const stats = document.querySelectorAll('.stat-value');
    if (stats.length >= 4 && data) {
        stats[0].textContent = data.total || '0';

        const conEan = productosCache.filter(p => p.codigo_ean).length;
        stats[1].textContent = conEan;

        const sinMarca = productosCache.filter(p => !p.marca).length;
        stats[2].textContent = sinMarca;

        stats[3].textContent = '0';
    }
}

function actualizarPaginacion() {
    const paginacion = document.getElementById("pagination");
    if (!paginacion) return;

    let html = '';

    html += `<button class="btn-secondary" ${paginaActual <= 1 ? "disabled" : ""}
             onclick="cargarPagina(${paginaActual - 1})">← Anterior</button>`;

    html += `<span style="padding: 0 20px;">Página ${paginaActual} de ${totalPaginas}</span>`;

    html += `<button class="btn-secondary" ${paginaActual >= totalPaginas ? "disabled" : ""}
             onclick="cargarPagina(${paginaActual + 1})">Siguiente →</button>`;

    paginacion.innerHTML = html;
}

function cargarPagina(num) {
    if (num < 1 || num > totalPaginas) return;
    cargarProductos(num);
}

// =============================================================
// Limpiar filtros
// =============================================================
function limpiarFiltros() {
    document.getElementById("busqueda").value = "";
    document.getElementById("filtro").value = "todos";

    if (timeoutBusqueda) {
        clearTimeout(timeoutBusqueda);
    }

    cargarProductos(1);
}

// =============================================================
// EDITAR PRODUCTO (con habilitación de campos)
// =============================================================
async function editarProducto(id) {
    console.log("✏️ Editando producto:", id);
    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${id}`);
        if (!response.ok) throw new Error("Producto no encontrado");

        const producto = await response.json();

        // Llenar el formulario
        document.getElementById("edit-id").value = producto.id;
        document.getElementById("edit-ean").value = producto.codigo_ean || "";
        document.getElementById("edit-nombre-norm").value = producto.nombre_normalizado || "";
        document.getElementById("edit-nombre-com").value = producto.nombre_comercial || "";
        document.getElementById("edit-marca").value = producto.marca || "";
        document.getElementById("edit-categoria").value = producto.categoria || "";
        document.getElementById("edit-subcategoria").value = producto.subcategoria || "";
        document.getElementById("edit-presentacion").value = producto.presentacion || "";

        // Estadísticas (solo lectura)
        document.getElementById("edit-veces-comprado").value = producto.veces_comprado || "0";
        document.getElementById("edit-precio-promedio").value = producto.precio_promedio ?
            `$${producto.precio_promedio.toLocaleString('es-CO')}` : "Sin datos";
        document.getElementById("edit-num-establecimientos").value = producto.num_establecimientos || "0";

        // IMPORTANTE: Habilitar los campos editables
        habilitarCamposEdicion();

        // Cargar PLUs
        if (typeof cargarPLUsProducto === "function") {
            await cargarPLUsProducto(id);
        }

        // Mostrar modal
        document.getElementById("modal-editar").classList.add("active");

        // Agregar helper para EAN
        agregarHelperEAN();

    } catch (error) {
        console.error("❌ Error:", error);
        alert("Error al cargar producto: " + error.message);
    }
}

// =============================================================
// Habilitar campos de edición
// =============================================================
function habilitarCamposEdicion() {
    // Habilitar campo EAN
    const eanInput = document.getElementById('edit-ean');
    if (eanInput) {
        eanInput.removeAttribute('readonly');
        eanInput.removeAttribute('disabled');
        eanInput.style.background = 'white';
        eanInput.style.cursor = 'text';
        eanInput.setAttribute('maxlength', '14'); // Para Ara con 0 inicial
    }

    // Habilitar campo nombre
    const nombreInput = document.getElementById('edit-nombre-norm');
    if (nombreInput) {
        nombreInput.removeAttribute('readonly');
        nombreInput.removeAttribute('disabled');
        nombreInput.style.background = 'white';
        nombreInput.style.cursor = 'text';
    }

    // Habilitar otros campos
    const campos = ['edit-nombre-com', 'edit-marca', 'edit-categoria', 'edit-subcategoria', 'edit-presentacion'];

    campos.forEach(id => {
        const campo = document.getElementById(id);
        if (campo) {
            campo.removeAttribute('readonly');
            campo.removeAttribute('disabled');
            campo.style.background = 'white';
        }
    });
}

// =============================================================
// Helper para EAN
// =============================================================
function agregarHelperEAN() {
    const eanInput = document.getElementById('edit-ean');
    if (!eanInput) return;

    // Eliminar helper anterior si existe
    const helperAnterior = document.getElementById('ean-helper');
    if (helperAnterior) helperAnterior.remove();

    // Crear helper
    const helper = document.createElement('div');
    helper.id = 'ean-helper';
    helper.style.cssText = 'margin-top: 5px; font-size: 12px; color: #666;';
    helper.innerHTML = `
        <span>Longitud actual: <strong id="ean-length">${eanInput.value.length}</strong> dígitos</span>
        <span style="margin-left: 10px;">
            <a href="#" onclick="completarEAN(); return false;" style="color: #2563eb;">
                Completar a 13 dígitos
            </a>
        </span>
    `;

    eanInput.parentNode.insertBefore(helper, eanInput.nextSibling);

    // Actualizar contador en tiempo real
    eanInput.addEventListener('input', function () {
        const lengthSpan = document.getElementById('ean-length');
        if (lengthSpan) {
            lengthSpan.textContent = this.value.length;
            lengthSpan.style.color = this.value.length === 13 ? '#059669' : '#666';
        }
    });
}

// =============================================================
// Completar EAN
// =============================================================
function completarEAN() {
    const eanInput = document.getElementById('edit-ean');
    if (!eanInput) return;

    let ean = eanInput.value.replace(/\D/g, ''); // Solo números

    if (ean.length === 12) {
        const digito = calcularDigitoControl(ean);
        eanInput.value = ean + digito;

        const lengthSpan = document.getElementById('ean-length');
        if (lengthSpan) {
            lengthSpan.textContent = '13';
            lengthSpan.style.color = '#059669';
        }

        console.log(`✅ EAN completado: ${eanInput.value}`);
    } else if (ean.length < 12) {
        alert(`El EAN tiene ${ean.length} dígitos. Necesita tener 12 para calcular el dígito de control.`);
    } else {
        alert('El EAN ya tiene 13 o más dígitos.');
    }
}

function calcularDigitoControl(ean12) {
    let suma = 0;
    for (let i = 0; i < 12; i++) {
        const digito = parseInt(ean12[i]);
        suma += digito * (i % 2 === 0 ? 1 : 3);
    }
    const modulo = suma % 10;
    return modulo === 0 ? 0 : 10 - modulo;
}

// =============================================================
// Guardar edición
// =============================================================
async function guardarEdicion(event) {
    if (event) event.preventDefault();

    const productoId = document.getElementById("edit-id").value;
    const apiBase = getApiBase();

    const datos = {
        codigo_ean: document.getElementById("edit-ean").value || null,
        nombre_consolidado: document.getElementById("edit-nombre-norm").value,
        nombre_comercial: document.getElementById("edit-nombre-com").value || null,
        marca: document.getElementById("edit-marca").value || null,
        categoria: document.getElementById("edit-categoria").value || null,
        subcategoria: document.getElementById("edit-subcategoria").value || null,
        presentacion: document.getElementById("edit-presentacion").value || null
    };

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${productoId}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(datos)
        });

        if (!response.ok) throw new Error("Error al guardar producto");

        // Guardar PLUs si la función existe
        if (typeof recopilarPLUs === "function") {
            const plus = recopilarPLUs();
            const datosConPLUs = { ...datos, plus };

            const responsePLUs = await fetch(`${apiBase}/api/v2/productos/${productoId}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(datosConPLUs)
            });

            if (!responsePLUs.ok) {
                console.warn("Advertencia: Error actualizando PLUs");
            }
        }

        alert("✅ Producto actualizado correctamente");
        cerrarModal("modal-editar");
        cargarProductos(paginaActual);

    } catch (error) {
        console.error("❌ Error guardando:", error);
        alert("Error al guardar: " + error.message);
    }
}
async function marcarRevisado(productoId) {
    if (!confirm('¿Marcar este producto como REVISADO y CORRECTO?\n\nEsto significa que el nombre, marca y categoría son correctos.')) {
        return;
    }

    try {
        const response = await fetch(`/api/productos/${productoId}/marcar-revisado`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        if (response.ok) {
            alert('✅ Producto marcado como revisado');
            cargarProductos(); // Recargar tabla
        } else {
            const error = await response.json();
            alert('Error: ' + error.detail);
        }
    } catch (error) {
        alert('Error de conexión: ' + error.message);
    }
}

// =============================================================
// Funciones auxiliares
// =============================================================
function cerrarModal(modalId) {
    document.getElementById(modalId)?.classList.remove("active");
}

function switchTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });

    document.querySelectorAll('.tab').forEach(btn => {
        btn.classList.remove('active');
    });

    document.getElementById(`tab-${tabName}`).classList.add('active');
    event.target.classList.add('active');
}

function toggleSelectAll() {
    const selectAll = document.getElementById('select-all');
    const checkboxes = document.querySelectorAll('#productos-body input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = selectAll.checked);
}

function toggleProductSelection(id) {
    const selected = document.querySelectorAll('#productos-body input[type="checkbox"]:checked').length;
    document.getElementById('selected-count').textContent = `${selected} seleccionados`;

    document.getElementById('btn-fusionar').disabled = selected < 2;
    document.getElementById('btn-deseleccionar').disabled = selected === 0;

    // Habilitar corrección masiva si hay al menos 1 seleccionado
    const btnCorreccion = document.getElementById('btn-correccion-masiva');
    if (btnCorreccion) {
        btnCorreccion.disabled = selected === 0;
    }
}

function deseleccionarTodos() {
    document.querySelectorAll('#productos-body input[type="checkbox"]').forEach(cb => cb.checked = false);
    document.getElementById('select-all').checked = false;
    document.getElementById('selected-count').textContent = '0 seleccionados';
    document.getElementById('btn-fusionar').disabled = true;
    document.getElementById('btn-deseleccionar').disabled = true;
}

function verHistorial(id) {
    console.log("Ver historial de producto:", id);
}

function fusionarSeleccionados() {
    console.log("Fusionar productos seleccionados");
}

function recargarColores() {
    console.log("Recargando colores...");
}

// =============================================================
// ELIMINAR PRODUCTO
// =============================================================
async function eliminarProducto(id, nombre) {
    if (!confirm(`⚠️ ¿Estás seguro de eliminar "${nombre}"?\n\nEsta acción NO se puede deshacer.`)) {
        return;
    }

    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${id}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Error al eliminar producto');
        }

        mostrarAlerta('✅ Producto eliminado correctamente', 'success');
        cargarProductos(paginaActual); // Recargar tabla

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarAlerta(`❌ Error: ${error.message}`, 'error');
    }
}

// =============================================================
// FUNCIÓN MOSTRAR ALERTAS (si no existe ya)
// =============================================================
function mostrarAlerta(mensaje, tipo = 'info') {
    // Buscar contenedor de alertas o crearlo
    let alertContainer = document.getElementById('alert-container');
    if (!alertContainer) {
        alertContainer = document.createElement('div');
        alertContainer.id = 'alert-container';
        alertContainer.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 9999;
            max-width: 400px;
        `;
        document.body.appendChild(alertContainer);
    }

    // Crear alerta
    const alert = document.createElement('div');
    const alertClasses = {
        'success': 'alert-success',
        'error': 'alert-error',
        'warning': 'alert-warning',
        'info': 'alert-info'
    };

    alert.className = `alert ${alertClasses[tipo] || 'alert-info'}`;
    alert.innerHTML = mensaje;
    alert.style.cssText = `
        margin-bottom: 10px;
        padding: 15px;
        border-radius: 6px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        animation: slideIn 0.3s ease-out;
        background: ${tipo === 'success' ? '#d1fae5' : tipo === 'error' ? '#fee2e2' : '#e0e7ff'};
        color: ${tipo === 'success' ? '#059669' : tipo === 'error' ? '#dc2626' : '#4338ca'};
        border-left: 4px solid ${tipo === 'success' ? '#059669' : tipo === 'error' ? '#dc2626' : '#4338ca'};
    `;

    alertContainer.appendChild(alert);

    // Auto-remover después de 5 segundos
    setTimeout(() => {
        alert.style.animation = 'slideOut 0.3s ease-out';
        setTimeout(() => alert.remove(), 300);
    }, 5000);
}

// Agregar estilos de animación si no existen
if (!document.getElementById('alert-animations')) {
    const style = document.createElement('style');
    style.id = 'alert-animations';
    style.textContent = `
        @keyframes slideIn {
            from {
                transform: translateX(400px);
                opacity: 0;
            }
            to {
                transform: translateX(0);
                opacity: 1;
            }
        }
        @keyframes slideOut {
            from {
                transform: translateX(0);
                opacity: 1;
            }
            to {
                transform: translateX(400px);
                opacity: 0;
            }
        }
    `;
    document.head.appendChild(style);
}

// =============================================================
// AGREGAR PLU NUEVO (si funciones_plu_modal.js no existe)
// =============================================================
function agregarPLU() {
    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) {
        console.error('No se encontró contenedorPLUs');
        return;
    }

    const mensaje = contenedor.querySelector('p');
    if (mensaje) {
        mensaje.remove();
    }

    const pluDiv = document.createElement('div');
    pluDiv.className = 'plu-item';
    pluDiv.innerHTML = `
        <div class="plu-row">
            <div class="form-group">
                <label>Establecimiento</label>
                <input type="text"
                       class="plu-establecimiento"
                       placeholder="Ej: EXITO, JUMBO, CARULLA">
            </div>
            <div class="form-group">
                <label>Código PLU</label>
                <input type="text"
                       class="plu-codigo"
                       placeholder="Ej: 1234, 5678">
            </div>
            <div class="form-group">
                <label>Última Vez Visto</label>
                <input type="text"
                       class="plu-fecha"
                       value="Nuevo"
                       readonly
                       style="background: #f0f0f0;">
            </div>
            <button type="button" class="btn-remove-plu" onclick="this.parentElement.parentElement.remove()">
                🗑️
            </button>
        </div>
    `;

    contenedor.appendChild(pluDiv);
}


// =============================================================
// CARGAR PLUs DEL PRODUCTO
// =============================================================
async function cargarPLUsProducto(productoId) {
    const apiBase = getApiBase();
    const contenedor = document.getElementById('contenedorPLUs');

    if (!contenedor) {
        console.warn('No se encontró contenedorPLUs');
        return;
    }

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${productoId}`);
        if (!response.ok) throw new Error('Error cargando PLUs');

        const data = await response.json();
        contenedor.innerHTML = '';

        if (!data.plus || data.plus.length === 0) {
            contenedor.innerHTML = '<p style="color: #666; padding: 10px;">No hay PLUs registrados</p>';
            return;
        }

        data.plus.forEach((plu, index) => {
            const pluDiv = document.createElement('div');
            pluDiv.className = 'plu-item';
            pluDiv.innerHTML = `
                <div class="plu-row">
                    <div class="form-group">
                        <label>Establecimiento</label>
                        <input type="text"
                               class="plu-establecimiento"
                               value="${plu.nombre_establecimiento || ''}"
                               placeholder="Ej: EXITO, JUMBO">
                    </div>
                    <div class="form-group">
                        <label>Código PLU</label>
                        <input type="text"
                               class="plu-codigo"
                               value="${plu.codigo_plu || ''}"
                               placeholder="Ej: 1234">
                    </div>
                    <div class="form-group">
                        <label>Última Vez Visto</label>
                        <input type="text"
                               class="plu-fecha"
                               value="${plu.ultima_vez_visto || 'N/A'}"
                               readonly
                               style="background: #f0f0f0;">
                    </div>
                    <button type="button" class="btn-remove-plu" onclick="this.parentElement.parentElement.remove()">
                        🗑️
                    </button>
                </div>
            `;
            contenedor.appendChild(pluDiv);
        });

    } catch (error) {
        console.error('Error cargando PLUs:', error);
        contenedor.innerHTML = '<p style="color: #dc2626; padding: 10px;">Error cargando PLUs</p>';
    }
}

// =============================================================
// RECOPILAR PLUs DEL FORMULARIO
// =============================================================
function recopilarPLUs() {
    const plusItems = document.querySelectorAll('.plu-item');
    const plus = [];

    plusItems.forEach(item => {
        const establecimiento = item.querySelector('.plu-establecimiento')?.value.trim();
        const codigo = item.querySelector('.plu-codigo')?.value.trim();

        if (establecimiento && codigo) {
            plus.push({
                nombre_establecimiento: establecimiento.toUpperCase(),
                codigo_plu: codigo,
                ultima_vez_visto: new Date().toISOString().split('T')[0]
            });
        }
    });

    return plus;
}

// =============================================================
// EXPORTAR FUNCIONES ADICIONALES
// =============================================================
window.eliminarProducto = eliminarProducto;
window.mostrarAlerta = mostrarAlerta;
window.agregarPLU = agregarPLU;
window.cargarPLUsProducto = cargarPLUsProducto;
window.recopilarPLUs = recopilarPLUs;

console.log('✅ Funciones de edición y eliminación cargadas');

// =============================================================
// Inicialización
// =============================================================
document.addEventListener("DOMContentLoaded", async function () {
    // Configurar búsqueda en tiempo real
    configurarBuscadorTiempoReal();

    // Permitir paste en el modal
    const modal = document.getElementById('modal-editar');
    if (modal) {
        modal.addEventListener('paste', function (e) {
            e.stopPropagation();
        }, true);
    }

    // Cargar productos
    await cargarProductos(1);

    console.log("✅ Sistema inicializado correctamente con establecimientos por PLU");
});
// =============================================================
// 🔧 FUNCIONES DE ADMINISTRACIÓN Y CORRECCIÓN
// =============================================================

let anomaliasCache = [];
let marcasSugeridas = [];
let categoriasSugeridas = [];

// =============================================================
// CARGAR ANOMALÍAS (Tab Calidad de Datos)
// =============================================================
async function cargarAnomalias() {
    const apiBase = getApiBase();
    const container = document.getElementById('calidad-stats');
    const recomendaciones = document.getElementById('recomendaciones');

    if (!container) return;

    container.innerHTML = '<div class="loading"></div> Analizando datos...';

    try {
        const response = await fetch(`${apiBase}/api/admin/anomalias`);
        if (!response.ok) throw new Error('Error cargando anomalías');

        const data = await response.json();
        anomaliasCache = data.productos || [];
        const stats = data.estadisticas;

        // Mostrar estadísticas
        container.innerHTML = `
            <div class="stat-card">
                <div class="stat-value">${stats.total}</div>
                <div class="stat-label">Total Productos</div>
            </div>
            <div class="stat-card" style="background: linear-gradient(135deg, #d1fae5, #a7f3d0);">
                <div class="stat-value" style="color: #059669;">${stats.completos}</div>
                <div class="stat-label">✅ Completos</div>
            </div>
            <div class="stat-card warning">
                <div class="stat-value">${stats.problematicos}</div>
                <div class="stat-label">⚠️ Con Problemas</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #2563eb;">${stats.porcentaje_calidad}%</div>
                <div class="stat-label">Calidad General</div>
            </div>
            <div class="stat-card warning">
                <div class="stat-value">${stats.sin_marca}</div>
                <div class="stat-label">Sin Marca</div>
            </div>
            <div class="stat-card warning">
                <div class="stat-value">${stats.sin_categoria}</div>
                <div class="stat-label">Sin Categoría</div>
            </div>
            <div class="stat-card danger">
                <div class="stat-value">${stats.nombres_cortos}</div>
                <div class="stat-label">Nombres Truncados</div>
            </div>
            <div class="stat-card warning">
                <div class="stat-value">${stats.sin_ean}</div>
                <div class="stat-label">Sin EAN</div>
            </div>
        `;

        // Mostrar recomendaciones y lista de problemas
        if (recomendaciones) {
            let html = '';

            // Barra de progreso
            html += `
                <div style="margin-bottom: 30px;">
                    <h3>📊 Progreso de Calidad</h3>
                    <div style="background: #e5e7eb; border-radius: 10px; height: 30px; overflow: hidden; margin-top: 10px;">
                        <div style="background: linear-gradient(90deg, #059669, #10b981); height: 100%; width: ${stats.porcentaje_calidad}%;
                                    display: flex; align-items: center; justify-content: center; color: white; font-weight: bold;">
                            ${stats.porcentaje_calidad}%
                        </div>
                    </div>
                </div>
            `;

            // Lista de productos problemáticos
            const problematicos = anomaliasCache.filter(p => p.tipo_problema !== 'ok').slice(0, 50);

            if (problematicos.length > 0) {
                html += `
                    <h3>🔧 Productos que Requieren Atención (${problematicos.length})</h3>
                    <div style="margin-top: 15px; max-height: 600px; overflow-y: auto;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <thead>
                                <tr style="background: #f3f4f6;">
                                    <th style="padding: 10px; text-align: left;">ID</th>
                                    <th style="padding: 10px; text-align: left;">Nombre</th>
                                    <th style="padding: 10px; text-align: left;">Problema</th>
                                    <th style="padding: 10px; text-align: left;">Acción</th>
                                </tr>
                            </thead>
                            <tbody>
                `;

                problematicos.forEach(p => {
                    const problemaBadge = {
                        'nombre_corto': '<span class="badge badge-error">Nombre muy corto</span>',
                        'nombre_truncado': '<span class="badge badge-warning">Nombre truncado</span>',
                        'sin_marca': '<span class="badge badge-warning">Sin marca</span>',
                        'sin_categoria': '<span class="badge badge-warning">Sin categoría</span>',
                        'sin_ean': '<span class="badge badge-info">Sin EAN</span>',
                        'ean_invalido': '<span class="badge badge-error">EAN inválido</span>',
                        'sin_precio': '<span class="badge badge-warning">Sin precio</span>'
                    }[p.tipo_problema] || '<span class="badge">Desconocido</span>';

                    html += `
                        <tr style="border-bottom: 1px solid #e5e7eb;">
                            <td style="padding: 10px;">${p.id}</td>
                            <td style="padding: 10px;">
                                <strong>${p.nombre || 'Sin nombre'}</strong>
                                <br><small style="color: #666;">EAN: ${p.codigo_ean || 'N/A'} | Marca: ${p.marca || 'N/A'}</small>
                            </td>
                            <td style="padding: 10px;">${problemaBadge}</td>
                            <td style="padding: 10px;">
                                <button class="btn-small btn-primary" onclick="editarProductoRapido(${p.id})">
                                    ✏️ Corregir
                                </button>
                            </td>
                        </tr>
                    `;
                });

                html += `
                            </tbody>
                        </table>
                    </div>
                `;
            } else {
                html += `
                    <div style="text-align: center; padding: 40px; color: #059669;">
                        <h3>🎉 ¡Excelente!</h3>
                        <p>Todos los productos están completos</p>
                    </div>
                `;
            }

            recomendaciones.innerHTML = html;
        }

        console.log(`✅ ${anomaliasCache.length} productos analizados`);

    } catch (error) {
        console.error('❌ Error:', error);
        container.innerHTML = `<div class="alert alert-error">Error: ${error.message}</div>`;
    }
}

// =============================================================
// EDICIÓN RÁPIDA INLINE
// =============================================================
async function editarProductoRapido(id) {
    // Usa la función existente de editar
    await editarProducto(id);
}

// =============================================================
// CORRECCIÓN MASIVA
// =============================================================
async function aplicarCorreccionMasiva() {
    const checkboxes = document.querySelectorAll('#productos-body input[type="checkbox"]:checked');
    const ids = Array.from(checkboxes).map(cb => parseInt(cb.value));

    if (ids.length === 0) {
        alert('Selecciona al menos un producto');
        return;
    }

    const marca = prompt('Marca para todos los seleccionados (dejar vacío para no cambiar):');
    const categoria = prompt('Categoría para todos los seleccionados (dejar vacío para no cambiar):');

    if (!marca && !categoria) {
        alert('Debes especificar al menos marca o categoría');
        return;
    }

    const datos = { ids };
    if (marca) datos.marca = marca.toUpperCase();
    if (categoria) datos.categoria = categoria;

    try {
        const apiBase = getApiBase();
        const response = await fetch(`${apiBase}/api/admin/correccion-masiva`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(datos)
        });

        if (!response.ok) throw new Error('Error en corrección masiva');

        const result = await response.json();
        mostrarAlerta(`✅ ${result.productos_actualizados} productos actualizados`, 'success');

        deseleccionarTodos();
        cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarAlerta(`Error: ${error.message}`, 'error');
    }
}

// =============================================================
// CARGAR SUGERENCIAS (autocompletado)
// =============================================================
async function cargarSugerencias() {
    const apiBase = getApiBase();

    try {
        // Cargar marcas
        const resMarcas = await fetch(`${apiBase}/api/admin/sugerencias-marca`);
        if (resMarcas.ok) {
            const data = await resMarcas.json();
            marcasSugeridas = data.marcas || [];
            console.log(`✅ ${marcasSugeridas.length} marcas cargadas`);
        }

        // Cargar categorías
        const resCat = await fetch(`${apiBase}/api/admin/sugerencias-categoria`);
        if (resCat.ok) {
            const data = await resCat.json();
            categoriasSugeridas = data.categorias || [];
            console.log(`✅ ${categoriasSugeridas.length} categorías cargadas`);
        }

    } catch (error) {
        console.warn('No se pudieron cargar sugerencias:', error);
    }
}

// =============================================================
// DETECTAR DUPLICADOS
// =============================================================
async function detectarDuplicados() {
    const container = document.getElementById('duplicados-container');
    if (!container) return;

    container.innerHTML = '<div class="loading"></div> Analizando duplicados...';

    const apiBase = getApiBase();

    try {
        // Por ahora, detectar duplicados localmente
        const response = await fetch(`${apiBase}/api/admin/anomalias`);
        if (!response.ok) throw new Error('Error');

        const data = await response.json();
        const productos = data.productos || [];

        // Agrupar por nombre similar
        const grupos = {};
        productos.forEach(p => {
            const nombreBase = (p.nombre || '').substring(0, 10).toUpperCase();
            if (!grupos[nombreBase]) grupos[nombreBase] = [];
            grupos[nombreBase].push(p);
        });

        // Filtrar grupos con más de 1 producto
        const duplicados = Object.entries(grupos)
            .filter(([key, items]) => items.length > 1)
            .slice(0, 20);

        if (duplicados.length === 0) {
            container.innerHTML = `
                <div style="text-align: center; padding: 40px; color: #059669;">
                    <h3>✅ No se encontraron duplicados obvios</h3>
                    <p>Los productos parecen estar bien diferenciados</p>
                </div>
            `;
            return;
        }

        let html = `<h3>⚠️ Posibles Duplicados Encontrados (${duplicados.length} grupos)</h3>`;

        duplicados.forEach(([nombre, items]) => {
            html += `
                <div class="duplicado-item">
                    <div class="duplicado-header">
                        <strong>Grupo: "${nombre}..."</strong>
                        <span class="badge badge-warning">${items.length} productos</span>
                    </div>
                    <div class="productos-duplicados">
            `;

            items.forEach((p, i) => {
                html += `
                    <div class="producto-dup-card ${i === 0 ? 'principal' : ''}">
                        <strong>ID ${p.id}:</strong> ${p.nombre}<br>
                        <small>EAN: ${p.codigo_ean || 'N/A'} | Marca: ${p.marca || 'N/A'}</small>
                        <button class="btn-small btn-primary" style="float: right;" onclick="editarProducto(${p.id})">
                            ✏️ Editar
                        </button>
                    </div>
                `;
            });

            html += `
                    </div>
                </div>
            `;
        });

        container.innerHTML = html;

    } catch (error) {
        console.error('❌ Error:', error);
        container.innerHTML = `<div class="alert alert-error">Error: ${error.message}</div>`;
    }
}

// =============================================================
// SOBRESCRIBIR switchTab PARA CARGAR DATOS
// =============================================================
const originalSwitchTab = window.switchTab;
window.switchTab = function (tabName) {
    // Llamar función original
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    document.querySelectorAll('.tab').forEach(btn => {
        btn.classList.remove('active');
    });
    document.getElementById(`tab-${tabName}`).classList.add('active');
    event.target.classList.add('active');

    // Cargar datos específicos del tab
    if (tabName === 'calidad') {
        cargarAnomalias();
    } else if (tabName === 'duplicados') {
        // El usuario debe hacer clic en "Analizar"
    }
};

// =============================================================
// INICIALIZACIÓN ADICIONAL
// =============================================================
document.addEventListener("DOMContentLoaded", async function () {
    // Cargar sugerencias para autocompletado
    await cargarSugerencias();

    console.log("✅ Módulo de administración inicializado");
});
// =============================================================
// Mostrar/ocultar indicador de búsqueda
// =============================================================
function mostrarIndicadorBusqueda(mostrar) {
    const indicator = document.getElementById('search-indicator');
    if (indicator) {
        indicator.style.display = mostrar ? 'block' : 'none';
    }
}

// Actualizar la función configurarBuscadorTiempoReal
function configurarBuscadorTiempoReal() {
    const inputBusqueda = document.getElementById('busqueda');
    const selectFiltro = document.getElementById('filtro');

    if (!inputBusqueda) {
        console.error('No se encontró el input de búsqueda');
        return;
    }

    // Búsqueda en tiempo real con debounce
    inputBusqueda.addEventListener('input', function (e) {
        if (timeoutBusqueda) {
            clearTimeout(timeoutBusqueda);
        }

        // Mostrar indicador si hay texto
        if (e.target.value.trim()) {
            mostrarIndicadorBusqueda(true);
        }

        timeoutBusqueda = setTimeout(() => {
            console.log('🔍 Búsqueda en tiempo real:', e.target.value);
            cargarProductos(1);
            mostrarIndicadorBusqueda(false);
        }, 500);
    });

    // También búsqueda con Enter
    inputBusqueda.addEventListener('keypress', function (e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            if (timeoutBusqueda) clearTimeout(timeoutBusqueda);
            console.log('🔍 Búsqueda con Enter:', e.target.value);
            mostrarIndicadorBusqueda(false);
            cargarProductos(1);
        }
    });

    // Cambio en filtro recarga automáticamente
    if (selectFiltro) {
        selectFiltro.addEventListener('change', function () {
            console.log('🏷️ Filtro cambiado:', this.value);
            cargarProductos(1);
        });
    }

    console.log('✅ Buscador en tiempo real configurado');
}

// Exportar
window.mostrarIndicadorBusqueda = mostrarIndicadorBusqueda;
// =============================================================
// EXPORTAR NUEVAS FUNCIONES
// =============================================================
window.cargarAnomalias = cargarAnomalias;
window.editarProductoRapido = editarProductoRapido;
window.aplicarCorreccionMasiva = aplicarCorreccionMasiva;
window.detectarDuplicados = detectarDuplicados;
window.cargarSugerencias = cargarSugerencias;
// Alias para el botón en la pestaña de duplicados
window.cargarDuplicados = detectarDuplicados;

console.log('✅ Funciones de administración cargadas');

// =============================================================
// Exportar funciones
// =============================================================
window.cargarProductos = cargarProductos;
window.editarProducto = editarProducto;
window.guardarEdicion = guardarEdicion;
window.cerrarModal = cerrarModal;
window.limpiarFiltros = limpiarFiltros;
window.cargarPagina = cargarPagina;
window.switchTab = switchTab;
window.toggleSelectAll = toggleSelectAll;
window.toggleProductSelection = toggleProductSelection;
window.deseleccionarTodos = deseleccionarTodos;
window.verHistorial = verHistorial;
window.fusionarSeleccionados = fusionarSeleccionados;
window.recargarColores = recargarColores;
window.habilitarCamposEdicion = habilitarCamposEdicion;
window.agregarHelperEAN = agregarHelperEAN;
window.completarEAN = completarEAN;
window.calcularDigitoControl = calcularDigitoControl;
