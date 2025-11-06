// productos.js - Gestión de productos (v2.2 - Fix URLs)

console.log("🚀 Inicializando Gestión de Productos v2.2");

// =============================================================
// Variables globales
// =============================================================
let paginaActual = 1;
let limite = 50;
let totalPaginas = 1;
let productosCache = [];
let coloresCache = null;

// =============================================================
// 🌐 Base API - IMPORTANTE: Sin slash final en los endpoints
// =============================================================
function getApiBase() {
    let origin = window.location.origin;
    if (origin.startsWith('http://')) {
        origin = origin.replace('http://', 'https://');
    }
    return origin;
}

// =============================================================
// Cargar productos (sin slash final en la URL)
// =============================================================
async function cargarProductos(pagina = 1) {
    try {
        const apiBase = getApiBase();
        // IMPORTANTE: Sin slash final aquí
        const url = `${apiBase}/api/productos?pagina=${pagina}&limite=${limite}`;
        console.log(`📦 Cargando productos - Página ${pagina}`);
        console.log("🌐 URL:", url);

        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Error ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        console.log("📊 Respuesta API:", data);

        productosCache = data.productos || [];
        totalPaginas = data.total_paginas || 1;
        paginaActual = pagina;

        console.log(`✅ ${productosCache.length} productos recibidos`);
        if (productosCache.length > 0) {
            console.log("🔍 Primer producto:", productosCache[0]);
        }

        mostrarProductos(productosCache);
        actualizarPaginacion();
        actualizarEstadisticas(data);

    } catch (error) {
        console.error("❌ Error cargando productos:", error);
        // Mostrar mensaje de error en la tabla
        const tbody = document.getElementById("productos-body");
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="11" style="text-align: center; padding: 40px; color: #dc2626;">
                        <p>❌ Error cargando productos</p>
                        <p style="font-size: 14px; color: #666;">${error.message}</p>
                        <button class="btn-primary" onclick="cargarProductos(${pagina})" style="margin-top: 10px;">
                            Reintentar
                        </button>
                    </td>
                </tr>
            `;
        }
    }
}

// =============================================================
// Mostrar productos en la tabla
// =============================================================
function mostrarProductos(productos) {
    const tbody = document.getElementById("productos-body");
    if (!tbody) return;

    tbody.innerHTML = "";

    if (!productos || productos.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="11" style="text-align: center; padding: 40px;">
                    No hay productos para mostrar
                </td>
            </tr>
        `;
        return;
    }

    productos.forEach((p) => {
        // Renderizar PLUs si existen
        let plusHTML = '';
        if (p.codigo_plu) {
            // Los PLUs vienen como string: "967509 (OLÍMPICA), 845123 (Éxito)"
            const plusArray = p.codigo_plu.split(', ');
            plusHTML = plusArray.map(plu => {
                const [codigo, est] = plu.split(' (');
                const establecimiento = est ? est.replace(')', '') : '';
                return `<span class="badge" style="background: #1e40af; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; margin: 2px;">${codigo} ${establecimiento}</span>`;
            }).join(' ');
        }

        // Renderizar precio
        const precioHTML = p.precio_promedio_global ?
            `$${p.precio_promedio_global.toLocaleString('es-CO')}` :
            '<span style="color: #999;">-</span>';

        // Renderizar estado
        const estadoBadges = [];
        if (!p.codigo_ean) estadoBadges.push('<span class="badge" style="background: #d97706; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; margin: 2px;">Sin EAN</span>');
        if (!p.marca) estadoBadges.push('<span class="badge" style="background: #d97706; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; margin: 2px;">Sin Marca</span>');
        if (!p.categoria) estadoBadges.push('<span class="badge" style="background: #d97706; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; margin: 2px;">Sin Categoría</span>');
        const estadoHTML = estadoBadges.length > 0 ? estadoBadges.join(' ') : '<span class="badge" style="background: #059669; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; margin: 2px;">Completo</span>';

        const row = `
            <tr>
                <td class="checkbox-cell">
                    <input type="checkbox" value="${p.id}" onchange="toggleProductSelection(${p.id})">
                </td>
                <td>${p.id}</td>
                <td>${p.codigo_ean || '-'}</td>
                <td>${plusHTML || '-'}</td>
                <td>${p.nombre_normalizado || p.nombre_comercial || '-'}</td>
                <td>${p.marca || '-'}</td>
                <td>${p.categoria || '-'}</td>
                <td>${precioHTML}</td>
                <td>${p.total_reportes || 0}</td>
                <td>${estadoHTML}</td>
                <td>
                    <button class="btn-small btn-primary" onclick="editarProducto(${p.id})" title="Editar">
                        ✏️
                    </button>
                    <button class="btn-small btn-secondary" onclick="verHistorial(${p.id})" title="Historial">
                        📜
                    </button>
                </td>
            </tr>
        `;
        tbody.insertAdjacentHTML("beforeend", row);
    });
}

// =============================================================
// Actualizar estadísticas
// =============================================================
function actualizarEstadisticas(data) {
    // Actualizar cards de estadísticas
    const stats = document.querySelectorAll('.stat-value');
    if (stats.length >= 4 && data) {
        stats[0].textContent = data.total || '0';

        // Calcular productos con EAN
        const conEan = productosCache.filter(p => p.codigo_ean).length;
        stats[1].textContent = conEan;

        // Calcular productos sin marca
        const sinMarca = productosCache.filter(p => !p.marca).length;
        stats[2].textContent = sinMarca;

        // Por ahora, duplicados en 0 (se actualiza con el botón detectar)
        stats[3].textContent = '0';
    }
}

// =============================================================
// Actualizar paginación
// =============================================================
function actualizarPaginacion() {
    const paginacion = document.getElementById("pagination");
    if (!paginacion) return;

    let html = '';

    // Botón anterior
    html += `<button class="btn-secondary" ${paginaActual <= 1 ? "disabled" : ""}
             onclick="cargarPagina(${paginaActual - 1})">← Anterior</button>`;

    // Información de página
    html += `<span style="padding: 0 20px;">Página ${paginaActual} de ${totalPaginas}</span>`;

    // Botón siguiente
    html += `<button class="btn-secondary" ${paginaActual >= totalPaginas ? "disabled" : ""}
             onclick="cargarPagina(${paginaActual + 1})">Siguiente →</button>`;

    paginacion.innerHTML = html;
}

function cargarPagina(num) {
    if (num < 1 || num > totalPaginas) return;
    cargarProductos(num);
}

// =============================================================
// Editar producto (compatible con el modal del HTML)
// =============================================================
async function editarProducto(id) {
    console.log("✏️ Editando producto:", id);
    const apiBase = getApiBase();

    try {
        // Sin slash final
        const response = await fetch(`${apiBase}/api/productos/${id}`);
        if (!response.ok) throw new Error("Producto no encontrado");

        const producto = await response.json();

        // Llenar el formulario con los IDs correctos del HTML
        document.getElementById("edit-id").value = producto.id;
        document.getElementById("edit-ean").value = producto.codigo_ean || "";
        document.getElementById("edit-nombre-norm").value = producto.nombre_normalizado || "";
        document.getElementById("edit-nombre-com").value = producto.nombre_comercial || "";
        document.getElementById("edit-marca").value = producto.marca || "";
        document.getElementById("edit-categoria").value = producto.categoria || "";
        document.getElementById("edit-subcategoria").value = producto.subcategoria || "";
        document.getElementById("edit-presentacion").value = producto.presentacion || "";

        // Estadísticas
        document.getElementById("edit-veces-comprado").value = producto.veces_comprado || "0";
        document.getElementById("edit-precio-promedio").value = producto.precio_promedio_global ?
            `$${producto.precio_promedio_global.toLocaleString('es-CO')}` : "Sin datos";
        document.getElementById("edit-num-establecimientos").value = producto.num_establecimientos || "0";

        // Cargar PLUs si la función existe
        if (typeof cargarPLUsProducto === "function") {
            await cargarPLUsProducto(id);
        }

        // Mostrar modal
        document.getElementById("modal-editar").classList.add("active");

    } catch (error) {
        console.error("❌ Error:", error);
        alert("Error al cargar producto: " + error.message);
    }
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
        nombre_normalizado: document.getElementById("edit-nombre-norm").value,
        nombre_comercial: document.getElementById("edit-nombre-com").value || null,
        marca: document.getElementById("edit-marca").value || null,
        categoria: document.getElementById("edit-categoria").value || null,
        subcategoria: document.getElementById("edit-subcategoria").value || null,
        presentacion: document.getElementById("edit-presentacion").value || null
    };

    try {
        // Sin slash final
        const response = await fetch(`${apiBase}/api/productos/${productoId}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(datos)
        });

        if (!response.ok) throw new Error("Error al guardar producto");

        // Guardar PLUs si la función existe
        if (typeof recopilarPLUs === "function") {
            const plus = recopilarPLUs();
            const responsePLUs = await fetch(`${apiBase}/api/productos/${productoId}/plus`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(plus)
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

// =============================================================
// Funciones auxiliares
// =============================================================
function cerrarModal(modalId) {
    document.getElementById(modalId)?.classList.remove("active");
}

function limpiarFiltros() {
    document.getElementById("busqueda").value = "";
    document.getElementById("filtro").value = "todos";
    cargarProductos(1);
}

function switchTab(tabName) {
    // Ocultar todos los tabs
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });

    // Desactivar todos los botones
    document.querySelectorAll('.tab').forEach(btn => {
        btn.classList.remove('active');
    });

    // Activar el tab seleccionado
    document.getElementById(`tab-${tabName}`).classList.add('active');
    event.target.classList.add('active');
}

function toggleSelectAll() {
    const selectAll = document.getElementById('select-all');
    const checkboxes = document.querySelectorAll('#productos-body input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = selectAll.checked);
}

function toggleProductSelection(id) {
    // Actualizar contador de seleccionados
    const selected = document.querySelectorAll('#productos-body input[type="checkbox"]:checked').length;
    document.getElementById('selected-count').textContent = `${selected} seleccionados`;

    // Habilitar/deshabilitar botones
    document.getElementById('btn-fusionar').disabled = selected < 2;
    document.getElementById('btn-deseleccionar').disabled = selected === 0;
}

function deseleccionarTodos() {
    document.querySelectorAll('#productos-body input[type="checkbox"]').forEach(cb => cb.checked = false);
    document.getElementById('select-all').checked = false;
    document.getElementById('selected-count').textContent = '0 seleccionados';
    document.getElementById('btn-fusionar').disabled = true;
    document.getElementById('btn-deseleccionar').disabled = true;
}

function verHistorial(id) {
    // Implementar vista de historial
    console.log("Ver historial de producto:", id);
}

function fusionarSeleccionados() {
    // Implementar fusión de productos
    console.log("Fusionar productos seleccionados");
}

function recargarColores() {
    // Recargar colores de establecimientos
    console.log("Recargando colores...");
}

// =============================================================
// Inicialización
// =============================================================
document.addEventListener("DOMContentLoaded", async function () {
    await cargarProductos(1);
    console.log("✅ Sistema inicializado correctamente");
});

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
