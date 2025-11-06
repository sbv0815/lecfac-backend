// productos.js - Gestión de productos (v2.1)

console.log("🚀 Inicializando Gestión de Productos v2.1");

// =============================================================
// 🌐 Base API
// =============================================================
function getApiBase() {
    let origin = window.location.origin;
    if (origin.startsWith('http://')) origin = origin.replace('http://', 'https://');
    if (!origin.startsWith('https://')) origin = 'https://' + window.location.host;
    return origin.replace(/\/+$/, '');
}

// Reintento seguro ante 307→http
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
let paginaActual = 1;
let limite = 50;
let totalPaginas = 1;
let productosCache = [];
let coloresCache = null;

// =============================================================
// Colores (manejar 404 con fallback)
// =============================================================
async function cargarColores() {
    if (coloresCache) return coloresCache;
    try {
        const apiBase = getApiBase();
        const resp = await fetchSeguro(`${apiBase}/api/colores/`);
        if (resp.ok) {
            coloresCache = await resp.json();
            console.log("🎨 Colores cargados:", coloresCache.length);
        } else {
            console.warn("⚠️ /api/colores/ no disponible (status:", resp.status, ") usando fallback.");
            coloresCache = [];
        }
    } catch (e) {
        console.warn("⚠️ Error cargando colores, usando fallback:", e);
        coloresCache = [];
    }
    return coloresCache;
}

// =============================================================
// Productos (paginación)
// =============================================================
async function cargarProductos(pagina = 1) {
    try {
        const apiBase = getApiBase();
        const url = `${apiBase}/api/productos/?pagina=${pagina}&limite=${limite}`; // “/” final
        console.log(`📦 Cargando productos - Página ${pagina}`);
        console.log("🌐 URL:", url);

        const response = await fetchSeguro(url);
        if (!response.ok) throw new Error("Error al cargar productos");

        const data = await response.json();
        productosCache = data.productos || [];
        totalPaginas = data.total_paginas || 1;

        mostrarProductos(productosCache);
        actualizarPaginacion();
        console.log("✅ Productos cargados:", productosCache.length);
    } catch (error) {
        console.error("❌ Error cargando productos:", error);
    }
}

function mostrarProductos(productos) {
    const tbody = document.getElementById("tablaProductosBody");
    if (!tbody) return;
    tbody.innerHTML = "";
    productos.forEach((p) => {
        const row = `
            <tr>
                <td>${p.id}</td>
                <td>${p.codigo_ean || ""}</td>
                <td>${p.nombre_normalizado || ""}</td>
                <td>${p.marca || ""}</td>
                <td>${p.categoria || ""}</td>
                <td>${p.subcategoria || ""}</td>
                <td>
                    <button class="btn btn-sm btn-primary" onclick="editarProducto(${p.id})">
                        <i class="bi bi-pencil"></i>
                    </button>
                </td>
            </tr>`;
        tbody.insertAdjacentHTML("beforeend", row);
    });
}

function actualizarPaginacion() {
    const paginacion = document.getElementById("paginacion");
    if (!paginacion) return;
    paginacion.innerHTML = `
        <button class="btn btn-secondary" ${paginaActual <= 1 ? "disabled" : ""} onclick="cargarPagina(${paginaActual - 1})">Anterior</button>
        <span> Página ${paginaActual} de ${totalPaginas} </span>
        <button class="btn btn-secondary" ${paginaActual >= totalPaginas ? "disabled" : ""} onclick="cargarPagina(${paginaActual + 1})">Siguiente</button>
    `;
}
function cargarPagina(num) { paginaActual = num; cargarProductos(num); }

// =============================================================
// Editar producto (abre modal)
// =============================================================
async function editarProducto(id) {
    console.log("✏️ Editando producto:", id);
    const apiBase = getApiBase();

    try {
        const resp = await fetchSeguro(`${apiBase}/api/productos/${id}/`); // “/” final
        if (!resp.ok) throw new Error("No se pudo obtener el producto");

        const producto = await resp.json();
        document.getElementById("productoId").value = producto.id;
        document.getElementById("codigoEan").value = producto.codigo_ean || "";
        document.getElementById("nombreNormalizado").value = producto.nombre_normalizado || "";
        document.getElementById("nombreComercial").value = producto.nombre_comercial || "";
        document.getElementById("marca").value = producto.marca || "";
        document.getElementById("categoria").value = producto.categoria || "";
        document.getElementById("subcategoria").value = producto.subcategoria || "";
        document.getElementById("presentacion").value = producto.presentacion || "";

        // Cargar PLUs
        if (typeof cargarPLUsProducto === "function") {
            await cargarPLUsProducto(id);
        }

        // Mostrar modal (si usas CSS propio)
        const modalEl = document.getElementById("modal-editar");
        if (modalEl) modalEl.classList.add("active");
        console.log("✅ Modal abierto correctamente");
    } catch (error) {
        console.error("❌ Error al editar producto:", error);
    }
}

// =============================================================
// Guardar producto (solo campos básicos desde este archivo)
// =============================================================
async function guardarProducto() {
    const productoId = document.getElementById("productoId").value;
    const apiBase = getApiBase();

    const datos = {
        codigo_ean: document.getElementById("codigoEan").value || null,
        nombre_normalizado: document.getElementById("nombreNormalizado").value,
        nombre_comercial: document.getElementById("nombreComercial").value || null,
        marca: document.getElementById("marca").value || null,
        categoria: document.getElementById("categoria").value || null,
        subcategoria: document.getElementById("subcategoria").value || null,
        presentacion: document.getElementById("presentacion").value || null,
    };

    try {
        const resp = await fetchSeguro(`${apiBase}/api/productos/${productoId}/`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(datos),
        });
        if (!resp.ok) throw new Error("Error al guardar producto");

        alert("✅ Producto actualizado correctamente");

        // Cerrar modal
        const modalEl = document.getElementById("modal-editar");
        const modal = modalEl ? bootstrap.Modal.getInstance(modalEl) : null;
        if (modal) modal.hide();
        else modalEl?.classList.remove("active");

        cargarProductos(paginaActual);
    } catch (error) {
        console.error("❌ Error guardando producto:", error);
        alert("Error al guardar producto: " + error.message);
    }
}

function cerrarModal() {
    document.getElementById("modal-editar")?.classList.remove("active");
}

// =============================================================
// Filtros de búsqueda
// =============================================================
async function filtrarProductos() {
    const termino = document.getElementById("buscarProducto").value.trim();
    const apiBase = getApiBase();

    try {
        const resp = await fetchSeguro(`${apiBase}/api/productos/?buscar=${encodeURIComponent(termino)}`);
        if (!resp.ok) throw new Error("Error al filtrar productos");
        const data = await resp.json();
        mostrarProductos(data.productos || []);
    } catch (error) {
        console.error("❌ Error filtrando productos:", error);
    }
}

// =============================================================
// Inicialización
// =============================================================
document.addEventListener("DOMContentLoaded", async function () {
    await cargarColores();
    await cargarProductos(paginaActual);
    console.log("✅ Sistema inicializado correctamente");
});

// =============================================================
// Exportar
// =============================================================
window.cargarProductos = cargarProductos;
window.editarProducto = editarProducto;
window.guardarProducto = guardarProducto;
window.cerrarModal = cerrarModal;
window.filtrarProductos = filtrarProductos;
