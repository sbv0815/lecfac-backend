// productos.js
// Gestión de productos (v2.1)

console.log("🚀 Inicializando Gestión de Productos v2.1");

// =============================================================
// 🌐 Función auxiliar global para evitar errores HTTP/HTTPS
// =============================================================
function getApiBase() {
    return window.location.origin.replace('http://', 'https://');
}

// =============================================================
// Variables globales
// =============================================================
let paginaActual = 1;
let limite = 50;
let totalPaginas = 1;
let productosCache = [];
let coloresCache = null;

// =============================================================
// Cargar colores desde cache o API
// =============================================================
async function cargarColores() {
    if (coloresCache) {
        console.log("✅ Usando colores desde cache");
        return coloresCache;
    }

    try {
        const apiBase = getApiBase();
        const response = await fetch(`${apiBase}/api/colores`);
        if (response.ok) {
            coloresCache = await response.json();
            console.log("🎨 Colores cargados:", coloresCache.length);
        }
    } catch (error) {
        console.error("❌ Error cargando colores:", error);
    }
    return coloresCache;
}

// =============================================================
// Cargar productos con paginación
// =============================================================
async function cargarProductos(pagina = 1) {
    try {
        const apiBase = getApiBase();
        console.log(`📦 Cargando productos - Página ${pagina}`);
        const response = await fetch(`${apiBase}/api/productos?pagina=${pagina}&limite=${limite}`);
        console.log("🌐 URL:", `${apiBase}/api/productos?pagina=${pagina}&limite=${limite}`);

        if (!response.ok) throw new Error("Error al cargar productos");

        const data = await response.json();
        console.log("📊 Respuesta API:", data);

        productosCache = data.productos || [];
        totalPaginas = data.total_paginas || 1;

        mostrarProductos(productosCache);
        actualizarPaginacion();
        console.log("✅ Productos cargados:", productosCache.length);
    } catch (error) {
        console.error("❌ Error cargando productos:", error);
    }
}

// =============================================================
// Mostrar productos en tabla
// =============================================================
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
            </tr>
        `;
        tbody.insertAdjacentHTML("beforeend", row);
    });
}

// =============================================================
// Actualizar botones de paginación
// =============================================================
function actualizarPaginacion() {
    const paginacion = document.getElementById("paginacion");
    if (!paginacion) return;

    let html = `
        <button class="btn btn-secondary" ${paginaActual <= 1 ? "disabled" : ""} onclick="cargarPagina(${paginaActual - 1})">Anterior</button>
        <span> Página ${paginaActual} de ${totalPaginas} </span>
        <button class="btn btn-secondary" ${paginaActual >= totalPaginas ? "disabled" : ""} onclick="cargarPagina(${paginaActual + 1})">Siguiente</button>
    `;

    paginacion.innerHTML = html;
}

function cargarPagina(num) {
    paginaActual = num;
    cargarProductos(num);
}

// =============================================================
// Editar producto (abre modal)
// =============================================================
async function editarProducto(id) {
    console.log("✏️ Editando producto:", id);
    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/productos/${id}`);
        if (!response.ok) throw new Error("No se pudo obtener el producto");

        const producto = await response.json();
        console.log("🧾 Producto:", producto);

        // Llenar formulario de edición
        document.getElementById("productoId").value = producto.id;
        document.getElementById("codigoEan").value = producto.codigo_ean || "";
        document.getElementById("nombreNormalizado").value = producto.nombre_normalizado || "";
        document.getElementById("nombreComercial").value = producto.nombre_comercial || "";
        document.getElementById("marca").value = producto.marca || "";
        document.getElementById("categoria").value = producto.categoria || "";
        document.getElementById("subcategoria").value = producto.subcategoria || "";
        document.getElementById("presentacion").value = producto.presentacion || "";

        // Cargar PLUs del producto
        if (typeof cargarPLUsProducto === "function") {
            await cargarPLUsProducto(id);
        }

        // Mostrar modal sin Bootstrap (usa tu CSS personalizado)
        document.getElementById("modal-editar").classList.add("active");

        console.log("✅ Modal abierto correctamente");
    } catch (error) {
        console.error("❌ Error al editar producto:", error);
    }
}

// =============================================================
// Guardar producto desde el modal
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
        const response = await fetch(`${apiBase}/api/productos/${productoId}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(datos),
        });

        if (!response.ok) throw new Error("Error al guardar producto");

        console.log("✅ Producto actualizado correctamente");
        alert("✅ Producto actualizado correctamente");

        // Cerrar modal (personalizado)
        document.getElementById("modal-editar").classList.remove("active");

        // Recargar productos
        cargarProductos(paginaActual);
    } catch (error) {
        console.error("❌ Error guardando producto:", error);
        alert("Error al guardar producto: " + error.message);
    }
}

// =============================================================
// Cerrar modal manualmente
// =============================================================
function cerrarModal() {
    document.getElementById("modal-editar").classList.remove("active");
}

// =============================================================
// Filtros de búsqueda
// =============================================================
async function filtrarProductos() {
    const termino = document.getElementById("buscarProducto").value.trim();
    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/productos?buscar=${encodeURIComponent(termino)}`);
        if (!response.ok) throw new Error("Error al filtrar productos");

        const data = await response.json();
        mostrarProductos(data.productos);
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
// Exportar funciones globales
// =============================================================
window.cargarProductos = cargarProductos;
window.editarProducto = editarProducto;
window.guardarProducto = guardarProducto;
window.cerrarModal = cerrarModal;
window.filtrarProductos = filtrarProductos;
