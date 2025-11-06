// productos.js - Gestión de productos (v2.3 - Buscador funcional)

console.log("🚀 Inicializando Gestión de Productos v2.3 con buscador");

// =============================================================
// Variables globales
// =============================================================
let paginaActual = 1;
let limite = 50;
let totalPaginas = 1;
let productosCache = [];
let coloresCache = null;
let timeoutBusqueda = null; // Para búsqueda en tiempo real

// =============================================================
// 🌐 Base API - IMPORTANTE: Sin slash final en los endpoints
// =============================================================
function getApiBase() {
    return 'https://lecfac-backend-production.up.railway.app';
}

// =============================================================
// Cargar productos (AHORA CON BÚSQUEDA)
// =============================================================
async function cargarProductos(pagina = 1) {
    try {
        const apiBase = getApiBase();

        // Obtener valores de búsqueda y filtro
        const busqueda = document.getElementById("busqueda")?.value || "";
        const filtro = document.getElementById("filtro")?.value || "todos";

        // Construir URL con parámetros
        let url = `${apiBase}/api/productos?pagina=${pagina}&limite=${limite}`;

        // Agregar parámetro de búsqueda si existe
        if (busqueda.trim()) {
            url += `&busqueda=${encodeURIComponent(busqueda.trim())}`;
        }

        // Agregar filtros según el valor seleccionado
        if (filtro === "sin_ean") {
            url += `&con_ean=false`;
        } else if (filtro === "sin_marca") {
            url += `&marca=`;  // Esto buscará productos donde marca es null o vacío
        } else if (filtro === "sin_categoria") {
            url += `&categoria=`;  // Esto buscará productos donde categoría es null o vacío
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

        productosCache = data.productos || [];
        totalPaginas = data.total_paginas || 1;
        paginaActual = pagina;

        console.log(`✅ ${productosCache.length} productos recibidos`);
        if (productosCache.length > 0) {
            console.log("🔍 Primer producto:", productosCache[0]);
        }

        // Mostrar mensaje especial si no hay resultados
        if (productosCache.length === 0 && busqueda) {
            mostrarSinResultados(busqueda);
        } else {
            mostrarProductos(productosCache);
        }

        actualizarPaginacion();
        actualizarEstadisticas(data);

    } catch (error) {
        console.error("❌ Error cargando productos:", error);
        mostrarError(error);
    }
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
        // Cancelar búsqueda anterior si existe
        if (timeoutBusqueda) {
            clearTimeout(timeoutBusqueda);
        }

        // Mostrar indicador de búsqueda
        mostrarBuscando();

        // Ejecutar búsqueda después de 500ms sin escribir
        timeoutBusqueda = setTimeout(() => {
            console.log('🔍 Búsqueda en tiempo real:', e.target.value);
            cargarProductos(1); // Siempre volver a página 1 al buscar
        }, 500);
    });

    // También permitir búsqueda con Enter
    inputBusqueda.addEventListener('keypress', function (e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            clearTimeout(timeoutBusqueda);
            console.log('🔍 Búsqueda con Enter:', e.target.value);
            cargarProductos(1);
        }
    });

    console.log('✅ Buscador en tiempo real configurado');
}

// =============================================================
// Mostrar indicador de búsqueda
// =============================================================
function mostrarBuscando() {
    const tbody = document.getElementById("productos-body");
    if (tbody && tbody.children.length === 1 && tbody.children[0].children.length === 1) {
        // Solo mostrar si actualmente hay un mensaje de carga/error
        tbody.innerHTML = `
            <tr>
                <td colspan="11" style="text-align: center; padding: 40px;">
                    <div class="loading"></div>
                    <p style="margin-top: 10px;">Buscando productos...</p>
                </td>
            </tr>
        `;
    }
}

// =============================================================
// Mostrar mensaje sin resultados
// =============================================================
function mostrarSinResultados(busqueda) {
    const tbody = document.getElementById("productos-body");
    if (tbody) {
        tbody.innerHTML = `
            <tr>
                <td colspan="11" style="text-align: center; padding: 40px;">
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

// =============================================================
// Mostrar error
// =============================================================
function mostrarError(error) {
    const tbody = document.getElementById("productos-body");
    if (tbody) {
        tbody.innerHTML = `
            <tr>
                <td colspan="11" style="text-align: center; padding: 40px; color: #dc2626;">
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
// Limpiar filtros
// =============================================================
function limpiarFiltros() {
    document.getElementById("busqueda").value = "";
    document.getElementById("filtro").value = "todos";

    // Cancelar cualquier búsqueda pendiente
    if (timeoutBusqueda) {
        clearTimeout(timeoutBusqueda);
    }

    cargarProductos(1);
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
    // Configurar búsqueda en tiempo real PRIMERO
    configurarBuscadorTiempoReal();

    // Luego cargar productos
    await cargarProductos(1);

    console.log("✅ Sistema inicializado correctamente con buscador funcional");
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
