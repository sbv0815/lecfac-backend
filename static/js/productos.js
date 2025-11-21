console.log("🚀 Inicializando Gestión de Productos v3.0 - INTEGRADO CON PLUs");

// =============================================================
// Variables globales
// =============================================================
let paginaActual = 1;
let limite = 50;
let totalPaginas = 1;
let productosCache = [];
let coloresCache = null;
let timeoutBusqueda = null;
let establecimientosCache = []; // ✅ NUEVO - Para gestión de PLUs

// =============================================================
// 🌐 Base API
// =============================================================
function getApiBase() {
    return 'https://lecfac-backend-production.up.railway.app';
}

// =============================================================
// ✅ CARGAR ESTABLECIMIENTOS (Integrado desde funciones_plu_modal.js)
// =============================================================
async function cargarEstablecimientosCache() {
    if (establecimientosCache.length > 0) {
        return establecimientosCache;
    }

    console.log("📍 Cargando establecimientos desde la API...");

    try {
        const apiBase = getApiBase();
        const response = await fetch(`${apiBase}/api/establecimientos`);

        if (!response.ok) {
            throw new Error(`Error ${response.status}`);
        }

        establecimientosCache = await response.json();
        console.log("✅ Establecimientos cargados:", establecimientosCache.length);
        return establecimientosCache;

    } catch (error) {
        console.error("❌ Error cargando establecimientos:", error);
        establecimientosCache = [
            { id: 1, nombre_normalizado: "CARULLA" },
            { id: 2, nombre_normalizado: "ÉXITO" },
            { id: 3, nombre_normalizado: "JUMBO" },
            { id: 6, nombre_normalizado: "CENCOSUD COLOMBIA" },
            { id: 9, nombre_normalizado: "OLÍMPICA" }
        ];
        return establecimientosCache;
    }
}

// =============================================================
// 🔍 INDICADOR DE BÚSQUEDA
// =============================================================
function mostrarIndicadorBusqueda(mostrar) {
    const indicator = document.getElementById('search-indicator');
    if (indicator) {
        indicator.style.display = mostrar ? 'block' : 'none';
    }
}

// =============================================================
// Cargar productos
// =============================================================
async function cargarProductos(pagina = 1) {
    try {
        const apiBase = getApiBase();
        const busqueda = document.getElementById("busqueda")?.value || "";
        const filtro = document.getElementById("filtro")?.value || "todos";

        let url = `${apiBase}/api/v2/productos?limite=500`;

        if (busqueda.trim()) {
            url += `&busqueda=${encodeURIComponent(busqueda.trim())}`;
        }

        if (filtro !== "todos") {
            url += `&filtro=${filtro}`;
        }

        console.log(`📦 Cargando productos - Página ${pagina}`);
        console.log("🌐 URL:", url);

        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Error ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        productosCache = data.productos || [];

        const productosPorPagina = limite;
        const totalProductos = productosCache.length;
        totalPaginas = Math.ceil(totalProductos / productosPorPagina) || 1;

        const inicio = (pagina - 1) * productosPorPagina;
        const fin = inicio + productosPorPagina;
        const productosPagina = productosCache.slice(inicio, fin);

        paginaActual = pagina;

        console.log(`✅ ${totalProductos} productos totales, mostrando ${productosPagina.length} en página ${pagina}`);

        if (productosCache.length === 0 && busqueda) {
            mostrarSinResultados(busqueda);
        } else {
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
// Función auxiliar para cambiar página
// =============================================================
function cargarPagina(num) {
    if (num < 1 || num > totalPaginas) return;

    paginaActual = num;

    const productosPorPagina = limite;
    const inicio = (num - 1) * productosPorPagina;
    const fin = inicio + productosPorPagina;
    const productosPagina = productosCache.slice(inicio, fin);

    mostrarProductos(productosPagina);
    actualizarPaginacion();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// =============================================================
// 🔍 CONFIGURAR BÚSQUEDA EN TIEMPO REAL
// =============================================================
function configurarBuscadorTiempoReal() {
    const inputBusqueda = document.getElementById('busqueda');
    const selectFiltro = document.getElementById('filtro');

    if (!inputBusqueda) {
        console.error('❌ No se encontró el input de búsqueda');
        return;
    }

    inputBusqueda.addEventListener('input', function (e) {
        if (timeoutBusqueda) {
            clearTimeout(timeoutBusqueda);
        }

        if (e.target.value.trim()) {
            mostrarIndicadorBusqueda(true);
        }

        timeoutBusqueda = setTimeout(() => {
            cargarProductos(1);
            mostrarIndicadorBusqueda(false);
        }, 500);
    });

    inputBusqueda.addEventListener('keypress', function (e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            if (timeoutBusqueda) clearTimeout(timeoutBusqueda);
            mostrarIndicadorBusqueda(false);
            cargarProductos(1);
        }
    });

    if (selectFiltro) {
        selectFiltro.addEventListener('change', function () {
            cargarProductos(1);
        });
    }

    console.log('✅ Buscador en tiempo real configurado correctamente');
}

// =============================================================
// Mostrar estados
// =============================================================
function mostrarSinResultados(busqueda) {
    const tbody = document.getElementById("productos-body");
    if (tbody) {
        tbody.innerHTML = `
            <tr>
                <td colspan="13" style="text-align: center; padding: 40px;">
                    <p style="font-size: 18px; margin-bottom: 10px;">
                        No se encontraron productos para: <strong>"${busqueda}"</strong>
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
                <td colspan="13" style="text-align: center; padding: 40px; color: #dc2626;">
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
// ⭐ MOSTRAR PRODUCTOS - CORREGIDO
// =============================================================
function mostrarProductos(productos) {
    const tbody = document.getElementById("productos-body");
    if (!tbody) return;

    tbody.innerHTML = "";

    if (!productos || productos.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="13" style="text-align: center; padding: 40px;">
                    No hay productos para mostrar
                </td>
            </tr>
        `;
        return;
    }

    productos.forEach((p) => {
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

        let precioHTML = '<span style="color: #999;">-</span>';
        if (plusArray && plusArray.length > 0 && plusArray[0].precio > 0) {
            precioHTML = `$${parseInt(plusArray[0].precio).toLocaleString('es-CO')}`;
        }

        const marcaHTML = p.marca || '<span style="color: #999;">Sin marca</span>';
        const categoriaHTML = p.categoria || '<span style="color: #999;">Sin categoría</span>';

        let codigoLecfacHTML = '<span style="color: #999;">-</span>';
        if (p.codigo_lecfac) {
            codigoLecfacHTML = `<code style="background: #e0f2fe; color: #0369a1; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 600;">${p.codigo_lecfac}</code>`;
        }

        const estadoBadges = [];
        if (!p.codigo_ean) estadoBadges.push('<span class="badge badge-warning">Sin EAN</span>');
        if (!p.marca) estadoBadges.push('<span class="badge badge-warning">Sin Marca</span>');
        if (p.categoria === 'Sin categoría' || !p.categoria) estadoBadges.push('<span class="badge badge-warning">Sin Categoría</span>');
        const estadoHTML = estadoBadges.length > 0 ?
            estadoBadges.join(' ') :
            '<span class="badge badge-success">Completo</span>';

        // ✅ CORREGIDO: Todos los botones dentro de la celda de acciones
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
                <td>${codigoLecfacHTML}</td>
                <td>${precioHTML}</td>
                <td>${p.num_establecimientos || 0}</td>
                <td>${estadoHTML}</td>
                <td>
                    <button class="btn-small btn-primary" onclick="editarProducto(${p.id})" title="Editar">
                        ✏️
                    </button>
                    <button class="btn-small btn-primary" onclick="verHistorial(${p.id})" title="Ver historial">
                        📊
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

function verHistorial(id) {
    window.open(`/historial_precios.html?id=${id}`, '_blank');
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
// ✅ EDITAR PRODUCTO
// =============================================================
async function editarProducto(id) {
    console.log("✏️ Editando producto:", id);
    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${id}`);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: Producto no encontrado`);
        }

        const producto = await response.json();
        console.log("✅ Producto cargado:", producto);

        // Llenar formulario
        document.getElementById("edit-id").value = producto.id;
        document.getElementById("edit-ean").value = producto.codigo_ean || "";
        document.getElementById("edit-nombre-norm").value = producto.nombre_consolidado || "";
        document.getElementById("edit-nombre-com").value = producto.nombre_comercial || "";
        document.getElementById("edit-marca").value = producto.marca || "";
        document.getElementById("edit-categoria").value = producto.categoria || "";
        document.getElementById("edit-subcategoria").value = producto.subcategoria || "";
        document.getElementById("edit-presentacion").value = producto.presentacion || "";

        // Estadísticas
        document.getElementById("edit-veces-comprado").value = producto.veces_comprado || "0";
        document.getElementById("edit-precio-promedio").value = producto.precio_promedio ?
            `$${producto.precio_promedio.toLocaleString('es-CO')}` : "Sin datos";
        document.getElementById("edit-num-establecimientos").value = producto.num_establecimientos || "0";

        // Habilitar campos
        habilitarCamposEdicion();

        // Cargar PLUs
        await cargarPLUsProducto(id);

        // Mostrar modal
        document.getElementById("modal-editar").classList.add("active");

    } catch (error) {
        console.error("❌ Error:", error);
        alert("Error al cargar producto: " + error.message);
    }
}

// =============================================================
// Habilitar campos de edición
// =============================================================
function habilitarCamposEdicion() {
    console.log('🔓 Habilitando campos de edición...');

    const camposEditables = [
        'edit-ean',
        'edit-nombre-norm',
        'edit-nombre-com',
        'edit-marca',
        'edit-categoria',
        'edit-subcategoria',
        'edit-presentacion'
    ];

    camposEditables.forEach(fieldId => {
        const campo = document.getElementById(fieldId);
        if (campo) {
            campo.removeAttribute('readonly');
            campo.removeAttribute('disabled');
            campo.style.background = 'white';
            campo.style.cursor = 'text';
            campo.style.border = '1px solid #d1d5db';
            console.log(`   ✅ Campo habilitado: ${fieldId}`);
        }
    });

    const eanInput = document.getElementById('edit-ean');
    if (eanInput) {
        eanInput.setAttribute('maxlength', '14');
    }
}

// =============================================================
// ✅ GUARDAR EDICIÓN - VERSIÓN INTEGRADA
// =============================================================
async function guardarEdicion() {
    console.log('💾 Iniciando guardado de edición...');

    const apiBase = getApiBase();
    const productoId = document.getElementById('edit-id').value;
    const nombreConsolidado = document.getElementById('edit-nombre-norm').value;
    const marca = document.getElementById('edit-marca').value;
    const codigoEan = document.getElementById('edit-ean').value;
    const categoria = document.getElementById('edit-categoria').value;

    if (!productoId) {
        mostrarAlerta('❌ Error: No se encontró el ID del producto', 'error');
        return;
    }

    if (!nombreConsolidado.trim()) {
        mostrarAlerta('❌ El nombre del producto no puede estar vacío', 'error');
        return;
    }

    try {
        // 1️⃣ GUARDAR PRODUCTO
        const datosProducto = {
            nombre_consolidado: nombreConsolidado.trim(),
            marca: marca.trim() || null,
            codigo_ean: codigoEan.trim() || null
        };

        // NO enviar categoría si es texto libre sin soporte backend
        // if (categoria && categoria.trim() && categoria !== 'Sin categoría') {
        //     datosProducto.categoria = categoria.trim();
        // }

        console.log('📦 Guardando producto:', datosProducto);

        const responseProducto = await fetch(`${apiBase}/api/v2/productos/${productoId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(datosProducto)
        });

        if (!responseProducto.ok) {
            const errorData = await responseProducto.json().catch(() => ({ error: 'Error desconocido' }));
            console.error('❌ Error del servidor:', errorData);
            throw new Error(errorData.detail || errorData.error || `HTTP ${responseProducto.status}`);
        }

        const resultadoProducto = await responseProducto.json();
        console.log('✅ Producto actualizado:', resultadoProducto);

        // Mostrar advertencia si hay EAN duplicado
        if (resultadoProducto.advertencia) {
            mostrarAlerta(resultadoProducto.advertencia, 'warning');
        }

        // 2️⃣ GUARDAR PLUs
        console.log('🔍 Recopilando PLUs...');
        const plusData = recopilarPLUsParaGuardar();

        console.log('📋 PLUs recopilados:', plusData);

        if (plusData.plus.length > 0) {
            console.log('💾 Enviando PLUs al backend...');
            console.log('📤 Payload:', JSON.stringify(plusData, null, 2));

            const responsePlus = await fetch(`${apiBase}/api/v2/productos/${productoId}/plus`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(plusData)
            });

            if (responsePlus.ok) {
                const resultadoPlus = await responsePlus.json();
                console.log('✅ PLUs actualizados:', resultadoPlus);

                const detalles = [];
                if (resultadoPlus.actualizados > 0) detalles.push(`${resultadoPlus.actualizados} actualizado(s)`);
                if (resultadoPlus.creados > 0) detalles.push(`${resultadoPlus.creados} creado(s)`);
                if (resultadoPlus.eliminados > 0) detalles.push(`${resultadoPlus.eliminados} eliminado(s)`);

                mostrarAlerta(
                    detalles.length > 0
                        ? `✅ Producto y PLUs guardados: ${detalles.join(', ')}`
                        : '✅ Producto actualizado correctamente',
                    'success'
                );
            } else {
                const errorPlus = await responsePlus.json().catch(() => ({ error: 'Error en PLUs' }));
                console.error('❌ Error guardando PLUs:', errorPlus);
                mostrarAlerta('⚠️ Producto guardado pero hubo error en PLUs: ' + (errorPlus.detail || errorPlus.error), 'warning');
            }
        } else {
            console.log('ℹ️ No hay PLUs para guardar');
            mostrarAlerta('✅ Producto actualizado correctamente', 'success');
        }

        cerrarModal('modal-editar');
        await cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error guardando:', error);
        mostrarAlerta(`❌ Error: ${error.message}`, 'error');
    }
}

// =============================================================
// ✅ CARGAR PLUs DEL PRODUCTO - VERSIÓN INTEGRADA
// =============================================================
async function cargarPLUsProducto(productoId) {
    console.log(`📋 Cargando PLUs del producto ${productoId}`);

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) {
        console.warn('⚠️ No se encontró contenedorPLUs');
        return;
    }

    // Asegurar que los establecimientos estén cargados
    await cargarEstablecimientosCache();

    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${productoId}/plus`);
        if (!response.ok) throw new Error('Error cargando PLUs');

        const data = await response.json();
        console.log('✅ PLUs recibidos:', data);

        contenedor.innerHTML = '';

        if (!data.plus || data.plus.length === 0) {
            console.log('ℹ️ No hay PLUs, agregando uno vacío');
            agregarPLUEditable();
            return;
        }

        data.plus.forEach((plu, index) => {
            const pluId = plu.id || '';

            console.log(`   PLU ${index + 1}:`, {
                id: pluId,
                codigo: plu.codigo_plu,
                establecimiento_id: plu.establecimiento_id,
                precio: plu.precio_unitario
            });

            const pluDiv = document.createElement('div');
            pluDiv.className = 'plu-item';
            pluDiv.dataset.pluId = pluId;

            pluDiv.innerHTML = `
                <div class="plu-row" style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 10px; align-items: end; padding: 10px; border: 1px solid #e5e7eb; border-radius: 6px; margin-bottom: 10px;">
                    <div class="form-group">
                        <label style="display: block; margin-bottom: 5px;">Establecimiento</label>
                        <select class="plu-establecimiento" style="width: 100%; padding: 8px;">
                            <option value="">Seleccionar...</option>
                            ${establecimientosCache.map(e =>
                `<option value="${e.id}" ${e.id == plu.establecimiento_id ? 'selected' : ''}>
                                    ${e.nombre_normalizado}
                                </option>`
            ).join('')}
                        </select>
                    </div>
                    <div class="form-group">
                        <label style="display: block; margin-bottom: 5px;">Código PLU</label>
                        <input type="text"
                               class="plu-codigo"
                               value="${plu.codigo_plu || ''}"
                               placeholder="Ej: 1234"
                               style="width: 100%; padding: 8px;">
                    </div>
                    <div class="form-group">
                        <label style="display: block; margin-bottom: 5px;">Precio</label>
                        <input type="number"
                               class="plu-precio"
                               value="${plu.precio_unitario || 0}"
                               placeholder="0"
                               min="0"
                               step="1"
                               style="width: 100%; padding: 8px;">
                    </div>
                    <button type="button"
                            class="btn-remove-plu"
                            onclick="this.closest('.plu-item').remove();"
                            style="padding: 8px 12px; background: #dc2626; color: white; border: none; border-radius: 4px; cursor: pointer;">
                        🗑️
                    </button>
                </div>
            `;

            contenedor.appendChild(pluDiv);
        });

        console.log(`✅ ${data.plus.length} PLUs cargados`);

    } catch (error) {
        console.error('❌ Error cargando PLUs:', error);
        contenedor.innerHTML = '<p style="color: #dc2626; padding: 10px;">Error cargando PLUs</p>';
    }
}

// =============================================================
// ✅ AGREGAR PLU EDITABLE
// =============================================================
async function agregarPLUEditable() {
    console.log('➕ Agregando nuevo PLU');

    await cargarEstablecimientosCache();

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) return;

    const mensaje = contenedor.querySelector('p');
    if (mensaje) mensaje.remove();

    const pluDiv = document.createElement('div');
    pluDiv.className = 'plu-item';
    pluDiv.dataset.pluId = '';

    pluDiv.innerHTML = `
        <div class="plu-row" style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 10px; align-items: end; padding: 10px; border: 1px solid #e5e7eb; border-radius: 6px; margin-bottom: 10px; background: #f0fdf4;">
            <div class="form-group">
                <label style="display: block; margin-bottom: 5px; font-weight: 500;">Establecimiento</label>
                <select class="plu-establecimiento" style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
                    <option value="">Seleccionar...</option>
                    ${establecimientosCache.map(e =>
        `<option value="${e.id}">${e.nombre_normalizado}</option>`
    ).join('')}
                </select>
            </div>
            <div class="form-group">
                <label style="display: block; margin-bottom: 5px; font-weight: 500;">Código PLU</label>
                <input type="text"
                       class="plu-codigo"
                       placeholder="Ej: 1234"
                       style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
            </div>
            <div class="form-group">
                <label style="display: block; margin-bottom: 5px; font-weight: 500;">Precio</label>
                <input type="number"
                       class="plu-precio"
                       placeholder="0"
                       min="0"
                       step="1"
                       style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
            </div>
            <button type="button"
                    class="btn-remove-plu"
                    onclick="this.closest('.plu-item').remove();"
                    style="padding: 8px 12px; background: #dc2626; color: white; border: none; border-radius: 4px; cursor: pointer;">
                🗑️
            </button>
        </div>
    `;

    contenedor.appendChild(pluDiv);
    console.log('✅ Nuevo PLU agregado');
}

// =============================================================
// ✅ RECOPILAR PLUs PARA GUARDAR - VERSIÓN INTEGRADA
// =============================================================
function recopilarPLUsParaGuardar() {
    const plusItems = document.querySelectorAll('.plu-item');
    const plus = [];

    console.log(`📋 Recopilando ${plusItems.length} PLUs del formulario...`);

    plusItems.forEach((item, index) => {
        const pluId = item.dataset.pluId;
        const establecimientoSelect = item.querySelector('.plu-establecimiento');
        const codigoInput = item.querySelector('.plu-codigo');
        const precioInput = item.querySelector('.plu-precio');

        if (!establecimientoSelect || !codigoInput) {
            console.warn(`⚠️ PLU ${index + 1}: Faltan campos`);
            return;
        }

        const establecimientoId = parseInt(establecimientoSelect.value);
        const codigo = codigoInput.value.trim();
        const precio = precioInput ? parseFloat(precioInput.value) || 0 : 0;

        console.log(`   PLU ${index + 1}:`, {
            pluId: pluId || 'NUEVO',
            establecimientoId,
            codigo,
            precio
        });

        if (!codigo || !establecimientoId) {
            console.warn(`   ⚠️ PLU ${index + 1} incompleto`);
            return;
        }

        const pluData = {
            codigo_plu: codigo,
            establecimiento_id: establecimientoId,
            precio_unitario: precio
        };

        if (pluId && pluId !== '' && pluId !== 'undefined' && pluId !== 'null') {
            pluData.id = parseInt(pluId);
        }

        plus.push(pluData);
        console.log(`   ✅ PLU válido:`, pluData);
    });

    console.log(`📊 Total PLUs válidos: ${plus.length}`);

    return {
        plus,
        plus_a_eliminar: []
    };
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

    if (tabName === 'calidad') {
        cargarAnomalias();
    }
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
        cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarAlerta(`❌ Error: ${error.message}`, 'error');
    }
}

// =============================================================
// FUNCIÓN MOSTRAR ALERTAS
// =============================================================
function mostrarAlerta(mensaje, tipo = 'info') {
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
        background: ${tipo === 'success' ? '#d1fae5' : tipo === 'error' ? '#fee2e2' : tipo === 'warning' ? '#fef3c7' : '#e0e7ff'};
        color: ${tipo === 'success' ? '#059669' : tipo === 'error' ? '#dc2626' : tipo === 'warning' ? '#d97706' : '#4338ca'};
        border-left: 4px solid ${tipo === 'success' ? '#059669' : tipo === 'error' ? '#dc2626' : tipo === 'warning' ? '#d97706' : '#4338ca'};
    `;

    alertContainer.appendChild(alert);

    setTimeout(() => {
        alert.style.animation = 'slideOut 0.3s ease-out';
        setTimeout(() => alert.remove(), 300);
    }, 5000);
}

// Agregar estilos de animación
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
// 🔧 FUNCIONES DE ADMINISTRACIÓN
// =============================================================
let anomaliasCache = [];

async function cargarAnomalias() {
    const apiBase = getApiBase();
    const container = document.getElementById('calidad-stats');

    if (!container) return;

    container.innerHTML = '<div class="loading"></div> Analizando datos...';

    try {
        const response = await fetch(`${apiBase}/api/admin/anomalias`);
        if (!response.ok) throw new Error('Error cargando anomalías');

        const data = await response.json();
        anomaliasCache = data.productos || [];
        const stats = data.estadisticas;

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
        `;

        console.log(`✅ ${anomaliasCache.length} productos analizados`);

    } catch (error) {
        console.error('❌ Error:', error);
        container.innerHTML = `<div class="alert alert-error">Error: ${error.message}</div>`;
    }
}

async function detectarDuplicados() {
    const container = document.getElementById('duplicados-container');
    if (!container) return;

    container.innerHTML = '<div class="loading"></div> Analizando duplicados...';

    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/productos?limite=1000`);
        if (!response.ok) throw new Error('Error cargando productos');

        const data = await response.json();
        const productos = data.productos || [];

        const grupos = {};
        productos.forEach(p => {
            if (!p.nombre) return;
            const nombreBase = p.nombre.substring(0, 15).toUpperCase().trim();
            if (!grupos[nombreBase]) grupos[nombreBase] = [];
            grupos[nombreBase].push(p);
        });

        const duplicados = Object.entries(grupos)
            .filter(([key, items]) => items.length > 1)
            .sort((a, b) => b[1].length - a[1].length)
            .slice(0, 30);

        if (duplicados.length === 0) {
            container.innerHTML = `
                <div style="text-align: center; padding: 40px; color: #059669;">
                    <h3>✅ No se encontraron duplicados obvios</h3>
                </div>
            `;
            return;
        }

        let html = `
            <div style="margin-bottom: 20px; padding: 15px; background: #fef3c7; border-left: 4px solid #f59e0b; border-radius: 6px;">
                <h3>⚠️ Se encontraron ${duplicados.length} grupos de posibles duplicados</h3>
            </div>
        `;

        duplicados.forEach(([nombre, items], index) => {
            html += `<div style="border: 1px solid #e5e7eb; border-radius: 8px; padding: 15px; margin-bottom: 15px;">
                <strong>Grupo ${index + 1}: "${nombre}..."</strong>
                <div style="display: grid; gap: 10px; margin-top: 10px;">`;

            items.forEach(p => {
                html += `
                    <div style="padding: 12px; border: 1px solid #e5e7eb; border-radius: 6px;">
                        <strong>ID ${p.id}: ${p.nombre || 'Sin nombre'}</strong>
                        <button class="btn-small btn-primary" onclick="editarProducto(${p.id})" style="margin-left: 10px;">
                            ✏️ Editar
                        </button>
                    </div>
                `;
            });

            html += `</div></div>`;
        });

        container.innerHTML = html;

    } catch (error) {
        console.error('❌ Error:', error);
        container.innerHTML = `<div class="alert alert-error">Error: ${error.message}</div>`;
    }
}

// =============================================================
// EXPORTAR FUNCIONES
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
window.eliminarProducto = eliminarProducto;
window.mostrarAlerta = mostrarAlerta;
window.cargarPLUsProducto = cargarPLUsProducto;
window.cargarAnomalias = cargarAnomalias;
window.detectarDuplicados = detectarDuplicados;
window.mostrarIndicadorBusqueda = mostrarIndicadorBusqueda;
window.recopilarPLUsParaGuardar = recopilarPLUsParaGuardar;
window.agregarPLUEditable = agregarPLUEditable;
window.habilitarCamposEdicion = habilitarCamposEdicion;
window.verHistorial = verHistorial;

console.log('✅ Productos.js v3.0 INTEGRADO cargado correctamente');

// =============================================================
// Inicialización
// =============================================================
document.addEventListener("DOMContentLoaded", async function () {
    console.log('🚀 Inicializando aplicación...');

    configurarBuscadorTiempoReal();

    // Precargar establecimientos
    await cargarEstablecimientosCache();

    await cargarProductos(1);

    console.log("✅ Sistema inicializado correctamente");
});
