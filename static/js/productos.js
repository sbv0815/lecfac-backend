console.log("🚀 Inicializando Gestión de Productos v4.0 - SISTEMA DE APRENDIZAJE PAPA");

// =============================================================
// Variables globales
// =============================================================
let paginaActual = 1;
let limite = 50;
let totalPaginas = 1;
let productosCache = [];
let coloresCache = null;
let timeoutBusqueda = null;
let establecimientosCache = [];

// =============================================================
// 🌐 Base API
// =============================================================
function getApiBase() {
    return 'https://lecfac-backend-production.up.railway.app';
}

// =============================================================
// ✅ CARGAR ESTABLECIMIENTOS
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

        console.log(`✅ ${totalProductos} productos totales, mostrando ${productosPagina.length}`);

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
                <td colspan="14" style="text-align: center; padding: 40px;">
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
                <td colspan="14" style="text-align: center; padding: 40px; color: #dc2626;">
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
// 🆕 V4.0: DETERMINAR FUENTE DEL DATO
// =============================================================
function determinarFuenteDato(producto) {
    /*
     * Determina de dónde viene el dato:
     * - 👑 PAPA: Producto padre validado (es_producto_papa = true)
     * - 🌐 WEB: Tiene EAN y nombre largo (vino del web enricher)
     * - 📝 OCR: Solo tiene datos del OCR (puede tener errores)
     */

    // Si es producto PAPA → máxima confianza
    if (producto.es_producto_papa) {
        return {
            fuente: 'PAPA',
            icono: '👑',
            color: '#059669',
            bgColor: '#d1fae5',
            texto: 'Validado',
            confianza: 100,
            descripcion: 'Datos verificados manualmente'
        };
    }

    // Usar fuente_datos del backend si existe
    if (producto.fuente_datos === 'WEB') {
        return {
            fuente: 'WEB',
            icono: '🌐',
            color: '#2563eb',
            bgColor: '#dbeafe',
            texto: 'Web',
            confianza: Math.round((producto.confianza_datos || 0.8) * 100),
            descripcion: 'Datos del catálogo web'
        };
    }

    // Si tiene EAN válido (13+ dígitos) y nombre largo → probablemente del WEB
    const tieneEAN = producto.codigo_ean && producto.codigo_ean.length >= 8;
    const nombreLargo = producto.nombre && producto.nombre.length > 20;

    if (tieneEAN && nombreLargo) {
        return {
            fuente: 'WEB',
            icono: '🌐',
            color: '#2563eb',
            bgColor: '#dbeafe',
            texto: 'Web',
            confianza: Math.round((producto.confianza_datos || 0.7) * 100),
            descripcion: 'Datos del catálogo web'
        };
    }

    // Por defecto → OCR (puede tener errores)
    return {
        fuente: 'OCR',
        icono: '📝',
        color: '#d97706',
        bgColor: '#fef3c7',
        texto: 'OCR',
        confianza: Math.round((producto.confianza_datos || 0.5) * 100),
        descripcion: '⚠️ Datos de factura - puede tener errores'
    };
}

// =============================================================
// ⭐ MOSTRAR PRODUCTOS - V4.0 CON FUENTE DE DATOS
// =============================================================
function mostrarProductos(productos) {
    const tbody = document.getElementById("productos-body");
    if (!tbody) return;

    tbody.innerHTML = "";

    if (!productos || productos.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="14" style="text-align: center; padding: 40px;">
                    No hay productos para mostrar
                </td>
            </tr>
        `;
        return;
    }

    productos.forEach((p) => {
        // Procesar PLUs
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

        // PLUs HTML
        let plusHTML = '<span style="color: #999; font-size: 11px;">Sin PLUs</span>';
        if (plusArray && plusArray.length > 0) {
            plusHTML = plusArray.map(plu =>
                `<code style="background: #e0f2fe; color: #0369a1; padding: 2px 6px; border-radius: 3px; font-size: 11px;">${plu.codigo || 'N/A'}</code>`
            ).join(' ');
        }

        // Establecimiento HTML
        let establecimientosHTML = '<span style="color: #999;">-</span>';
        if (plusArray && plusArray.length > 0) {
            establecimientosHTML = plusArray.map(plu =>
                `<span style="background: #d1fae5; color: #059669; padding: 2px 6px; border-radius: 3px; font-size: 10px;">${plu.establecimiento || '?'}</span>`
            ).join(' ');
        }

        // Precio
        let precioHTML = '<span style="color: #999;">-</span>';
        if (plusArray && plusArray.length > 0 && plusArray[0].precio > 0) {
            precioHTML = `$${parseInt(plusArray[0].precio).toLocaleString('es-CO')}`;
        }

        // 🆕 V4.0: FUENTE DEL DATO
        const fuenteInfo = determinarFuenteDato(p);
        const fuenteHTML = `
            <span style="
                display: inline-flex;
                align-items: center;
                gap: 4px;
                background: ${fuenteInfo.bgColor};
                color: ${fuenteInfo.color};
                padding: 3px 8px;
                border-radius: 4px;
                font-size: 11px;
                font-weight: 600;
                cursor: help;
            " title="${fuenteInfo.descripcion} (Confianza: ${fuenteInfo.confianza}%)">
                ${fuenteInfo.icono} ${fuenteInfo.texto}
            </span>
        `;

        // EAN con indicador
        let eanHTML = '<span style="color: #999; font-size: 11px;">-</span>';
        if (p.codigo_ean) {
            const eanColor = fuenteInfo.fuente === 'PAPA' ? '#059669' :
                fuenteInfo.fuente === 'WEB' ? '#2563eb' : '#666';
            eanHTML = `<code style="font-size: 11px; color: ${eanColor};">${p.codigo_ean}</code>`;
        }

        // Nombre con estilo según fuente
        const nombreStyle = fuenteInfo.fuente === 'OCR' ?
            'color: #92400e; font-style: italic;' :
            'color: #111;';

        // Marca
        const marcaHTML = p.marca || '<span style="color: #999; font-size: 11px;">-</span>';

        // Estado badges
        const estadoBadges = [];
        if (!p.codigo_ean) estadoBadges.push('<span class="badge badge-warning" style="font-size: 10px;">Sin EAN</span>');
        if (!p.marca) estadoBadges.push('<span class="badge badge-warning" style="font-size: 10px;">Sin Marca</span>');

        // 🆕 Botón para marcar como PAPA
        const botonPapa = p.es_producto_papa ?
            `<button class="btn-small" style="background: #d1fae5; color: #059669; border: 1px solid #059669; cursor: pointer;"
                     onclick="quitarPapa(${p.id})" title="✓ Validado - Click para quitar">
                👑
            </button>` :
            `<button class="btn-small" style="background: #f3f4f6; color: #6b7280; cursor: pointer;"
                     onclick="marcarComoPapa(${p.id}, '${(p.nombre || '').replace(/'/g, "\\'")}')" title="Marcar como validado (PAPA)">
                ⭐
            </button>`;

        // Fila con estilo según fuente
        const rowStyle = fuenteInfo.fuente === 'OCR' ? 'background: #fffbeb;' : '';

        const row = `
            <tr style="${rowStyle}" data-id="${p.id}" data-ean="${p.codigo_ean || ''}" data-nombre="${(p.nombre || '').replace(/"/g, '&quot;')}">
                <td class="checkbox-cell">
                    <input type="checkbox" value="${p.id}" onchange="toggleProductSelection(${p.id})">
                </td>
                <td style="font-size: 12px;">${p.id}</td>
                <td>
                    ${p.codigo_ean
                ? `<button class="btn-imagen" onclick="verImagenProducto(${p.id}, '${p.codigo_ean}', '${(p.nombre || '').replace(/'/g, "\\'")}')">📷</button>`
                : `<button class="btn-imagen sin-imagen" disabled title="Sin EAN">📷</button>`
            }
                </td>
                <td>${fuenteHTML}</td>
                <td>${eanHTML}</td>
                <td>${plusHTML}</td>
                <td>${establecimientosHTML}</td>
                <td style="${nombreStyle}"><strong>${p.nombre || '-'}</strong></td>
                <td style="font-size: 12px;">${marcaHTML}</td>
                <td style="font-size: 12px;">${precioHTML}</td>
                <td style="font-size: 11px;">${estadoBadges.join(' ') || '<span style="color: #059669;">✓</span>'}</td>
                <td style="white-space: nowrap;">
    ${botonPapa}
    <button class="btn-small btn-primary" onclick="editarProducto(${p.id})" title="Editar">
        ✏️
    </button>
    <button class="btn-small" style="background: #fef3c7; color: #92400e;"
            onclick="verFacturaOriginal(${p.id}, '${(p.nombre || '').replace(/'/g, "\\'")}')"
            title="Ver factura original">
                🧾
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
// 🆕 V4.0: MARCAR COMO PRODUCTO PAPA (VALIDADO)
// =============================================================
async function marcarComoPapa(id, nombre) {
    const confirmMsg = `¿Marcar "${nombre}" como producto VALIDADO (PAPA)?

Esto significa que:
✅ El PLU es correcto
✅ El EAN es correcto
✅ El nombre es correcto

Los futuros escaneos usarán estos datos como referencia.`;

    if (!confirm(confirmMsg)) {
        return;
    }

    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                es_producto_papa: true,
                confianza_datos: 1.0,
                fuente_datos: 'PAPA'
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Error al marcar como PAPA');
        }

        mostrarAlerta(`✅ "${nombre}" marcado como producto VALIDADO`, 'success');
        cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarAlerta(`❌ Error: ${error.message}`, 'error');
    }
}

// =============================================================
// 🆕 V4.0: QUITAR MARCA DE PAPA
// =============================================================
async function quitarPapa(id) {
    if (!confirm('¿Quitar la validación de este producto?\n\nVolverá a mostrar la fuente original (WEB u OCR).')) {
        return;
    }

    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                es_producto_papa: false,
                fuente_datos: null  // El backend determinará si es WEB u OCR
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Error');
        }

        mostrarAlerta('✅ Validación removida', 'success');
        cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarAlerta(`❌ Error: ${error.message}`, 'error');
    }
}

// =============================================================
// Actualizar estadísticas y paginación
// =============================================================
function actualizarEstadisticas(data) {
    const stats = document.querySelectorAll('.stat-value');
    if (stats.length >= 4 && data) {
        stats[0].textContent = data.total || productosCache.length || '0';

        // Contar por fuente
        const papas = productosCache.filter(p => p.es_producto_papa).length;
        const conEanWeb = productosCache.filter(p => p.codigo_ean && !p.es_producto_papa).length;
        const soloOcr = productosCache.filter(p => !p.codigo_ean && !p.es_producto_papa).length;

        if (stats.length >= 4) {
            stats[1].textContent = papas;
            stats[2].textContent = conEanWeb;
            stats[3].textContent = soloOcr;
        }
    }

    // Actualizar labels si existen
    const labels = document.querySelectorAll('.stat-label');
    if (labels.length >= 4) {
        labels[0].textContent = 'Total Productos';
        labels[1].textContent = '👑 Validados (PAPA)';
        labels[2].textContent = '🌐 Con EAN (Web)';
        labels[3].textContent = '📝 Solo OCR (revisar)';
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
// ✅ EDITAR PRODUCTO - V4.0 CON FUENTE
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
        document.getElementById("edit-nombre-norm").value = producto.nombre_consolidado || producto.nombre || "";
        document.getElementById("edit-nombre-com").value = producto.nombre_comercial || "";
        document.getElementById("edit-marca").value = producto.marca || "";

        // Código LecFac (solo lectura)
        const lecfacInput = document.getElementById("edit-lecfac");
        if (lecfacInput) {
            lecfacInput.value = producto.codigo_lecfac || "-";
        }
        document.getElementById("edit-categoria").value = producto.categoria || "";
        document.getElementById("edit-subcategoria").value = producto.subcategoria || "";
        document.getElementById("edit-presentacion").value = producto.presentacion || "";

        // Estadísticas
        document.getElementById("edit-veces-comprado").value = producto.veces_comprado || "0";
        document.getElementById("edit-precio-promedio").value = producto.precio_promedio ?
            `$${producto.precio_promedio.toLocaleString('es-CO')}` : "Sin datos";
        document.getElementById("edit-num-establecimientos").value = producto.num_establecimientos || "0";

        // 🆕 V4.0: Mostrar fuente del dato en el modal
        const fuenteInfo = determinarFuenteDato(producto);
        mostrarFuenteEnModal(fuenteInfo, producto);

        // Checkbox de PAPA
        const checkPapa = document.getElementById("edit-es-papa");
        if (checkPapa) {
            checkPapa.checked = producto.es_producto_papa || false;
        }

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
// 🆕 V4.0: MOSTRAR FUENTE EN MODAL
// =============================================================
function mostrarFuenteEnModal(fuenteInfo, producto) {
    // Buscar o crear contenedor de fuente
    let fuenteContainer = document.getElementById("edit-fuente-container");

    if (!fuenteContainer) {
        // Crear el contenedor si no existe (lo insertamos antes del primer form-group)
        const formRow = document.querySelector('.modal-body .form-row');
        if (formRow) {
            fuenteContainer = document.createElement('div');
            fuenteContainer.id = 'edit-fuente-container';
            fuenteContainer.style.cssText = 'grid-column: 1 / -1; margin-bottom: 15px;';
            formRow.parentNode.insertBefore(fuenteContainer, formRow);
        }
    }

    if (fuenteContainer) {
        const warningHtml = fuenteInfo.fuente === 'OCR' ?
            `<p style="color: #d97706; margin-top: 8px; font-size: 13px;">
                ⚠️ Este producto viene del OCR y puede tener errores.
                <strong>Verifica los datos antes de marcar como PAPA.</strong>
            </p>` : '';

        fuenteContainer.innerHTML = `
            <div style="
                display: flex;
                align-items: center;
                gap: 12px;
                background: ${fuenteInfo.bgColor};
                color: ${fuenteInfo.color};
                padding: 12px 16px;
                border-radius: 8px;
                border-left: 4px solid ${fuenteInfo.color};
            ">
                <span style="font-size: 24px;">${fuenteInfo.icono}</span>
                <div>
                    <div style="font-weight: 600; font-size: 14px;">
                        Fuente: ${fuenteInfo.texto}
                    </div>
                    <div style="font-size: 12px; opacity: 0.9;">
                        Confianza: ${fuenteInfo.confianza}% - ${fuenteInfo.descripcion}
                    </div>
                </div>
            </div>
            ${warningHtml}
        `;
    }
}

// =============================================================
// Habilitar campos de edición
// =============================================================
function habilitarCamposEdicion() {
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
        }
    });

    const eanInput = document.getElementById('edit-ean');
    if (eanInput) {
        eanInput.setAttribute('maxlength', '14');
    }
}

// =============================================================
// ✅ GUARDAR EDICIÓN - V4.0 CON PAPA
// =============================================================
async function guardarEdicion() {
    console.log('💾 Guardando edición...');

    const apiBase = getApiBase();
    const productoId = document.getElementById('edit-id').value;
    const nombreConsolidado = document.getElementById('edit-nombre-norm').value;
    const marca = document.getElementById('edit-marca').value;
    const codigoEan = document.getElementById('edit-ean').value;

    // 🆕 V4.0: Checkbox de PAPA
    const checkPapa = document.getElementById("edit-es-papa");
    const esPapa = checkPapa ? checkPapa.checked : false;

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
            codigo_ean: codigoEan.trim() || null,
            es_producto_papa: esPapa
        };

        // Si se marca como PAPA, establecer máxima confianza
        if (esPapa) {
            datosProducto.confianza_datos = 1.0;
            datosProducto.fuente_datos = 'PAPA';
        }

        console.log('📦 Guardando producto:', datosProducto);

        const responseProducto = await fetch(`${apiBase}/api/v2/productos/${productoId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(datosProducto)
        });

        if (!responseProducto.ok) {
            const errorData = await responseProducto.json().catch(() => ({ error: 'Error desconocido' }));
            throw new Error(errorData.detail || errorData.error || `HTTP ${responseProducto.status}`);
        }

        const resultadoProducto = await responseProducto.json();
        console.log('✅ Producto actualizado:', resultadoProducto);

        // Mostrar advertencia si hay EAN duplicado
        if (resultadoProducto.advertencia) {
            mostrarAlerta(resultadoProducto.advertencia, 'warning');
        }

        // 2️⃣ GUARDAR PLUs
        const plusData = recopilarPLUsParaGuardar();

        if (plusData.plus.length > 0) {
            const responsePlus = await fetch(`${apiBase}/api/v2/productos/${productoId}/plus`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(plusData)
            });

            if (responsePlus.ok) {
                const resultadoPlus = await responsePlus.json();
                console.log('✅ PLUs actualizados:', resultadoPlus);
            }
        }

        const mensajeExito = esPapa ?
            '✅ Producto VALIDADO (PAPA) y guardado correctamente' :
            '✅ Producto actualizado correctamente';

        mostrarAlerta(mensajeExito, 'success');
        cerrarModal('modal-editar');
        await cargarProductos(paginaActual);

    } catch (error) {
        console.error('❌ Error guardando:', error);
        mostrarAlerta(`❌ Error: ${error.message}`, 'error');
    }
}
// =============================================================
// ✅ AGREGAR PLU EDITABLE
// =============================================================
async function agregarPLUEditable() {
    await cargarEstablecimientosCache();

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) return;

    const mensaje = contenedor.querySelector('p');
    if (mensaje) mensaje.remove();

    const pluDiv = document.createElement('div');
    pluDiv.className = 'plu-item';
    pluDiv.dataset.pluId = '';

    pluDiv.innerHTML = `
        <div class="plu-row" style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 10px; align-items: end; padding: 10px; border: 1px solid #10b981; border-radius: 6px; margin-bottom: 10px; background: #f0fdf4;">
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
                <input type="text" class="plu-codigo" placeholder="Ej: 1234"
                       style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
            </div>
            <div class="form-group">
                <label style="display: block; margin-bottom: 5px; font-weight: 500;">Precio</label>
                <input type="number" class="plu-precio" placeholder="0" min="0" step="1"
                       style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
            </div>
            <button type="button" class="btn-remove-plu" onclick="this.closest('.plu-item').remove();"
                    style="padding: 8px 12px; background: #dc2626; color: white; border: none; border-radius: 4px; cursor: pointer;">
                🗑️
            </button>
        </div>
    `;

    contenedor.appendChild(pluDiv);
}

// =============================================================
// ✅ RECOPILAR PLUs PARA GUARDAR
// =============================================================
function recopilarPLUsParaGuardar() {
    const plusItems = document.querySelectorAll('.plu-item');
    const plus = [];

    plusItems.forEach((item, index) => {
        const pluId = item.dataset.pluId;
        const establecimientoSelect = item.querySelector('.plu-establecimiento');
        const codigoInput = item.querySelector('.plu-codigo');
        const precioInput = item.querySelector('.plu-precio');

        if (!establecimientoSelect || !codigoInput) return;

        const establecimientoId = parseInt(establecimientoSelect.value);
        const codigo = codigoInput.value.trim();
        const precio = precioInput ? parseFloat(precioInput.value) || 0 : 0;

        if (!codigo || !establecimientoId) return;

        const pluData = {
            codigo_plu: codigo,
            establecimiento_id: establecimientoId,
            precio_unitario: precio
        };

        if (pluId && pluId !== '' && pluId !== 'undefined' && pluId !== 'null') {
            pluData.id = parseInt(pluId);
        }

        plus.push(pluData);
    });

    return { plus, plus_a_eliminar: [] };
}

// =============================================================
// Funciones auxiliares
// =============================================================
function cerrarModal(modalId) {
    document.getElementById(modalId)?.classList.remove("active");

    // Limpiar contenedor de fuente al cerrar
    const fuenteContainer = document.getElementById("edit-fuente-container");
    if (fuenteContainer) {
        fuenteContainer.innerHTML = '';
    }
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

// =============================================================
// CARGAR ANOMALÍAS - V4.0 CON ESTADÍSTICAS DE FUENTE
// =============================================================
async function cargarAnomalias() {
    const apiBase = getApiBase();
    const container = document.getElementById('calidad-stats');

    if (!container) return;

    container.innerHTML = '<div class="loading"></div> Analizando datos...';

    try {
        // Usar los datos en cache para estadísticas rápidas
        const papas = productosCache.filter(p => p.es_producto_papa).length;
        const web = productosCache.filter(p => !p.es_producto_papa && p.codigo_ean).length;
        const ocr = productosCache.filter(p => !p.es_producto_papa && !p.codigo_ean).length;
        const total = productosCache.length;

        const porcentajeCalidad = total > 0 ?
            Math.round(((papas + web) / total) * 100) : 0;

        container.innerHTML = `
            <div class="stat-card" style="background: linear-gradient(135deg, #d1fae5, #a7f3d0);">
                <div class="stat-value" style="color: #059669;">👑 ${papas}</div>
                <div class="stat-label">Validados (PAPA)</div>
            </div>
            <div class="stat-card" style="background: linear-gradient(135deg, #dbeafe, #bfdbfe);">
                <div class="stat-value" style="color: #2563eb;">🌐 ${web}</div>
                <div class="stat-label">Desde Web</div>
            </div>
            <div class="stat-card" style="background: linear-gradient(135deg, #fef3c7, #fde68a);">
                <div class="stat-value" style="color: #d97706;">📝 ${ocr}</div>
                <div class="stat-label">Solo OCR (revisar)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #2563eb;">${porcentajeCalidad}%</div>
                <div class="stat-label">Calidad de Datos</div>
            </div>
        `;

    } catch (error) {
        console.error('❌ Error:', error);
        container.innerHTML = `<div class="alert alert-error">Error: ${error.message}</div>`;
    }
}

// =============================================================
// DETECTAR DUPLICADOS
// =============================================================
async function detectarDuplicados() {
    const container = document.getElementById('duplicados-container');
    if (!container) return;

    container.innerHTML = '<div class="loading"></div> Analizando duplicados...';

    try {
        const productos = productosCache;

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
                const fuenteInfo = determinarFuenteDato(p);
                html += `
                    <div style="padding: 12px; border: 1px solid #e5e7eb; border-radius: 6px; background: ${fuenteInfo.bgColor};">
                        <span style="font-size: 12px; color: ${fuenteInfo.color};">${fuenteInfo.icono} ${fuenteInfo.texto}</span>
                        <strong> ID ${p.id}: ${p.nombre || 'Sin nombre'}</strong>
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
window.cargarAnomalias = cargarAnomalias;
window.detectarDuplicados = detectarDuplicados;
window.mostrarIndicadorBusqueda = mostrarIndicadorBusqueda;
window.recopilarPLUsParaGuardar = recopilarPLUsParaGuardar;
window.agregarPLUEditable = agregarPLUEditable;
window.habilitarCamposEdicion = habilitarCamposEdicion;
window.verHistorial = verHistorial;
window.marcarComoPapa = marcarComoPapa;
window.quitarPapa = quitarPapa;
window.determinarFuenteDato = determinarFuenteDato;

console.log('✅ Productos.js v4.0 SISTEMA DE APRENDIZAJE PAPA cargado');

// =============================================================
// Inicialización
// =============================================================
document.addEventListener("DOMContentLoaded", async function () {
    console.log('🚀 Inicializando aplicación v4.0...');

    // Agregar estilos de animación
    if (!document.getElementById('alert-animations')) {
        const style = document.createElement('style');
        style.id = 'alert-animations';
        style.textContent = `
            @keyframes slideIn {
                from { transform: translateX(400px); opacity: 0; }
                to { transform: translateX(0); opacity: 1; }
            }
            @keyframes slideOut {
                from { transform: translateX(0); opacity: 1; }
                to { transform: translateX(400px); opacity: 0; }
            }
        `;
        document.head.appendChild(style);
    }

    configurarBuscadorTiempoReal();
    await cargarEstablecimientosCache();
    await cargarProductos(1);

    console.log("✅ Sistema v4.0 inicializado");
});
// =============================================================
// 🧾 V5.2: VER FACTURA ORIGINAL - CON SCROLL AUTOMÁTICO
// =============================================================
// Reemplaza las funciones existentes en productos.js
// NUEVO: Hace scroll automático a la posición del producto
// =============================================================

// Variables para controles de imagen
let imagenControles = {
    zoom: 1,
    brillo: 100,
    contraste: 100,
    rotacion: 0
};

// =============================================================
// 🧾 VER FACTURA ORIGINAL - CON SCROLL AUTOMÁTICO
// =============================================================
async function verFacturaOriginal(productoId, nombreProducto) {
    console.log(`🧾 Buscando factura del producto ${productoId}`);

    // Reset controles
    imagenControles = { zoom: 1, brillo: 100, contraste: 100, rotacion: 0 };

    const apiBase = getApiBase();

    mostrarModalFactura(null, nombreProducto, true);

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${productoId}/factura`);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        if (!data.success || !data.facturas || data.facturas.length === 0) {
            mostrarModalFactura({
                error: true,
                mensaje: "No se encontraron facturas para este producto"
            }, nombreProducto);
            return;
        }

        mostrarModalFactura(data, nombreProducto);

    } catch (error) {
        console.error('❌ Error:', error);
        mostrarModalFactura({
            error: true,
            mensaje: `Error: ${error.message}`
        }, nombreProducto);
    }
}

// =============================================================
// 🖼️ MOSTRAR MODAL DE FACTURA - CON SCROLL AUTOMÁTICO
// =============================================================
function mostrarModalFactura(data, nombreProducto, loading = false) {
    let modal = document.getElementById('modal-factura');

    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'modal-factura';
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-content" style="max-width: 1000px; max-height: 95vh; overflow: auto;">
                <div class="modal-header">
                    <h2>🧾 Factura Original</h2>
                    <button class="modal-close" onclick="cerrarModal('modal-factura')">&times;</button>
                </div>
                <div class="modal-body" id="modal-factura-body">
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    }

    const body = document.getElementById('modal-factura-body');

    if (loading) {
        body.innerHTML = `
            <div style="text-align: center; padding: 40px;">
                <div class="loading" style="width: 40px; height: 40px; margin: 0 auto 20px;"></div>
                <p>Buscando facturas donde apareció "${nombreProducto}"...</p>
            </div>
        `;
        modal.classList.add('active');
        return;
    }

    if (data.error) {
        body.innerHTML = `
            <div style="text-align: center; padding: 40px; color: #dc2626;">
                <p style="font-size: 48px;">📭</p>
                <p style="font-size: 18px; margin-top: 10px;">${data.mensaje}</p>
                <p style="font-size: 14px; color: #6b7280; margin-top: 10px;">
                    Este producto puede haber sido creado manualmente o la factura fue eliminada.
                </p>
            </div>
        `;
        modal.classList.add('active');
        return;
    }

    let html = `
        <div style="margin-bottom: 20px;">
            <h3 style="margin-bottom: 10px;">📦 Producto: ${nombreProducto}</h3>
            <p style="color: #6b7280;">Encontrado en ${data.total_facturas} factura(s)</p>
        </div>
    `;

    data.facturas.forEach((factura, index) => {
        const item = factura.item;
        const posicionVertical = item.posicion_vertical || 50; // Default: mitad

        html += `
            <div style="border: 1px solid #e5e7eb; border-radius: 8px; margin-bottom: 20px; overflow: hidden;">
                <div style="background: #f9fafb; padding: 12px 15px; border-bottom: 1px solid #e5e7eb;">
                    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                        <div>
                            <strong>📋 Factura #${factura.numero_factura || factura.factura_id}</strong>
                            <span style="margin-left: 15px; color: #6b7280;">
                                📅 ${factura.fecha || 'Sin fecha'}
                            </span>
                            <span style="margin-left: 15px; background: #d1fae5; color: #059669; padding: 2px 8px; border-radius: 4px; font-size: 12px;">
                                ${factura.establecimiento || 'Sin establecimiento'}
                            </span>
                        </div>
                    </div>
                </div>

                <div style="padding: 15px;">
                    <div style="background: #fef3c7; border-left: 4px solid #d97706; padding: 12px; border-radius: 4px; margin-bottom: 15px;">
                        <h4 style="color: #92400e; margin-bottom: 8px;">📝 Datos leídos por OCR:</h4>
                        <table style="width: 100%; font-size: 14px;">
                            <tr>
                                <td style="padding: 4px 0; color: #6b7280; width: 120px;">Código PLU:</td>
                                <td><code style="background: #fff; padding: 2px 6px; border-radius: 3px;">${item.codigo_leido || '-'}</code></td>
                            </tr>
                            <tr>
                                <td style="padding: 4px 0; color: #6b7280;">Nombre leído:</td>
                                <td><strong>${item.nombre_leido || '-'}</strong></td>
                            </tr>
                            <tr>
                                <td style="padding: 4px 0; color: #6b7280;">Precio:</td>
                                <td>$${(item.precio_pagado || 0).toLocaleString('es-CO')}</td>
                            </tr>
                            <tr>
                                <td style="padding: 4px 0; color: #6b7280;">Cantidad:</td>
                                <td>${item.cantidad || 1}</td>
                            </tr>
                            <tr>
                                <td style="padding: 4px 0; color: #6b7280;">📍 Posición:</td>
                                <td>
                                    <span style="background: #dbeafe; color: #1e40af; padding: 2px 8px; border-radius: 4px; font-size: 12px;">
                                        ${posicionVertical}% desde arriba
                                    </span>
                                </td>
                            </tr>
                        </table>
                    </div>

                    ${factura.tiene_imagen && factura.imagen_base64 ? `
                        <div style="margin-top: 15px;">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                                <h4>🖼️ Imagen de la factura:</h4>
                                <button onclick="scrollAProducto(${index}, ${posicionVertical})"
                                        style="padding: 6px 12px; background: #2563eb; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 13px;">
                                    🎯 Ir al producto (${posicionVertical}%)
                                </button>
                            </div>

                            <!-- 🆕 CONTROLES DE IMAGEN -->
                            <div id="controles-imagen-${index}" style="background: #f3f4f6; padding: 12px; border-radius: 8px; margin-bottom: 10px;">
                                <div style="display: flex; flex-wrap: wrap; gap: 15px; align-items: center;">

                                    <!-- Zoom -->
                                    <div style="display: flex; align-items: center; gap: 8px;">
                                        <span style="font-size: 13px; color: #374151;">🔍 Zoom:</span>
                                        <button onclick="ajustarImagen(${index}, 'zoom', -0.25)"
                                                style="padding: 4px 10px; border: 1px solid #d1d5db; border-radius: 4px; background: white; cursor: pointer;">−</button>
                                        <span id="zoom-valor-${index}" style="min-width: 50px; text-align: center; font-size: 13px;">100%</span>
                                        <button onclick="ajustarImagen(${index}, 'zoom', 0.25)"
                                                style="padding: 4px 10px; border: 1px solid #d1d5db; border-radius: 4px; background: white; cursor: pointer;">+</button>
                                    </div>

                                    <!-- Brillo -->
                                    <div style="display: flex; align-items: center; gap: 8px;">
                                        <span style="font-size: 13px; color: #374151;">☀️ Brillo:</span>
                                        <input type="range" id="brillo-${index}" min="50" max="200" value="100"
                                               onchange="ajustarImagen(${index}, 'brillo', this.value)"
                                               style="width: 80px; cursor: pointer;">
                                        <span id="brillo-valor-${index}" style="font-size: 12px; min-width: 35px;">100%</span>
                                    </div>

                                    <!-- Contraste -->
                                    <div style="display: flex; align-items: center; gap: 8px;">
                                        <span style="font-size: 13px; color: #374151;">🎨 Contraste:</span>
                                        <input type="range" id="contraste-${index}" min="50" max="200" value="100"
                                               onchange="ajustarImagen(${index}, 'contraste', this.value)"
                                               style="width: 80px; cursor: pointer;">
                                        <span id="contraste-valor-${index}" style="font-size: 12px; min-width: 35px;">100%</span>
                                    </div>

                                    <!-- Rotación -->
                                    <div style="display: flex; align-items: center; gap: 8px;">
                                        <span style="font-size: 13px; color: #374151;">🔄</span>
                                        <button onclick="ajustarImagen(${index}, 'rotacion', -90)"
                                                style="padding: 4px 8px; border: 1px solid #d1d5db; border-radius: 4px; background: white; cursor: pointer;">↺</button>
                                        <button onclick="ajustarImagen(${index}, 'rotacion', 90)"
                                                style="padding: 4px 8px; border: 1px solid #d1d5db; border-radius: 4px; background: white; cursor: pointer;">↻</button>
                                    </div>

                                    <!-- Reset -->
                                    <button onclick="resetearImagen(${index})"
                                            style="padding: 4px 12px; border: 1px solid #dc2626; border-radius: 4px; background: #fef2f2; color: #dc2626; cursor: pointer; font-size: 12px;">
                                        🔄 Reset
                                    </button>
                                </div>
                            </div>

                            <!-- Contenedor de imagen con scroll -->
                            <div id="imagen-container-${index}" style="max-height: 500px; overflow: auto; border: 1px solid #e5e7eb; border-radius: 8px; background: #1f2937; position: relative;">
                                <!-- Indicador de posición del producto -->
                                <div id="indicador-posicion-${index}" style="
                                    position: absolute;
                                    left: 0;
                                    right: 0;
                                    height: 3px;
                                    background: #ef4444;
                                    box-shadow: 0 0 10px #ef4444;
                                    z-index: 10;
                                    top: ${posicionVertical}%;
                                    display: none;
                                "></div>

                                <img id="factura-img-${index}"
                                     src="data:${factura.imagen_mime || 'image/jpeg'};base64,${factura.imagen_base64}"
                                     style="max-width: 100%; display: block; margin: auto; transition: all 0.2s ease;"
                                     onload="scrollAProducto(${index}, ${posicionVertical})"
                                     onclick="ampliarImagenConControles(this.src, ${posicionVertical})"
                                     title="Click para ampliar">
                            </div>
                            <p style="font-size: 12px; color: #6b7280; margin-top: 8px;">
                                💡 La imagen se posiciona automáticamente donde está el producto. Click 🎯 para recentrar.
                            </p>
                        </div>
                    ` : `
                        <div style="text-align: center; padding: 30px; background: #f3f4f6; border-radius: 8px; color: #6b7280;">
                            <p style="font-size: 36px;">🖼️</p>
                            <p>Imagen no disponible para esta factura</p>
                        </div>
                    `}
                </div>
            </div>
        `;
    });

    body.innerHTML = html;
    modal.classList.add('active');
}

// =============================================================
// 🎯 SCROLL AUTOMÁTICO A LA POSICIÓN DEL PRODUCTO
// =============================================================
function scrollAProducto(index, posicionVertical) {
    const container = document.getElementById(`imagen-container-${index}`);
    const img = document.getElementById(`factura-img-${index}`);
    const indicador = document.getElementById(`indicador-posicion-${index}`);

    if (!container || !img) return;

    // Esperar a que la imagen cargue
    if (!img.complete) {
        img.onload = () => scrollAProducto(index, posicionVertical);
        return;
    }

    // Calcular posición de scroll
    const imgHeight = img.offsetHeight;
    const containerHeight = container.offsetHeight;
    const scrollPosition = (imgHeight * posicionVertical / 100) - (containerHeight / 2);

    console.log(`🎯 Scroll a posición ${posicionVertical}%: ${scrollPosition}px de ${imgHeight}px`);

    // Hacer scroll suave
    container.scrollTo({
        top: Math.max(0, scrollPosition),
        behavior: 'smooth'
    });

    // Mostrar indicador temporalmente
    if (indicador) {
        indicador.style.display = 'block';
        indicador.style.top = `${posicionVertical}%`;

        // Hacer parpadear el indicador
        let parpadeos = 0;
        const intervalo = setInterval(() => {
            indicador.style.opacity = indicador.style.opacity === '0' ? '1' : '0';
            parpadeos++;
            if (parpadeos >= 6) {
                clearInterval(intervalo);
                indicador.style.display = 'none';
                indicador.style.opacity = '1';
            }
        }, 300);
    }
}

// =============================================================
// 🎛️ AJUSTAR IMAGEN (zoom, brillo, contraste, rotación)
// =============================================================
function ajustarImagen(index, tipo, valor) {
    const img = document.getElementById(`factura-img-${index}`);
    if (!img) return;

    if (tipo === 'zoom') {
        imagenControles.zoom = Math.max(0.5, Math.min(3, imagenControles.zoom + valor));
        document.getElementById(`zoom-valor-${index}`).textContent = Math.round(imagenControles.zoom * 100) + '%';
    } else if (tipo === 'brillo') {
        imagenControles.brillo = parseInt(valor);
        document.getElementById(`brillo-valor-${index}`).textContent = valor + '%';
    } else if (tipo === 'contraste') {
        imagenControles.contraste = parseInt(valor);
        document.getElementById(`contraste-valor-${index}`).textContent = valor + '%';
    } else if (tipo === 'rotacion') {
        imagenControles.rotacion = (imagenControles.rotacion + valor) % 360;
    }

    img.style.transform = `scale(${imagenControles.zoom}) rotate(${imagenControles.rotacion}deg)`;
    img.style.filter = `brightness(${imagenControles.brillo}%) contrast(${imagenControles.contraste}%)`;
    img.style.transformOrigin = 'center center';
}

// =============================================================
// 🔄 RESETEAR IMAGEN
// =============================================================
function resetearImagen(index) {
    imagenControles = { zoom: 1, brillo: 100, contraste: 100, rotacion: 0 };

    const img = document.getElementById(`factura-img-${index}`);
    if (img) {
        img.style.transform = 'scale(1) rotate(0deg)';
        img.style.filter = 'brightness(100%) contrast(100%)';
    }

    const brilloSlider = document.getElementById(`brillo-${index}`);
    const contrasteSlider = document.getElementById(`contraste-${index}`);

    if (brilloSlider) brilloSlider.value = 100;
    if (contrasteSlider) contrasteSlider.value = 100;

    document.getElementById(`zoom-valor-${index}`).textContent = '100%';
    document.getElementById(`brillo-valor-${index}`).textContent = '100%';
    document.getElementById(`contraste-valor-${index}`).textContent = '100%';
}

// =============================================================
// 🔍 AMPLIAR IMAGEN CON CONTROLES Y POSICIÓN
// =============================================================
function ampliarImagenConControles(src, posicionVertical = 50) {
    let modal = document.getElementById('modal-imagen-ampliada');

    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'modal-imagen-ampliada';
        modal.style.cssText = `
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0,0,0,0.95);
            z-index: 2000;
            overflow: auto;
            padding: 20px;
            flex-direction: column;
        `;
        document.body.appendChild(modal);
    }

    modal.innerHTML = `
        <!-- Barra de controles superior -->
        <div style="position: fixed; top: 0; left: 0; right: 0; background: rgba(0,0,0,0.8); padding: 15px;
                    display: flex; justify-content: center; gap: 20px; align-items: center; flex-wrap: wrap; z-index: 2001;">

            <!-- Ir al producto -->
            <button onclick="scrollImagenAmpliada(${posicionVertical})"
                    style="padding: 8px 15px; border: none; border-radius: 4px; background: #2563eb; color: white; cursor: pointer; font-weight: 600;">
                🎯 Ir al producto (${posicionVertical}%)
            </button>

            <!-- Zoom -->
            <div style="display: flex; align-items: center; gap: 8px; color: white;">
                <span>🔍</span>
                <button onclick="ajustarImagenAmpliada('zoom', -0.25)"
                        style="padding: 8px 15px; border: none; border-radius: 4px; background: #374151; color: white; cursor: pointer; font-size: 16px;">−</button>
                <span id="zoom-ampliado" style="min-width: 60px; text-align: center;">100%</span>
                <button onclick="ajustarImagenAmpliada('zoom', 0.25)"
                        style="padding: 8px 15px; border: none; border-radius: 4px; background: #374151; color: white; cursor: pointer; font-size: 16px;">+</button>
            </div>

            <!-- Brillo -->
            <div style="display: flex; align-items: center; gap: 8px; color: white;">
                <span>☀️</span>
                <input type="range" id="brillo-ampliado-slider" min="50" max="200" value="${imagenControles.brillo}"
                       onchange="ajustarImagenAmpliada('brillo', this.value)"
                       style="width: 100px; cursor: pointer;">
            </div>

            <!-- Contraste -->
            <div style="display: flex; align-items: center; gap: 8px; color: white;">
                <span>🎨</span>
                <input type="range" id="contraste-ampliado-slider" min="50" max="200" value="${imagenControles.contraste}"
                       onchange="ajustarImagenAmpliada('contraste', this.value)"
                       style="width: 100px; cursor: pointer;">
            </div>

            <!-- Rotación -->
            <div style="display: flex; align-items: center; gap: 8px;">
                <button onclick="ajustarImagenAmpliada('rotacion', -90)"
                        style="padding: 8px 12px; border: none; border-radius: 4px; background: #374151; color: white; cursor: pointer;">↺</button>
                <button onclick="ajustarImagenAmpliada('rotacion', 90)"
                        style="padding: 8px 12px; border: none; border-radius: 4px; background: #374151; color: white; cursor: pointer;">↻</button>
            </div>

            <!-- Reset y Cerrar -->
            <button onclick="resetearImagenAmpliada()"
                    style="padding: 8px 15px; border: none; border-radius: 4px; background: #dc2626; color: white; cursor: pointer;">
                🔄 Reset
            </button>
            <button onclick="document.getElementById('modal-imagen-ampliada').style.display='none'"
                    style="padding: 8px 20px; border: none; border-radius: 4px; background: #059669; color: white; cursor: pointer; font-weight: 600;">
                ✕ Cerrar
            </button>
        </div>

        <!-- Contenedor con scroll -->
        <div id="imagen-ampliada-container" style="flex: 1; margin-top: 70px; padding: 20px; overflow: auto;">
            <!-- Indicador de posición -->
            <div id="indicador-ampliado" style="
                position: absolute;
                left: 20px;
                right: 20px;
                height: 4px;
                background: #ef4444;
                box-shadow: 0 0 15px #ef4444;
                z-index: 10;
                display: none;
            "></div>

            <img id="imagen-ampliada-src" src="${src}"
                 style="max-width: 100%; margin: auto; display: block; transition: all 0.2s ease;
                        transform: scale(${imagenControles.zoom}) rotate(${imagenControles.rotacion}deg);
                        filter: brightness(${imagenControles.brillo}%) contrast(${imagenControles.contraste}%);"
                 onload="scrollImagenAmpliada(${posicionVertical})">
        </div>

        <!-- Instrucciones -->
        <div style="position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
                    background: rgba(0,0,0,0.7); color: #9ca3af; padding: 8px 20px; border-radius: 20px; font-size: 13px;">
            🎯 Click en "Ir al producto" para ver dónde está • ESC para cerrar
        </div>
    `;

    modal.style.display = 'flex';

    document.getElementById('zoom-ampliado').textContent = Math.round(imagenControles.zoom * 100) + '%';
}

// =============================================================
// 🎯 SCROLL EN IMAGEN AMPLIADA
// =============================================================
function scrollImagenAmpliada(posicionVertical) {
    const container = document.getElementById('imagen-ampliada-container');
    const img = document.getElementById('imagen-ampliada-src');
    const indicador = document.getElementById('indicador-ampliado');

    if (!container || !img) return;

    const imgHeight = img.offsetHeight;
    const containerHeight = container.offsetHeight;
    const scrollPosition = (imgHeight * posicionVertical / 100) - (containerHeight / 2);

    container.scrollTo({
        top: Math.max(0, scrollPosition),
        behavior: 'smooth'
    });

    // Mostrar indicador
    if (indicador) {
        const indicadorTop = (imgHeight * posicionVertical / 100) + 70; // +70 por el header
        indicador.style.top = `${indicadorTop}px`;
        indicador.style.display = 'block';

        let parpadeos = 0;
        const intervalo = setInterval(() => {
            indicador.style.opacity = indicador.style.opacity === '0' ? '1' : '0';
            parpadeos++;
            if (parpadeos >= 6) {
                clearInterval(intervalo);
                indicador.style.display = 'none';
                indicador.style.opacity = '1';
            }
        }, 300);
    }
}

// =============================================================
// 🎛️ AJUSTAR IMAGEN AMPLIADA
// =============================================================
function ajustarImagenAmpliada(tipo, valor) {
    const img = document.getElementById('imagen-ampliada-src');
    if (!img) return;

    if (tipo === 'zoom') {
        imagenControles.zoom = Math.max(0.25, Math.min(4, imagenControles.zoom + valor));
        document.getElementById('zoom-ampliado').textContent = Math.round(imagenControles.zoom * 100) + '%';
    } else if (tipo === 'brillo') {
        imagenControles.brillo = parseInt(valor);
    } else if (tipo === 'contraste') {
        imagenControles.contraste = parseInt(valor);
    } else if (tipo === 'rotacion') {
        imagenControles.rotacion = (imagenControles.rotacion + valor) % 360;
    }

    img.style.transform = `scale(${imagenControles.zoom}) rotate(${imagenControles.rotacion}deg)`;
    img.style.filter = `brightness(${imagenControles.brillo}%) contrast(${imagenControles.contraste}%)`;
}

// =============================================================
// 🔄 RESETEAR IMAGEN AMPLIADA
// =============================================================
function resetearImagenAmpliada() {
    imagenControles = { zoom: 1, brillo: 100, contraste: 100, rotacion: 0 };

    const img = document.getElementById('imagen-ampliada-src');
    if (img) {
        img.style.transform = 'scale(1) rotate(0deg)';
        img.style.filter = 'brightness(100%) contrast(100%)';
    }

    document.getElementById('zoom-ampliado').textContent = '100%';
    document.getElementById('brillo-ampliado-slider').value = 100;
    document.getElementById('contraste-ampliado-slider').value = 100;
}

// Mantener compatibilidad
function ampliarImagen(src) {
    ampliarImagenConControles(src, 50);
}

// =============================================================
// 🔍 BUSCADOR VTEX - Para agregar al modal de edición
// =============================================================
// Agregar estas funciones a productos.js
// =============================================================

// =============================================================
// 🔍 BUSCAR PLU EN VTEX
// =============================================================
async function buscarEnVTEX() {
    const codigo = document.getElementById('vtex-codigo').value.trim();
    const establecimiento = document.getElementById('vtex-establecimiento').value;
    const resultadoDiv = document.getElementById('vtex-resultado');

    if (!codigo) {
        resultadoDiv.innerHTML = `
            <div style="background: #fef3c7; border-left: 4px solid #d97706; padding: 12px; border-radius: 4px;">
                <p style="color: #92400e;">⚠️ Ingresa un código PLU o EAN para buscar</p>
            </div>
        `;
        return;
    }

    if (!establecimiento) {
        resultadoDiv.innerHTML = `
            <div style="background: #fef3c7; border-left: 4px solid #d97706; padding: 12px; border-radius: 4px;">
                <p style="color: #92400e;">⚠️ Selecciona un supermercado</p>
            </div>
        `;
        return;
    }

    // Mostrar loading
    resultadoDiv.innerHTML = `
        <div style="text-align: center; padding: 20px;">
            <div class="loading" style="width: 30px; height: 30px; margin: 0 auto 10px;"></div>
            <p style="color: #6b7280;">Buscando "${codigo}" en ${establecimiento}...</p>
        </div>
    `;

    const apiBase = getApiBase();

    try {
        const response = await fetch(`${apiBase}/api/v2/buscar-vtex/${encodeURIComponent(establecimiento)}/${encodeURIComponent(codigo)}`);
        const data = await response.json();

        if (data.success && data.resultado) {
            const prod = data.resultado;

            resultadoDiv.innerHTML = `
                <div style="background: #d1fae5; border-left: 4px solid #059669; padding: 12px; border-radius: 4px; margin-bottom: 15px;">
                    <p style="color: #059669; font-weight: 600;">✅ Producto encontrado (${data.fuente})</p>
                </div>

                <div style="border: 1px solid #e5e7eb; border-radius: 8px; padding: 15px; background: #f9fafb;">
                    <h4 style="margin: 0 0 12px 0; color: #111;">${prod.nombre}</h4>

                    <table style="width: 100%; font-size: 14px;">
                        <tr>
                            <td style="padding: 6px 0; color: #6b7280; width: 100px;">Marca:</td>
                            <td><strong>${prod.marca || '-'}</strong></td>
                        </tr>
                        <tr>
                            <td style="padding: 6px 0; color: #6b7280;">EAN:</td>
                            <td><code style="background: #e0f2fe; padding: 2px 6px; border-radius: 3px;">${prod.ean || '-'}</code></td>
                        </tr>
                        <tr>
                            <td style="padding: 6px 0; color: #6b7280;">PLU:</td>
                            <td><code style="background: #fef3c7; padding: 2px 6px; border-radius: 3px;">${prod.plu || '-'}</code></td>
                        </tr>
                        <tr>
                            <td style="padding: 6px 0; color: #6b7280;">Precio web:</td>
                            <td style="color: #059669; font-weight: 600;">$${(prod.precio || 0).toLocaleString('es-CO')}</td>
                        </tr>
                        ${prod.presentacion ? `
                        <tr>
                            <td style="padding: 6px 0; color: #6b7280;">Presentación:</td>
                            <td>${prod.presentacion}</td>
                        </tr>
                        ` : ''}
                        ${prod.categoria ? `
                        <tr>
                            <td style="padding: 6px 0; color: #6b7280;">Categoría:</td>
                            <td>${prod.categoria}</td>
                        </tr>
                        ` : ''}
                    </table>

                    ${prod.url ? `
                    <div style="margin-top: 10px;">
                        <a href="${prod.url}" target="_blank" style="color: #2563eb; font-size: 13px;">
                            🔗 Ver en ${establecimiento}
                        </a>
                    </div>
                    ` : ''}

                    <div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #e5e7eb;">
                        <button onclick="aplicarDatosVTEX('${prod.nombre.replace(/'/g, "\\'")}', '${prod.marca || ''}', '${prod.ean || ''}')"
                                style="width: 100%; padding: 10px; background: #059669; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600; font-size: 14px;">
                            📥 Aplicar estos datos al producto
                        </button>
                    </div>
                </div>
            `;
        } else {
            // No encontrado
            resultadoDiv.innerHTML = `
                <div style="background: #fef2f2; border-left: 4px solid #dc2626; padding: 12px; border-radius: 4px;">
                    <p style="color: #dc2626; font-weight: 600;">❌ ${data.error || 'Producto no encontrado'}</p>
                    ${data.sugerencias ? `
                    <ul style="margin: 10px 0 0 0; padding-left: 20px; color: #6b7280; font-size: 13px;">
                        ${data.sugerencias.map(s => `<li>${s}</li>`).join('')}
                    </ul>
                    ` : ''}
                    ${data.supermercados_disponibles ? `
                    <p style="margin-top: 10px; font-size: 13px; color: #6b7280;">
                        Supermercados disponibles: ${data.supermercados_disponibles.join(', ')}
                    </p>
                    ` : ''}
                </div>
            `;
        }

    } catch (error) {
        console.error('❌ Error:', error);
        resultadoDiv.innerHTML = `
            <div style="background: #fef2f2; border-left: 4px solid #dc2626; padding: 12px; border-radius: 4px;">
                <p style="color: #dc2626;">❌ Error de conexión: ${error.message}</p>
            </div>
        `;
    }
}

// =============================================================
// 📥 APLICAR DATOS DE VTEX AL FORMULARIO
// =============================================================
function aplicarDatosVTEX(nombre, marca, ean) {
    console.log('📥 Aplicando datos VTEX:', { nombre, marca, ean });

    // Aplicar al formulario de edición
    const nombreInput = document.getElementById('edit-nombre-norm');
    const marcaInput = document.getElementById('edit-marca');
    const eanInput = document.getElementById('edit-ean');

    if (nombreInput && nombre) {
        nombreInput.value = nombre;
        nombreInput.style.background = '#d1fae5';
        setTimeout(() => nombreInput.style.background = '', 1000);
    }

    if (marcaInput && marca) {
        marcaInput.value = marca;
        marcaInput.style.background = '#d1fae5';
        setTimeout(() => marcaInput.style.background = '', 1000);
    }

    if (eanInput && ean) {
        eanInput.value = ean;
        eanInput.style.background = '#d1fae5';
        setTimeout(() => eanInput.style.background = '', 1000);
    }

    // Mostrar confirmación
    const resultadoDiv = document.getElementById('vtex-resultado');
    if (resultadoDiv) {
        const contenidoActual = resultadoDiv.innerHTML;
        resultadoDiv.innerHTML = `
            <div style="background: #d1fae5; border-left: 4px solid #059669; padding: 15px; border-radius: 4px; text-align: center;">
                <p style="color: #059669; font-weight: 600; font-size: 16px;">✅ Datos aplicados correctamente</p>
                <p style="color: #065f46; margin-top: 5px;">No olvides guardar los cambios</p>
            </div>
        `;
    }

    mostrarAlerta('✅ Datos aplicados al formulario', 'success');
}

// =============================================================
// 🔄 AUTO-LLENAR BUSCADOR CON PLU DEL PRODUCTO
// =============================================================
function autoLlenarBuscadorVTEX() {
    // Buscar el primer PLU del producto
    const primerPlu = document.querySelector('.plu-codigo');
    const primerEst = document.querySelector('.plu-establecimiento');

    if (primerPlu && primerPlu.value) {
        const inputCodigo = document.getElementById('vtex-codigo');
        if (inputCodigo) {
            inputCodigo.value = primerPlu.value;
        }
    }

    if (primerEst && primerEst.value) {
        // Obtener el nombre del establecimiento
        const selectVtex = document.getElementById('vtex-establecimiento');
        if (selectVtex) {
            const nombreEst = primerEst.options[primerEst.selectedIndex]?.text || '';

            // Mapear a las opciones de VTEX
            const mapeo = {
                'OLIMPICA': 'OLIMPICA',
                'OLÍMPICA': 'OLIMPICA',
                'EXITO': 'EXITO',
                'ÉXITO': 'EXITO',
                'CARULLA': 'CARULLA',
                'JUMBO': 'JUMBO',
                'ALKOSTO': 'ALKOSTO',
                'MAKRO': 'MAKRO',
                'COLSUBSIDIO': 'COLSUBSIDIO'
            };

            for (let opt of selectVtex.options) {
                if (nombreEst.toUpperCase().includes(opt.value) ||
                    mapeo[nombreEst.toUpperCase()] === opt.value) {
                    selectVtex.value = opt.value;
                    break;
                }
            }
        }
    }
}

// =============================================================
// 🔍 FUNCIONES VTEX - VERSIÓN LIMPIA Y UNIFICADA
// =============================================================

// Variable global para resultados VTEX
window.resultadosVTEXActuales = [];

// =============================================================
// BUSCAR PRODUCTOS EN VTEX
// =============================================================
async function buscarProductosVTEX() {
    const termino = document.getElementById('vtex-busqueda').value.trim();
    const establecimiento = document.getElementById('vtex-establecimiento').value;
    const contenedor = document.getElementById('vtex-resultados');

    if (!termino || termino.length < 2) {
        contenedor.innerHTML = `
            <div style="padding: 20px; background: #fef3c7; border-radius: 8px; text-align: center; color: #92400e;">
                ⚠️ Ingresa al menos 2 caracteres para buscar
            </div>
        `;
        return;
    }

    // Mostrar loading
    contenedor.innerHTML = `
        <div style="padding: 30px; text-align: center;">
            <div class="loading" style="margin: 0 auto 10px;"></div>
            <p style="color: #6b7280;">Buscando "${termino}" en ${establecimiento}...</p>
        </div>
    `;

    const apiBase = typeof getApiBase === 'function' ? getApiBase() : '';

    try {
        const response = await fetch(
            `${apiBase}/api/v2/buscar-productos/${encodeURIComponent(establecimiento)}?q=${encodeURIComponent(termino)}&limite=15`
        );
        const data = await response.json();

        if (!data.success) {
            contenedor.innerHTML = `
                <div style="padding: 20px; background: #fee2e2; border-radius: 8px; text-align: center; color: #991b1b;">
                    ❌ ${data.error || 'Error en la búsqueda'}
                </div>
            `;
            return;
        }

        if (!data.resultados || data.resultados.length === 0) {
            contenedor.innerHTML = `
                <div style="padding: 20px; background: #f3f4f6; border-radius: 8px; text-align: center; color: #6b7280;">
                    🔍 No se encontraron productos para "${termino}"
                    <br><br>
                    <small>Intenta con menos dígitos o busca por nombre</small>
                </div>
            `;
            return;
        }

        // Guardar resultados en variable global
        window.resultadosVTEXActuales = data.resultados;

        // Renderizar resultados
        let html = `
            <div style="margin-bottom: 10px; font-size: 13px; color: #6b7280;">
                📦 ${data.total} producto(s) encontrado(s)
            </div>
            <div style="max-height: 400px; overflow-y: auto;">
        `;

        data.resultados.forEach((producto, index) => {
            const precioFormateado = producto.precio
                ? `$${producto.precio.toLocaleString('es-CO')}`
                : 'Sin precio';

            html += `
                <div class="vtex-resultado-item" style="
                    display: flex;
                    gap: 12px;
                    padding: 12px;
                    border: 1px solid #e5e7eb;
                    border-radius: 8px;
                    margin-bottom: 8px;
                    background: white;
                    align-items: center;
                    transition: all 0.2s;
                    cursor: pointer;
                " onmouseover="this.style.borderColor='#2563eb'; this.style.background='#f8fafc';"
                   onmouseout="this.style.borderColor='#e5e7eb'; this.style.background='white';"
                   onclick="seleccionarProductoVTEX(${index})">

                    <!-- Imagen -->
                    <div style="flex-shrink: 0;">
                        ${producto.imagen
                    ? `<img src="${producto.imagen}"
                                   style="width: 60px; height: 60px; object-fit: contain; border-radius: 6px; border: 1px solid #e5e7eb;"
                                   onerror="this.style.display='none'">`
                    : `<div style="width: 60px; height: 60px; background: #f3f4f6; border-radius: 6px; display: flex; align-items: center; justify-content: center; color: #9ca3af;">📦</div>`
                }
                    </div>

                    <!-- Info -->
                    <div style="flex: 1; min-width: 0;">
                        <div style="font-weight: 600; color: #111827; margin-bottom: 4px; font-size: 14px; line-height: 1.3;">
                            ${producto.nombre}
                        </div>
                        <div style="font-size: 12px; color: #6b7280; margin-bottom: 4px;">
                            ${producto.marca ? `<span style="background: #e0f2fe; color: #0369a1; padding: 2px 6px; border-radius: 4px; margin-right: 6px;">${producto.marca}</span>` : ''}
                            ${producto.plu ? `PLU: <strong>${producto.plu}</strong>` : ''}
                            ${producto.ean ? ` · EAN: ${producto.ean}` : ''}
                        </div>
                        <div style="font-size: 14px; font-weight: 600; color: #059669;">
                            ${precioFormateado}
                        </div>
                    </div>

                    <!-- Botón -->
                    <div style="flex-shrink: 0;">
                        <span id="btn-usar-${index}" style="
                            display: inline-block;
                            padding: 8px 16px;
                            background: #2563eb;
                            color: white;
                            border-radius: 6px;
                            font-weight: 600;
                            font-size: 13px;
                        ">✓ Usar</span>
                    </div>
                </div>
            `;
        });

        html += `</div>`;
        contenedor.innerHTML = html;

    } catch (error) {
        console.error('Error buscando en VTEX:', error);
        contenedor.innerHTML = `
            <div style="padding: 20px; background: #fee2e2; border-radius: 8px; text-align: center; color: #991b1b;">
                ❌ Error de conexión: ${error.message}
            </div>
        `;
    }
}

// =============================================================
// SELECCIONAR PRODUCTO VTEX - APLICA DATOS AL FORMULARIO
// =============================================================
async function seleccionarProductoVTEX(index) {
    const producto = window.resultadosVTEXActuales[index];
    if (!producto) {
        alert('Error: Producto no encontrado');
        return;
    }

    console.log('✅ Producto VTEX seleccionado:', producto);

    // Cambiar botón a "aplicando..."
    const btn = document.getElementById(`btn-usar-${index}`);
    if (btn) {
        btn.innerHTML = '⏳...';
        btn.style.background = '#6b7280';
    }

    // 1. Aplicar datos al formulario con animación
    const aplicarCampo = (id, valor) => {
        const input = document.getElementById(id);
        if (input && valor) {
            input.value = valor;
            input.style.background = '#d1fae5';
            input.style.transition = 'background 0.3s';
            setTimeout(() => { input.style.background = ''; }, 1500);
        }
    };

    aplicarCampo('edit-nombre-norm', producto.nombre);
    aplicarCampo('edit-marca', producto.marca);
    aplicarCampo('edit-ean', producto.ean);
    aplicarCampo('edit-categoria', producto.categoria);
    aplicarCampo('edit-presentacion', producto.presentacion);

    // 2. Agregar PLU de VTEX
    if (producto.plu) {
        await agregarPLUDeVTEX(producto.plu, producto.establecimiento, producto.precio || 0);
    }

    // 3. Guardar imagen en cache (background, silencioso)
    guardarEnCacheVTEX(producto).catch(err => {
        console.log('⚠️ Cache VTEX:', err.message);
    });

    // 4. Cambiar botón a "aplicado"
    if (btn) {
        btn.innerHTML = '✅';
        btn.style.background = '#10b981';
    }

    // 5. Mostrar confirmación
    const contenedor = document.getElementById('vtex-resultados');
    if (contenedor) {
        contenedor.innerHTML = `
            <div style="background: #d1fae5; border: 2px solid #059669; padding: 20px; border-radius: 8px; text-align: center;">
                <p style="font-size: 24px; margin-bottom: 10px;">✅</p>
                <p style="color: #059669; font-weight: 600; font-size: 16px;">Datos aplicados</p>
                <p style="color: #065f46; font-size: 14px; margin: 10px 0;">${producto.nombre}</p>
                <p style="color: #047857; font-size: 13px; background: #ecfdf5; padding: 10px; border-radius: 6px; margin-top: 10px;">
                    ⬆️ Revisa los datos arriba y haz click en <strong>"💾 Guardar Cambios"</strong>
                </p>
            </div>
        `;
    }

    // 6. Scroll al formulario
    document.getElementById('edit-nombre-norm')?.scrollIntoView({ behavior: 'smooth', block: 'center' });

    if (typeof mostrarAlerta === 'function') {
        mostrarAlerta('✅ Datos aplicados. No olvides guardar.', 'success');
    }
}

// =============================================================
// GUARDAR EN CACHE VTEX (CON IMAGEN) - BACKGROUND
// =============================================================
async function guardarEnCacheVTEX(producto) {
    if (!producto.plu && !producto.ean) {
        return { success: false, error: 'Sin PLU ni EAN' };
    }

    try {
        const response = await fetch('/api/v2/vtex-cache/guardar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                establecimiento: producto.establecimiento || '',
                plu: producto.plu || '',
                ean: producto.ean || '',
                nombre: producto.nombre || '',
                marca: producto.marca || '',
                precio: producto.precio || 0,
                categoria: producto.categoria || '',
                presentacion: producto.presentacion || '',
                url_producto: producto.url || '',
                imagen_url: producto.imagen || ''
            })
        });

        const data = await response.json();

        if (data.success) {
            console.log(`✅ Cache VTEX: ${data.accion} - ${producto.nombre}`);
        }

        return data;

    } catch (error) {
        console.error('Error guardando en cache:', error);
        return { success: false, error: error.message };
    }
}


async function agregarPLUDeVTEX(pluVtex, establecimientoNombre, precio) {
    console.log(`🌐 PLU VTEX: ${pluVtex} de ${establecimientoNombre}`);

    // Cargar establecimientos si no están en cache
    if (typeof cargarEstablecimientosCache === 'function') {
        await cargarEstablecimientosCache();
    }

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) return;

    // Mapear nombre de establecimiento
    const establecimientoMap = {
        'OLIMPICA': 'OLIMPICA', 'OLÍMPICA': 'OLIMPICA',
        'EXITO': 'EXITO', 'ÉXITO': 'EXITO',
        'CARULLA': 'CARULLA', 'JUMBO': 'JUMBO',
        'ALKOSTO': 'ALKOSTO', 'MAKRO': 'MAKRO',
        'COLSUBSIDIO': 'COLSUBSIDIO'
    };

    const estNorm = establecimientoMap[establecimientoNombre?.toUpperCase()] || establecimientoNombre?.toUpperCase();

    // Buscar ID del establecimiento
    let establecimientoId = null;
    if (typeof establecimientosCache !== 'undefined' && establecimientosCache) {
        for (let est of establecimientosCache) {
            if (est.nombre_normalizado?.toUpperCase().includes(estNorm)) {
                establecimientoId = est.id;
                break;
            }
        }
    }

    // ============================================
    // BUSCAR SI YA EXISTE UN PLU PARA ESTE ESTABLECIMIENTO
    // ============================================
    const plusExistentes = contenedor.querySelectorAll('.plu-item');
    let pluExistente = null;

    for (let item of plusExistentes) {
        const selectEst = item.querySelector('.plu-establecimiento');
        if (selectEst && parseInt(selectEst.value) === establecimientoId) {
            pluExistente = item;
            break;
        }
    }

    if (pluExistente) {
        // ============================================
        // REEMPLAZAR EL PLU EXISTENTE
        // ============================================
        console.log(`🔄 Reemplazando PLU existente para establecimiento ${estNorm}`);

        const inputCodigo = pluExistente.querySelector('.plu-codigo');
        const inputPrecio = pluExistente.querySelector('.plu-precio');

        if (inputCodigo) {
            const pluAnterior = inputCodigo.value;
            inputCodigo.value = pluVtex;

            // Animación visual de cambio
            inputCodigo.style.background = '#fef3c7';
            inputCodigo.style.transition = 'background 0.3s';
            setTimeout(() => {
                inputCodigo.style.background = '#dbeafe';
            }, 1500);

            console.log(`   PLU: ${pluAnterior} → ${pluVtex}`);
        }

        if (inputPrecio && precio) {
            inputPrecio.value = precio;
        }

        // Cambiar origen a VTEX
        pluExistente.dataset.origen = 'VTEX';

        // Actualizar badge visual
        const badge = pluExistente.querySelector('span[style*="border-radius: 4px"]');
        if (badge) {
            badge.style.background = '#2563eb';
            badge.style.color = 'white';
            badge.innerHTML = '🌐 VTEX';
        }

        // Actualizar borde
        const pluRow = pluExistente.querySelector('.plu-row');
        if (pluRow) {
            pluRow.style.borderColor = '#2563eb';
            pluRow.style.background = '#eff6ff';
        }

        // Scroll al PLU actualizado
        pluExistente.scrollIntoView({ behavior: 'smooth', block: 'center' });

        if (typeof mostrarAlerta === 'function') {
            mostrarAlerta(`✅ PLU actualizado: ${pluVtex}`, 'success');
        }

    } else {
        // ============================================
        // CREAR NUEVO PLU (no existía para este establecimiento)
        // ============================================
        console.log(`➕ Creando nuevo PLU para establecimiento ${estNorm}`);

        // Quitar mensaje de "no hay PLUs" si existe
        const mensaje = contenedor.querySelector('p');
        if (mensaje) mensaje.remove();

        const optionsHTML = typeof establecimientosCache !== 'undefined' && establecimientosCache
            ? establecimientosCache.map(e =>
                `<option value="${e.id}" ${e.id == establecimientoId ? 'selected' : ''}>${e.nombre_normalizado}</option>`
            ).join('')
            : '';

        const pluDiv = document.createElement('div');
        pluDiv.className = 'plu-item';
        pluDiv.dataset.pluId = '';
        pluDiv.dataset.origen = 'VTEX';

        pluDiv.innerHTML = `
            <div class="plu-row" style="display: grid; grid-template-columns: 1fr 1fr 1fr auto auto; gap: 10px; align-items: end; padding: 12px; border: 2px solid #2563eb; border-radius: 8px; margin-bottom: 10px; background: #eff6ff;">
                <div class="form-group" style="margin: 0;">
                    <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">🌐 Establecimiento</label>
                    <select class="plu-establecimiento" style="width: 100%; padding: 8px; border: 1px solid #93c5fd; border-radius: 4px; background: white;">
                        <option value="">Seleccionar...</option>
                        ${optionsHTML}
                    </select>
                </div>
                <div class="form-group" style="margin: 0;">
                    <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Código PLU</label>
                    <input type="text" class="plu-codigo" value="${pluVtex}" style="width: 100%; padding: 8px; border: 1px solid #93c5fd; border-radius: 4px; background: #dbeafe;">
                </div>
                <div class="form-group" style="margin: 0;">
                    <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Precio</label>
                    <input type="number" class="plu-precio" value="${precio || 0}" min="0" step="1" style="width: 100%; padding: 8px; border: 1px solid #93c5fd; border-radius: 4px;">
                </div>
                <div style="display: flex; align-items: center; padding-bottom: 3px;">
                    <span style="background: #2563eb; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600;">🌐 VTEX</span>
                </div>
                <button type="button" onclick="this.closest('.plu-item').remove();" style="padding: 8px 12px; background: #dc2626; color: white; border: none; border-radius: 4px; cursor: pointer;">🗑️</button>
            </div>
        `;

        contenedor.appendChild(pluDiv);
        pluDiv.scrollIntoView({ behavior: 'smooth', block: 'center' });

        if (typeof mostrarAlerta === 'function') {
            mostrarAlerta(`✅ PLU agregado: ${pluVtex}`, 'success');
        }
    }
}

// Exportar
window.agregarPLUDeVTEX = agregarPLUDeVTEX;

// =============================================================
// CARGAR PLUs DEL PRODUCTO (CON ORIGEN)
// =============================================================
async function cargarPLUsProducto(productoId) {
    console.log(`📋 Cargando PLUs del producto ${productoId}`);

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) return;

    if (typeof cargarEstablecimientosCache === 'function') {
        await cargarEstablecimientosCache();
    }

    const apiBase = typeof getApiBase === 'function' ? getApiBase() : '';

    try {
        const response = await fetch(`${apiBase}/api/v2/productos/${productoId}/plus`);
        if (!response.ok) throw new Error('Error cargando PLUs');

        const data = await response.json();
        contenedor.innerHTML = '';

        if (!data.plus || data.plus.length === 0) {
            contenedor.innerHTML = `
                <p style="color: #6b7280; padding: 15px; text-align: center; background: #f9fafb; border-radius: 8px;">
                    No hay PLUs registrados. Usa el buscador VTEX para agregar.
                </p>
            `;
            return;
        }

        const optionsHTML = typeof establecimientosCache !== 'undefined' && establecimientosCache
            ? establecimientosCache.map(e => `<option value="${e.id}">${e.nombre_normalizado}</option>`).join('')
            : '';

        data.plus.forEach(plu => {
            const origen = plu.origen_codigo || 'FACTURA';
            const esVTEX = origen === 'VTEX';
            const borderColor = esVTEX ? '#2563eb' : '#10b981';
            const bgColor = esVTEX ? '#eff6ff' : '#f0fdf4';
            const badgeColor = esVTEX ? '#2563eb' : '#059669';
            const badgeBg = esVTEX ? '#dbeafe' : '#d1fae5';
            const badgeText = esVTEX ? '🌐 VTEX' : '📄 Factura';

            const pluDiv = document.createElement('div');
            pluDiv.className = 'plu-item';
            pluDiv.dataset.pluId = plu.id || '';
            pluDiv.dataset.origen = origen;

            pluDiv.innerHTML = `
                <div class="plu-row" style="display: grid; grid-template-columns: 1fr 1fr 1fr auto auto; gap: 10px; align-items: end; padding: 12px; border: 2px solid ${borderColor}; border-radius: 8px; margin-bottom: 10px; background: ${bgColor};">
                    <div class="form-group" style="margin: 0;">
                        <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Establecimiento</label>
                        <select class="plu-establecimiento" style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
                            <option value="">Seleccionar...</option>
                            ${optionsHTML.replace(`value="${plu.establecimiento_id}"`, `value="${plu.establecimiento_id}" selected`)}
                        </select>
                    </div>
                    <div class="form-group" style="margin: 0;">
                        <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Código PLU</label>
                        <input type="text" class="plu-codigo" value="${plu.codigo_plu || ''}" style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
                    </div>
                    <div class="form-group" style="margin: 0;">
                        <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Precio</label>
                        <input type="number" class="plu-precio" value="${plu.precio_unitario || 0}" min="0" step="1" style="width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 4px;">
                    </div>
                    <div style="display: flex; align-items: center; padding-bottom: 3px;">
                        <span style="background: ${badgeBg}; color: ${badgeColor}; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600;">${badgeText}</span>
                    </div>
                    <button type="button" onclick="this.closest('.plu-item').remove();" style="padding: 8px 12px; background: #dc2626; color: white; border: none; border-radius: 4px; cursor: pointer;">🗑️</button>
                </div>
            `;

            contenedor.appendChild(pluDiv);
        });

    } catch (error) {
        console.error('❌ Error cargando PLUs:', error);
        contenedor.innerHTML = '<p style="color: #dc2626; padding: 10px;">Error cargando PLUs</p>';
    }
}
// =============================================================
// RECOPILAR PLUs PARA GUARDAR
// =============================================================
function recopilarPLUsParaGuardar() {
    const plusItems = document.querySelectorAll('.plu-item');
    const plus = [];

    plusItems.forEach(item => {
        const pluId = item.dataset.pluId;
        const origen = item.dataset.origen || 'MANUAL';
        const establecimientoSelect = item.querySelector('.plu-establecimiento');
        const codigoInput = item.querySelector('.plu-codigo');
        const precioInput = item.querySelector('.plu-precio');

        if (!establecimientoSelect || !codigoInput) return;

        const establecimientoId = parseInt(establecimientoSelect.value);
        const codigo = codigoInput.value.trim();
        const precio = precioInput ? parseFloat(precioInput.value) || 0 : 0;

        if (!codigo || !establecimientoId) return;

        const pluData = {
            codigo_plu: codigo,
            establecimiento_id: establecimientoId,
            precio_unitario: precio,
            origen_codigo: origen
        };

        if (pluId && pluId !== '' && pluId !== 'undefined' && pluId !== 'null') {
            pluData.id = parseInt(pluId);
        }

        plus.push(pluData);
    });

    console.log('📦 PLUs a guardar:', plus);
    return { plus, plus_a_eliminar: [] };
}

// =============================================================
// AGREGAR PLU MANUAL
// =============================================================
async function agregarPLUEditable() {
    if (typeof cargarEstablecimientosCache === 'function') {
        await cargarEstablecimientosCache();
    }

    const contenedor = document.getElementById('contenedorPLUs');
    if (!contenedor) return;

    // Quitar mensaje vacío
    const mensaje = contenedor.querySelector('p');
    if (mensaje) mensaje.remove();

    const optionsHTML = typeof establecimientosCache !== 'undefined' && establecimientosCache
        ? establecimientosCache.map(e => `<option value="${e.id}">${e.nombre_normalizado}</option>`).join('')
        : '';

    const pluDiv = document.createElement('div');
    pluDiv.className = 'plu-item';
    pluDiv.dataset.pluId = '';
    pluDiv.dataset.origen = 'MANUAL';

    pluDiv.innerHTML = `
        <div class="plu-row" style="display: grid; grid-template-columns: 1fr 1fr 1fr auto auto; gap: 10px; align-items: end; padding: 12px; border: 2px solid #f59e0b; border-radius: 8px; margin-bottom: 10px; background: #fffbeb;">
            <div class="form-group" style="margin: 0;">
                <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Establecimiento</label>
                <select class="plu-establecimiento" style="width: 100%; padding: 8px; border: 1px solid #fcd34d; border-radius: 4px; background: white;">
                    <option value="">Seleccionar...</option>
                    ${optionsHTML}
                </select>
            </div>
            <div class="form-group" style="margin: 0;">
                <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Código PLU</label>
                <input type="text" class="plu-codigo" placeholder="Ej: 1234" style="width: 100%; padding: 8px; border: 1px solid #fcd34d; border-radius: 4px;">
            </div>
            <div class="form-group" style="margin: 0;">
                <label style="display: block; margin-bottom: 5px; font-weight: 500; font-size: 12px;">Precio</label>
                <input type="number" class="plu-precio" placeholder="0" min="0" step="1" style="width: 100%; padding: 8px; border: 1px solid #fcd34d; border-radius: 4px;">
            </div>
            <div style="display: flex; align-items: center; padding-bottom: 3px;">
                <span style="background: #fef3c7; color: #d97706; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 600;">✏️ Manual</span>
            </div>
            <button type="button" onclick="this.closest('.plu-item').remove();" style="padding: 8px 12px; background: #dc2626; color: white; border: none; border-radius: 4px; cursor: pointer;">🗑️</button>
        </div>
    `;

    contenedor.appendChild(pluDiv);
    pluDiv.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

// =============================================================
// CONFIGURAR BUSCADOR VTEX (ENTER)
// =============================================================
function configurarBuscadorVTEX() {
    const input = document.getElementById('vtex-busqueda');
    if (input) {
        input.addEventListener('keypress', function (e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                buscarProductosVTEX();
            }
        });
    }
}

// =============================================================
// EXPORTAR FUNCIONES
// =============================================================
window.buscarProductosVTEX = buscarProductosVTEX;
window.seleccionarProductoVTEX = seleccionarProductoVTEX;
window.agregarPLUDeVTEX = agregarPLUDeVTEX;
window.cargarPLUsProducto = cargarPLUsProducto;
window.recopilarPLUsParaGuardar = recopilarPLUsParaGuardar;
window.agregarPLUEditable = agregarPLUEditable;
window.configurarBuscadorVTEX = configurarBuscadorVTEX;
// =============================================================
// 🖼️ SISTEMA DE IMÁGENES VTEX CACHE - V4.1
// =============================================================

// Cache local de imágenes para evitar llamadas repetidas
let imagenesCache = {};

// Buscar imagen en cache VTEX
async function buscarImagenCache(ean, establecimiento = null) {
    if (!ean) return null;

    const cacheKey = `${ean}-${establecimiento || 'any'}`;
    if (imagenesCache[cacheKey] !== undefined) {
        return imagenesCache[cacheKey];
    }

    const apiBase = getApiBase();

    try {
        let url = `${apiBase}/api/v2/vtex-cache/buscar?q=${encodeURIComponent(ean)}&limite=1`;
        if (establecimiento) {
            url += `&establecimiento=${encodeURIComponent(establecimiento)}`;
        }

        const response = await fetch(url);
        if (!response.ok) return null;

        const data = await response.json();

        if (data.productos && data.productos.length > 0 && data.productos[0].tiene_imagen_local) {
            const resultado = {
                id: data.productos[0].id,
                nombre: data.productos[0].nombre,
                establecimiento: data.productos[0].establecimiento,
                ean: data.productos[0].ean
            };
            imagenesCache[cacheKey] = resultado;
            return resultado;
        }

        imagenesCache[cacheKey] = null;
        return null;

    } catch (error) {
        console.log('⚠️ Error buscando imagen:', error.message);
        return null;
    }
}

// Obtener URL de imagen del cache
function getImagenCacheUrl(cacheId) {
    const apiBase = getApiBase();
    return `${apiBase}/api/v2/vtex-cache/${cacheId}/imagen`;
}

// Ver imagen de producto (desde tabla)
async function verImagenProducto(productoId, ean, nombre) {
    if (!ean) {
        mostrarAlerta('❌ Este producto no tiene EAN', 'warning');
        return;
    }

    // Crear modal si no existe
    let modal = document.getElementById('modal-imagen-producto');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'modal-imagen-producto';
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-content" style="max-width: 500px;">
                <div class="modal-header">
                    <h2>🖼️ Imagen del Producto</h2>
                    <button class="modal-close" onclick="cerrarModal('modal-imagen-producto')">&times;</button>
                </div>
                <div class="modal-body" id="modal-imagen-body" style="text-align: center; padding: 20px;">
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    }

    const body = document.getElementById('modal-imagen-body');
    body.innerHTML = `
        <div style="padding: 30px;">
            <div class="loading" style="margin: 0 auto 15px;"></div>
            <p>Buscando imagen...</p>
        </div>
    `;
    modal.classList.add('active');

    // Buscar imagen
    const imagenInfo = await buscarImagenCache(ean);

    if (imagenInfo) {
        try {
            const response = await fetch(getImagenCacheUrl(imagenInfo.id));
            const data = await response.json();

            if (data.data_url) {
                body.innerHTML = `
                    <img src="${data.data_url}"
                        style="max-width: 100%; max-height: 400px; border-radius: 8px; margin-bottom: 15px;">
                    <div style="text-align: left; background: #f3f4f6; padding: 12px; border-radius: 6px;">
                        <p style="margin: 0 0 5px;"><strong>${nombre || data.nombre}</strong></p>
                        <p style="margin: 0; font-size: 13px; color: #6b7280;">
                            ${data.establecimiento} · EAN: ${ean}
                        </p>
                    </div>
                `;
                return;
            }
        } catch (e) {
            console.log('Error cargando imagen:', e);
        }
    }

    body.innerHTML = `
        <div style="padding: 30px; color: #6b7280;">
            <p style="font-size: 48px;">📷</p>
            <p>Sin imagen disponible</p>
            <p style="font-size: 13px;">EAN: ${ean}</p>
        </div>
    `;
}

// Cargar imagen en modal de edición
async function cargarImagenEnModal(ean) {
    const seccion = document.getElementById('producto-imagen-section');
    if (!seccion) return;

    if (!ean) {
        seccion.style.display = 'none';
        return;
    }

    const imagenInfo = await buscarImagenCache(ean);

    if (!imagenInfo) {
        seccion.style.display = 'none';
        return;
    }

    try {
        const response = await fetch(getImagenCacheUrl(imagenInfo.id));
        const data = await response.json();

        if (data.data_url) {
            const imgElement = document.getElementById('producto-imagen-modal');
            if (imgElement) {
                imgElement.src = data.data_url;
            }

            const infoElement = document.getElementById('producto-imagen-info');
            if (infoElement) {
                infoElement.innerHTML = `
                    <span style="background: #dbeafe; color: #1e40af; padding: 2px 8px; border-radius: 4px; font-size: 11px;">
                        🌐 VTEX Cache
                    </span>
                    <span style="font-size: 12px; color: #6b7280; margin-left: 8px;">
                        ${imagenInfo.establecimiento}
                    </span>
                `;
            }

            seccion.style.display = 'flex';
            return;
        }
    } catch (e) {
        console.log('Error:', e);
    }

    seccion.style.display = 'none';
}

// Exportar nuevas funciones
window.verImagenProducto = verImagenProducto;
window.cargarImagenEnModal = cargarImagenEnModal;
window.buscarImagenCache = buscarImagenCache;

console.log('✅ Sistema de imágenes VTEX Cache cargado');
// =============================================================
// 🖼️ SISTEMA DE IMÁGENES DUAL - V2.0
// =============================================================
// Agregar al final de productos.js
// Muestra lado a lado: Auditoría (foto real) + VTEX (catálogo)
// =============================================================

/**
 * Cargar TODAS las imágenes del producto en el modal de edición
 * Se llama desde editarProducto() después de cargar los datos
 */
async function cargarImagenesDuales(productoId, ean) {
    console.log(`🖼️ Cargando imágenes duales - Producto: ${productoId}, EAN: ${ean}`);

    const seccion = document.getElementById('producto-imagenes-section');
    if (!seccion) {
        console.log('⚠️ Sección de imágenes no encontrada en el DOM');
        return;
    }

    // Resetear estados
    resetearEstadosImagenes();

    // Si no hay EAN, ocultar sección
    if (!ean) {
        seccion.style.display = 'none';
        return;
    }

    // Mostrar sección con loading
    seccion.style.display = 'block';

    const apiBase = typeof getApiBase === 'function' ? getApiBase() : '';

    try {
        // Llamar al endpoint que trae AMBAS imágenes
        const response = await fetch(`${apiBase}/api/v2/productos/${productoId}/imagenes`);
        const data = await response.json();

        if (!data.success) {
            console.log('⚠️ Error obteniendo imágenes:', data.error);
            mostrarSinImagenes();
            return;
        }

        let tieneAlguna = false;

        // 1. Procesar imagen de AUDITORÍA
        if (data.imagenes.auditoria && data.imagenes.auditoria.data_url) {
            mostrarImagenAuditoria(data.imagenes.auditoria);
            tieneAlguna = true;
        } else {
            mostrarSinImagenAuditoria();
        }

        // 2. Procesar imagen de VTEX
        if (data.imagenes.vtex && data.imagenes.vtex.data_url) {
            mostrarImagenVTEX(data.imagenes.vtex);
            tieneAlguna = true;
        } else {
            mostrarSinImagenVTEX();
        }

        // Si no hay ninguna imagen
        if (!tieneAlguna) {
            mostrarSinImagenes();
        }

        console.log(`✅ Imágenes cargadas: Auditoría=${!!data.imagenes.auditoria}, VTEX=${!!data.imagenes.vtex}`);

    } catch (error) {
        console.error('❌ Error cargando imágenes:', error);
        mostrarSinImagenes();
    }
}

/**
 * Resetear estados de las imágenes (loading)
 */
function resetearEstadosImagenes() {
    // Auditoría
    const audLoading = document.getElementById('imagen-auditoria-loading');
    const audImg = document.getElementById('imagen-auditoria-img');
    const audEmpty = document.getElementById('imagen-auditoria-empty');
    const audInfo = document.getElementById('imagen-auditoria-info');

    if (audLoading) audLoading.style.display = 'block';
    if (audImg) { audImg.style.display = 'none'; audImg.src = ''; }
    if (audEmpty) audEmpty.style.display = 'none';
    if (audInfo) { audInfo.style.display = 'none'; audInfo.innerHTML = ''; }

    // VTEX
    const vtexLoading = document.getElementById('imagen-vtex-loading');
    const vtexImg = document.getElementById('imagen-vtex-img');
    const vtexEmpty = document.getElementById('imagen-vtex-empty');
    const vtexInfo = document.getElementById('imagen-vtex-info');

    if (vtexLoading) vtexLoading.style.display = 'block';
    if (vtexImg) { vtexImg.style.display = 'none'; vtexImg.src = ''; }
    if (vtexEmpty) vtexEmpty.style.display = 'none';
    if (vtexInfo) { vtexInfo.style.display = 'none'; vtexInfo.innerHTML = ''; }

    // Mensaje de sin imágenes
    const noDisponibles = document.getElementById('imagenes-no-disponibles');
    if (noDisponibles) noDisponibles.style.display = 'none';
}

/**
 * Mostrar imagen de AUDITORÍA
 */
function mostrarImagenAuditoria(datos) {
    const loading = document.getElementById('imagen-auditoria-loading');
    const img = document.getElementById('imagen-auditoria-img');
    const info = document.getElementById('imagen-auditoria-info');

    if (loading) loading.style.display = 'none';

    if (img && datos.data_url) {
        img.src = datos.data_url;
        img.style.display = 'block';
        img.title = 'Click para ampliar';
    }

    if (info) {
        info.innerHTML = `
            <strong>${datos.nombre || ''}</strong><br>
            ${datos.marca ? `Marca: ${datos.marca}<br>` : ''}
            ${datos.fecha ? `📅 ${formatearFecha(datos.fecha)}` : ''}
        `;
        info.style.display = 'block';
    }
}

/**
 * Mostrar que NO hay imagen de auditoría
 */
function mostrarSinImagenAuditoria() {
    const loading = document.getElementById('imagen-auditoria-loading');
    const empty = document.getElementById('imagen-auditoria-empty');

    if (loading) loading.style.display = 'none';
    if (empty) empty.style.display = 'block';
}

/**
 * Mostrar imagen de VTEX
 */
function mostrarImagenVTEX(datos) {
    const loading = document.getElementById('imagen-vtex-loading');
    const img = document.getElementById('imagen-vtex-img');
    const info = document.getElementById('imagen-vtex-info');

    if (loading) loading.style.display = 'none';

    if (img && datos.data_url) {
        img.src = datos.data_url;
        img.style.display = 'block';
        img.title = 'Click para ampliar';
    }

    if (info) {
        info.innerHTML = `
            <strong>${datos.nombre || ''}</strong><br>
            ${datos.establecimiento ? `📍 ${datos.establecimiento}<br>` : ''}
            ${datos.fecha ? `📅 ${formatearFecha(datos.fecha)}` : ''}
        `;
        info.style.display = 'block';
    }
}

/**
 * Mostrar que NO hay imagen de VTEX
 */
function mostrarSinImagenVTEX() {
    const loading = document.getElementById('imagen-vtex-loading');
    const empty = document.getElementById('imagen-vtex-empty');

    if (loading) loading.style.display = 'none';
    if (empty) empty.style.display = 'block';
}

/**
 * Mostrar mensaje de que no hay NINGUNA imagen
 */
function mostrarSinImagenes() {
    mostrarSinImagenAuditoria();
    mostrarSinImagenVTEX();

    const noDisponibles = document.getElementById('imagenes-no-disponibles');
    if (noDisponibles) {
        noDisponibles.style.display = 'block';
    }
}

/**
 * Ampliar imagen en modal grande
 */
function ampliarImagenModal(src, titulo = 'Imagen del Producto') {
    if (!src) return;

    let modal = document.getElementById('modal-imagen-ampliada-simple');

    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'modal-imagen-ampliada-simple';
        modal.style.cssText = `
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.9);
            z-index: 3000;
            padding: 20px;
            cursor: pointer;
            overflow: auto;
        `;
        modal.onclick = function (e) {
            if (e.target === modal) {
                modal.style.display = 'none';
            }
        };
        document.body.appendChild(modal);
    }

    modal.innerHTML = `
        <div style="
            position: fixed;
            top: 15px;
            left: 50%;
            transform: translateX(-50%);
            background: rgba(0,0,0,0.7);
            color: white;
            padding: 10px 25px;
            border-radius: 25px;
            font-weight: 500;
            z-index: 3001;
        ">
            ${titulo}
            <button onclick="document.getElementById('modal-imagen-ampliada-simple').style.display='none'"
                    style="margin-left: 15px; background: #dc2626; color: white; border: none; padding: 5px 15px; border-radius: 15px; cursor: pointer;">
                ✕ Cerrar
            </button>
        </div>

        <div style="
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100%;
            padding-top: 60px;
        ">
            <img src="${src}" style="
                max-width: 95%;
                max-height: 85vh;
                object-fit: contain;
                border-radius: 8px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.5);
            ">
        </div>

        <div style="
            position: fixed;
            bottom: 20px;
            left: 50%;
            transform: translateX(-50%);
            color: #9ca3af;
            font-size: 13px;
        ">
            Click fuera de la imagen para cerrar
        </div>
    `;

    modal.style.display = 'block';

    // Cerrar con ESC
    const handleEsc = (e) => {
        if (e.key === 'Escape') {
            modal.style.display = 'none';
            document.removeEventListener('keydown', handleEsc);
        }
    };
    document.addEventListener('keydown', handleEsc);
}

/**
 * Formatear fecha ISO a formato legible
 */
function formatearFecha(fechaISO) {
    if (!fechaISO) return '';
    try {
        const fecha = new Date(fechaISO);
        return fecha.toLocaleDateString('es-CO', {
            day: '2-digit',
            month: 'short',
            year: 'numeric'
        });
    } catch (e) {
        return fechaISO;
    }
}

// =============================================================
// 🔧 MODIFICAR editarProducto() PARA CARGAR IMÁGENES
// =============================================================
// Busca la función editarProducto() existente y agrega esta línea
// DESPUÉS de mostrar el modal:
//
//     // Cargar imágenes duales
//     await cargarImagenesDuales(producto.id, producto.codigo_ean);
//
// Ejemplo de dónde agregarlo:
/*
async function editarProducto(id) {
    // ... código existente ...

    // Mostrar modal
    document.getElementById("modal-editar").classList.add("active");

    // 🆕 AGREGAR ESTA LÍNEA:
    await cargarImagenesDuales(id, producto.codigo_ean);

    // ... resto del código ...
}
*/

// =============================================================
// EXPORTAR FUNCIONES
// =============================================================
window.cargarImagenesDuales = cargarImagenesDuales;
window.ampliarImagenModal = ampliarImagenModal;
window.mostrarImagenAuditoria = mostrarImagenAuditoria;
window.mostrarImagenVTEX = mostrarImagenVTEX;
window.resetearEstadosImagenes = resetearEstadosImagenes;

console.log('✅ Sistema de imágenes duales cargado');
