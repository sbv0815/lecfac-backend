// ============================================
// RENDERIZAR PRODUCTO CON CLASIFICACIÓN
// ============================================

function renderizarProductoItem(producto, dupId, numero, seleccionado, resultadoIA, clasificacion) {
    const esSeleccionado = seleccionado === producto.id || 
                          (resultadoIA && resultadoIA.decision === numero);
    
    // Determinar color del badge según tipo de código
    let badgeCodigo = '';
    const tipoCodigo = producto.tipo_codigo || 'otro';
    
    if (tipoCodigo === 'ean_valido') {
        badgeCodigo = '<span class="criteria-tag" style="background: #e8f5e9; color: #2e7d32;">✅ EAN Válido</span>';
    } else if (tipoCodigo === 'ean_invalido') {
        badgeCodigo = '<span class="criteria-tag" style="background: #fff3e0; color: #e65100;">⚠️ EAN Inválido</span>';
    } else if (tipoCodigo === 'sin_codigo') {
        badgeCodigo = '<span class="criteria-tag" style="background: #ffebee; color: #c62828;">❌ Sin Código</span>';
    }
    
    return `
        <div id="producto-${producto.id}" class="duplicate-item ${esSeleccionado ? 'selected' : ''}" data-id="${producto.id}">
            <div class="selection-mark ${esSeleccionado ? 'selected' : 'not-selected'}">${numero}</div>
            
            <div class="duplicate-metadata">
                <h3>Producto #${producto.id} ${badgeCodigo}</h3>
                <p style="margin-top: 5px; color: #666;">
                    <strong>Nombre:</strong> ${producto.nombre}<br>
                    <strong>Código:</strong> ${producto.codigo || 'Sin código'}<br>
                    <strong>Establecimiento:</strong> ${producto.establecimiento || 'Desconocido'}<br>
                    <strong>Precio actual:</strong> $${typeof producto.precio === 'number' ? producto.precio.toLocaleString() : (producto.precio || 'N/A')}<br>
                    <strong>Última actualización:</strong> ${producto.ultima_actualizacion ? new Date(producto.ultima_actualizacion).toLocaleDateString() : 'N/A'}
                </p>
            </div>
            
            ${tipoCodigo === 'ean_invalido' ? `
                <div style="background: #fff3e0; padding: 10px; border-radius: 4px; margin: 10px 0;">
                    <strong>⚠️ Código inválido</strong>
                    <button class="btn btn-secondary btn-sm" onclick="mostrarModalCorregirCodigo(${producto.id}, '${producto.codigo}', '${producto.nombre}')" style="margin-top: 5px;">
                        ✏️ Corregir Código
                    </button>
                </div>
            ` : ''}
            
            <button class="btn ${esSeleccionado ? 'btn-primary' : 'btn-secondary'} select-button" 
                    onclick="seleccionarProducto('${dupId}', ${producto.id})">
                ${esSeleccionado ? '✓ Seleccionado' : 'Seleccionar Este'}
            </button>
        </div>
    `;
}

// ============================================
// RENDERIZAR PÁGINA DE PRODUCTOS CON CLASIFICACIÓN
// ============================================

function renderizarPaginaProductos(pagina) {
    paginaActualProductos = pagina;
    
    const inicio = (pagina - 1) * elementosPorPagina;
    const fin = Math.min(inicio + elementosPorPagina, productosDuplicados.length);
    const duplicadosPagina = productosDuplicados.slice(inicio, fin);
    
    const container = document.getElementById('productos-duplicados-container');
    
    if (duplicadosPagina.length === 0) {
        container.innerHTML = '<div class="empty-state"><h3>No hay más duplicados</h3></div>';
        return;
    }
    
    let html = '';
    
    duplicadosPagina.forEach((dup, localIndex) => {
        const globalIndex = inicio + localIndex;
        const dupId = dup.id || `dup-${globalIndex}`;
        
        const seleccionado = dup.seleccionado || dup.producto1.id;
        const resultadoIA = resultadosProductosIA.find(r => r.dupId === dupId);
        
        // Determinar color del header según clasificación
        let headerClass = '';
        let headerIcon = '';
        let accionRecomendada = '';
        
        if (dup.clasificacion === 'auto') {
            headerClass = 'auto-fusion';
            headerIcon = '✅';
            accionRecomendada = 'Fusión automática segura';
        } else if (dup.clasificacion === 'manual') {
            headerClass = 'manual-review';
            headerIcon = '⚠️';
            accionRecomendada = 'Requiere revisión manual';
        }
        
        html += `
            <div id="duplicado-${dupId}" class="duplicate-container ${headerClass}">
                <div class="duplicate-header">
                    <div>
                        <span class="similarity-tag similarity-${dup.similitud >= 85 ? 'high' : dup.similitud >= 70 ? 'medium' : 'low'}">
                            ${headerIcon} Similitud: ${dup.similitud}% | Confianza: ${dup.confianza}%
                        </span>
                        ${dup.necesita_revision ? '<span class="criteria-tag" style="background: #fff3e0; color: #e65100;">Revisión Manual</span>' : '<span class="criteria-tag" style="background: #e8f5e9; color: #2e7d32;">Fusión Automática</span>'}
                        ${resultadoIA ? `<span class="criteria-tag" style="background: #e3f2fd; color: #1565c0;">IA: Mantener #${resultadoIA.decision}</span>` : ''}
                    </div>
                    <div class="btn-group">
                        ${!dup.necesita_revision ? `
                            <button class="btn btn-success btn-sm" onclick="fusionarProductos('${dupId}')">
                                ✅ Fusionar (Confianza ${dup.confianza}%)
                            </button>
                        ` : `
                            <button class="btn btn-warning btn-sm" onclick="fusionarProductos('${dupId}')">
                                ⚠️ Fusionar (Revisar antes)
                            </button>
                        `}
                    </div>
                </div>
                
                <div style="padding: 10px; background: ${dup.clasificacion === 'auto' ? '#e8f5e9' : '#fff3e0'}; border-radius: 4px; margin-bottom: 15px;">
                    <strong>${headerIcon} ${accionRecomendada}</strong><br>
                    <small>${dup.razon}</small>
                </div>
                
                <div class="duplicate-grid">
                    ${renderizarProductoItem(dup.producto1, dupId, 1, seleccionado, resultadoIA, dup.clasificacion)}
                    ${renderizarProductoItem(dup.producto2, dupId, 2, seleccionado, resultadoIA, dup.clasificacion)}
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
    generarPaginacion('productos', productosDuplicados.length);
}

// ============================================
// MODAL PARA CORREGIR CÓDIGO EAN
// ============================================

function mostrarModalCorregirCodigo(productoId, codigoActual, nombreProducto) {
    const modal = document.createElement('div');
    modal.id = 'modal-corregir-codigo';
    modal.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); display: flex; align-items: center; justify-content: center; z-index: 10000;';
    
    modal.innerHTML = `
        <div style="background: white; padding: 30px; border-radius: 8px; max-width: 500px; width: 90%;">
            <h2 style="margin-bottom: 20px;">✏️ Corregir Código EAN</h2>
            
            <div style="background: #f5f5f5; padding: 15px; border-radius: 4px; margin-bottom: 20px;">
                <strong>Producto:</strong> ${nombreProducto}<br>
                <strong>Código actual:</strong> <code>${codigoActual}</code>
            </div>
            
            <label style="display: block; margin-bottom: 10px;">
                <strong>Nuevo código EAN (13 o 8 dígitos):</strong>
            </label>
            <input type="text" id="nuevo-codigo-ean" placeholder="7702312482231" 
                   style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; margin-bottom: 10px; font-family: monospace; font-size: 16px;">
            
            <div id="validacion-codigo" style="margin-bottom: 15px; min-height: 24px;"></div>
            
            <div style="display: flex; gap: 10px;">
                <button class="btn btn-primary" onclick="guardarCodigoCorregido(${productoId})" id="btn-guardar-codigo">
                    💾 Guardar
                </button>
                <button class="btn btn-secondary" onclick="cerrarModalCorregirCodigo()">
                    ❌ Cancelar
                </button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
    
    // Validación en tiempo real
    const input = document.getElementById('nuevo-codigo-ean');
    const validacionDiv = document.getElementById('validacion-codigo');
    const btnGuardar = document.getElementById('btn-guardar-codigo');
    
    input.addEventListener('input', function() {
        const codigo = this.value.trim();
        
        if (!codigo) {
            validacionDiv.innerHTML = '';
            btnGuardar.disabled = true;
            return;
        }
        
        if (!/^\d+$/.test(codigo)) {
            validacionDiv.innerHTML = '<span style="color: #c62828;">❌ Solo se permiten números</span>';
            btnGuardar.disabled = true;
            return;
        }
        
        if (codigo.length !== 8 && codigo.length !== 13) {
            validacionDiv.innerHTML = '<span style="color: #e65100;">⚠️ Debe tener 8 o 13 dígitos</span>';
            btnGuardar.disabled = true;
            return;
        }
        
        // Validar checksum
        if (validarChecksumEAN(codigo)) {
            validacionDiv.innerHTML = '<span style="color: #2e7d32;">✅ Código EAN válido</span>';
            btnGuardar.disabled = false;
        } else {
            validacionDiv.innerHTML = '<span style="color: #c62828;">❌ Checksum inválido</span>';
            btnGuardar.disabled = true;
        }
    });
    
    input.focus();
}

function validarChecksumEAN(codigo) {
    try {
        const digitos = codigo.split('').map(d => parseInt(d));
        const checksum = digitos[digitos.length - 1];
        
        let suma = 0;
        for (let i = 0; i < digitos.length - 1; i++) {
            if (i % 2 === 0) {
                suma += digitos[i];
            } else {
                suma += digitos[i] * 3;
            }
        }
        
        const checksumCalculado = (10 - (suma % 10)) % 10;
        return checksum === checksumCalculado;
    } catch {
        return false;
    }
}

async function guardarCodigoCorregido(productoId) {
    const codigoNuevo = document.getElementById('nuevo-codigo-ean').value.trim();
    
    if (!codigoNuevo) {
        mostrarToast('❌ Ingrese un código válido', 'error');
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/admin/duplicados/productos/corregir-codigo?producto_id=${productoId}&codigo_nuevo=${codigoNuevo}`, {
            method: 'POST'
        });
        
        const data = await response.json();
        
        if (!response.ok || !data.success) {
            throw new Error(data.detail || 'Error al actualizar código');
        }
        
        mostrarToast('✅ Código EAN actualizado correctamente');
        cerrarModalCorregirCodigo();
        
        // Recargar duplicados
        setTimeout(() => {
            detectarProductosDuplicados();
        }, 500);
        
    } catch (error) {
        console.error('❌ Error:', error);
        mostrarToast('❌ ' + error.message, 'error');
    }
}

function cerrarModalCorregirCodigo() {
    const modal = document.getElementById('modal-corregir-codigo');
    if (modal) {
        modal.remove();
    }
}

// ============================================
// DETECCIÓN CON FILTROS
// ============================================

async function detectarProductosDuplicados() {
    const container = document.getElementById('productos-duplicados-container');
    container.innerHTML = '<div class="loading"><div class="spinner"></div><p>Analizando duplicados con validación inteligente...</p></div>';
    
    try {
        const umbral = document.getElementById('umbralSimilitud')?.value || '85';
        const criterio = document.getElementById('criteriosProductos')?.value || 'todos';
        const incluirManual = document.getElementById('incluirRevisionManual')?.checked ?? true;
        
        console.log(`🔍 Detectando con validación IA: umbral=${umbral}, incluir_manual=${incluirManual}`);
        
        const response = await fetch(`${API_URL}/admin/duplicados/productos?umbral=${umbral}&criterio=${criterio}&incluir_revision_manual=${incluirManual}`);
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        productosDuplicados = data.duplicados || [];
        
        // Actualizar estadísticas
        document.getElementById('total-duplicados').textContent = data.total || 0;
        
        // Mostrar estadísticas de clasificación
        if (data.estadisticas) {
            const stats = data.estadisticas;
            console.log('📊 Estadísticas:', stats);
            
            const statsHTML = `
                <div style="background: #f5f5f5; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
                    <h3 style="margin-bottom: 10px;">📊 Clasificación de Duplicados</h3>
                    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;">
                        <div style="background: #e8f5e9; padding: 10px; border-radius: 4px; text-align: center;">
                            <strong style="color: #2e7d32;">${stats.auto || 0}</strong><br>
                            <small>Fusión Automática</small>
                        </div>
                        <div style="background: #fff3e0; padding: 10px; border-radius: 4px; text-align: center;">
                            <strong style="color: #e65100;">${stats.manual || 0}</strong><br>
                            <small>Revisión Manual</small>
                        </div>
                        <div style="background: #ffebee; padding: 10px; border-radius: 4px; text-align: center;">
                            <strong style="color: #c62828;">${stats.rechazados || 0}</strong><br>
                            <small>Rechazados</small>
                        </div>
                    </div>
                </div>
            `;
            
            container.innerHTML = statsHTML + container.innerHTML;
        }
        
        if (productosDuplicados.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>✅ No se encontraron productos duplicados</h3>
                    <p>Todos los productos son únicos o los duplicados fueron rechazados por validación</p>
                </div>
            `;
            document.getElementById('productos-pagination').innerHTML = '';
            document.getElementById('productos-ia-panel').style.display = 'none';
            return;
        }
        
        console.log(`✅ Encontrados ${productosDuplicados.length} productos duplicados`);
        
        document.getElementById('productos-ia-panel').style.display = 'block';
        renderizarPaginaProductos(1);
        
    } catch (error) {
        console.error('❌ Error detectando duplicados:', error);
        
        container.innerHTML = `
            <div style="padding: 20px; background: #ffebee; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="color: #c62828; margin-bottom: 10px;">❌ Error al buscar duplicados</h3>
                <p>${error.message || 'Error desconocido'}</p>
                <button class="btn btn-primary" onclick="detectarProductosDuplicados()" style="margin-top: 15px;">
                    🔄 Intentar nuevamente
                </button>
            </div>
        `;
        document.getElementById('productos-ia-panel').style.display = 'none';
    }
}

console.log('✅ duplicados.js con validación inteligente cargado');
console.log('📦 Versión: 3.0.0');
console.log('🔧 Funcionalidades: Detección de duplicados + IA + Fusión/Eliminación');
