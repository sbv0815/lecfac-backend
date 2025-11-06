// fix_productos_errors.js
// Script para corregir todos los errores en productos.html

// 1. Función editarProducto corregida
async function editarProducto(id) {
    console.log('✏️ Editando producto:', id);

    try {
        const response = await fetch(`/api/productos/${id}`);
        if (!response.ok) {
            throw new Error('Producto no encontrado');
        }

        const producto = await response.json();

        // Llenar el formulario con los IDs correctos
        document.getElementById('edit-id').value = producto.id;
        document.getElementById('edit-ean').value = producto.codigo_ean || '';
        document.getElementById('edit-nombre-norm').value = producto.nombre_normalizado || '';
        document.getElementById('edit-nombre-com').value = producto.nombre_comercial || '';
        document.getElementById('edit-marca').value = producto.marca || '';
        document.getElementById('edit-categoria').value = producto.categoria || '';
        document.getElementById('edit-subcategoria').value = producto.subcategoria || '';
        document.getElementById('edit-presentacion').value = producto.presentacion || '';

        // Estadísticas
        document.getElementById('edit-veces-comprado').value = producto.veces_comprado || '0';
        document.getElementById('edit-precio-promedio').value = producto.precio_promedio_global ?
            `$${producto.precio_promedio_global.toLocaleString('es-CO')}` : 'Sin datos';
        document.getElementById('edit-num-establecimientos').value = producto.num_establecimientos || '0';

        // Cargar PLUs si existe la función
        if (typeof cargarPLUsProducto === 'function') {
            await cargarPLUsProducto(id);
        }

        // Mostrar modal (sin Bootstrap)
        document.getElementById('modal-editar').classList.add('active');

    } catch (error) {
        console.error('❌ Error:', error);
        alert('Error al cargar producto: ' + error.message);
    }
}

// 2. Función para cerrar modal
function cerrarModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// 3. Función guardarEdicion corregida
async function guardarEdicion(event) {
    event.preventDefault();

    const productoId = document.getElementById('edit-id').value;

    // 1. Guardar datos básicos del producto
    const datosProducto = {
        codigo_ean: document.getElementById('edit-ean').value || null,
        nombre_normalizado: document.getElementById('edit-nombre-norm').value,
        nombre_comercial: document.getElementById('edit-nombre-com').value || null,
        marca: document.getElementById('edit-marca').value || null,
        categoria: document.getElementById('edit-categoria').value || null,
        subcategoria: document.getElementById('edit-subcategoria').value || null,
        presentacion: document.getElementById('edit-presentacion').value || null
    };

    try {
        // Actualizar producto
        const responseProducto = await fetch(`/api/productos/${productoId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(datosProducto)
        });

        if (!responseProducto.ok) {
            throw new Error('Error actualizando producto');
        }

        // 2. Actualizar PLUs si existe la función
        if (typeof recopilarPLUs === 'function') {
            const plus = recopilarPLUs();
            console.log('PLUs a guardar:', plus);

            const responsePLUs = await fetch(`/api/productos/${productoId}/plus`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(plus)
            });

            if (!responsePLUs.ok) {
                throw new Error('Error actualizando PLUs');
            }
        }

        // Cerrar modal
        cerrarModal('modal-editar');

        // Recargar productos
        if (typeof cargarProductos === 'function') {
            cargarProductos();
        }

        // Mostrar mensaje de éxito
        alert('✅ Producto actualizado correctamente');

    } catch (error) {
        console.error('❌ Error guardando:', error);
        alert('Error al guardar: ' + error.message);
    }
}

// 4. Arreglar cargarEstablecimientos
async function cargarEstablecimientos() {
    try {
        // Usar URL con protocolo correcto
        const url = `${window.location.protocol}//${window.location.host}/api/establecimientos`;
        const response = await fetch(url);

        if (response.ok) {
            establecimientosCache = await response.json();
            console.log('✅ Establecimientos cargados:', establecimientosCache.length);
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    } catch (error) {
        console.error('❌ Error cargando establecimientos:', error);
        // Usar establecimientos por defecto
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
    }
}

// 5. Mostrar duplicados sin Bootstrap
function mostrarDuplicados(duplicados) {
    let html = '<h3>Posibles Duplicados Encontrados:</h3><div style="padding: 20px;">';

    duplicados.forEach(dup => {
        html += `
            <div style="border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 5px;">
                <strong>${dup.nombre1}</strong> (ID: ${dup.id1})
                <br>↔️<br>
                <strong>${dup.nombre2}</strong> (ID: ${dup.id2})
                <br>
                <span style="color: #666;">Similitud: ${(dup.similitud * 100).toFixed(1)}%</span>
            </div>
        `;
    });

    html += '</div>';

    // Crear o actualizar contenedor
    let container = document.getElementById('modal-duplicados-simple');
    if (!container) {
        container = document.createElement('div');
        container.id = 'modal-duplicados-simple';
        container.style.cssText = `
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            z-index: 10000;
            max-width: 600px;
            max-height: 80vh;
            overflow-y: auto;
        `;
        document.body.appendChild(container);

        // Agregar botón de cerrar
        html += '<button onclick="document.getElementById(\'modal-duplicados-simple\').style.display=\'none\';" style="padding: 10px 20px; background: #666; color: white; border: none; border-radius: 5px; cursor: pointer; margin-top: 20px;">Cerrar</button>';
    }

    container.innerHTML = html;
    container.style.display = 'block';
}

// Exportar funciones globales
window.editarProducto = editarProducto;
window.cerrarModal = cerrarModal;
window.guardarEdicion = guardarEdicion;
window.cargarEstablecimientos = cargarEstablecimientos;
window.mostrarDuplicados = mostrarDuplicados;

// Inicializar cuando cargue la página
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', cargarEstablecimientos);
} else {
    cargarEstablecimientos();
}

console.log('✅ Fixes aplicados para productos.html');
