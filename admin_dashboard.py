<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard Admin - LecFac</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #f5f7fa;
        }
        
        .navbar {
            background: linear-gradient(135deg, #1a73e8 0%, #4285f4 100%);
            color: white;
            padding: 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .navbar-header {
            padding: 20px 30px;
            border-bottom: 1px solid rgba(255,255,255,0.2);
        }
        
        .navbar h1 {
            font-size: 24px;
            font-weight: 600;
        }
        
        .tabs {
            display: flex;
            gap: 0;
            padding: 0 30px;
        }
        
        .tab {
            padding: 16px 24px;
            cursor: pointer;
            background: none;
            border: none;
            color: rgba(255,255,255,0.8);
            font-size: 15px;
            font-weight: 500;
            border-bottom: 3px solid transparent;
            transition: all 0.3s;
        }
        
        .tab:hover {
            color: white;
            background: rgba(255,255,255,0.1);
        }
        
        .tab.active {
            color: white;
            border-bottom-color: white;
            background: rgba(255,255,255,0.15);
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 30px;
        }
        
        .tab-content {
            display: none;
            animation: fadeIn 0.3s;
        }
        
        .tab-content.active {
            display: block;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            transition: transform 0.2s;
        }
        
        .stat-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 4px 16px rgba(0,0,0,0.12);
        }
        
        .stat-card .label {
            color: #666;
            font-size: 13px;
            text-transform: uppercase;
            margin-bottom: 8px;
        }
        
        .stat-card .value {
            font-size: 36px;
            font-weight: 700;
            color: #1a73e8;
        }
        
        .card {
            background: white;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            overflow: hidden;
            margin-bottom: 20px;
        }
        
        .card-header {
            padding: 20px 25px;
            border-bottom: 1px solid #e0e0e0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .card-header h2 {
            font-size: 18px;
            font-weight: 600;
        }
        
        .card-body {
            padding: 0;
            overflow-x: auto;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
        }
        
        thead {
            background: #f8f9fa;
        }
        
        th {
            padding: 14px 16px;
            text-align: left;
            font-size: 13px;
            font-weight: 600;
            color: #666;
            text-transform: uppercase;
            border-bottom: 2px solid #e0e0e0;
        }
        
        td {
            padding: 16px;
            border-bottom: 1px solid #f0f0f0;
            font-size: 14px;
        }
        
        tbody tr:hover {
            background: #f8f9fa;
        }
        
        .badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }
        
        .badge-success { background: #e8f5e9; color: #2e7d32; }
        .badge-warning { background: #fff3e0; color: #f57c00; }
        .badge-danger { background: #ffebee; color: #c62828; }
        .badge-info { background: #e3f2fd; color: #1976d2; }
        
        .btn {
            padding: 8px 16px;
            border: none;
            border-radius: 6px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .btn-primary {
            background: #1a73e8;
            color: white;
        }
        
        .btn-primary:hover {
            background: #1557b0;
        }
        
        .btn-danger {
            background: #ea4335;
            color: white;
        }
        
        .btn-danger:hover {
            background: #d33;
        }
        
        .btn-success {
            background: #34a853;
            color: white;
        }
        
        .btn-success:hover {
            background: #2d9148;
        }
        
        .btn-secondary {
            background: #f1f3f4;
            color: #5f6368;
        }
        
        .btn-secondary:hover {
            background: #e8eaed;
        }
        
        .btn-sm {
            padding: 6px 12px;
            font-size: 12px;
        }
        
        .btn-group {
            display: flex;
            gap: 8px;
        }
        
        .checkbox {
            width: 18px;
            height: 18px;
            cursor: pointer;
        }
        
        .toolbar {
            display: flex;
            gap: 10px;
            align-items: center;
            margin-bottom: 20px;
            padding: 15px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        
        .search-box {
            flex: 1;
            padding: 10px 16px;
            border: 2px solid #e0e0e0;
            border-radius: 6px;
            font-size: 14px;
        }
        
        .search-box:focus {
            outline: none;
            border-color: #1a73e8;
        }
        
        .duplicate-card {
            background: white;
            border: 2px solid #ffa726;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 15px;
        }
        
        .duplicate-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        .duplicate-content {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        
        .duplicate-item {
            padding: 15px;
            background: #f8f9fa;
            border-radius: 6px;
        }
        
        .duplicate-item h4 {
            margin-bottom: 8px;
            color: #1a73e8;
        }
        
        .duplicate-item p {
            margin: 4px 0;
            font-size: 13px;
            color: #666;
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }
        
        .spinner {
            border: 3px solid #f3f3f3;
            border-top: 3px solid #1a73e8;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #999;
        }
        
        .toast {
            position: fixed;
            top: 20px;
            right: 20px;
            background: #34a853;
            color: white;
            padding: 16px 24px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            z-index: 10000;
            animation: slideIn 0.3s;
        }
        
        @keyframes slideIn {
            from { transform: translateX(400px); }
            to { transform: translateX(0); }
        }
    </style>
</head>
<body>
    <nav class="navbar">
        <div class="navbar-header">
            <h1>🎯 Dashboard de Administración - LecFac</h1>
        </div>
        <div class="tabs">
            <button class="tab active" onclick="switchTab('stats')">📊 Estadísticas</button>
            <button class="tab" onclick="switchTab('facturas')">🧾 Facturas</button>
            <button class="tab" onclick="switchTab('productos')">📦 Productos</button>
            <button class="tab" onclick="switchTab('duplicados')">⚠️ Duplicados</button>
        </div>
    </nav>
    
    <div class="container">
        <!-- TAB: ESTADÍSTICAS -->
        <div id="tab-stats" class="tab-content active">
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="label">Total Facturas</div>
                    <div class="value" id="stat-facturas">-</div>
                </div>
                <div class="stat-card">
                    <div class="label">Productos Únicos</div>
                    <div class="value" id="stat-productos">-</div>
                </div>
                <div class="stat-card">
                    <div class="label">Duplicados Detectados</div>
                    <div class="value" id="stat-duplicados">-</div>
                </div>
                <div class="stat-card">
                    <div class="label">Pendientes Revisión</div>
                    <div class="value" id="stat-pendientes">-</div>
                </div>
            </div>
            
            <div class="card">
                <div class="card-header">
                    <h2>Resumen del Sistema</h2>
                </div>
                <div class="card-body">
                    <div style="padding: 30px;">
                        <p style="color: #666; line-height: 1.8;">
                            Bienvenido al panel de administración de LecFac. Usa las pestañas superiores para:
                        </p>
                        <ul style="margin: 20px 0; padding-left: 20px; color: #666; line-height: 2;">
                            <li><strong>Facturas:</strong> Ver, editar y eliminar facturas subidas</li>
                            <li><strong>Productos:</strong> Gestionar el catálogo de productos</li>
                            <li><strong>Duplicados:</strong> Detectar y fusionar productos/facturas duplicadas</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- TAB: FACTURAS -->
        <div id="tab-facturas" class="tab-content">
            <div class="toolbar">
                <input type="text" class="search-box" id="searchFacturas" placeholder="🔍 Buscar por establecimiento..." oninput="filtrarFacturas()">
                <button class="btn btn-danger" onclick="eliminarSeleccionadas()" id="btnEliminarFacturas" style="display:none;">
                    Eliminar Seleccionadas (<span id="countSeleccionadas">0</span>)
                </button>
                <button class="btn btn-secondary" onclick="cargarFacturas()">🔄 Recargar</button>
            </div>
            
            <div class="card">
                <div class="card-header">
                    <h2>Lista de Facturas</h2>
                    <button class="btn btn-primary btn-sm" onclick="toggleTodas()">Seleccionar Todas</button>
                </div>
                <div class="card-body">
                    <table>
                        <thead>
                            <tr>
                                <th width="40"><input type="checkbox" id="checkAll" onchange="toggleTodas()"></th>
                                <th>ID</th>
                                <th>Establecimiento</th>
                                <th>Total</th>
                                <th>Productos</th>
                                <th>Fecha</th>
                                <th>Estado</th>
                                <th>Acciones</th>
                            </tr>
                        </thead>
                        <tbody id="facturasBody">
                            <tr><td colspan="8" class="loading"><div class="spinner"></div>Cargando...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <!-- TAB: PRODUCTOS -->
        <div id="tab-productos" class="tab-content">
            <div class="toolbar">
                <input type="text" class="search-box" id="searchProductos" placeholder="🔍 Buscar productos..." oninput="filtrarProductos()">
                <button class="btn btn-secondary" onclick="cargarProductos()">🔄 Recargar</button>
            </div>
            
            <div class="card">
                <div class="card-header">
                    <h2>Catálogo de Productos</h2>
                </div>
                <div class="card-body">
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Código EAN</th>
                                <th>Nombre</th>
                                <th>Visto</th>
                                <th>Precio Min-Max</th>
                                <th>Estado</th>
                                <th>Acciones</th>
                            </tr>
                        </thead>
                        <tbody id="productosBody">
                            <tr><td colspan="7" class="loading"><div class="spinner"></div>Cargando...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <!-- TAB: DUPLICADOS -->
        <div id="tab-duplicados" class="tab-content">
            <div class="toolbar">
                <button class="btn btn-primary" onclick="detectarDuplicados()">🔍 Detectar Duplicados</button>
                <button class="btn btn-secondary" onclick="limpiarDuplicados()">🧹 Limpiar Vista</button>
            </div>
            
            <div id="duplicadosContainer">
                <div class="empty-state">
                    <h3>Haz clic en "Detectar Duplicados" para comenzar</h3>
                    <p>El sistema analizará facturas y productos similares</p>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        const API_URL = 'https://lecfac-api.onrender.com';
        let facturasData = [];
        let productosData = [];
        let facturasSeleccionadas = new Set();
        
        function switchTab(tabName) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            
            event.target.classList.add('active');
            document.getElementById('tab-' + tabName).classList.add('active');
            
            if (tabName === 'stats') cargarEstadisticas();
            if (tabName === 'facturas') cargarFacturas();
            if (tabName === 'productos') cargarProductos();
        }
        
        async function cargarEstadisticas() {
            try {
                const response = await fetch(`${API_URL}/admin/stats`);
                const data = await response.json();
                
                document.getElementById('stat-facturas').textContent = data.total_facturas || 0;
                document.getElementById('stat-productos').textContent = data.productos_unicos || 0;
                document.getElementById('stat-pendientes').textContent = data.facturas_pendientes || 0;
                
                const dupResponse = await fetch(`${API_URL}/admin/duplicados/productos`);
                const dupData = await dupResponse.json();
                document.getElementById('stat-duplicados').textContent = dupData.total || 0;
            } catch (error) {
                console.error('Error cargando estadísticas:', error);
            }
        }
        
        async function cargarFacturas() {
            const tbody = document.getElementById('facturasBody');
            tbody.innerHTML = '<tr><td colspan="8" class="loading"><div class="spinner"></div>Cargando...</td></tr>';
            
            try {
                const response = await fetch(`${API_URL}/admin/facturas`);
                const data = await response.json();
                facturasData = data.facturas || [];
                
                if (facturasData.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="8" class="empty-state"><h3>No hay facturas</h3></td></tr>';
                    return;
                }
                
                renderFacturas(facturasData);
            } catch (error) {
                tbody.innerHTML = `<tr><td colspan="8" class="empty-state"><h3>Error: ${error.message}</h3></td></tr>`;
                console.error('Error cargando facturas:', error);
            }
        }
        
        function renderFacturas(facturas) {
            const tbody = document.getElementById('facturasBody');
            tbody.innerHTML = facturas.map(f => `
                <tr>
                    <td><input type="checkbox" class="checkbox check-factura" value="${f.id}" onchange="updateSeleccion()"></td>
                    <td><strong>#${f.id}</strong></td>
                    <td>${f.establecimiento || 'Sin datos'}</td>
                    <td>$${(f.total_factura || f.total || 0).toLocaleString()}</td>
                    <td>${f.productos || 0}</td>
                    <td>${new Date(f.fecha_cargue || f.fecha).toLocaleDateString()}</td>
                    <td><span class="badge badge
