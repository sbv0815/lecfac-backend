"""
🔔 Sistema de Notificaciones Push - Backend
Integración con Firebase Cloud Messaging (FCM)

⚠️ PENDIENTE DE ACTIVAR:
1. Crear proyecto Firebase
2. Generar Service Account Key (JSON)
3. Instalar dependencias: pip install firebase-admin
4. Descomentar código de este archivo
"""

import os
from datetime import datetime, timedelta
from database import get_db_connection

# ⚠️ DESCOMENTAR CUANDO FIREBASE ESTÉ CONFIGURADO
# import firebase_admin
# from firebase_admin import credentials, messaging


class NotificationManager:
    """
    Gestor de notificaciones push usando Firebase Cloud Messaging
    """

    _initialized = False

    @classmethod
    def initialize(cls):
        """
        Inicializar Firebase Admin SDK

        ⚠️ Requiere archivo de credenciales:
        - Ir a Firebase Console > Project Settings > Service Accounts
        - Generate New Private Key
        - Guardar como firebase_credentials.json
        """
        if cls._initialized:
            return

        # ⚠️ DESCOMENTAR Y CONFIGURAR
        # try:
        #     cred = credentials.Certificate('firebase_credentials.json')
        #     firebase_admin.initialize_app(cred)
        #     cls._initialized = True
        #     print('✅ Firebase Admin SDK inicializado')
        # except Exception as e:
        #     print(f'❌ Error inicializando Firebase: {e}')
        pass

    @staticmethod
    def send_stock_alert(user_id: int, producto_data: dict):
        """
        Enviar notificación de stock bajo

        Args:
            user_id: ID del usuario
            producto_data: {
                'producto_id': int,
                'nombre': str,
                'dias_restantes': int,
                'cantidad_actual': float
            }
        """
        # ⚠️ DESCOMENTAR PARA ACTIVAR
        # try:
        #     # Obtener token FCM del usuario
        #     token = NotificationManager._get_user_token(user_id)
        #     if not token:
        #         print(f'⚠️ Usuario {user_id} no tiene token FCM')
        #         return False
        #
        #     dias = producto_data['dias_restantes']
        #     nombre = producto_data['nombre']
        #
        #     # Construir mensaje
        #     message = messaging.Message(
        #         notification=messaging.Notification(
        #             title=f'🔔 Stock bajo: {nombre}',
        #             body=f'Necesitarás comprar {nombre} en {dias} {"día" if dias == 1 else "días"}',
        #         ),
        #         data={
        #             'tipo': 'stock_bajo',
        #             'producto_id': str(producto_data['producto_id']),
        #             'producto_nombre': nombre,
        #             'dias_restantes': str(dias),
        #             'cantidad_actual': str(producto_data['cantidad_actual']),
        #         },
        #         token=token,
        #         android=messaging.AndroidConfig(
        #             priority='high',
        #             notification=messaging.AndroidNotification(
        #                 icon='stock_icon',
        #                 color='#4CAF50',
        #                 sound='default',
        #             )
        #         ),
        #         apns=messaging.APNSConfig(
        #             payload=messaging.APNSPayload(
        #                 aps=messaging.Aps(
        #                     sound='default',
        #                     badge=1,
        #                 )
        #             )
        #         )
        #     )
        #
        #     # Enviar
        #     response = messaging.send(message)
        #     print(f'✅ Notificación enviada: {response}')
        #
        #     # Registrar en base de datos
        #     NotificationManager._log_notification(
        #         user_id=user_id,
        #         tipo='stock_bajo',
        #         titulo=f'Stock bajo: {nombre}',
        #         mensaje=f'Necesitarás comprar en {dias} días',
        #         producto_id=producto_data['producto_id'],
        #     )
        #
        #     return True
        #
        # except Exception as e:
        #     print(f'❌ Error enviando notificación: {e}')
        #     return False

        print(
            f"📲 [SIMULADO] Notificación para usuario {user_id}: Stock bajo de {producto_data['nombre']}"
        )
        return True

    @staticmethod
    def send_price_alert(user_id: int, producto_data: dict):
        """
        Enviar notificación de mejor precio

        Args:
            user_id: ID del usuario
            producto_data: {
                'producto_id': int,
                'nombre': str,
                'precio_actual': int,
                'precio_promedio': int,
                'establecimiento': str,
                'ahorro': int
            }
        """
        # Similar a send_stock_alert pero con datos de precio
        print(
            f"📲 [SIMULADO] Notificación para usuario {user_id}: Mejor precio de {producto_data['nombre']}"
        )
        return True

    @staticmethod
    def send_custom_reminder(user_id: int, mensaje: str, producto_id: int = None):
        """
        Enviar recordatorio personalizado
        """
        print(f"📲 [SIMULADO] Recordatorio para usuario {user_id}: {mensaje}")
        return True

    @staticmethod
    def _get_user_token(user_id: int) -> str:
        """
        Obtener token FCM del usuario desde la base de datos
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    SELECT fcm_token, token_updated_at
                    FROM usuarios
                    WHERE id = %s AND fcm_token IS NOT NULL
                """,
                    (user_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT fcm_token, token_updated_at
                    FROM usuarios
                    WHERE id = ? AND fcm_token IS NOT NULL
                """,
                    (user_id,),
                )

            result = cursor.fetchone()
            conn.close()

            if result:
                return result[0]
            return None

        except Exception as e:
            print(f"❌ Error obteniendo token: {e}")
            conn.close()
            return None

    @staticmethod
    def _log_notification(
        user_id: int,
        tipo: str,
        titulo: str,
        mensaje: str,
        producto_id: int = None,
    ):
        """
        Registrar notificación enviada en la base de datos
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    INSERT INTO notificaciones_enviadas
                    (usuario_id, tipo, titulo, mensaje, producto_maestro_id, enviada_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                    (user_id, tipo, titulo, mensaje, producto_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO notificaciones_enviadas
                    (usuario_id, tipo, titulo, mensaje, producto_maestro_id, enviada_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                    (user_id, tipo, titulo, mensaje, producto_id),
                )

            conn.commit()
            conn.close()

        except Exception as e:
            print(f"❌ Error registrando notificación: {e}")
            conn.close()


def verificar_alertas_stock_y_notificar():
    """
    Función que se ejecuta periódicamente (ej: cada hora)
    Verifica qué productos necesitan alerta y envía notificaciones

    ⚠️ CONFIGURAR EN CRON O TASK SCHEDULER:
    - Linux: agregar a crontab
    - Railway: usar Railway Cron Jobs
    - Manual: ejecutar desde endpoint
    """
    print("\n" + "=" * 80)
    print("🔔 VERIFICANDO ALERTAS DE STOCK")
    print("=" * 80)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Buscar productos que necesitan reposición pronto
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.usuario_id,
                    iu.producto_maestro_id,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    iu.fecha_ultima_compra,
                    iu.frecuencia_compra_dias,
                    iu.fecha_estimada_agotamiento,
                    EXTRACT(DAY FROM (iu.fecha_estimada_agotamiento - CURRENT_DATE)) as dias_restantes
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.alerta_activa = TRUE
                  AND iu.fecha_estimada_agotamiento IS NOT NULL
                  AND iu.fecha_estimada_agotamiento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '3 days'
                  AND NOT EXISTS (
                      SELECT 1 FROM notificaciones_enviadas ne
                      WHERE ne.usuario_id = iu.usuario_id
                        AND ne.producto_maestro_id = iu.producto_maestro_id
                        AND ne.tipo = 'stock_bajo'
                        AND ne.enviada_at > CURRENT_DATE - INTERVAL '1 day'
                  )
            """
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.usuario_id,
                    iu.producto_maestro_id,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    iu.fecha_ultima_compra,
                    iu.frecuencia_compra_dias,
                    iu.fecha_estimada_agotamiento,
                    CAST((JULIANDAY(iu.fecha_estimada_agotamiento) - JULIANDAY(DATE('now'))) AS INTEGER) as dias_restantes
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.alerta_activa = 1
                  AND iu.fecha_estimada_agotamiento IS NOT NULL
                  AND iu.fecha_estimada_agotamiento BETWEEN DATE('now') AND DATE('now', '+3 days')
                  AND NOT EXISTS (
                      SELECT 1 FROM notificaciones_enviadas ne
                      WHERE ne.usuario_id = iu.usuario_id
                        AND ne.producto_maestro_id = iu.producto_maestro_id
                        AND ne.tipo = 'stock_bajo'
                        AND ne.enviada_at > DATE('now', '-1 day')
                  )
            """
            )

        productos_alerta = cursor.fetchall()

        print(f"📊 Productos que necesitan alerta: {len(productos_alerta)}")

        notificaciones_enviadas = 0

        for producto in productos_alerta:
            user_id = producto[0]
            producto_id = producto[1]
            nombre = producto[2]
            cantidad = producto[3]
            dias_restantes = producto[7]

            print(f"\n📲 Enviando notificación:")
            print(f"   Usuario: {user_id}")
            print(f"   Producto: {nombre}")
            print(f"   Días restantes: {dias_restantes}")

            # Enviar notificación
            exito = NotificationManager.send_stock_alert(
                user_id=user_id,
                producto_data={
                    "producto_id": producto_id,
                    "nombre": nombre,
                    "dias_restantes": dias_restantes,
                    "cantidad_actual": cantidad,
                },
            )

            if exito:
                notificaciones_enviadas += 1

        conn.close()

        print(f"\n✅ Notificaciones enviadas: {notificaciones_enviadas}")
        print("=" * 80)

        return {
            "success": True,
            "productos_revisados": len(productos_alerta),
            "notificaciones_enviadas": notificaciones_enviadas,
        }

    except Exception as e:
        print(f"❌ Error verificando alertas: {e}")
        conn.close()
        return {"success": False, "error": str(e)}


"""
═══════════════════════════════════════════════════════════════════════════════
📝 SCRIPT SQL PARA CREAR TABLAS NECESARIAS
═══════════════════════════════════════════════════════════════════════════════

-- 1. Agregar columna fcm_token a usuarios
ALTER TABLE usuarios
ADD COLUMN fcm_token VARCHAR(255),
ADD COLUMN token_updated_at TIMESTAMP;

-- 2. Crear tabla de notificaciones enviadas (log)
CREATE TABLE IF NOT EXISTS notificaciones_enviadas (
    id SERIAL PRIMARY KEY,
    usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    tipo VARCHAR(50) NOT NULL,  -- 'stock_bajo', 'mejor_precio', 'recordatorio'
    titulo VARCHAR(255) NOT NULL,
    mensaje TEXT NOT NULL,
    producto_maestro_id INTEGER REFERENCES productos_maestros_v2(id),
    enviada_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    leida BOOLEAN DEFAULT FALSE,
    leida_at TIMESTAMP
);

CREATE INDEX idx_notif_usuario ON notificaciones_enviadas(usuario_id);
CREATE INDEX idx_notif_tipo ON notificaciones_enviadas(tipo);
CREATE INDEX idx_notif_fecha ON notificaciones_enviadas(enviada_at);

═══════════════════════════════════════════════════════════════════════════════
🔧 ENDPOINTS A AGREGAR EN MAIN.PY
═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/notificaciones/token")
async def save_notification_token(request: Request):
    '''Guardar token FCM del dispositivo'''
    data = await request.json()
    user_id = data.get('user_id')
    token = data.get('fcm_token')

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE usuarios
        SET fcm_token = %s, token_updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    ''', (token, user_id))

    conn.commit()
    conn.close()

    return {'success': True, 'message': 'Token guardado'}

@app.get("/api/notificaciones/verificar")
async def verificar_alertas_manual():
    '''Endpoint manual para ejecutar verificación de alertas'''
    resultado = verificar_alertas_stock_y_notificar()
    return resultado

═══════════════════════════════════════════════════════════════════════════════
⏰ CONFIGURAR CRON JOB (EJECUTAR CADA HORA)
═══════════════════════════════════════════════════════════════════════════════

# En Railway (railway.json):
{
  "cron": {
    "schedule": "0 * * * *",  // Cada hora
    "command": "python -c 'from firebase_notifications import verificar_alertas_stock_y_notificar; verificar_alertas_stock_y_notificar()'"
  }
}

# O en Linux crontab:
0 * * * * cd /ruta/proyecto && python3 -c "from firebase_notifications import verificar_alertas_stock_y_notificar; verificar_alertas_stock_y_notificar()"

═══════════════════════════════════════════════════════════════════════════════
"""
