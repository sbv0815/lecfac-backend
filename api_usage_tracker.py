# api_usage_tracker.py - Sistema de tracking de uso de API Anthropic

from database import get_db_connection

# Precios por modelo (USD por 1M tokens) - Actualizar según Anthropic
PRECIOS_MODELOS = {
    "claude-haiku-4-5-20251001": {"input": 1.00, "output": 5.00},
    "claude-sonnet-4-20250514": {"input": 3.00, "output": 15.00},
    "claude-sonnet-4-5-20250929": {"input": 3.00, "output": 15.00},
}


def registrar_uso_api(
    user_id: int,
    tipo_operacion: str,
    modelo: str,
    tokens_input: int,
    tokens_output: int,
    referencia_id: int = None,
    referencia_tipo: str = None,
    exitoso: bool = True,
    error_mensaje: str = None,
) -> dict:
    """
    Registra el uso de la API de Claude y actualiza los límites del usuario.
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Calcular costos
        precios = PRECIOS_MODELOS.get(modelo, {"input": 3.00, "output": 15.00})
        costo_input = (tokens_input / 1_000_000) * precios["input"]
        costo_output = (tokens_output / 1_000_000) * precios["output"]
        costo_total = costo_input + costo_output
        tokens_total = tokens_input + tokens_output

        # 1. Insertar registro de uso
        cursor.execute(
            """
            INSERT INTO uso_api (
                user_id, tipo_operacion, modelo,
                tokens_input, tokens_output,
                costo_input_usd, costo_output_usd,
                referencia_id, referencia_tipo,
                exitoso, error_mensaje
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """,
            (
                user_id,
                tipo_operacion,
                modelo,
                tokens_input,
                tokens_output,
                costo_input,
                costo_output,
                referencia_id,
                referencia_tipo,
                exitoso,
                error_mensaje,
            ),
        )

        registro_id = cursor.fetchone()[0]

        # 2. Determinar campo adicional a incrementar
        campo_extra = ""
        if tipo_operacion == "ocr_factura":
            campo_extra = (
                ", facturas_usadas_mes = limites_usuario.facturas_usadas_mes + 1"
            )
        elif tipo_operacion == "generar_menu":
            campo_extra = ", menus_usados_mes = limites_usuario.menus_usados_mes + 1"

        # 3. Actualizar o crear límites del usuario
        cursor.execute(
            f"""
            INSERT INTO limites_usuario (user_id, tokens_usados_mes, costo_acumulado_mes, costo_total_historico)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                tokens_usados_mes = limites_usuario.tokens_usados_mes + %s,
                costo_acumulado_mes = limites_usuario.costo_acumulado_mes + %s,
                costo_total_historico = limites_usuario.costo_total_historico + %s,
                updated_at = NOW()
                {campo_extra}
            RETURNING
                tokens_usados_mes, limite_tokens_mes,
                costo_acumulado_mes, plan
        """,
            (
                user_id,
                tokens_total,
                costo_total,
                costo_total,
                tokens_total,
                costo_total,
                costo_total,
            ),
        )

        limites = cursor.fetchone()
        conn.commit()

        # Calcular porcentaje de uso
        porcentaje_usado = (limites[0] / limites[1] * 100) if limites[1] > 0 else 0

        print(f"📊 API Usage registrado:")
        print(f"   👤 User: {user_id} | 📝 {tipo_operacion}")
        print(
            f"   🔢 Tokens: {tokens_input:,} in + {tokens_output:,} out = {tokens_total:,}"
        )
        print(f"   💰 Costo: ${costo_total:.6f} USD")
        print(f"   📈 Uso mes: {porcentaje_usado:.1f}% ({limites[0]:,}/{limites[1]:,})")

        return {
            "success": True,
            "registro_id": registro_id,
            "tokens_usados": tokens_total,
            "costo_usd": costo_total,
            "limites": {
                "tokens_usados_mes": limites[0],
                "limite_tokens_mes": limites[1],
                "porcentaje_usado": porcentaje_usado,
                "costo_mes_usd": float(limites[2]),
                "plan": limites[3],
            },
        }

    except Exception as e:
        conn.rollback()
        print(f"❌ Error registrando uso API: {e}")
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


def verificar_limite_usuario(user_id: int, tipo_operacion: str) -> dict:
    """
    Verifica si el usuario puede realizar la operación según sus límites.
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                plan, tokens_usados_mes, limite_tokens_mes,
                facturas_usadas_mes, limite_facturas_mes,
                menus_usados_mes, limite_menus_mes,
                bloquear_al_limite
            FROM limites_usuario
            WHERE user_id = %s
        """,
            (user_id,),
        )

        row = cursor.fetchone()

        if not row:
            # Usuario nuevo, crear con plan free
            cursor.execute(
                """
                INSERT INTO limites_usuario (user_id, plan)
                VALUES (%s, 'free')
                RETURNING plan, tokens_usados_mes, limite_tokens_mes,
                          facturas_usadas_mes, limite_facturas_mes,
                          menus_usados_mes, limite_menus_mes, bloquear_al_limite
            """,
                (user_id,),
            )
            row = cursor.fetchone()
            conn.commit()

        (
            plan,
            tokens_usados,
            limite_tokens,
            facturas_usadas,
            limite_facturas,
            menus_usados,
            limite_menus,
            bloquear,
        ) = row

        # Verificar límite según tipo de operación
        permitido = True
        razon = None

        if bloquear:
            if tokens_usados >= limite_tokens:
                permitido = False
                razon = f"Límite de tokens mensual alcanzado ({limite_tokens:,})"
            elif tipo_operacion == "ocr_factura" and facturas_usadas >= limite_facturas:
                permitido = False
                razon = f"Límite de facturas mensual alcanzado ({limite_facturas})"
            elif tipo_operacion == "generar_menu" and menus_usados >= limite_menus:
                permitido = False
                razon = f"Límite de menús mensual alcanzado ({limite_menus})"

        return {
            "permitido": permitido,
            "razon": razon,
            "plan": plan,
            "uso": {
                "tokens": {"usado": tokens_usados, "limite": limite_tokens},
                "facturas": {"usado": facturas_usadas, "limite": limite_facturas},
                "menus": {"usado": menus_usados, "limite": limite_menus},
            },
        }

    except Exception as e:
        print(f"❌ Error verificando límites: {e}")
        return {"permitido": True, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


def obtener_estadisticas_usuario(user_id: int) -> dict:
    """
    Obtiene estadísticas completas de uso del usuario.
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Estadísticas del mes actual por operación
        cursor.execute(
            """
            SELECT
                tipo_operacion,
                COUNT(*) as total_operaciones,
                SUM(tokens_total) as total_tokens,
                SUM(costo_total_usd) as total_costo
            FROM uso_api
            WHERE user_id = %s
            AND mes_año = TO_CHAR(NOW(), 'YYYY-MM')
            GROUP BY tipo_operacion
        """,
            (user_id,),
        )

        por_operacion = {}
        for row in cursor.fetchall():
            por_operacion[row[0]] = {
                "operaciones": row[1],
                "tokens": row[2] or 0,
                "costo_usd": float(row[3] or 0),
            }

        # Límites actuales
        cursor.execute(
            """
            SELECT
                plan, tokens_usados_mes, limite_tokens_mes,
                facturas_usadas_mes, limite_facturas_mes,
                menus_usados_mes, limite_menus_mes,
                costo_acumulado_mes, costo_total_historico
            FROM limites_usuario
            WHERE user_id = %s
        """,
            (user_id,),
        )

        limites = cursor.fetchone()

        # Historial últimos 6 meses
        cursor.execute(
            """
            SELECT
                mes_año,
                SUM(tokens_total) as tokens,
                SUM(costo_total_usd) as costo
            FROM uso_api
            WHERE user_id = %s
            GROUP BY mes_año
            ORDER BY mes_año DESC
            LIMIT 6
        """,
            (user_id,),
        )

        historial = []
        for row in cursor.fetchall():
            historial.append(
                {"mes": row[0], "tokens": row[1] or 0, "costo_usd": float(row[2] or 0)}
            )

        return {
            "success": True,
            "mes_actual": por_operacion,
            "limites": {
                "plan": limites[0] if limites else "free",
                "tokens": {
                    "usado": limites[1] if limites else 0,
                    "limite": limites[2] if limites else 100000,
                },
                "facturas": {
                    "usado": limites[3] if limites else 0,
                    "limite": limites[4] if limites else 10,
                },
                "menus": {
                    "usado": limites[5] if limites else 0,
                    "limite": limites[6] if limites else 20,
                },
                "costo_mes_usd": float(limites[7]) if limites else 0,
                "costo_total_usd": float(limites[8]) if limites else 0,
            },
            "historial": historial,
        }

    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


print("✅ API Usage Tracker cargado")
