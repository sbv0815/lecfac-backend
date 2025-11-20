# menus.py - Sistema de generación de menús con IA (CON TRACKING)

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional
import json
from anthropic import Anthropic
import os

# ✅ IMPORTAR TRACKER
from api_usage_tracker import registrar_uso_api, verificar_limite_usuario

router = APIRouter(prefix="/api/menus", tags=["Menús"])

# ============================================================================
# MODELOS PYDANTIC
# ============================================================================


class MenuRequest(BaseModel):
    tipo_comida: str
    num_personas: int = 2
    ocasion: str = "casual"
    preferencias: Optional[List[str]] = []


class MenuResponse(BaseModel):
    success: bool
    menu_id: Optional[int] = None
    receta: Optional[dict] = None
    mensaje: Optional[str] = None
    uso_api: Optional[dict] = None  # ✅ NUEVO: Info de uso


# ============================================================================
# PROMPT PARA CLAUDE
# ============================================================================

SYSTEM_PROMPT = """Eres un chef profesional colombiano especializado en crear recetas
usando ingredientes disponibles. Generas menús creativos, realistas y deliciosos
optimizando lo que el usuario tiene en casa.

REGLAS CRÍTICAS:
1. USA PRIMERO los ingredientes del inventario disponible (máximo aprovechamiento)
2. Si falta algo crítico, sugiere MÁXIMO 2-3 ingredientes adicionales económicos
3. Recetas típicas colombianas o internacionales adaptadas a productos locales
4. Cantidades precisas en gramos/unidades/cucharadas
5. Tiempo de preparación realista (considerar habilidad promedio)
6. Pasos claros, concisos y numerados
7. SIEMPRE responde en formato JSON válido (sin markdown, sin ```json)

FORMATO JSON REQUERIDO:
{
  "nombre": "Nombre atractivo del plato",
  "descripcion": "Breve descripción apetitosa (máx 100 caracteres)",
  "tiempo_prep": 25,
  "porciones": 4,
  "dificultad": "fácil",
  "ingredientes": [
    {
      "nombre": "HUEVOS AA",
      "cantidad": 8,
      "unidad": "unidades",
      "en_inventario": true,
      "codigo_lecfac": "lf_huevos-aa"
    }
  ],
  "pasos": [
    "Batir los huevos con sal y pimienta al gusto",
    "Calentar mantequilla en sartén a fuego medio"
  ],
  "calorias_aprox": 450,
  "costo_estimado": 15000,
  "tags": ["desayuno", "proteico", "rápido"]
}

IMPORTANTE:
- Marca en_inventario=true solo para ingredientes que están en el inventario proporcionado
- Incluye codigo_lecfac cuando el ingrediente esté en el inventario
- Sé generoso con las cantidades (mejor que sobre a que falte)
- Prioriza recetas que usen ingredientes próximos a vencer"""


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================


def format_inventario_para_prompt(inventario: List[dict]) -> str:
    """Convierte el inventario en formato legible para Claude"""
    if not inventario:
        return "El usuario no tiene productos en inventario."

    items = []
    for item in inventario:
        vencimiento = item.get("fecha_vencimiento", "Sin fecha")
        items.append(
            f"- {item['nombre']}: {item['cantidad']} {item['unidad']} "
            f"(Categoría: {item.get('categoria', 'Sin categoría')}, "
            f"Vence: {vencimiento}, codigo_lecfac: {item.get('codigo_lecfac', 'N/A')})"
        )

    return "\n".join(items)


def extract_json_from_text(text: str) -> dict:
    """Extrae JSON de texto que puede contener markdown"""
    try:
        if "```json" in text:
            start = text.find("```json") + 7
            end = text.find("```", start)
            json_text = text[start:end].strip()
        elif "```" in text:
            start = text.find("```") + 3
            end = text.find("```", start)
            json_text = text[start:end].strip()
        else:
            start = text.find("{")
            end = text.rfind("}") + 1
            json_text = text[start:end].strip()

        return json.loads(json_text)
    except Exception as e:
        print(f"❌ Error extrayendo JSON: {str(e)}")
        raise


def get_inventario_disponible(user_id: int) -> List[dict]:
    """Obtiene productos en inventario con cantidad > 0"""
    from database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT
            pm.nombre_consolidado as nombre,
            i.cantidad_actual as cantidad,
            i.unidad_medida as unidad,
            'Sin categoría' as categoria,
            i.fecha_estimada_agotamiento as fecha_vencimiento,
            pm.codigo_lecfac,
            COALESCE(i.marca, pm.marca) as marca,
            pm.codigo_ean as ean
        FROM inventario_usuario i
        LEFT JOIN productos_maestros_v2 pm ON i.producto_maestro_id = pm.id
        WHERE i.usuario_id = %s AND i.cantidad_actual > 0
        ORDER BY i.fecha_estimada_agotamiento ASC NULLS LAST
        """

        cursor.execute(query, (user_id,))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()


def save_menu_to_db(user_id: int, receta: dict, request: MenuRequest) -> int:
    """Guarda el menú generado en la base de datos"""
    from database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        INSERT INTO menus_generados (
            user_id, nombre, descripcion, tipo_comida, num_personas, ocasion,
            tiempo_prep, porciones, dificultad, calorias_aprox, costo_estimado,
            ingredientes, pasos, tags
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """

        cursor.execute(
            query,
            (
                user_id,
                receta.get("nombre"),
                receta.get("descripcion"),
                request.tipo_comida,
                request.num_personas,
                request.ocasion,
                receta.get("tiempo_prep"),
                receta.get("porciones"),
                receta.get("dificultad"),
                receta.get("calorias_aprox"),
                receta.get("costo_estimado"),
                json.dumps(receta.get("ingredientes", [])),
                json.dumps(receta.get("pasos", [])),
                json.dumps(receta.get("tags", [])),
            ),
        )

        menu_id = cursor.fetchone()[0]
        conn.commit()

        return menu_id
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINTS
# ============================================================================


@router.post("/generar", response_model=MenuResponse)
async def generar_menu(
    request: MenuRequest, user_id: int = Header(..., alias="X-User-ID")
):
    """Genera un menú personalizado usando Claude Haiku 3.5"""

    try:
        # ✅ PASO 1: VERIFICAR LÍMITES ANTES DE LLAMAR A CLAUDE
        verificacion = verificar_limite_usuario(user_id, "generar_menu")

        if not verificacion["permitido"]:
            return MenuResponse(
                success=False,
                mensaje=f"⚠️ {verificacion['razon']}. Actualiza tu plan para continuar.",
                uso_api={"plan": verificacion["plan"], "uso": verificacion["uso"]},
            )

        print(f"✅ Usuario {user_id} puede generar menú (Plan: {verificacion['plan']})")

        # Validaciones
        tipos_validos = ["desayuno", "almuerzo", "cena", "merienda", "postre"]
        if request.tipo_comida not in tipos_validos:
            raise HTTPException(400, f"tipo_comida debe ser uno de: {tipos_validos}")

        if request.num_personas < 1 or request.num_personas > 20:
            raise HTTPException(400, "num_personas debe estar entre 1 y 20")

        # Obtener inventario
        inventario = get_inventario_disponible(user_id)

        if not inventario:
            return MenuResponse(
                success=False,
                mensaje="No tienes productos en tu inventario. Agrega productos primero.",
            )

        print(f"📦 Inventario obtenido: {len(inventario)} productos")

        # Preparar prompt
        inventario_str = format_inventario_para_prompt(inventario)
        preferencias_str = (
            ", ".join(request.preferencias) if request.preferencias else "Ninguna"
        )

        user_prompt = f"""
INVENTARIO DISPONIBLE DEL USUARIO:
{inventario_str}

SOLICITUD DEL USUARIO:
- Tipo de comida: {request.tipo_comida}
- Número de personas: {request.num_personas}
- Ocasión: {request.ocasion}
- Preferencias especiales: {preferencias_str}

Genera UNA receta deliciosa y creativa usando PRINCIPALMENTE los ingredientes del inventario.
Responde ÚNICAMENTE con el JSON, sin texto adicional ni markdown.
"""

        # Verificar API key
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise HTTPException(500, "ANTHROPIC_API_KEY no configurada")

        client = Anthropic(api_key=api_key)
        modelo = "claude-haiku-4-5-20251001"

        print(f"🤖 Llamando a Claude ({modelo})...")

        # ✅ LLAMAR A CLAUDE
        message = client.messages.create(
            model=modelo,
            max_tokens=2500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        response_text = message.content[0].text
        input_tokens = message.usage.input_tokens
        output_tokens = message.usage.output_tokens

        print(f"📄 Respuesta de Claude (primeros 200 chars): {response_text[:200]}")

        # Parsear JSON
        try:
            receta = json.loads(response_text)
        except json.JSONDecodeError:
            print("⚠️ Respuesta no es JSON puro, intentando extraer...")
            receta = extract_json_from_text(response_text)

        # Validar campos requeridos
        campos_requeridos = ["nombre", "ingredientes", "pasos"]
        for campo in campos_requeridos:
            if campo not in receta:
                raise ValueError(f"La receta generada no tiene el campo '{campo}'")

        print(f"✅ Receta generada: {receta['nombre']}")

        # Guardar en BD
        menu_id = save_menu_to_db(user_id, receta, request)
        print(f"✅ Menú guardado - ID: {menu_id}")

        # ✅ PASO 2: REGISTRAR USO DE API
        uso_resultado = registrar_uso_api(
            user_id=user_id,
            tipo_operacion="generar_menu",
            modelo=modelo,
            tokens_input=input_tokens,
            tokens_output=output_tokens,
            referencia_id=menu_id,
            referencia_tipo="menu",
            exitoso=True,
        )

        return MenuResponse(
            success=True,
            menu_id=menu_id,
            receta=receta,
            uso_api=uso_resultado.get("limites") if uso_resultado["success"] else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error generando menú: {str(e)}")
        import traceback

        traceback.print_exc()

        # ✅ REGISTRAR ERROR TAMBIÉN
        try:
            registrar_uso_api(
                user_id=user_id,
                tipo_operacion="generar_menu",
                modelo="claude-haiku-4-5-20251001",
                tokens_input=0,
                tokens_output=0,
                exitoso=False,
                error_mensaje=str(e),
            )
        except:
            pass

        raise HTTPException(500, f"Error generando menú: {str(e)}")


@router.get("/historial")
def get_historial_menus(user_id: int = Header(..., alias="X-User-ID")):
    """Obtiene el historial de menús generados del usuario"""
    from database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT
            id, nombre, descripcion, tipo_comida, num_personas,
            tiempo_prep, porciones, dificultad, calorias_aprox,
            fecha_generacion, usado, favorito, calificacion
        FROM menus_generados
        WHERE user_id = %s
        ORDER BY fecha_generacion DESC
        LIMIT 50
        """

        cursor.execute(query, (user_id,))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        return {"success": True, "menus": [dict(zip(columns, row)) for row in rows]}
    finally:
        cursor.close()
        conn.close()


@router.get("/{menu_id}")
def get_menu_detalle(menu_id: int, user_id: int = Header(..., alias="X-User-ID")):
    """Obtiene el detalle completo de un menú"""
    from database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT * FROM menus_generados
        WHERE id = %s AND user_id = %s
        """

        cursor.execute(query, (menu_id, user_id))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(404, "Menú no encontrado")

        columns = [desc[0] for desc in cursor.description]
        menu = dict(zip(columns, row))

        menu["ingredientes"] = (
            json.loads(menu["ingredientes"]) if menu["ingredientes"] else []
        )
        menu["pasos"] = json.loads(menu["pasos"]) if menu["pasos"] else []
        menu["tags"] = json.loads(menu["tags"]) if menu["tags"] else []

        return {"success": True, "menu": menu}
    finally:
        cursor.close()
        conn.close()


@router.patch("/{menu_id}/favorito")
def toggle_favorito(menu_id: int, user_id: int = Header(..., alias="X-User-ID")):
    """Marca/desmarca un menú como favorito"""
    from database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        UPDATE menus_generados
        SET favorito = NOT favorito
        WHERE id = %s AND user_id = %s
        RETURNING favorito
        """

        cursor.execute(query, (menu_id, user_id))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(404, "Menú no encontrado")

        conn.commit()
        return {"success": True, "favorito": row[0]}
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


@router.patch("/{menu_id}/usado")
def marcar_usado(menu_id: int, user_id: int = Header(..., alias="X-User-ID")):
    """Marca un menú como usado (ya cocinado)"""
    from database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        UPDATE menus_generados
        SET usado = true
        WHERE id = %s AND user_id = %s
        RETURNING usado
        """

        cursor.execute(query, (menu_id, user_id))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(404, "Menú no encontrado")

        conn.commit()
        return {"success": True, "usado": row[0]}
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


print("✅ Módulo menus.py cargado (con tracking de uso)")
