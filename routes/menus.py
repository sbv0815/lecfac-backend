# backend/routes/menus.py

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import json
from anthropic import Anthropic
import os

router = APIRouter()

# ============================================================================
# MODELOS PYDANTIC
# ============================================================================


class MenuRequest(BaseModel):
    tipo_comida: str  # desayuno, almuerzo, cena, merienda, postre
    num_personas: int = 2
    ocasion: str = "casual"  # casual, formal, rapido, dietetico
    preferencias: Optional[List[str]] = []


class MenuResponse(BaseModel):
    success: bool
    menu_id: Optional[int] = None
    receta: Optional[dict] = None
    mensaje: Optional[str] = None


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
    },
    {
      "nombre": "MANTEQUILLA",
      "cantidad": 50,
      "unidad": "gramos",
      "en_inventario": false,
      "codigo_lecfac": null
    }
  ],
  "pasos": [
    "Batir los huevos con sal y pimienta al gusto",
    "Calentar mantequilla en sartén a fuego medio",
    "..."
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
    """Extrae JSON de texto que puede contener markdown o texto adicional"""
    # Intentar encontrar JSON entre ```json y ```
    if "```json" in text:
        start = text.find("```json") + 7
        end = text.find("```", start)
        json_text = text[start:end].strip()
    elif "```" in text:
        start = text.find("```") + 3
        end = text.find("```", start)
        json_text = text[start:end].strip()
    else:
        # Buscar el primer { y el último }
        start = text.find("{")
        end = text.rfind("}") + 1
        json_text = text[start:end].strip()

    return json.loads(json_text)


async def get_inventario_disponible(user_id: int, conn) -> List[dict]:
    """Obtiene productos en inventario con cantidad > 0"""
    query = """
    SELECT
        i.nombre,
        i.cantidad,
        i.unidad,
        i.categoria,
        i.fecha_vencimiento,
        i.codigo_lecfac,
        p.marca,
        p.ean
    FROM inventario_personal i
    LEFT JOIN productos_comunitarios p ON i.codigo_lecfac = p.codigo_lecfac
    WHERE i.user_id = $1 AND i.cantidad > 0
    ORDER BY i.fecha_vencimiento ASC NULLS LAST
    """

    rows = await conn.fetch(query, user_id)
    return [dict(row) for row in rows]


async def save_menu_to_db(
    user_id: int, receta: dict, request: MenuRequest, conn
) -> int:
    """Guarda el menú generado en la base de datos"""
    query = """
    INSERT INTO menus_generados (
        user_id, nombre, descripcion, tipo_comida, num_personas, ocasion,
        tiempo_prep, porciones, dificultad, calorias_aprox, costo_estimado,
        ingredientes, pasos, tags
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    RETURNING id
    """

    row = await conn.fetchrow(
        query,
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
    )

    return row["id"]


# ============================================================================
# ENDPOINT PRINCIPAL
# ============================================================================


@router.post("/api/menus/generar", response_model=MenuResponse)
async def generar_menu(
    request: MenuRequest,
    user_id: int = Depends(get_current_user),  # Ajusta según tu auth
    conn=Depends(get_db_connection),
):
    """
    Genera un menú personalizado usando Claude Haiku 3.5
    basado en el inventario del usuario
    """

    try:
        # 1. Validar entrada
        tipos_validos = ["desayuno", "almuerzo", "cena", "merienda", "postre"]
        if request.tipo_comida not in tipos_validos:
            raise HTTPException(400, f"tipo_comida debe ser uno de: {tipos_validos}")

        if request.num_personas < 1 or request.num_personas > 20:
            raise HTTPException(400, "num_personas debe estar entre 1 y 20")

        # 2. Obtener inventario actual del usuario
        inventario = await get_inventario_disponible(user_id, conn)

        if not inventario:
            return MenuResponse(
                success=False,
                mensaje="No tienes productos en tu inventario. Agrega productos primero.",
            )

        # 3. Preparar prompt para Claude
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

        # 4. Llamar a Claude API
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        message = client.messages.create(
            model="claude-haiku-4-5-20251001",  # Haiku 3.5
            max_tokens=2500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        # 5. Parsear respuesta JSON
        response_text = message.content[0].text

        try:
            receta = json.loads(response_text)
        except json.JSONDecodeError:
            # Si falla, intentar extraer JSON del texto
            receta = extract_json_from_text(response_text)

        # 6. Validar estructura básica
        campos_requeridos = ["nombre", "ingredientes", "pasos"]
        for campo in campos_requeridos:
            if campo not in receta:
                raise ValueError(f"La receta generada no tiene el campo '{campo}'")

        # 7. Guardar en base de datos
        menu_id = await save_menu_to_db(user_id, receta, request, conn)

        # 8. Calcular costo aproximado de la API
        input_tokens = message.usage.input_tokens
        output_tokens = message.usage.output_tokens
        costo_usd = (input_tokens * 0.001 / 1000000) + (output_tokens * 0.005 / 1000000)

        print(f"✅ Menú generado - ID: {menu_id}, Costo: ${costo_usd:.4f}")

        return MenuResponse(success=True, menu_id=menu_id, receta=receta)

    except Exception as e:
        print(f"❌ Error generando menú: {str(e)}")
        raise HTTPException(500, f"Error generando menú: {str(e)}")


# ============================================================================
# ENDPOINTS AUXILIARES
# ============================================================================


@router.get("/api/menus/historial")
async def get_historial_menus(
    user_id: int = Depends(get_current_user), conn=Depends(get_db_connection)
):
    """Obtiene el historial de menús generados del usuario"""
    query = """
    SELECT
        id, nombre, descripcion, tipo_comida, num_personas,
        tiempo_prep, porciones, dificultad, calorias_aprox,
        fecha_generacion, usado, favorito, calificacion
    FROM menus_generados
    WHERE user_id = $1
    ORDER BY fecha_generacion DESC
    LIMIT 50
    """

    rows = await conn.fetch(query, user_id)
    return {"success": True, "menus": [dict(row) for row in rows]}


@router.get("/api/menus/{menu_id}")
async def get_menu_detalle(
    menu_id: int,
    user_id: int = Depends(get_current_user),
    conn=Depends(get_db_connection),
):
    """Obtiene el detalle completo de un menú"""
    query = """
    SELECT * FROM menus_generados
    WHERE id = $1 AND user_id = $2
    """

    row = await conn.fetchrow(query, menu_id, user_id)

    if not row:
        raise HTTPException(404, "Menú no encontrado")

    menu = dict(row)
    # Parsear JSONB a objetos Python
    menu["ingredientes"] = (
        json.loads(menu["ingredientes"]) if menu["ingredientes"] else []
    )
    menu["pasos"] = json.loads(menu["pasos"]) if menu["pasos"] else []
    menu["tags"] = json.loads(menu["tags"]) if menu["tags"] else []

    return {"success": True, "menu": menu}


@router.patch("/api/menus/{menu_id}/favorito")
async def toggle_favorito(
    menu_id: int,
    user_id: int = Depends(get_current_user),
    conn=Depends(get_db_connection),
):
    """Marca/desmarca un menú como favorito"""
    query = """
    UPDATE menus_generados
    SET favorito = NOT favorito
    WHERE id = $1 AND user_id = $2
    RETURNING favorito
    """

    row = await conn.fetchrow(query, menu_id, user_id)

    if not row:
        raise HTTPException(404, "Menú no encontrado")

    return {"success": True, "favorito": row["favorito"]}
