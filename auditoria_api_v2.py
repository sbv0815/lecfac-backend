"""
============================================================================
API AUDITORÍA - ENDPOINTS PARA APP DE ESCANEO
============================================================================
Versión: 2.0
Fecha: 2025-11-29

Endpoints:
- POST /api/auditoria/login - Login de auditores
- GET  /api/productos-referencia/{ean} - Buscar producto por EAN
- POST /api/productos-referencia - Crear producto escaneado
- PUT  /api/productos-referencia/{ean} - Actualizar producto
- POST /api/auditoria/analizar-imagen - Analizar foto con Claude Vision

============================================================================
"""

from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
import jwt
import os
import json
import re
import anthropic

router = APIRouter()

# Configuración
SECRET_KEY = os.getenv("SECRET_KEY", "lecfac-secret-key-2024")
ALGORITHM = "HS256"


# ============================================================================
# MODELOS
# ============================================================================


class LoginRequest(BaseModel):
    email: str
    password: str


class ProductoReferenciaCreate(BaseModel):
    codigo_ean: str
    nombre: str
    marca: Optional[str] = None
    categoria: Optional[str] = None
    presentacion: Optional[str] = None
    unidad_medida: Optional[str] = "unidades"
    fuente: Optional[str] = "AUDITORIA"
    imagen_base64: Optional[str] = None  # Foto del producto
    imagen_mime: Optional[str] = "image/jpeg"


class ProductoReferenciaUpdate(BaseModel):
    nombre: Optional[str] = None
    marca: Optional[str] = None
    categoria: Optional[str] = None
    presentacion: Optional[str] = None
    imagen_base64: Optional[str] = None  # Actualizar foto
    imagen_mime: Optional[str] = None


class ImagenAnalisisRequest(BaseModel):
    imagen_base64: str
    mime_type: str = "image/jpeg"


# ============================================================================
# AUTENTICACIÓN
# ============================================================================


def get_current_user(authorization: str = Header(None)):
    """Valida el token JWT y retorna el usuario"""
    if not authorization:
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Formato de token inválido")

    token = authorization.replace("Bearer ", "")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return {
            "id": payload.get("user_id"),
            "email": payload.get("email"),
            "rol": payload.get("rol"),
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")


# ============================================================================
# ENDPOINT: LOGIN
# ============================================================================


@router.post("/api/auditoria/login")
async def login_auditoria(request: LoginRequest):
    """
    Login para auditores.
    Solo permite usuarios con rol 'auditor' o 'admin'.
    """
    from database import get_db_connection, verify_password

    print(f"🔐 [AUDITORÍA] Intento de login: {request.email}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, email, nombre, password_hash, rol
            FROM usuarios
            WHERE email = %s
        """,
            (request.email,),
        )

        user = cursor.fetchone()

        if not user:
            print(f"❌ [AUDITORÍA] Usuario no encontrado: {request.email}")
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        user_id, email, nombre, password_hash, rol = user

        # Verificar contraseña
        if not verify_password(request.password, password_hash):
            print(f"❌ [AUDITORÍA] Contraseña incorrecta: {request.email}")
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        # Verificar rol
        if rol not in ["auditor", "admin"]:
            print(
                f"❌ [AUDITORÍA] Sin permiso de auditoría: {request.email} (rol: {rol})"
            )
            raise HTTPException(
                status_code=403, detail=f"Sin permiso de auditoría. Tu rol es: {rol}"
            )

        # Generar token
        token_data = {
            "user_id": user_id,
            "email": email,
            "rol": rol,
            "exp": datetime.utcnow() + timedelta(days=30),
        }
        token = jwt.encode(token_data, SECRET_KEY, algorithm=ALGORITHM)

        print(f"✅ [AUDITORÍA] Login exitoso: {email} (rol: {rol})")

        return {
            "success": True,
            "message": "Login exitoso",
            "token": token,
            "user": {"id": user_id, "email": email, "nombre": nombre, "rol": rol},
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en login: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT: BUSCAR PRODUCTO POR EAN
# ============================================================================


@router.get("/api/productos-referencia/{codigo_ean}")
async def buscar_producto_referencia(
    codigo_ean: str, current_user: dict = Depends(get_current_user)
):
    """
    Busca un producto por código EAN en productos_referencia_ean.
    """
    from database import get_db_connection

    print(f"🔍 [AUDITORÍA] Buscando producto referencia: {codigo_ean}")
    print(
        f"✅ Token válido - Usuario: {current_user['id']}, Rol: {current_user['rol']}"
    )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                id, codigo_ean, nombre, marca, presentacion, categoria,
                validaciones, fuente, fecha_creacion,
                CASE WHEN imagen_base64 IS NOT NULL THEN TRUE ELSE FALSE END as tiene_imagen
            FROM productos_referencia_ean
            WHERE codigo_ean = %s
        """,
            (codigo_ean,),
        )

        row = cursor.fetchone()

        if row:
            print(f"✅ [AUDITORÍA] Producto encontrado: {row[2]}")
            return {
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "marca": row[3],
                "presentacion": row[4],
                "categoria": row[5],
                "validaciones": row[6],
                "fuente": row[7],
                "fecha_creacion": row[8].isoformat() if row[8] else None,
                "tiene_imagen": row[9],
            }
        else:
            print(f"⚠️ [AUDITORÍA] Producto no encontrado: {codigo_ean}")
            raise HTTPException(status_code=404, detail="Producto no encontrado")

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error buscando producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT: CREAR PRODUCTO REFERENCIA
# ============================================================================


@router.post("/api/productos-referencia")
async def crear_producto_referencia(
    producto: ProductoReferenciaCreate, current_user: dict = Depends(get_current_user)
):
    """
    Crea un nuevo producto en productos_referencia_ean.
    Si ya existe, incrementa validaciones.
    """
    from database import get_db_connection

    print(f"💾 [AUDITORÍA] Creando producto: {producto.codigo_ean} - {producto.nombre}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Verificar si ya existe
        cursor.execute(
            """
            SELECT id, validaciones FROM productos_referencia_ean
            WHERE codigo_ean = %s
        """,
            (producto.codigo_ean,),
        )

        existente = cursor.fetchone()

        if existente:
            # Actualizar existente e incrementar validaciones
            # Si viene imagen nueva, actualizarla también
            if producto.imagen_base64:
                cursor.execute(
                    """
                    UPDATE productos_referencia_ean
                    SET nombre = %s,
                        marca = COALESCE(%s, marca),
                        presentacion = COALESCE(%s, presentacion),
                        categoria = COALESCE(%s, categoria),
                        imagen_base64 = %s,
                        imagen_mime = %s,
                        validaciones = validaciones + 1,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE codigo_ean = %s
                    RETURNING id, codigo_ean, nombre, marca, presentacion, categoria, validaciones
                """,
                    (
                        producto.nombre.upper(),
                        producto.marca,
                        producto.presentacion,
                        producto.categoria,
                        producto.imagen_base64,
                        producto.imagen_mime or "image/jpeg",
                        producto.codigo_ean,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE productos_referencia_ean
                    SET nombre = %s,
                        marca = COALESCE(%s, marca),
                        presentacion = COALESCE(%s, presentacion),
                        categoria = COALESCE(%s, categoria),
                        validaciones = validaciones + 1,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE codigo_ean = %s
                    RETURNING id, codigo_ean, nombre, marca, presentacion, categoria, validaciones
                """,
                    (
                        producto.nombre.upper(),
                        producto.marca,
                        producto.presentacion,
                        producto.categoria,
                        producto.codigo_ean,
                    ),
                )

            row = cursor.fetchone()
            conn.commit()

            print(f"🔄 [AUDITORÍA] Producto actualizado (validaciones: {row[6]})")

            return {
                "success": True,
                "message": "Producto actualizado",
                "producto": {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "presentacion": row[4],
                    "categoria": row[5],
                    "validaciones": row[6],
                },
            }

        else:
            # Crear nuevo con imagen si viene
            cursor.execute(
                """
                INSERT INTO productos_referencia_ean (
                    codigo_ean, nombre, marca, presentacion, categoria,
                    fuente, usuario_id, validaciones, imagen_base64, imagen_mime
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s, %s)
                RETURNING id, codigo_ean, nombre, marca, presentacion, categoria, validaciones
            """,
                (
                    producto.codigo_ean,
                    producto.nombre.upper(),
                    producto.marca,
                    producto.presentacion,
                    producto.categoria,
                    producto.fuente,
                    current_user["id"],
                    producto.imagen_base64,
                    (
                        producto.imagen_mime or "image/jpeg"
                        if producto.imagen_base64
                        else None
                    ),
                ),
            )

            row = cursor.fetchone()
            conn.commit()

            tiene_imagen = "📷" if producto.imagen_base64 else ""
            print(f"✅ [AUDITORÍA] Producto creado: ID {row[0]} {tiene_imagen}")

            return {
                "success": True,
                "message": "Producto creado exitosamente",
                "producto": {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "presentacion": row[4],
                    "categoria": row[5],
                    "validaciones": row[6],
                    "tiene_imagen": producto.imagen_base64 is not None,
                },
            }

    except Exception as e:
        print(f"❌ [AUDITORÍA] Error creando producto: {e}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT: ACTUALIZAR PRODUCTO REFERENCIA
# ============================================================================


@router.put("/api/productos-referencia/{codigo_ean}")
async def actualizar_producto_referencia(
    codigo_ean: str,
    datos: ProductoReferenciaUpdate,
    current_user: dict = Depends(get_current_user),
):
    """
    Actualiza un producto existente en productos_referencia_ean.
    """
    from database import get_db_connection

    print(f"✏️ [AUDITORÍA] Actualizando producto: {codigo_ean}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Construir UPDATE dinámico
        updates = []
        params = []

        if datos.nombre:
            updates.append("nombre = %s")
            params.append(datos.nombre.upper())
        if datos.marca is not None:
            updates.append("marca = %s")
            params.append(datos.marca)
        if datos.presentacion is not None:
            updates.append("presentacion = %s")
            params.append(datos.presentacion)
        if datos.categoria is not None:
            updates.append("categoria = %s")
            params.append(datos.categoria)
        if datos.imagen_base64 is not None:
            updates.append("imagen_base64 = %s")
            params.append(datos.imagen_base64)
            updates.append("imagen_mime = %s")
            params.append(datos.imagen_mime or "image/jpeg")

        if not updates:
            raise HTTPException(status_code=400, detail="No hay datos para actualizar")

        updates.append("fecha_actualizacion = CURRENT_TIMESTAMP")
        params.append(codigo_ean)

        query = f"""
            UPDATE productos_referencia_ean
            SET {', '.join(updates)}
            WHERE codigo_ean = %s
            RETURNING id, codigo_ean, nombre, marca, presentacion, categoria
        """

        cursor.execute(query, params)
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        conn.commit()

        print(f"✅ [AUDITORÍA] Producto actualizado: {row[2]}")

        return {
            "success": True,
            "message": "Producto actualizado",
            "producto": {
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "marca": row[3],
                "presentacion": row[4],
                "categoria": row[5],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error actualizando producto: {e}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT: OBTENER IMAGEN DEL PRODUCTO
# ============================================================================


@router.get("/api/productos-referencia/{codigo_ean}/imagen")
async def obtener_imagen_producto(
    codigo_ean: str, current_user: dict = Depends(get_current_user)
):
    """
    Obtiene la imagen de un producto por su EAN.
    Retorna la imagen en base64 con su mime type.
    """
    from database import get_db_connection

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT imagen_base64, imagen_mime, nombre
            FROM productos_referencia_ean
            WHERE codigo_ean = %s
        """,
            (codigo_ean,),
        )

        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        if not row[0]:
            raise HTTPException(status_code=404, detail="Producto no tiene imagen")

        print(f"📷 [AUDITORÍA] Imagen obtenida: {row[2]}")

        return {
            "success": True,
            "codigo_ean": codigo_ean,
            "imagen_base64": row[0],
            "imagen_mime": row[1] or "image/jpeg",
            "nombre": row[2],
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error obteniendo imagen: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT: ANALIZAR IMAGEN CON CLAUDE VISION
# ============================================================================


@router.post("/api/auditoria/analizar-imagen")
async def analizar_imagen_producto(request: ImagenAnalisisRequest):
    """
    Analiza una imagen de producto usando Claude Vision.
    Extrae: nombre, marca, presentación, categoría.
    """
    try:
        print("📸 [VISION] Recibida imagen para análisis")
        print(f"   Tamaño base64: {len(request.imagen_base64)} caracteres")

        # Validar API key
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise HTTPException(
                status_code=500, detail="API key de Anthropic no configurada"
            )

        client = anthropic.Anthropic(api_key=api_key)

        # Limpiar base64
        imagen_b64 = request.imagen_base64
        if "," in imagen_b64:
            imagen_b64 = imagen_b64.split(",")[1]

        # Prompt para extracción
        prompt = """Analiza esta imagen de un producto de supermercado colombiano y extrae la información visible.

INSTRUCCIONES:
1. Lee la etiqueta/empaque del producto
2. Extrae los datos que puedas identificar claramente
3. Si no puedes leer algo, déjalo vacío
4. El nombre debe ser descriptivo y en MAYÚSCULAS
5. La presentación incluye peso, volumen o cantidad

RESPONDE SOLO en formato JSON:
{
    "nombre": "NOMBRE DEL PRODUCTO EN MAYÚSCULAS",
    "marca": "Nombre de la marca",
    "presentacion": "cantidad con unidad (ej: 500g, 1L, 12 unidades)",
    "categoria": "categoría sugerida",
    "confianza": 0.9
}

CATEGORÍAS COMUNES EN COLOMBIA:
- Lácteos y Huevos
- Carnes y Embutidos
- Granos y Cereales
- Bebidas
- Snacks y Galletas
- Frutas y Verduras
- Aseo Personal
- Aseo Hogar
- Panadería
- Congelados
- Enlatados
- Salsas y Condimentos

Si la imagen no es clara:
{"nombre": "", "marca": "", "presentacion": "", "categoria": "", "confianza": 0.0}"""

        print("🤖 [VISION] Enviando a Claude...")

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": request.mime_type,
                                "data": imagen_b64,
                            },
                        },
                        {"type": "text", "text": prompt},
                    ],
                }
            ],
        )

        respuesta_texto = message.content[0].text
        print(f"📝 [VISION] Respuesta: {respuesta_texto[:150]}...")

        # Parsear JSON
        json_match = re.search(r"\{[\s\S]*\}", respuesta_texto)

        if json_match:
            datos = json.loads(json_match.group())

            print(f"✅ [VISION] Datos extraídos:")
            print(f"   Nombre: {datos.get('nombre', 'N/A')}")
            print(f"   Marca: {datos.get('marca', 'N/A')}")
            print(f"   Presentación: {datos.get('presentacion', 'N/A')}")

            return {
                "success": True,
                "nombre": datos.get("nombre", ""),
                "marca": datos.get("marca", ""),
                "presentacion": datos.get("presentacion", ""),
                "categoria": datos.get("categoria", ""),
                "confianza": datos.get("confianza", 0.8),
            }
        else:
            print("⚠️ [VISION] No se pudo parsear JSON")
            return {
                "success": False,
                "error": "No se pudo extraer información de la imagen",
            }

    except anthropic.BadRequestError as e:
        print(f"❌ [VISION] Error de Anthropic: {e}")
        raise HTTPException(
            status_code=400, detail=f"Error al procesar imagen: {str(e)}"
        )
    except json.JSONDecodeError as e:
        print(f"❌ [VISION] Error parseando JSON: {e}")
        return {"success": False, "error": "Error al interpretar la respuesta"}
    except Exception as e:
        print(f"❌ [VISION] Error: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT: ESTADÍSTICAS DE AUDITORÍA
# ============================================================================


@router.get("/api/auditoria/estadisticas")
async def obtener_estadisticas_auditoria(
    current_user: dict = Depends(get_current_user),
):
    """
    Obtiene estadísticas de productos escaneados.
    """
    from database import get_db_connection

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Total de productos en referencia
        cursor.execute("SELECT COUNT(*) FROM productos_referencia_ean")
        total = cursor.fetchone()[0]

        # Por categoría
        cursor.execute(
            """
            SELECT categoria, COUNT(*)
            FROM productos_referencia_ean
            WHERE categoria IS NOT NULL
            GROUP BY categoria
            ORDER BY COUNT(*) DESC
        """
        )
        por_categoria = [
            {"categoria": r[0], "cantidad": r[1]} for r in cursor.fetchall()
        ]

        # Productos con más validaciones
        cursor.execute(
            """
            SELECT codigo_ean, nombre, validaciones
            FROM productos_referencia_ean
            ORDER BY validaciones DESC
            LIMIT 10
        """
        )
        mas_validados = [
            {"ean": r[0], "nombre": r[1], "validaciones": r[2]}
            for r in cursor.fetchall()
        ]

        # Mis aportes (del usuario actual)
        cursor.execute(
            """
            SELECT COUNT(*) FROM productos_referencia_ean
            WHERE usuario_id = %s
        """,
            (current_user["id"],),
        )
        mis_aportes = cursor.fetchone()[0]

        return {
            "success": True,
            "estadisticas": {
                "total_productos": total,
                "mis_aportes": mis_aportes,
                "por_categoria": por_categoria,
                "mas_validados": mas_validados,
            },
        }

    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# RESUMEN
# ============================================================================

print("=" * 60)
print("✅ API AUDITORÍA V2.0 CARGADA")
print("   Endpoints:")
print("   - POST /api/auditoria/login")
print("   - GET  /api/productos-referencia/{ean}")
print("   - POST /api/productos-referencia")
print("   - PUT  /api/productos-referencia/{ean}")
print("   - POST /api/auditoria/analizar-imagen")
print("   - GET  /api/auditoria/estadisticas")
print("=" * 60)
