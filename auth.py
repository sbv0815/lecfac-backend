"""
Sistema de Autenticación con JWT para LecFac
Incluye: Registro, Login, Recuperación de Contraseña, Protección de Endpoints
"""

import os
import jwt
import secrets
import string
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, validator
from database import get_db_connection, hash_password, verify_password

# ============================================
# CONFIGURACIÓN
# ============================================

# Secret key para JWT (en producción debe estar en variable de entorno)
SECRET_KEY = os.environ.get(
    "JWT_SECRET_KEY", "tu_clave_secreta_super_segura_cambiar_en_produccion"
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7  # 7 días para desarrollo, ajustar según necesidad

# Sistema de seguridad HTTP Bearer
security = HTTPBearer()

# Router de FastAPI
router = APIRouter(prefix="/auth", tags=["Autenticación"])


# ============================================
# MODELOS PYDANTIC
# ============================================


# ============================================
# MODELOS PYDANTIC
# ============================================


class UserRegister(BaseModel):
    email: EmailStr
    password: str
    nombres: Optional[str] = None
    apellidos: Optional[str] = None
    celular: Optional[str] = None
    # ✅ NUEVOS CAMPOS - Aceptación de términos
    acepto_politica_privacidad: bool = False
    acepto_tratamiento_datos: bool = False

    @validator("password")
    def validate_password(cls, v):
        if len(v) < 6:
            raise ValueError("La contraseña debe tener al menos 6 caracteres")
        return v

    @validator("email")
    def validate_email(cls, v):
        return v.lower().strip()

    # ✅ NUEVO: Validar que ambos términos estén aceptados
    @validator("acepto_tratamiento_datos")
    def validate_terminos(cls, v, values):
        if not values.get("acepto_politica_privacidad", False):
            raise ValueError("Debe aceptar la Política de Privacidad")
        if not v:
            raise ValueError("Debe aceptar la Autorización de Tratamiento de Datos")
        return v


class UserLogin(BaseModel):
    email: EmailStr
    password: str

    @validator("email")
    def validate_email(cls, v):
        return v.lower().strip()


class PasswordResetRequest(BaseModel):
    email: EmailStr

    @validator("email")
    def validate_email(cls, v):
        return v.lower().strip()


class PasswordResetConfirm(BaseModel):
    email: EmailStr
    reset_code: str
    new_password: str

    @validator("email")
    def validate_email(cls, v):
        return v.lower().strip()

    @validator("new_password")
    def validate_password(cls, v):
        if len(v) < 6:
            raise ValueError("La contraseña debe tener al menos 6 caracteres")
        return v


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    usuario: dict


class MessageResponse(BaseModel):
    message: str
    data: Optional[dict] = None


# ============================================
# FUNCIONES DE TOKENS JWT
# ============================================


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Crea un token JWT

    Args:
        data: Diccionario con los datos a incluir en el token (normalmente user_id y email)
        expires_delta: Tiempo de expiración personalizado

    Returns:
        Token JWT como string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_jwt


def decode_token(token: str) -> dict:
    """
    Decodifica y valida un token JWT

    Args:
        token: Token JWT como string

    Returns:
        Diccionario con los datos del token

    Raises:
        HTTPException: Si el token es inválido o expiró
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ============================================
# FUNCIONES ADICIONALES PARA COMPATIBILIDAD
# ============================================


def create_jwt_token(user_id: int, email: str, rol: str = "usuario") -> str:
    """
    Función de compatibilidad para crear tokens JWT
    Usa la misma lógica que create_access_token pero con nombre legacy

    Args:
        user_id: ID del usuario
        email: Email del usuario
        rol: Rol del usuario (usuario, admin, auditor)

    Returns:
        Token JWT como string
    """
    token_data = {"user_id": user_id, "email": email, "rol": rol}
    return create_access_token(token_data)


# ============================================
# DEPENDENCIAS PARA PROTEGER ENDPOINTS
# ============================================


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    """
    Dependencia para proteger endpoints
    Extrae y valida el token JWT del header Authorization

    Usage:
        @app.get("/protected")
        async def protected_route(current_user: dict = Depends(get_current_user)):
            return {"user_id": current_user["user_id"]}

    Returns:
        Diccionario con información del usuario actual

    Raises:
        HTTPException: Si el token es inválido o el usuario no existe
    """
    token = credentials.credentials
    payload = decode_token(token)

    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido - falta user_id",
        )

    # Verificar que el usuario existe en la base de datos
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, email, nombre, activo, rol
                FROM usuarios
                WHERE id = %s
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, email, nombre, activo, rol
                FROM usuarios
                WHERE id = ?
            """,
                (user_id,),
            )

        user = cursor.fetchone()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Usuario no encontrado"
            )

        # Verificar si el usuario está activo
        user_dict = (
            dict(user)
            if hasattr(user, "keys")
            else {
                "id": user[0],
                "email": user[1],
                "nombre": user[2],
                "activo": user[3],
                "rol": user[4],
            }
        )

        if not user_dict.get("activo", True):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Usuario desactivado"
            )

        # Actualizar último acceso
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                UPDATE usuarios
                SET ultimo_acceso = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                UPDATE usuarios
                SET ultimo_acceso = CURRENT_TIMESTAMP
                WHERE id = ?
            """,
                (user_id,),
            )

        conn.commit()

        return {
            "user_id": user_dict["id"],
            "email": user_dict["email"],
            "nombre": user_dict.get("nombre"),
            "rol": user_dict.get("rol", "usuario"),
        }

    finally:
        conn.close()


# ============================================
# ENDPOINTS DE AUTENTICACIÓN
# ============================================


@router.post(
    "/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED
)
async def register(user_data: UserRegister):
    """
    Registra un nuevo usuario
    ✅ ACTUALIZADO: Incluye aceptación de términos legales

    Returns:
        Token JWT y datos del usuario
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verificar si el email ya existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT id FROM usuarios WHERE email = %s", (user_data.email,)
            )
        else:
            cursor.execute(
                "SELECT id FROM usuarios WHERE email = ?", (user_data.email,)
            )

        if cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El email ya está registrado",
            )

        # Hashear la contraseña
        password_hash = hash_password(user_data.password)

        # Construir nombre completo
        nombre_completo = (
            f"{user_data.nombres or ''} {user_data.apellidos or ''}".strip()
        )
        if not nombre_completo:
            nombre_completo = user_data.email.split("@")[0]

        # ✅ NUEVO: Fecha de aceptación de términos
        fecha_aceptacion = (
            datetime.utcnow()
            if (
                user_data.acepto_politica_privacidad
                and user_data.acepto_tratamiento_datos
            )
            else None
        )

        # Insertar usuario CON campos de términos
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                INSERT INTO usuarios (
                    email, password_hash, nombre, activo, rol,
                    acepto_politica_privacidad, acepto_tratamiento_datos, fecha_aceptacion_terminos
                )
                VALUES (%s, %s, %s, TRUE, 'usuario', %s, %s, %s)
                RETURNING id, email, nombre, fecha_registro
            """,
                (
                    user_data.email,
                    password_hash,
                    nombre_completo,
                    user_data.acepto_politica_privacidad,
                    user_data.acepto_tratamiento_datos,
                    fecha_aceptacion,
                ),
            )

            new_user = cursor.fetchone()
            user_id = new_user[0]
            email = new_user[1]
            nombre = new_user[2]
            fecha_registro = new_user[3]

        else:
            cursor.execute(
                """
                INSERT INTO usuarios (
                    email, password_hash, nombre, activo, rol,
                    acepto_politica_privacidad, acepto_tratamiento_datos, fecha_aceptacion_terminos
                )
                VALUES (?, ?, ?, 1, 'usuario', ?, ?, ?)
            """,
                (
                    user_data.email,
                    password_hash,
                    nombre_completo,
                    1 if user_data.acepto_politica_privacidad else 0,
                    1 if user_data.acepto_tratamiento_datos else 0,
                    fecha_aceptacion.isoformat() if fecha_aceptacion else None,
                ),
            )

            user_id = cursor.lastrowid
            email = user_data.email
            nombre = nombre_completo
            fecha_registro = datetime.utcnow()

        conn.commit()

        # ✅ LOG: Registrar aceptación de términos
        print(f"✅ Usuario registrado: {email}")
        print(
            f"   📋 Acepta Política Privacidad: {user_data.acepto_politica_privacidad}"
        )
        print(f"   📋 Acepta Tratamiento Datos: {user_data.acepto_tratamiento_datos}")
        print(f"   📅 Fecha aceptación: {fecha_aceptacion}")

        # Crear token JWT
        token_data = {"user_id": user_id, "email": email}
        access_token = create_access_token(token_data)

        return {
            "access_token": access_token,
            "token_type": "bearer",
            "usuario": {
                "id": user_id,
                "email": email,
                "nombre": nombre,
                "fecha_registro": str(fecha_registro),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en registro: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al registrar usuario: {str(e)}",
        )
    finally:
        conn.close()


@router.post("/login", response_model=TokenResponse)
async def login(credentials: UserLogin):
    """
    Inicia sesión con email y contraseña

    Returns:
        Token JWT y datos del usuario
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Buscar usuario por email
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, email, password_hash, nombre, activo, rol,
                       facturas_aportadas, productos_aportados, puntos_contribucion
                FROM usuarios
                WHERE email = %s
            """,
                (credentials.email,),
            )
        else:
            cursor.execute(
                """
                SELECT id, email, password_hash, nombre, activo, rol,
                       facturas_aportadas, productos_aportados, puntos_contribucion
                FROM usuarios
                WHERE email = ?
            """,
                (credentials.email,),
            )

        user = cursor.fetchone()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Email o contraseña incorrectos",
            )

        # Convertir a diccionario
        user_dict = (
            dict(user)
            if hasattr(user, "keys")
            else {
                "id": user[0],
                "email": user[1],
                "password_hash": user[2],
                "nombre": user[3],
                "activo": user[4],
                "rol": user[5],
                "facturas_aportadas": user[6],
                "productos_aportados": user[7],
                "puntos_contribucion": user[8],
            }
        )

        # Verificar si el usuario está activo
        if not user_dict.get("activo", True):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Usuario desactivado"
            )

        # Verificar contraseña
        if not verify_password(credentials.password, user_dict["password_hash"]):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Email o contraseña incorrectos",
            )

        # Actualizar último acceso
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                UPDATE usuarios
                SET ultimo_acceso = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (user_dict["id"],),
            )
        else:
            cursor.execute(
                """
                UPDATE usuarios
                SET ultimo_acceso = CURRENT_TIMESTAMP
                WHERE id = ?
            """,
                (user_dict["id"],),
            )

        conn.commit()

        # Crear token JWT
        token_data = {"user_id": user_dict["id"], "email": user_dict["email"]}
        access_token = create_access_token(token_data)

        return {
            "access_token": access_token,
            "token_type": "bearer",
            "usuario": {
                "id": user_dict["id"],
                "email": user_dict["email"],
                "nombre": user_dict.get("nombre"),
                "rol": user_dict.get("rol", "usuario"),
                "facturas_aportadas": user_dict.get("facturas_aportadas", 0),
                "productos_aportados": user_dict.get("productos_aportados", 0),
                "puntos_contribucion": user_dict.get("puntos_contribucion", 0),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en login: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al iniciar sesión: {str(e)}",
        )
    finally:
        conn.close()


@router.get("/me", response_model=dict)
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """
    Obtiene información del usuario actual (requiere autenticación)

    Returns:
        Información completa del usuario actual
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, email, nombre, facturas_aportadas, productos_aportados,
                       puntos_contribucion, fecha_registro, ultimo_acceso, rol
                FROM usuarios
                WHERE id = %s
            """,
                (current_user["user_id"],),
            )
        else:
            cursor.execute(
                """
                SELECT id, email, nombre, facturas_aportadas, productos_aportados,
                       puntos_contribucion, fecha_registro, ultimo_acceso, rol
                FROM usuarios
                WHERE id = ?
            """,
                (current_user["user_id"],),
            )

        user = cursor.fetchone()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado"
            )

        user_dict = (
            dict(user)
            if hasattr(user, "keys")
            else {
                "id": user[0],
                "email": user[1],
                "nombre": user[2],
                "facturas_aportadas": user[3],
                "productos_aportados": user[4],
                "puntos_contribucion": user[5],
                "fecha_registro": user[6],
                "ultimo_acceso": user[7],
                "rol": user[8],
            }
        )

        return {
            "id": user_dict["id"],
            "email": user_dict["email"],
            "nombre": user_dict.get("nombre"),
            "facturas_aportadas": user_dict.get("facturas_aportadas", 0),
            "productos_aportados": user_dict.get("productos_aportados", 0),
            "puntos_contribucion": user_dict.get("puntos_contribucion", 0),
            "fecha_registro": str(user_dict.get("fecha_registro")),
            "ultimo_acceso": (
                str(user_dict.get("ultimo_acceso"))
                if user_dict.get("ultimo_acceso")
                else None
            ),
            "rol": user_dict.get("rol", "usuario"),
        }

    finally:
        conn.close()


@router.post("/password-reset-request", response_model=MessageResponse)
async def request_password_reset(request: PasswordResetRequest):
    """
    Solicita un código de recuperación de contraseña

    Returns:
        Código de 6 dígitos para resetear contraseña
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verificar que el usuario existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT id FROM usuarios WHERE email = %s", (request.email,))
        else:
            cursor.execute("SELECT id FROM usuarios WHERE email = ?", (request.email,))

        user = cursor.fetchone()

        if not user:
            # Por seguridad, no revelar si el email existe o no
            return {
                "message": "Si el email existe, recibirás un código de recuperación",
                "data": None,
            }

        user_id = user[0] if isinstance(user, tuple) else user["id"]

        # Generar código de 6 dígitos
        reset_code = "".join(secrets.choice(string.digits) for _ in range(6))
        expire_at = datetime.utcnow() + timedelta(hours=1)  # Expira en 1 hora

        # Eliminar códigos anteriores del usuario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("DELETE FROM password_resets WHERE user_id = %s", (user_id,))
        else:
            cursor.execute("DELETE FROM password_resets WHERE user_id = ?", (user_id,))

        # Insertar nuevo código
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                INSERT INTO password_resets (user_id, reset_code, expire_at)
                VALUES (%s, %s, %s)
            """,
                (user_id, reset_code, expire_at),
            )
        else:
            cursor.execute(
                """
                INSERT INTO password_resets (user_id, reset_code, expire_at)
                VALUES (?, ?, ?)
            """,
                (user_id, reset_code, expire_at),
            )

        conn.commit()

        # En producción, enviar por email
        # Por ahora, devolver el código en la respuesta (solo para desarrollo)
        return {
            "message": "Código de recuperación generado",
            "data": {
                "reset_code": reset_code,  # QUITAR EN PRODUCCIÓN
                "expires_in": "1 hora",
            },
        }

    except Exception as e:
        conn.rollback()
        print(f"Error generando código de recuperación: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al generar código de recuperación",
        )
    finally:
        conn.close()


@router.post("/password-reset-confirm", response_model=MessageResponse)
async def confirm_password_reset(reset_data: PasswordResetConfirm):
    """
    Confirma el reseteo de contraseña con el código

    Returns:
        Mensaje de éxito
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Buscar usuario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT id FROM usuarios WHERE email = %s", (reset_data.email,)
            )
        else:
            cursor.execute(
                "SELECT id FROM usuarios WHERE email = ?", (reset_data.email,)
            )

        user = cursor.fetchone()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Email no encontrado"
            )

        user_id = user[0] if isinstance(user, tuple) else user["id"]

        # Verificar código
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, expire_at, used
                FROM password_resets
                WHERE user_id = %s AND reset_code = %s
            """,
                (user_id, reset_data.reset_code),
            )
        else:
            cursor.execute(
                """
                SELECT id, expire_at, used
                FROM password_resets
                WHERE user_id = ? AND reset_code = ?
            """,
                (user_id, reset_data.reset_code),
            )

        reset_record = cursor.fetchone()

        if not reset_record:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Código inválido"
            )

        reset_dict = (
            dict(reset_record)
            if hasattr(reset_record, "keys")
            else {
                "id": reset_record[0],
                "expire_at": reset_record[1],
                "used": reset_record[2],
            }
        )

        # Verificar si ya fue usado
        if reset_dict["used"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Código ya utilizado"
            )

        # Verificar si expiró
        expire_at = reset_dict["expire_at"]
        if isinstance(expire_at, str):
            expire_at = datetime.fromisoformat(expire_at.replace("Z", "+00:00"))

        if datetime.utcnow() > expire_at:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Código expirado"
            )

        # Actualizar contraseña
        new_password_hash = hash_password(reset_data.new_password)

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                UPDATE usuarios
                SET password_hash = %s
                WHERE id = %s
            """,
                (new_password_hash, user_id),
            )

            # Marcar código como usado
            cursor.execute(
                """
                UPDATE password_resets
                SET used = TRUE
                WHERE id = %s
            """,
                (reset_dict["id"],),
            )
        else:
            cursor.execute(
                """
                UPDATE usuarios
                SET password_hash = ?
                WHERE id = ?
            """,
                (new_password_hash, user_id),
            )

            cursor.execute(
                """
                UPDATE password_resets
                SET used = 1
                WHERE id = ?
            """,
                (reset_dict["id"],),
            )

        conn.commit()

        return {"message": "Contraseña actualizada exitosamente", "data": None}

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        print(f"Error confirmando reseteo de contraseña: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al resetear contraseña",
        )
    finally:
        conn.close()


# ============================================
# ENDPOINT DE PRUEBA (SOLO DESARROLLO)
# ============================================


@router.get("/test-protected")
async def test_protected_route(current_user: dict = Depends(get_current_user)):
    """
    Endpoint de prueba para verificar que la autenticación funciona
    """
    return {"message": "Autenticación exitosa", "user": current_user}
