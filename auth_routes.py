from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
import jwt
from datetime import datetime, timedelta
import os
import bcrypt
import secrets
import string

from database import get_db_connection

router = APIRouter(prefix="/api/auth", tags=["Authentication"])

SECRET_KEY = os.environ.get("JWT_SECRET", "69Sbv8v15nf*!@")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 30


class RegisterRequest(BaseModel):
    nombres: str
    apellidos: str
    email: EmailStr
    celular: str
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    email: EmailStr
    reset_code: str
    new_password: str


def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def generate_reset_code() -> str:
    """Generar código de 6 dígitos para recuperación"""
    return "".join(secrets.choice(string.digits) for _ in range(6))


@router.post("/register")
async def register(request: RegisterRequest):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verificar si el email ya existe
        cursor.execute("SELECT id FROM usuarios WHERE email = %s", (request.email,))

        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El correo ya está registrado")

        password_hash = hash_password(request.password)
        nombre_completo = f"{request.nombres} {request.apellidos}"

        cursor.execute(
            """
            INSERT INTO usuarios (email, password_hash, nombre)
            VALUES (%s, %s, %s)
            RETURNING id
        """,
            (request.email, password_hash, nombre_completo),
        )

        user_id = cursor.fetchone()[0]
        conn.commit()

        access_token = create_access_token({"user_id": user_id, "email": request.email})

        return {
            "success": True,
            "token": access_token,
            "user": {
                "id": str(user_id),
                "email": request.email,
                "name": nombre_completo,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/login")
async def login(request: LoginRequest):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT id, email, password_hash, nombre FROM usuarios WHERE email = %s",
            (request.email,),
        )

        user = cursor.fetchone()

        if not user or not verify_password(request.password, user[2]):
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        access_token = create_access_token({"user_id": user[0], "email": user[1]})

        return {
            "success": True,
            "token": access_token,
            "user": {
                "id": str(user[0]),
                "email": user[1],
                "name": user[3] or "Usuario",
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/forgot-password")
async def forgot_password(request: ForgotPasswordRequest):
    """
    Generar código de recuperación de contraseña.
    Versión SIMPLE: Retorna el código (para desarrollo/pruebas).
    Versión PRODUCCIÓN: Envía el código por email.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. Verificar que el email existe
        cursor.execute(
            "SELECT id, nombre FROM usuarios WHERE email = %s", (request.email,)
        )

        user = cursor.fetchone()

        if not user:
            # Por seguridad, no revelar si el email existe o no
            return {
                "success": True,
                "message": "Si el correo existe, recibirás instrucciones para recuperar tu contraseña",
            }

        user_id, nombre = user

        # 2. Generar código de recuperación
        reset_code = generate_reset_code()
        expire_time = datetime.utcnow() + timedelta(hours=1)  # Expira en 1 hora

        # 3. Guardar código en la base de datos
        # Primero verificar si ya existe un código para este usuario
        cursor.execute("SELECT id FROM password_resets WHERE user_id = %s", (user_id,))

        if cursor.fetchone():
            # Actualizar código existente
            cursor.execute(
                """
                UPDATE password_resets
                SET reset_code = %s, expire_at = %s, used = FALSE, created_at = NOW()
                WHERE user_id = %s
            """,
                (reset_code, expire_time, user_id),
            )
        else:
            # Insertar nuevo código
            cursor.execute(
                """
                INSERT INTO password_resets (user_id, reset_code, expire_at, used)
                VALUES (%s, %s, %s, FALSE)
            """,
                (user_id, reset_code, expire_time),
            )

        conn.commit()

        # 4. TODO: Enviar email con el código
        # Por ahora, en desarrollo, retornamos el código (QUITAR EN PRODUCCIÓN)
        print(f"🔐 Código de recuperación para {request.email}: {reset_code}")

        # VERSIÓN DESARROLLO (retorna el código para pruebas)
        return {
            "success": True,
            "message": "Código de recuperación generado",
            "reset_code": reset_code,  # ⚠️ QUITAR EN PRODUCCIÓN
            "email": request.email,
        }

        # VERSIÓN PRODUCCIÓN (descomentar esto y comentar el return de arriba)
        # return {
        #     "success": True,
        #     "message": "Si el correo existe, recibirás instrucciones para recuperar tu contraseña"
        # }

    except Exception as e:
        conn.rollback()
        print(f"Error en forgot_password: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest):
    """
    Restablecer contraseña usando el código de recuperación
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. Buscar el código de recuperación
        cursor.execute(
            """
            SELECT pr.id, pr.user_id, pr.expire_at, pr.used, u.email
            FROM password_resets pr
            JOIN usuarios u ON pr.user_id = u.id
            WHERE u.email = %s AND pr.reset_code = %s
        """,
            (request.email, request.reset_code),
        )

        reset_data = cursor.fetchone()

        if not reset_data:
            raise HTTPException(
                status_code=400, detail="Código de recuperación inválido"
            )

        reset_id, user_id, expire_at, used, email = reset_data

        # 2. Verificar que no esté usado
        if used:
            raise HTTPException(status_code=400, detail="Este código ya fue utilizado")

        # 3. Verificar que no esté expirado
        if datetime.utcnow() > expire_at:
            raise HTTPException(
                status_code=400, detail="El código ha expirado. Solicita uno nuevo"
            )

        # 4. Actualizar la contraseña
        new_password_hash = hash_password(request.new_password)

        cursor.execute(
            """
            UPDATE usuarios
            SET password_hash = %s
            WHERE id = %s
        """,
            (new_password_hash, user_id),
        )

        # 5. Marcar el código como usado
        cursor.execute(
            """
            UPDATE password_resets
            SET used = TRUE
            WHERE id = %s
        """,
            (reset_id,),
        )

        conn.commit()

        # 6. Generar nuevo token de acceso
        access_token = create_access_token({"user_id": user_id, "email": email})

        return {
            "success": True,
            "message": "Contraseña actualizada exitosamente",
            "token": access_token,
            "user": {"id": str(user_id), "email": email},
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        print(f"Error en reset_password: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
