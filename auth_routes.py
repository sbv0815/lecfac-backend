# ============= auth_routes.py =============
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from typing import Optional
import jwt
from datetime import datetime, timedelta
import os

from database import get_db_connection, hash_password, verify_password

router = APIRouter(prefix="/api/auth", tags=["Authentication"])

# Configuración JWT
SECRET_KEY = os.environ.get("JWT_SECRET", "tu-secret-key-super-segura")
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

class UserResponse(BaseModel):
    id: int
    email: str
    nombre: str
    apellidos: str
    celular: str

def create_access_token(data: dict):
    """Crear token JWT"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

@router.post("/register")
async def register(request: RegisterRequest):
    """Registro de nuevo usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar si el email ya existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT id FROM usuarios WHERE email = %s", (request.email,))
        else:
            cursor.execute("SELECT id FROM usuarios WHERE email = ?", (request.email,))
        
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El correo ya está registrado")
        
        # Hash de la contraseña
        password_hash = hash_password(request.password)
        
        # Crear nombre completo
        nombre_completo = f"{request.nombres} {request.apellidos}"
        
        # Insertar nuevo usuario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO usuarios (email, password_hash, nombre, celular, nombres, apellidos)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (request.email, password_hash, nombre_completo, request.celular, 
                  request.nombres, request.apellidos))
            user_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO usuarios (email, password_hash, nombre, celular, nombres, apellidos)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (request.email, password_hash, nombre_completo, request.celular,
                  request.nombres, request.apellidos))
            user_id = cursor.lastrowid
        
        conn.commit()
        
        # Crear token
        access_token = create_access_token({"user_id": user_id, "email": request.email})
        
        return {
            "success": True,
            "message": "Usuario registrado exitosamente",
            "token": access_token,
            "user": {
                "id": str(user_id),
                "email": request.email,
                "name": nombre_completo,
                "nombres": request.nombres,
                "apellidos": request.apellidos,
                "celular": request.celular
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        print(f"Error en registro: {e}")
        raise HTTPException(status_code=500, detail="Error al registrar usuario")
    finally:
        conn.close()

@router.post("/login")
async def login(request: LoginRequest):
    """Login de usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Buscar usuario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT id, email, password_hash, nombre, celular, 
                       nombres, apellidos 
                FROM usuarios WHERE email = %s
            """, (request.email,))
        else:
            cursor.execute("""
                SELECT id, email, password_hash, nombre, celular,
                       nombres, apellidos 
                FROM usuarios WHERE email = ?
            """, (request.email,))
        
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        
        # Verificar contraseña
        if not verify_password(request.password, user[2]):
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        
        # Crear token
        access_token = create_access_token({"user_id": user[0], "email": user[1]})
        
        # Preparar respuesta
        user_data = {
            "id": str(user[0]),
            "email": user[1],
            "name": user[3] or f"{user[5]} {user[6]}",
            "celular": user[4]
        }
        
        # Agregar nombres y apellidos si existen
        if len(user) > 5:
            user_data["nombres"] = user[5]
            user_data["apellidos"] = user[6]
        
        return {
            "success": True,
            "token": access_token,
            "user": user_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en login: {e}")
        raise HTTPException(status_code=500, detail="Error al iniciar sesión")
    finally:
        conn.close()

@router.get("/verify")
async def verify_token(token: str):
    """Verificar si un token es válido"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return {
            "valid": True,
            "user_id": payload.get("user_id"),
            "email": payload.get("email")
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Token inválido")

# ============= Agregar a main.py =============
# En tu archivo main.py, agrega estas líneas:

from auth_routes import router as auth_router
app.include_router(auth_router)
