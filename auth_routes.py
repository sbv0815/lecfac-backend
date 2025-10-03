from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
import jwt
from datetime import datetime, timedelta
import os
import bcrypt

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

def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

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
        
        cursor.execute("""
            INSERT INTO usuarios (email, password_hash, nombre)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (request.email, password_hash, nombre_completo))
        
        user_id = cursor.fetchone()[0]
        conn.commit()
        
        access_token = create_access_token({"user_id": user_id, "email": request.email})
        
        return {
            "success": True,
            "token": access_token,
            "user": {
                "id": str(user_id),
                "email": request.email,
                "name": nombre_completo
            }
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
            (request.email,)
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
                "name": user[3] or "Usuario"
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

from auth_routes import router as auth_router
app.include_router(auth_router)
