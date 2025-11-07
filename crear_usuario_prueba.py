"""
crear_usuario_prueba.py
Script para crear un usuario de prueba en la base de datos
"""

from database import get_db_connection, hash_password
import os

def crear_usuario_prueba():
    """Crea usuario: santiago@tscamp.co / 123456"""

    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        # Email y contraseña del usuario
        email = "santiago@tscamp.co"
        password = "123456"
        nombre = "Santiago"

        # Verificar si ya existe
        if database_type == "postgresql":
            cursor.execute("SELECT id FROM usuarios WHERE email = %s", (email,))
        else:
            cursor.execute("SELECT id FROM usuarios WHERE email = ?", (email,))

        if cursor.fetchone():
            print(f"⚠️  El usuario {email} ya existe")
            conn.close()
            return

        # Hashear contraseña
        password_hash = hash_password(password)

        # Crear usuario
        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO usuarios (email, password_hash, nombre, rol)
                VALUES (%s, %s, %s, 'admin')
                RETURNING id
            """, (email, password_hash, nombre))
            user_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO usuarios (email, password_hash, nombre, rol)
                VALUES (?, ?, ?, 'admin')
            """, (email, password_hash, nombre))
            user_id = cursor.lastrowid

        conn.commit()

        print("=" * 60)
        print("✅ USUARIO DE PRUEBA CREADO EXITOSAMENTE")
        print("=" * 60)
        print(f"ID:          {user_id}")
        print(f"Email:       {email}")
        print(f"Contraseña:  {password}")
        print(f"Nombre:      {nombre}")
        print(f"Rol:         admin")
        print("=" * 60)
        print("\n🔐 Usa estas credenciales en la app Flutter")

        conn.close()

    except Exception as e:
        print(f"❌ Error creando usuario: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()


if __name__ == "__main__":
    crear_usuario_prueba()
