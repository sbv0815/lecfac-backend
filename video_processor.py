# Agregar esta función mejorada en video_processor.py

import re
from typing import List, Dict
from difflib import SequenceMatcher


def extraer_consecutivo(nombre: str) -> tuple:
    """
    Extrae el número consecutivo del inicio del nombre del producto.

    Ejemplos:
    - "01 un 14.800 1" → (1, "un 14.800 1")
    - "2 0.595/KGM x 9.620" → (2, "0.595/KGM x 9.620")
    - "HUEVO AAA RJO 15UN" → (None, "HUEVO AAA RJO 15UN")

    Returns:
        tuple: (consecutivo, nombre_limpio)
    """
    # Patrones para detectar consecutivo al inicio
    patrones = [
        r"^(\d{1,3})\s+un\b",  # "01 un", "1 un"
        r"^(\d{1,3})\s+",  # "1 ", "01 "
        r"^(\d{1,3})[./]",  # "1/", "1."
    ]

    for patron in patrones:
        match = re.match(patron, nombre.strip(), re.IGNORECASE)
        if match:
            consecutivo = int(match.group(1))
            nombre_limpio = nombre[match.end() :].strip()
            return (consecutivo, nombre_limpio)

    return (None, nombre)


def limpiar_nombre_producto(nombre: str) -> str:
    """
    Limpia el nombre del producto para comparación.

    - Elimina consecutivos (01, 02, etc.)
    - Elimina "un", "1", cantidades
    - Normaliza espacios
    - Convierte a minúsculas
    """
    # Remover consecutivo si existe
    _, nombre_sin_consecutivo = extraer_consecutivo(nombre)

    # Limpiar el nombre
    nombre_limpio = nombre_sin_consecutivo.lower()

    # Remover palabras comunes que no identifican al producto
    palabras_ignorar = ["un", "und", "unidad", "unidades"]
    for palabra in palabras_ignorar:
        nombre_limpio = re.sub(
            rf"\b{palabra}\b", "", nombre_limpio, flags=re.IGNORECASE
        )

    # Normalizar espacios
    nombre_limpio = " ".join(nombre_limpio.split())

    return nombre_limpio.strip()


def similitud_productos(prod1: str, prod2: str) -> float:
    """
    Calcula similitud entre dos nombres de productos.

    Returns:
        float: Similitud de 0.0 a 1.0
    """
    nombre1 = limpiar_nombre_producto(prod1)
    nombre2 = limpiar_nombre_producto(prod2)

    if not nombre1 or not nombre2:
        return 0.0

    return SequenceMatcher(None, nombre1, nombre2).ratio()


def deduplicar_productos(productos: List[Dict]) -> List[Dict]:
    """
    Deduplica productos usando SOLO código + precio.

    REGLA DE NEGOCIO:
    - Mismo código + mismo precio = MISMO PRODUCTO (consolidar cantidades)
    - Mismo código + precio diferente = PRODUCTOS DIFERENTES (mantener separados)
    - Sin código = usar nombre normalizado + precio

    NO usa similitud de nombres para evitar eliminar productos válidos.
    """
    if not productos:
        return []

    print(f"🔍 Deduplicando {len(productos)} productos...")

    # Agrupar por clave única: (código, precio)
    grupos = {}

    for prod in productos:
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip().upper()
        precio = float(prod.get("precio", 0))
        cantidad = int(prod.get("cantidad", 1))

        # Crear clave única
        if codigo and codigo.isdigit():
            # Si tiene código, usar: código + precio
            clave = f"COD:{codigo}|PRECIO:{precio}"
        else:
            # Sin código, usar: nombre + precio
            clave = f"NOMBRE:{nombre}|PRECIO:{precio}"

        if clave not in grupos:
            grupos[clave] = {
                "codigo": codigo,
                "nombre": nombre,
                "precio": precio,
                "cantidad": 0,
                "ocurrencias": 0
            }

        # Acumular cantidad
        grupos[clave]["cantidad"] += cantidad
        grupos[clave]["ocurrencias"] += 1

    # Convertir grupos a lista de productos
    productos_unicos = []
    consolidados = 0

    for clave, grupo in grupos.items():
        productos_unicos.append({
            "codigo": grupo["codigo"],
            "nombre": grupo["nombre"],
            "precio": grupo["precio"],
            "cantidad": grupo["cantidad"]
        })

        if grupo["ocurrencias"] > 1:
            consolidados += grupo["ocurrencias"] - 1
            print(f"   🔄 Consolidado: {grupo['nombre'][:40]} x{grupo['ocurrencias']} → cantidad {grupo['cantidad']}")

    print(f"✅ Productos únicos: {len(productos_unicos)}")
    if consolidados > 0:
        print(f"   📉 Consolidados: {consolidados} productos con mismo código+precio")

    return productos_unicos


# ==========================================
# FUNCIÓN PARA VALIDAR Y LIMPIAR FECHAS
# ==========================================
def validar_fecha(fecha_str: str) -> str:
    """
    Valida y limpia una fecha detectada por OCR.

    Maneja formatos comunes:
    - DD/MM/YY
    - DD/MM/YYYY
    - DD-MM-YY

    Returns:
        str: Fecha en formato ISO (YYYY-MM-DD) o None si inválida
    """
    if not fecha_str or not isinstance(fecha_str, str):
        return None

    # Limpiar la fecha
    fecha_limpia = fecha_str.strip()

    # Remover caracteres extraños al final (como "-03")
    fecha_limpia = re.sub(r"-\d+$", "", fecha_limpia)

    # Intentar parsear diferentes formatos
    from datetime import datetime

    formatos = [
        "%d/%m/%Y",  # 25/10/2024
        "%d/%m/%y",  # 25/10/24
        "%d-%m-%Y",  # 25-10-2024
        "%d-%m-%y",  # 25-10-24
        "%Y-%m-%d",  # 2024-10-25 (ISO)
    ]

    for formato in formatos:
        try:
            fecha_obj = datetime.strptime(fecha_limpia, formato)

            # Validar que la fecha sea razonable
            año_actual = datetime.now().year

            # Si el año es muy antiguo (< 2000), probablemente es error de OCR
            if fecha_obj.year < 2000:
                # Intentar ajustar el año (ej: "02" → "2002" o "2024")
                if fecha_obj.year < 100:
                    # Años de 2 dígitos: 00-25 → 2000-2025, 26-99 → 1926-1999
                    if fecha_obj.year <= 25:
                        fecha_obj = fecha_obj.replace(year=2000 + fecha_obj.year)
                    else:
                        fecha_obj = fecha_obj.replace(year=1900 + fecha_obj.year)

            # Validar que no sea una fecha futura (más de 1 día)
            if fecha_obj > datetime.now():
                from datetime import timedelta

                if (fecha_obj - datetime.now()) > timedelta(days=1):
                    print(
                        f"   ⚠️ Fecha futura detectada: {fecha_obj.date()}, usando fecha actual"
                    )
                    return datetime.now().date().isoformat()

            # Retornar fecha en formato ISO
            return fecha_obj.date().isoformat()

        except ValueError:
            continue

    # Si no se pudo parsear ningún formato
    print(f"   ⚠️ Fecha inválida: '{fecha_str}', usando fecha actual")
    return datetime.now().date().isoformat()


# ==========================================
# EJEMPLO DE USO
# ==========================================
if __name__ == "__main__":
    # Ejemplo 1: Productos con consecutivo
    productos_test = [
        {"nombre": "01 un HUEVO AAA RJO 15UN", "codigo": "2136304", "precio": 14800},
        {
            "nombre": "HUEVO AAA RJO 15UN",
            "codigo": "2136304",
            "precio": 14800,
        },  # Duplicado sin consecutivo
        {"nombre": "02 un HUEVO AAA RJO 15UN", "codigo": "2136304", "precio": 14800},
        {"nombre": "03 un PAPA MC CAIN RAPIP", "codigo": "1369018", "precio": 14990},
    ]

    unicos = deduplicar_productos(productos_test)
    print("\n📦 Productos únicos:")
    for p in unicos:
        print(f"   - {p['nombre']}: ${p['precio']:,}")

    # Ejemplo 2: Validar fechas
    print("\n📅 Validando fechas:")
    fechas_test = [
        "25/10/02",
        "26/10/52-03",  # Error de OCR
        "13/10/2024",
        "fecha inválida",
    ]

    for fecha in fechas_test:
        fecha_valida = validar_fecha(fecha)
        print(f"   '{fecha}' → {fecha_valida}")

# ==========================================
# FUNCIONES DE PROCESAMIENTO DE VIDEO
# ==========================================


def extraer_frames_video(video_path: str, intervalo: float = 1.0) -> List[str]:
    """
    Extrae frames de un video a intervalos regulares.

    Args:
        video_path: Ruta del video
        intervalo: Segundos entre cada frame (default: 1.0)

    Returns:
        Lista de rutas de frames extraídos
    """
    import cv2
    import os

    frames_paths = []

    try:
        # Abrir el video
        cap = cv2.VideoCapture(video_path)

        if not cap.isOpened():
            print(f"❌ No se pudo abrir el video: {video_path}")
            return []

        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        if fps == 0:
            fps = 30  # Fallback

        frame_interval = int(fps * intervalo)

        print(f"📹 Video: {fps:.1f} FPS, {total_frames} frames totales")
        print(f"🎯 Extrayendo 1 frame cada {frame_interval} frames ({intervalo}s)")

        frame_count = 0
        saved_count = 0

        while True:
            ret, frame = cap.read()

            if not ret:
                break

            # Guardar frame cada 'frame_interval' frames
            if frame_count % frame_interval == 0:
                frame_filename = f"/tmp/frame_{saved_count:04d}.jpg"
                cv2.imwrite(frame_filename, frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                frames_paths.append(frame_filename)
                saved_count += 1
                print(f"   ✓ Frame {saved_count} guardado")

            frame_count += 1

        cap.release()

        print(f"✅ Extraídos {len(frames_paths)} frames del video")
        return frames_paths

    except Exception as e:
        print(f"❌ Error extrayendo frames: {e}")
        import traceback

        traceback.print_exc()
        return []


def limpiar_frames_temporales(frames_paths: List[str]) -> None:
    """
    Elimina archivos temporales de frames.

    Args:
        frames_paths: Lista de rutas de frames a eliminar
    """
    import os

    eliminados = 0
    errores = 0

    for frame_path in frames_paths:
        try:
            if os.path.exists(frame_path):
                os.remove(frame_path)
                eliminados += 1
        except Exception as e:
            print(f"⚠️ Error eliminando {frame_path}: {e}")
            errores += 1

    if eliminados > 0:
        print(f"🧹 {eliminados} frames temporales eliminados")
    if errores > 0:
        print(f"⚠️ {errores} errores al eliminar frames")


def combinar_frames_vertical(
    frames_paths: List[str], output_path: str, max_width: int = 800
) -> str:
    """
    Combina múltiples frames en una sola imagen vertical.

    Args:
        frames_paths: Lista de rutas de frames
        output_path: Ruta donde guardar la imagen combinada
        max_width: Ancho máximo de la imagen final (para optimizar tamaño)

    Returns:
        Ruta de la imagen combinada, o None si falla
    """
    try:
        from PIL import Image
        import os

        if not frames_paths:
            print("⚠️ No hay frames para combinar")
            return None

        print(f"🖼️ Combinando {len(frames_paths)} frames...")

        # Cargar todas las imágenes
        imagenes = []
        for frame_path in frames_paths:
            if os.path.exists(frame_path):
                img = Image.open(frame_path)

                # Redimensionar si es muy ancha
                if img.width > max_width:
                    ratio = max_width / img.width
                    new_height = int(img.height * ratio)
                    img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)

                imagenes.append(img)

        if not imagenes:
            print("⚠️ No se pudieron cargar imágenes")
            return None

        # Calcular dimensiones de la imagen final
        widths = [img.width for img in imagenes]
        heights = [img.height for img in imagenes]

        max_width_img = max(widths)
        total_height = sum(heights)

        # Crear imagen combinada
        imagen_combinada = Image.new(
            "RGB", (max_width_img, total_height), color="white"
        )

        # Pegar cada frame
        y_offset = 0
        for img in imagenes:
            imagen_combinada.paste(img, (0, y_offset))
            y_offset += img.height

        # Guardar
        imagen_combinada.save(output_path, "JPEG", quality=85, optimize=True)

        print(f"✅ Imagen combinada guardada: {output_path}")
        print(f"   📐 Dimensiones: {max_width_img}x{total_height}px")

        # Cerrar imágenes
        for img in imagenes:
            img.close()

        return output_path

    except Exception as e:
        print(f"❌ Error combinando frames: {e}")
        import traceback

        traceback.print_exc()
        return None
