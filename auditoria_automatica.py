# auditoria_automatica.py
"""
Sistema de auditoría automática para validar y normalizar datos de facturas
Sin uso de IA - Solo reglas de código
"""

import re
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Optional
from datetime import datetime


class AuditoriaAutomatica:
    """Validaciones y normalizaciones automáticas sin IA"""

    # Listas de palabras clave para categorización
    FRUTAS = [
        "manzana",
        "pera",
        "banano",
        "platano",
        "naranja",
        "mandarina",
        "uva",
        "fresa",
        "mora",
        "mango",
        "papaya",
        "sandia",
        "melon",
        "piña",
        "guayaba",
        "lulo",
        "maracuya",
    ]

    VERDURAS = [
        "tomate",
        "cebolla",
        "papa",
        "zanahoria",
        "lechuga",
        "repollo",
        "brocoli",
        "coliflor",
        "pepino",
        "pimenton",
        "aji",
        "cilantro",
        "perejil",
        "apio",
        "espinaca",
        "acelga",
    ]

    CARNES = [
        "carne",
        "res",
        "pollo",
        "cerdo",
        "pechuga",
        "pierna",
        "lomo",
        "molida",
        "chuleta",
        "costilla",
        "pescado",
        "salmon",
        "tilapia",
        "atun",
    ]

    ESTABLECIMIENTOS_CONOCIDOS = {
        "olimpica": "Olímpica",
        "exito": "Éxito",
        "carulla": "Carulla",
        "jumbo": "Jumbo",
        "metro": "Metro",
        "makro": "Makro",
        "d1": "D1",
        "ara": "Ara",
        "camacho": "Camacho",
        "surtimax": "Surtimax",
        "colsubsidio": "Colsubsidio",
        "cruz verde": "Cruz Verde",
        "farmatodo": "Farmatodo",
    }

    @staticmethod
    def validar_matematicas(items: List[Dict], total_factura: float) -> Dict:
        """
        Valida que la suma de items coincida con el total de la factura

        Returns:
            {
                "valido": bool,
                "suma_items": float,
                "total_factura": float,
                "diferencia": float,
                "porcentaje_error": float
            }
        """
        suma_items = sum(float(item.get("precio_total", 0) or 0) for item in items)

        diferencia = abs(suma_items - total_factura)
        porcentaje_error = (
            (diferencia / total_factura * 100) if total_factura > 0 else 0
        )

        # Toleramos hasta 1% de error (por redondeos)
        valido = porcentaje_error <= 1.0

        return {
            "valido": valido,
            "suma_items": round(suma_items, 2),
            "total_factura": round(total_factura, 2),
            "diferencia": round(diferencia, 2),
            "porcentaje_error": round(porcentaje_error, 2),
        }

    @staticmethod
    def validar_precios_logicos(items: List[Dict]) -> List[Dict]:
        """
        Detecta precios anormales (negativos, muy altos, etc.)

        Returns:
            Lista de items con problemas
        """
        problemas = []

        for item in items:
            precio_unitario = float(item.get("precio_unitario", 0) or 0)
            precio_total = float(item.get("precio_total", 0) or 0)
            cantidad = float(item.get("cantidad", 0) or 0)

            # Validaciones
            if precio_unitario < 0 or precio_total < 0:
                problemas.append(
                    {
                        **item,
                        "problema": "precio_negativo",
                        "descripcion": "Precio no puede ser negativo",
                    }
                )

            elif precio_unitario > 1_000_000:
                problemas.append(
                    {
                        **item,
                        "problema": "precio_muy_alto",
                        "descripcion": "Precio unitario sospechosamente alto (>$1M)",
                    }
                )

            elif (
                cantidad > 0 and abs(precio_total - (precio_unitario * cantidad)) > 100
            ):
                problemas.append(
                    {
                        **item,
                        "problema": "calculo_incorrecto",
                        "descripcion": f"Total ({precio_total}) != Unitario ({precio_unitario}) x Cantidad ({cantidad})",
                    }
                )

        return problemas

    @staticmethod
    def normalizar_nombre_producto(nombre: str) -> str:
        """
        Limpia y normaliza el nombre de un producto

        Ejemplos:
            "LECHE COLANTA 1100ML" → "Leche Colanta 1100ml"
            "   arroz  diana  X 500GR  " → "Arroz Diana 500gr"
        """
        if not nombre:
            return ""

        # 1. Remover espacios extras
        nombre = " ".join(nombre.split())

        # 2. Convertir a Title Case (primera letra mayúscula)
        nombre = nombre.title()

        # 3. Normalizar unidades de medida
        nombre = re.sub(r"\s*X\s*", " ", nombre, flags=re.IGNORECASE)
        nombre = re.sub(r"(\d+)\s*ML\b", r"\1ml", nombre, flags=re.IGNORECASE)
        nombre = re.sub(r"(\d+)\s*GR?\b", r"\1gr", nombre, flags=re.IGNORECASE)
        nombre = re.sub(r"(\d+)\s*KG\b", r"\1kg", nombre, flags=re.IGNORECASE)
        nombre = re.sub(r"(\d+)\s*L\b", r"\1L", nombre, flags=re.IGNORECASE)
        nombre = re.sub(r"(\d+)\s*UND?\b", r"\1 unidades", nombre, flags=re.IGNORECASE)

        # 4. Remover caracteres especiales innecesarios
        nombre = re.sub(r"[*#@]", "", nombre)

        return nombre.strip()

    @staticmethod
    def detectar_categoria(nombre: str) -> Optional[str]:
        """
        Detecta si un producto es fruta, verdura o carne (sin código EAN)

        Returns:
            "FRUTA", "VERDURA", "CARNE" o None
        """
        nombre_lower = nombre.lower()

        for fruta in AuditoriaAutomatica.FRUTAS:
            if fruta in nombre_lower:
                return "FRUTA"

        for verdura in AuditoriaAutomatica.VERDURAS:
            if verdura in nombre_lower:
                return "VERDURA"

        for carne in AuditoriaAutomatica.CARNES:
            if carne in nombre_lower:
                return "CARNE"

        return None

    @staticmethod
    def generar_codigo_interno(nombre: str, categoria: str) -> str:
        """
        Genera un código interno para productos sin EAN

        Ejemplos:
            "Manzana Roja" → "FRUTA-MANZANA-ROJA"
            "Tomate Chonto" → "VERDURA-TOMATE-CHONTO"
        """
        # Limpiar nombre y convertir a mayúsculas
        nombre_limpio = re.sub(r"[^\w\s]", "", nombre).upper()
        palabras = nombre_limpio.split()[:3]  # Máximo 3 palabras

        codigo = f"{categoria}-{'-'.join(palabras)}"
        return codigo

    @staticmethod
    def calcular_similitud(texto1: str, texto2: str) -> float:
        """
        Calcula similitud entre dos textos (0.0 a 1.0)

        Usa SequenceMatcher de Python (Algoritmo de Ratcliff-Obershelp)
        """
        return SequenceMatcher(None, texto1.lower(), texto2.lower()).ratio()

    @staticmethod
    def buscar_producto_similar(
        nombre: str, catalogo: List[Dict], umbral: float = 0.85
    ) -> Optional[Dict]:
        """
        Busca un producto similar en el catálogo maestro

        Args:
            nombre: Nombre del producto a buscar
            catalogo: Lista de productos maestros
            umbral: Similitud mínima para considerar match (0.85 = 85%)

        Returns:
            Producto más similar o None
        """
        mejor_match = None
        mejor_similitud = 0.0

        for producto in catalogo:
            nombre_catalogo = producto.get("nombre_normalizado", "")
            similitud = AuditoriaAutomatica.calcular_similitud(nombre, nombre_catalogo)

            if similitud > mejor_similitud and similitud >= umbral:
                mejor_similitud = similitud
                mejor_match = producto

        return mejor_match

    @staticmethod
    def normalizar_establecimiento(nombre: str) -> Tuple[str, bool]:
        """
        Normaliza el nombre de un establecimiento

        Returns:
            (nombre_normalizado, es_conocido)
        """
        if not nombre:
            return "Sin establecimiento", False

        nombre_lower = nombre.lower().strip()

        # Buscar en establecimientos conocidos
        for key, valor in AuditoriaAutomatica.ESTABLECIMIENTOS_CONOCIDOS.items():
            if key in nombre_lower:
                return valor, True

        # No está en la lista → normalizar manualmente
        nombre_normalizado = " ".join(nombre.split()).title()
        return nombre_normalizado, False

    @staticmethod
    def validar_fecha(fecha_str: str) -> bool:
        """Valida que la fecha sea válida y no sea futura"""
        try:
            fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
            hoy = datetime.now()

            # No puede ser fecha futura
            if fecha > hoy:
                return False

            # No puede ser muy antigua (más de 5 años)
            if (hoy - fecha).days > 1825:
                return False

            return True
        except:
            return False


class ReporteAuditoria:
    """Generador de reportes de auditoría"""

    @staticmethod
    def generar_reporte_factura(factura: Dict, items: List[Dict]) -> Dict:
        """
        Genera un reporte completo de auditoría para una factura

        Returns:
            {
                "factura_id": int,
                "puntaje_calidad": float (0-100),
                "estado_sugerido": str ("validada" | "pendiente" | "rechazada"),
                "problemas": List[Dict],
                "sugerencias": List[str],
                "validaciones": {
                    "matematicas": {...},
                    "precios_logicos": [...],
                    "establecimiento": {...}
                }
            }
        """
        audit = AuditoriaAutomatica()
        problemas = []
        sugerencias = []
        puntaje = 100.0

        # 1. Validar matemáticas
        val_matematicas = audit.validar_matematicas(
            items, factura.get("total_factura", 0)
        )
        if not val_matematicas["valido"]:
            problemas.append(
                {
                    "tipo": "error_matematico",
                    "severidad": "alta",
                    "descripcion": f"Suma de items (${val_matematicas['suma_items']}) no coincide con total (${val_matematicas['total_factura']})",
                }
            )
            puntaje -= 30
            sugerencias.append("Revisar manualmente los precios de los productos")

        # 2. Validar precios lógicos
        precios_problematicos = audit.validar_precios_logicos(items)
        if precios_problematicos:
            problemas.append(
                {
                    "tipo": "precios_anormales",
                    "severidad": "media",
                    "descripcion": f"Se encontraron {len(precios_problematicos)} items con precios sospechosos",
                }
            )
            puntaje -= len(precios_problematicos) * 5
            sugerencias.append("Verificar precios anormalmente altos o negativos")

        # 3. Validar establecimiento
        nombre_est, es_conocido = audit.normalizar_establecimiento(
            factura.get("establecimiento", "")
        )
        if not es_conocido:
            problemas.append(
                {
                    "tipo": "establecimiento_desconocido",
                    "severidad": "baja",
                    "descripcion": f"Establecimiento '{nombre_est}' no está en la base de datos",
                }
            )
            puntaje -= 10
            sugerencias.append(
                f"Agregar '{nombre_est}' a la lista de establecimientos conocidos"
            )

        # 4. Validar productos sin código EAN
        productos_sin_codigo = [item for item in items if not item.get("codigo_ean")]
        if productos_sin_codigo:
            problemas.append(
                {
                    "tipo": "productos_sin_codigo",
                    "severidad": "baja",
                    "descripcion": f"{len(productos_sin_codigo)} productos sin código EAN (frutas/verduras/carnes)",
                }
            )
            puntaje -= len(productos_sin_codigo) * 2

        # 5. Determinar estado sugerido
        if puntaje >= 80:
            estado_sugerido = "validada"
        elif puntaje >= 50:
            estado_sugerido = "pendiente"
        else:
            estado_sugerido = "rechazada"

        return {
            "factura_id": factura.get("id"),
            "puntaje_calidad": max(0, round(puntaje, 1)),
            "estado_sugerido": estado_sugerido,
            "problemas": problemas,
            "sugerencias": sugerencias,
            "validaciones": {
                "matematicas": val_matematicas,
                "precios_logicos": precios_problematicos,
                "establecimiento": {
                    "nombre_original": factura.get("establecimiento"),
                    "nombre_normalizado": nombre_est,
                    "es_conocido": es_conocido,
                },
                "productos_sin_codigo": len(productos_sin_codigo),
            },
            "timestamp": datetime.now().isoformat(),
        }


# ==========================================
# EJEMPLO DE USO
# ==========================================

if __name__ == "__main__":
    # Datos de prueba
    factura_test = {
        "id": 123,
        "establecimiento": "OLIMPICA SUBA",
        "total_factura": 50000,
    }

    items_test = [
        {
            "nombre": "LECHE COLANTA 1100ML",
            "codigo_ean": "7702001006663",
            "cantidad": 2,
            "precio_unitario": 5500,
            "precio_total": 11000,
        },
        {
            "nombre": "MANZANA ROJA X KILO",
            "codigo_ean": None,
            "cantidad": 1.5,
            "precio_unitario": 6000,
            "precio_total": 9000,
        },
        {
            "nombre": "ARROZ DIANA 500GR",
            "codigo_ean": "7702001020799",
            "cantidad": 3,
            "precio_unitario": 3500,
            "precio_total": 10500,
        },
    ]

    # Generar reporte
    reporte = ReporteAuditoria.generar_reporte_factura(factura_test, items_test)

    print("=" * 60)
    print("REPORTE DE AUDITORÍA")
    print("=" * 60)
    print(f"Factura ID: {reporte['factura_id']}")
    print(f"Puntaje de Calidad: {reporte['puntaje_calidad']}/100")
    print(f"Estado Sugerido: {reporte['estado_sugerido'].upper()}")
    print(f"\nProblemas encontrados: {len(reporte['problemas'])}")
    for prob in reporte["problemas"]:
        print(f"  ⚠️  [{prob['severidad'].upper()}] {prob['descripcion']}")

    print(f"\nSugerencias: {len(reporte['sugerencias'])}")
    for sug in reporte["sugerencias"]:
        print(f"  💡 {sug}")
