"""
Script de prueba para verificar la lógica de clasificación EAN/PLU
"""

# Casos de prueba según tu ejemplo
casos_prueba = [
    {
        "descripcion": "Salsa de tomate con EAN en Jumbo",
        "codigo": "7702047041482",
        "nombre": "SALSA DE TOMATE",
        "establecimiento": "JUMBO",
        "esperado": {
            "productos_maestros": {
                "codigo_ean": "7702047041482",
                "nombre_normalizado": "salsa de tomate"
            },
            "productos_por_establecimiento": {
                "debe_guardar": True,
                "codigo_plu": "7702047041482",
                "razon": "Jumbo: EAN también es PLU específico"
            }
        }
    },
    {
        "descripcion": "Misma salsa con PLU en Olímpica",
        "codigo": "1123456",
        "nombre": "SALSA DE TOMATE",
        "establecimiento": "OLIMPICA",
        "esperado": {
            "productos_maestros": {
                "codigo_ean": "7702047041482",  # Debe encontrar el mismo producto
                "nombre_normalizado": "salsa de tomate"
            },
            "productos_por_establecimiento": {
                "debe_guardar": True,
                "codigo_plu": "1123456",
                "razon": "PLU corto (7 dígitos)"
            }
        }
    },
    {
        "descripcion": "Misma salsa con PLU en D1",
        "codigo": "345674",
        "nombre": "SALSA DE TOMATE",
        "establecimiento": "D1",
        "esperado": {
            "productos_maestros": {
                "codigo_ean": "7702047041482",  # Debe encontrar el mismo producto
                "nombre_normalizado": "salsa de tomate"
            },
            "productos_por_establecimiento": {
                "debe_guardar": True,
                "codigo_plu": "345674",
                "razon": "D1: EAN también es PLU específico (o PLU corto)"
            }
        }
    },
    {
        "descripcion": "Producto con EAN en Éxito (NO Jumbo/Ara)",
        "codigo": "7702001234567",
        "nombre": "ACEITE GIRASOL 900ML",
        "establecimiento": "EXITO",
        "esperado": {
            "productos_maestros": {
                "codigo_ean": "7702001234567",
                "nombre_normalizado": "aceite girasol 900ml"
            },
            "productos_por_establecimiento": {
                "debe_guardar": False,
                "razon": "EAN universal - no se guarda como PLU"
            }
        }
    },
    {
        "descripcion": "Zanahoria con PLU en Jumbo",
        "codigo": "1045",
        "nombre": "ZANAHORIA",
        "establecimiento": "JUMBO",
        "esperado": {
            "productos_maestros": {
                "codigo_ean": None,  # PLU corto no tiene EAN
                "nombre_normalizado": "zanahoria"
            },
            "productos_por_establecimiento": {
                "debe_guardar": True,
                "codigo_plu": "1045",
                "razon": "PLU corto (4 dígitos)"
            }
        }
    },
    {
        "descripcion": "Pan con EAN en Ara",
        "codigo": "2000123456789",
        "nombre": "PAN TAJADO INTEGRAL",
        "establecimiento": "ARA",
        "esperado": {
            "productos_maestros": {
                "codigo_ean": "2000123456789",
                "nombre_normalizado": "pan tajado integral"
            },
            "productos_por_establecimiento": {
                "debe_guardar": True,
                "codigo_plu": "2000123456789",
                "razon": "Ara: EAN también es PLU específico"
            }
        }
    }
]


def clasificar_codigo_test(codigo: str) -> tuple:
    """Replica la función clasificar_codigo para testing"""
    if not codigo:
        return 'DESCONOCIDO', ''

    codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))

    if not codigo_limpio:
        return 'DESCONOCIDO', ''

    longitud = len(codigo_limpio)

    if longitud >= 8:
        return 'EAN', codigo_limpio
    elif 3 <= longitud <= 7:
        return 'PLU', codigo_limpio
    else:
        return 'DESCONOCIDO', codigo_limpio


def decidir_guardado_plu(tipo_codigo: str, cadena: str, longitud: int) -> tuple:
    """Replica la lógica de decisión para guardar PLU"""
    debe_guardar = False
    razon = ""

    if tipo_codigo == 'PLU':
        debe_guardar = True
        razon = f"PLU corto ({longitud} dígitos)"
    elif tipo_codigo == 'EAN':
        cadena_upper = cadena.upper()
        if cadena_upper in ['JUMBO', 'ARA', 'D1']:
            debe_guardar = True
            razon = f"{cadena}: EAN de {longitud} dígitos también es PLU específico"
        else:
            debe_guardar = False
            razon = "EAN universal - no se guarda como PLU"

    return debe_guardar, razon


def ejecutar_pruebas():
    """Ejecuta todos los casos de prueba"""
    print("=" * 80)
    print("🧪 PRUEBAS DE LÓGICA EAN/PLU")
    print("=" * 80)
    print()

    todos_exitosos = True

    for i, caso in enumerate(casos_prueba, 1):
        print(f"[Caso {i}] {caso['descripcion']}")
        print(f"  Código: {caso['codigo']}")
        print(f"  Nombre: {caso['nombre']}")
        print(f"  Establecimiento: {caso['establecimiento']}")
        print()

        # Clasificar código
        tipo_codigo, codigo_limpio = clasificar_codigo_test(caso['codigo'])
        print(f"  ✓ Clasificación: {tipo_codigo} ({len(codigo_limpio)} dígitos)")

        # Decidir si guardar PLU
        debe_guardar, razon = decidir_guardado_plu(
            tipo_codigo,
            caso['establecimiento'],
            len(codigo_limpio)
        )

        print(f"  ✓ Guardar PLU: {debe_guardar}")
        print(f"  ✓ Razón: {razon}")
        print()

        # Verificar contra esperado
        esperado_debe_guardar = caso['esperado']['productos_por_establecimiento']['debe_guardar']

        if debe_guardar == esperado_debe_guardar:
            print(f"  ✅ RESULTADO CORRECTO")
        else:
            print(f"  ❌ RESULTADO INCORRECTO")
            print(f"     Esperado: {esperado_debe_guardar}")
            print(f"     Obtenido: {debe_guardar}")
            todos_exitosos = False

        print()
        print("-" * 80)
        print()

    print("=" * 80)
    if todos_exitosos:
        print("✅ TODAS LAS PRUEBAS PASARON")
    else:
        print("❌ ALGUNAS PRUEBAS FALLARON")
    print("=" * 80)


if __name__ == "__main__":
    ejecutar_pruebas()
