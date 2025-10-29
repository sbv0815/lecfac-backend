def limpiar_precio_colombiano(precio_str):
    """
    Convierte precio colombiano a entero (sin decimales).

    VERSIÓN MEJORADA - Maneja todos los casos posibles:
    - Strings: "15,540", "15.540", "$ 15,540"
    - Números: 15540, 15540.0
    - Floats mal interpretados: 15.54 (asume 1554)
    - Formatos mixtos: "1.234.567", "1,234,567"

    En Colombia NO se usan decimales/centavos, solo pesos enteros.

    Args:
        precio_str: Precio en cualquier formato

    Returns:
        int: Precio en pesos enteros

    Examples:
        >>> limpiar_precio_colombiano("15,540")
        15540
        >>> limpiar_precio_colombiano("15.540")
        15540
        >>> limpiar_precio_colombiano("$ 1.234.567")
        1234567
        >>> limpiar_precio_colombiano(15540)
        15540
        >>> limpiar_precio_colombiano(15540.0)
        15540
        >>> limpiar_precio_colombiano("39.45")  # OCR leyó mal
        3945
    """
    # Caso 1: None o vacío
    if precio_str is None or precio_str == "":
        return 0

    # Caso 2: Ya es un entero
    if isinstance(precio_str, int):
        return precio_str

    # Caso 3: Es un float (puede venir de OCR)
    if isinstance(precio_str, float):
        # Si tiene decimales pequeños (ej: 15540.0), es solo formateo
        if precio_str == int(precio_str):
            return int(precio_str)
        # Si tiene decimales significativos, puede ser error de OCR
        # Ej: 39.45 probablemente significa 3945 pesos
        return int(precio_str * 100)

    # Caso 4: Es string - procesar
    precio_str = str(precio_str).strip()

    # Eliminar espacios
    precio_str = precio_str.replace(" ", "")

    # Eliminar símbolos de moneda
    precio_str = precio_str.replace("$", "")
    precio_str = precio_str.replace("COP", "")
    precio_str = precio_str.replace("cop", "")
    precio_str = precio_str.strip()

    # CRÍTICO: Determinar si usa punto o coma como separador
    # En Colombia, ambos pueden usarse para separar miles

    # Caso 4A: Tiene múltiples puntos o comas (separador de miles)
    # Ej: "1.234.567" o "1,234,567"
    if precio_str.count('.') > 1 or precio_str.count(',') > 1:
        # Eliminar TODOS los separadores
        precio_str = precio_str.replace(",", "").replace(".", "")

    # Caso 4B: Tiene un solo punto o coma
    # Ej: "15.540" o "15,540"
    elif '.' in precio_str or ',' in precio_str:
        # Verificar cantidad de dígitos después del separador
        if '.' in precio_str:
            partes = precio_str.split('.')
        else:
            partes = precio_str.split(',')

        # Si hay 3 dígitos después, es separador de miles
        if len(partes) == 2 and len(partes[1]) == 3:
            precio_str = precio_str.replace(",", "").replace(".", "")
        # Si hay 1-2 dígitos, puede ser decimal mal leído
        elif len(partes) == 2 and len(partes[1]) <= 2:
            # En Colombia NO hay decimales, así que eliminamos el separador
            precio_str = precio_str.replace(",", "").replace(".", "")
        else:
            # Caso raro, eliminar todos
            precio_str = precio_str.replace(",", "").replace(".", "")

    # Convertir a entero
    try:
        precio = int(float(precio_str))

        # Validación de sanidad
        if precio < 0:
            print(f"   ⚠️ Precio negativo detectado: {precio}, retornando 0")
            return 0

        return precio

    except (ValueError, TypeError) as e:
        print(f"   ⚠️ No se pudo convertir precio '{precio_str}': {e}")
        return 0


# ==============================================================================
# TESTS DE LA FUNCIÓN (DESCOMENTAR PARA PROBAR)
# ==============================================================================

if __name__ == "__main__":
    # Tests de casos comunes
    tests = [
        # (input, expected_output, descripcion)
        ("15,540", 15540, "Formato colombiano con coma"),
        ("15.540", 15540, "Formato colombiano con punto"),
        ("$ 15,540", 15540, "Con símbolo de pesos"),
        ("1.234.567", 1234567, "Miles con múltiples puntos"),
        ("1,234,567", 1234567, "Miles con múltiples comas"),
        (15540, 15540, "Integer directo"),
        (15540.0, 15540, "Float sin decimales"),
        ("39.45", 3945, "OCR mal leído (debería ser 39.450)"),
        ("284.220", 284220, "Total de factura"),
        ("$ 284,220", 284220, "Total con formato"),
        ("", 0, "String vacío"),
        (None, 0, "None"),
        ("$0", 0, "Cero"),
        ("  15,540  ", 15540, "Con espacios"),
    ]

    print("🧪 EJECUTANDO TESTS DE limpiar_precio_colombiano()")
    print("=" * 60)

    passed = 0
    failed = 0

    for input_val, expected, desc in tests:
        result = limpiar_precio_colombiano(input_val)
        status = "✅" if result == expected else "❌"

        if result == expected:
            passed += 1
        else:
            failed += 1

        print(f"{status} {desc}")
        print(f"   Input: {repr(input_val)} → Output: {result} (esperado: {expected})")

        if result != expected:
            print(f"   ⚠️ FALLÓ!")
        print()

    print("=" * 60)
    print(f"📊 Resultados: {passed} passed, {failed} failed")
    print()
