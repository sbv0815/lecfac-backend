# buscar_uso_product_matcher.py
import os

print("🔍 BUSCANDO USO DE product_matcher...\n")

archivos_a_revisar = [
    'ocr_processor.py',
    'claude_invoice.py',
    'main.py'
]

for archivo in archivos_a_revisar:
    if os.path.exists(archivo):
        print(f"\n{'='*60}")
        print(f"📄 {archivo}")
        print('='*60)

        with open(archivo, 'r', encoding='utf-8') as f:
            lineas = f.readlines()

        for i, linea in enumerate(lineas, 1):
            if any(palabra in linea.lower() for palabra in ['buscar_o_crear', 'product_matcher', 'productresolver', 'resolver_producto']):
                print(f"Línea {i}: {linea.rstrip()}")
