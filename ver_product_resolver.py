# ver_product_resolver.py
import os

# Buscar archivos que contengan ProductResolver
archivos_python = []

for root, dirs, files in os.walk('.'):
    # Ignorar carpetas comunes
    if any(d in root for d in ['.git', '__pycache__', 'venv', 'env', 'node_modules']):
        continue

    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if 'ProductResolver' in content or 'product_resolver' in content:
                        archivos_python.append(filepath)
            except:
                pass

print("🔍 ARCHIVOS CON ProductResolver:\n")
for archivo in archivos_python:
    print(f"  • {archivo}")

if archivos_python:
    print("\n" + "="*60)
    print("📄 Mostrando primer archivo encontrado:")
    print("="*60)

    with open(archivos_python[0], 'r', encoding='utf-8') as f:
        print(f.read()[:5000])  # Primeros 5000 caracteres
