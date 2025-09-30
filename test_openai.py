#!/usr/bin/env python3
"""
Script para probar el procesamiento de facturas con OpenAI Vision
Uso: python test_openai.py ruta/a/factura.jpg
"""

import sys
import os
import json
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Verificar API key
if not os.getenv("OPENAI_API_KEY"):
    print("❌ Error: OPENAI_API_KEY no está configurada")
    print("Configura la variable de entorno o crea un archivo .env")
    sys.exit(1)

from openai_invoice import parse_invoice_with_openai

def main():
    if len(sys.argv) < 2:
        print("Uso: python test_openai.py ruta/a/factura.jpg")
        sys.exit(1)
    
    image_path = sys.argv[1]
    
    if not os.path.exists(image_path):
        print(f"❌ Error: El archivo {image_path} no existe")
        sys.exit(1)
    
    print(f"🔍 Procesando: {image_path}")
    print("=" * 70)
    
    try:
        resultado = parse_invoice_with_openai(image_path)
        
        print("\n✅ RESULTADO:")
        print("=" * 70)
        print(f"📍 Establecimiento: {resultado['establecimiento']}")
        print(f"💰 Total: ${resultado['total']:,}" if resultado['total'] else "💰 Total: No detectado")
        print(f"📦 Productos detectados: {len(resultado['productos'])}")
        print(f"🤖 Modelo: {resultado['metadatos']['model']}")
        
        print("\n📋 PRODUCTOS:")
        print("-" * 70)
        for i, prod in enumerate(resultado['productos'][:10], 1):
            codigo = prod.get('codigo') or '(sin código)'
            nombre = prod.get('nombre')
            valor = prod.get('valor', 0)
            print(f"{i:2d}. [{codigo:15s}] {nombre:40s} ${valor:>8,}")
        
        if len(resultado['productos']) > 10:
            print(f"\n... y {len(resultado['productos']) - 10} productos más")
        
        # Guardar resultado completo
        output_file = image_path.rsplit('.', 1)[0] + '_result.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Resultado completo guardado en: {output_file}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
