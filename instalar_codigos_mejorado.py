from database import instalar_sistema_codigos_mejorado

if __name__ == "__main__":
    print("🚀 Instalando sistema de códigos mejorado...")
    print("-" * 80)

    exito = instalar_sistema_codigos_mejorado()

    if exito:
        print("\\n✅ ¡Instalación completada con éxito!")
        print("\\n📱 Ahora puedes:")
        print("   • Escanear facturas de JUMBO, ARA, D1 → detectará EAN automáticamente")
        print("   • Escanear facturas de ÉXITO, CARULLA, OLÍMPICA → detectará PLU locales")
        print("   • Escanear de supermercados nuevos → inferencia inteligente")
    else:
        print("\\n❌ Error en la instalación")
        print("   Revisa los mensajes de error arriba")
