# mobile_endpoints.py
"""
ENDPOINTS MÓVILES PARA LECFAC
Procesa facturas desde la app móvil con OCR completo
"""

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import tempfile
import os
from typing import Optional
from datetime import datetime

# Importar procesador OCR
from ocr_processor import OCRProcessor

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

# Inicializar procesador
processor = OCRProcessor()


@router.post("/process-invoice")
async def process_invoice(
    image: UploadFile = File(...),
    user_id: Optional[str] = Form(None),
    device_info: Optional[str] = Form(None),
    timestamp: Optional[str] = Form(None),
):
    """
    🎯 ENDPOINT PRINCIPAL MÓVIL

    Procesa una imagen/video de factura:
    1. Extrae datos con OCR (Claude Vision)
    2. Crea productos en productos_maestros
    3. Vincula con items_factura
    4. Actualiza inventario del usuario

    Returns:
        JSON con factura_id y productos creados
    """

    temp_file = None

    try:
        print("\n" + "="*80)
        print(f"📱 NUEVO REQUEST MÓVIL")
        print(f"   User ID: {user_id}")
        print(f"   Archivo: {image.filename}")
        print(f"   Device: {device_info}")
        print("="*80)

        # 1. Validar user_id
        if not user_id:
            raise HTTPException(
                status_code=400,
                detail="user_id es requerido"
            )

        usuario_id = int(user_id)

        # 2. Guardar archivo temporal
        file_extension = os.path.splitext(image.filename)[1] or '.jpg'
        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            suffix=file_extension
        )

        content = await image.read()
        temp_file.write(content)
        temp_file.close()

        print(f"💾 Archivo guardado: {temp_file.name}")
        print(f"📊 Tamaño: {len(content) / 1024:.1f} KB")

        # 3. 🚀 PROCESAR CON OCR
        print(f"\n🔍 Iniciando OCR para usuario {usuario_id}...")

        result = processor.process_invoice(
            image_path=temp_file.name,
            user_id=usuario_id,
            save_to_db=True  # ✅ Guardar automáticamente
        )

        print(f"\n✅ OCR completado")
        print(f"   Factura ID: {result.get('factura_id')}")
        print(f"   Productos: {result.get('productos_guardados', 0)}")
        print(f"   Estado: {result.get('estado', 'desconocido')}")

        # 4. Preparar respuesta
        response = {
            "success": True,
            "factura_id": result.get("factura_id"),
            "establecimiento": result.get("establecimiento", "Desconocido"),
            "total": result.get("total", 0),
            "productos_guardados": result.get("productos_guardados", 0),
            "items": result.get("items", []),
            "mensaje": "Factura procesada exitosamente",
            "timestamp": datetime.now().isoformat(),
        }

        # 5. Verificar si se crearon productos maestros
        if result.get("productos_en_maestro", 0) > 0:
            response["productos_maestros_creados"] = result["productos_en_maestro"]
            print(f"   ✅ Productos maestros creados: {result['productos_en_maestro']}")
        else:
            print(f"   ⚠️ No se crearon productos maestros")

        return JSONResponse(content=response)

    except ValueError as e:
        print(f"❌ Error de validación: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        import traceback
        print(f"\n❌ ERROR EN PROCESS-INVOICE:")
        print(traceback.format_exc())

        raise HTTPException(
            status_code=500,
            detail=f"Error procesando factura: {str(e)}"
        )

    finally:
        # Limpiar archivo temporal
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
                print(f"🗑️ Archivo temporal eliminado")
            except:
                pass


@router.get("/process-status/{factura_id}")
async def get_process_status(factura_id: int):
    """
    Consultar estado de procesamiento de una factura
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                id,
                establecimiento,
                total_factura,
                estado_validacion,
                fecha_creacion,
                (SELECT COUNT(*) FROM items_factura WHERE factura_id = f.id) as items_count,
                (SELECT COUNT(*) FROM items_factura
                 WHERE factura_id = f.id AND producto_maestro_id IS NOT NULL) as items_vinculados
            FROM facturas f
            WHERE id = ?
        """, (factura_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        return {
            "success": True,
            "factura_id": row[0],
            "establecimiento": row[1],
            "total": row[2],
            "estado": row[3],
            "fecha": row[4],
            "items_total": row[5],
            "items_vinculados": row[6],
            "procesamiento_completo": row[5] == row[6] and row[5] > 0
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error consultando estado: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/my-stats")
async def get_my_stats(user_id: Optional[int] = None):
    """
    Estadísticas del usuario desde móvil
    """
    try:
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id requerido")

        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # Contar facturas
        cursor.execute(
            "SELECT COUNT(*) FROM facturas WHERE usuario_id = ?",
            (user_id,)
        )
        total_facturas = cursor.fetchone()[0]

        # Contar productos en inventario
        cursor.execute(
            "SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = ?",
            (user_id,)
        )
        total_inventario = cursor.fetchone()[0]

        # Total gastado
        cursor.execute(
            "SELECT SUM(total_factura) FROM facturas WHERE usuario_id = ?",
            (user_id,)
        )
        total_gastado = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "success": True,
            "total_facturas": total_facturas,
            "total_inventario": total_inventario,
            "total_gastado": float(total_gastado),
        }

    except Exception as e:
        print(f"❌ Error en stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Mobile endpoints cargados")
