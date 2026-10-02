import csv
import re
import psycopg2
from psycopg2 import sql
from datetime import datetime

def procesar_y_cargar_ventas(archivo_entrada):
    """
    Procesa zdiaria.txt y carga directamente a PostgreSQL en la tabla ventas_diarias
    """
    registros = []
    sucursal_id = None
    sucursal_nombre = None
    fecha_reporte = None

    print(f"📄 Procesando {archivo_entrada}...")
    
    # Leer archivo con latin-1
    with open(archivo_entrada, 'r', encoding='latin-1', errors='replace') as f:
        lineas = f.readlines()

    for linea in lineas:
        linea_limpia = linea.strip()
        if not linea_limpia: 
            continue

        # 1. Capturar Fecha
        if "Del " in linea_limpia and not fecha_reporte:
            match_f = re.search(r'(\d{2})/(\d{2})/(\d{4})', linea_limpia)
            if match_f:
                # Convertir a formato YYYY-MM-DD para PostgreSQL
                dia = match_f.group(1)
                mes = match_f.group(2)
                anio = match_f.group(3)
                fecha_reporte = f"{anio}-{mes}-{dia}"
                print(f"📅 Fecha detectada: {fecha_reporte}")

        # 2. Capturar Sucursal
        if "Sucursal" in linea_limpia:
            match_suc = re.search(r'Sucursal\s+(\d+)\s+(.+)', linea_limpia)
            if match_suc:
                sucursal_id = int(match_suc.group(1).strip())
                sucursal_nombre = re.split(r'\s{2,}', match_suc.group(2).strip())[0]
                print(f"🏢 Sucursal detectada: {sucursal_id} - {sucursal_nombre}")
            continue

        # 3. Capturar Artículos
        # Buscar líneas que contengan "Art" (con o sin basura antes)
        if "Art" in linea_limpia and "culo" in linea_limpia:
            # Buscar el código (8 dígitos que puede empezar con 0)
            match_codigo = re.search(r'\b(\d{7,9})\b', linea)
            if not match_codigo:
                continue
                
            codigo = match_codigo.group(1).lstrip('0')  # Quitar ceros a la izquierda
            if not codigo:  # Si queda vacío, significa que era 00000000
                codigo = '0'
            
            # Buscar la cantidad (número con posibles puntos de miles y coma decimal)
            # Patrón: cantidad seguida de número (nivel) y asteriscos
            match_cantidad = re.search(r'\s+(\d{1,6}(?:[,\.]\d{2})?)\s+\d+\s+\*+', linea)
            if not match_cantidad:
                continue
            
            # Extraer descripción (está entre el código y la cantidad)
            inicio_desc = match_codigo.end()
            fin_desc = match_cantidad.start()
            contenido_desde_desc = linea[inicio_desc:fin_desc]
            # Extraer descripción (está entre el código y la cantidad)
            inicio_desc = match_codigo.end()
            fin_desc = match_cantidad.start()
            contenido_desde_desc = linea[inicio_desc:fin_desc]
            
            # Limpiar descripción
            descripcion = contenido_desde_desc.strip()
            descripcion = re.sub(r'[^\w\s\.\-\/\(\)xX]', '', descripcion)
            descripcion = ' '.join(descripcion.split())
            
            if not descripcion:
                continue
            
            # Limpiar cantidad (quitar puntos de miles, convertir coma a punto)
            cantidad_raw = match_cantidad.group(1)
            cantidad_str = cantidad_raw.replace('.', '').replace(',', '.')
            
            try:
                cantidad = float(cantidad_str)
            except ValueError:
                print(f"⚠️ Error convirtiendo cantidad para código {codigo}: {cantidad_str}")
                continue

            registros.append({
                'fecha': fecha_reporte if fecha_reporte else None,
                'sucursal_id': sucursal_id,
                'sucursal_nombre': sucursal_nombre,
                'codigo': int(codigo),
                'descripcion': descripcion,
                'cantidad': cantidad
            })

    print(f"✅ Procesados {len(registros)} registros")
    
    if not registros:
        print("❌ No se encontraron registros para cargar")
        return 0

    # Conectar a PostgreSQL y cargar
    try:
        print("\n🔌 Conectando a PostgreSQL...")
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="Precios",
            user="postgres",
            password="central10"
        )
        
        cursor = conn.cursor()
        
        # Verificar si ya existen registros para esta fecha
        fecha_carga = registros[0]['fecha']
        cursor.execute(
            "SELECT COUNT(*) FROM ventas_diarias WHERE fecha = %s",
            (fecha_carga,)
        )
        existentes = cursor.fetchone()[0]
        
        if existentes > 0:
            print(f"⚠️ Ya existen {existentes} registros para la fecha {fecha_carga}")
            respuesta = input("¿Deseas eliminarlos y recargar? (s/n): ").lower()
            if respuesta == 's':
                cursor.execute("DELETE FROM ventas_diarias WHERE fecha = %s", (fecha_carga,))
                conn.commit()
                print(f"🗑️ Eliminados {existentes} registros anteriores")
            else:
                print("❌ Carga cancelada")
                cursor.close()
                conn.close()
                return 0
        
        # Insertar registros
        print(f"\n📥 Cargando {len(registros)} registros a PostgreSQL...")
        
        insert_query = sql.SQL("""
            INSERT INTO ventas_diarias (fecha, sucursal_id, sucursal_nombre, codigo, descripcion, cantidad)
            VALUES (%s, %s, %s, %s, %s, %s)
        """)
        
        for registro in registros:
            cursor.execute(
                insert_query,
                (
                    registro['fecha'],
                    registro['sucursal_id'],
                    registro['sucursal_nombre'],
                    registro['codigo'],
                    registro['descripcion'],
                    registro['cantidad']
                )
            )
        
        conn.commit()
        
        # Verificar carga
        cursor.execute("SELECT COUNT(*) FROM ventas_diarias WHERE fecha = %s", (fecha_carga,))
        total_cargado = cursor.fetchone()[0]
        
        print(f"\n✅ ¡ÉXITO! Se cargaron {total_cargado} registros para la fecha {fecha_carga}")
        
        cursor.close()
        conn.close()
        
        return total_cargado
        
    except psycopg2.Error as e:
        print(f"\n❌ Error de PostgreSQL: {e}")
        return 0
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        return 0

if __name__ == "__main__":
    try:
        total = procesar_y_cargar_ventas('zdiaria.txt')
        if total > 0:
            print(f"\n🎉 Proceso completado exitosamente")
    except FileNotFoundError:
        print("❌ Error: No se encontró el archivo 'zdiaria.txt'")
        print("   Asegúrate de que el archivo esté en la misma carpeta que este script")
    except Exception as e:
        print(f"❌ Error: {e}")