import csv
import re
from datetime import datetime

def procesar_todas_las_sucursales(archivo_entrada, archivo_salida):
    registros = []
    sucursal_id = None
    sucursal_nombre = None
    fecha_reporte = None

    # Leemos con latin-1 para no perder caracteres
    with open(archivo_entrada, 'r', encoding='latin-1', errors='replace') as f:
        lineas = f.readlines()

    for linea in lineas:
        linea_limpia = linea.strip()
        if not linea_limpia: continue

        # 1. Capturar Fecha
        if "Del " in linea_limpia and not fecha_reporte:
            match_f = re.search(r'(\d{2})/(\d{2})/(\d{4})', linea_limpia)
            if match_f:
                fecha_reporte = f"{match_f.group(1)}/{match_f.group(2)}/{match_f.group(3)}"

        # 2. Capturar Sucursal
        if "Sucursal" in linea_limpia:
            match_suc = re.search(r'Sucursal\s+(\d+)\s+(.+)', linea_limpia)
            if match_suc:
                sucursal_id = match_suc.group(1).strip()
                sucursal_nombre = re.split(r'\s{2,}', match_suc.group(2).strip())[0]
            continue

        # 3. Capturar Artículos (Técnica de recorte central)
        if linea_limpia.startswith("Art"):
            # Buscamos el código (segunda palabra)
            partes = linea_limpia.split()
            if len(partes) < 6: continue
            
            codigo = partes[1]
            if not codigo.isdigit(): continue

            # --- LÓGICA DE EXTRACCIÓN TOTAL ---
            # 1. Quitamos el encabezado "Articulo [CODIGO]"
            inicio_desc = linea.find(codigo) + len(codigo)
            contenido_desde_desc = linea[inicio_desc:]
            
            # 2. Buscamos donde empiezan las columnas numéricas finales (precio/cantidad)
            # Buscamos el patrón de los asteriscos "****" o el primer número de la columna de precio
            match_final = re.search(r'\s+(\d+[\d\.,]*)\s+(\d+)\s+\*{4}', contenido_desde_desc)
            
            if match_final:
                # La descripción es todo lo que hay DESDE el código HASTA los números finales
                fin_desc = match_final.start()
                descripcion = contenido_desde_desc[:fin_desc].strip()
                
                # Limpiamos los caracteres extraños (rombos)
                descripcion = re.sub(r'[^\w\s\.\-\/]', '', descripcion)
                # Quitamos espacios dobles
                descripcion = ' '.join(descripcion.split())
                
                cantidad_raw = match_final.group(1)
                cantidad_str = cantidad_raw.replace('.', '').replace(',', '.')

                registros.append({
                    'fecha': fecha_reporte if fecha_reporte else "",
                    'sucursal_id': sucursal_id,
                    'sucursal_no': sucursal_nombre,
                    'codigo': codigo,
                    'descripcion': descripcion,
                    'cantidad': cantidad_str
                })

    # Guardar a CSV
    with open(archivo_salida, 'w', newline='', encoding='utf-8-sig') as f:
        campos = ['fecha', 'sucursal_id', 'sucursal_no', 'codigo', 'descripcion', 'cantidad']
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(registros)

    return len(registros)

# Ejecutar
try:
    total = procesar_todas_las_sucursales('zdiaria.txt', 'ventas_final.csv')
    print(f"✅ ¡Proceso terminado! Se cargaron {total} productos con descripción completa.")
except Exception as e:
    print(f"❌ Error: {e}")