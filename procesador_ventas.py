import csv
import re
from datetime import datetime

def procesar_todas_las_sucursales(archivo_entrada, archivo_salida):
    # Fecha por defecto (hoy) por si no se encuentra en el archivo
    fecha_reporte = datetime.now().strftime('%Y-%m-%d')
    registros = []
    sucursal_id = None
    sucursal_nombre = None

    with open(archivo_entrada, 'r', encoding='latin-1') as f:
        lineas = f.readlines()

    # --- PASO 1: Buscar la fecha en todo el archivo ---
    for linea in lineas:
        # Busca patrones como 09/01/2026 o 09-01-2026
        match_fecha = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', linea)
        if match_fecha:
            dia, mes, anio = match_fecha.groups()
            fecha_reporte = f"{anio}-{mes}-{dia}"
            print(f"Fecha detectada en el archivo: {fecha_reporte}")
            break # Detenemos la búsqueda al encontrar la primera fecha

    # --- PASO 2: Procesar los datos ---
    for num_linea, linea in enumerate(lineas, 1):
        linea = linea.strip('\n')
        
        # Detectar Sucursal
        if "Sucursal" in linea:
            match_suc = re.search(r'Sucursal\s+(\d+)\s+(.+)', linea)
            if match_suc:
                sucursal_id = match_suc.group(1).strip()
                # Limpiamos el nombre de restos numéricos que vimos en tu terminal
                sucursal_nombre = re.split(r'\s{2,}', match_suc.group(2).strip())[0]
                print(f"Procesando Sucursal: {sucursal_id} - {sucursal_nombre}")
            continue

        # Detectar Artículos
        match_art = re.search(r'(\d{5,9})\s+(.*?)\s+([\d\.,]+)(?=\s|\Z)', linea)
        
        if match_art and sucursal_id:
            try:
                codigo = match_art.group(1)
                descripcion = match_art.group(2).strip()
                total_raw = match_art.group(3)
                
                total_str = total_raw.replace('.', '').replace(',', '.')
                
                registros.append({
                    'fecha': fecha_reporte,
                    'sucursal_id': int(sucursal_id),
                    'sucursal_nombre': sucursal_nombre,
                    'codigo': int(codigo),
                    'descripcion': descripcion,
                    'cantidad': float(total_str) if total_str else 0.0
                })
            except ValueError:
                continue

    # Guardar a CSV
    with open(archivo_salida, 'w', newline='', encoding='utf-8') as f:
        campos = ['fecha', 'sucursal_id', 'sucursal_nombre', 'codigo', 'descripcion', 'cantidad']
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(registros)

    return len(registros), fecha_reporte

# Ejecutar
try:
    total, fecha_usada = procesar_todas_las_sucursales('zdiaria.txt', 'ventas_todas_las_sucursales.csv')
    print(f"--- PROCESO TERMINADO ---")
    print(f"Fecha aplicada: {fecha_usada}")
    print(f"Total de registros: {total}")
except FileNotFoundError:
    print("Error: No se encontró el archivo 'zdiaria.txt'")