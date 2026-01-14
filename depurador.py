import csv
import os

# --- Configuración ---
ARCHIVO_ENTRADA = 'tabla-precios.CSV' # Asegúrate que este sea el nombre correcto de tu archivo de entrada
ARCHIVO_SALIDA = 'DATOS_DEPURADOS_FINAL.CSV'
DELIMITADOR = ';' # Mantenemos el punto y coma como delimitador

# Definimos los índices (posiciones) de las columnas que queremos mantener:
# C=2, E=4, L=11, V=21, W=22, X=23, Y=24
INDICES_A_MANTENER = [2, 4, 11, 21, 22, 23, 24] 
NUM_COLUMNAS_ESPERADAS = len(INDICES_A_MANTENER)

def depurar_csv(archivo_entrada, archivo_salida, delimitador, indices_mantener):
    """
    Lee el archivo CSV y selecciona únicamente las columnas especificadas por sus índices.
    """
    print(f"Iniciando depuración de: {archivo_entrada}")
    
    lineas_procesadas = 0
    lineas_saltadas = 0
    
    # Abrir archivos con codificación UTF-8, crucial para caracteres especiales.
    with open(archivo_entrada, mode='r', encoding='utf-8') as infile:
        with open(archivo_salida, mode='w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.reader(infile, delimiter=delimitador)
            writer = csv.writer(outfile, delimiter=delimitador, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            for fila in reader:
                lineas_procesadas += 1
                
                # --- Lógica de Selección y Filtrado ---
                
                # Comprobación básica: ¿La fila tiene al menos la última columna requerida?
                # La última columna es el índice más alto: Y=24, por lo que necesitamos 25 campos (índice 0 a 24).
                if len(fila) < 25: 
                    # Esta fila es demasiado corta, la saltamos para evitar un error de índice.
                    # Esto podría ser el encabezado o una línea en blanco.
                    # Si tu archivo tiene encabezado, se saltará aquí y no se incluirá en la salida.
                    print(f"ADVERTENCIA: Fila {lineas_procesadas} saltada. Demasiados pocos campos ({len(fila)}).")
                    lineas_saltadas += 1
                    continue # Pasar a la siguiente fila
                    
                # Selecciona solo las columnas deseadas usando la lista de índices
                # Se usa una "list comprehension" para construir la nueva fila
                fila_depurada = [fila[i] for i in indices_mantener]
                
                # Escribe la fila depurada en el nuevo archivo
                writer.writerow(fila_depurada)

    print(f"\nProceso terminado.")
    print(f"Líneas totales procesadas: {lineas_procesadas - lineas_saltadas}")
    print(f"Líneas saltadas (cortas): {lineas_saltadas}")
    print(f"Archivo limpio guardado como: {os.path.abspath(archivo_salida)}")


# --- Ejecución ---
try:
    depurar_csv(ARCHIVO_ENTRADA, ARCHIVO_SALIDA, DELIMITADOR, INDICES_A_MANTENER)
except FileNotFoundError:
    print(f"\nERROR: No se encontró el archivo de entrada: '{ARCHIVO_ENTRADA}'")
    print("Asegúrate de que el archivo esté en la misma carpeta que el script de Python.")
except Exception as e:
    print(f"\nOcurrió un error inesperado: {e}")