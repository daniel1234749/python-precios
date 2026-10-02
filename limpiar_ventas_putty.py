import pandas as pd
import re

def limpiar_archivo():
    archivo_entrada = "../zventas.txt"
    archivo_salida = "ventas_final_para_pgadmin.csv"

    columnas = [
        "codigos", "productos", "uxb", "fair_ventas", "burzaco_ventas",
        "a_korn_ventas", "tucuman_ventas", "fair_stock", "burzaco_stock",
        "a_korn_stock", "tucuman_stock", "cdistrib", "ventas", "stock"
    ]

    # Un token valido para las columnas numericas: entero/decimal (con . o , como
    # separador), negativo opcional, o una serie de asteriscos (overflow del reporte).
    # Si alguno de los 13 campos finales NO matchea esto, es porque el conteo de
    # columnas se desalineo en esa linea (celdas vacias en el reporte original, etc.)
    # y la fila se descarta en vez de meter basura en uxb u otras columnas.
    token_numerico = re.compile(r'^-?\d[\d.,]*$|^\*+$')

    print(f"Analizando {archivo_entrada}...")
    nuevas_filas = []

    with open(archivo_entrada, "r", encoding="latin1") as f:
        for linea in f:
            # Limpiamos basura de PuTTY
            linea_limpia = re.sub(r'\[[0-9;]*[a-zA-Z]', '', linea).strip()

            if re.match(r'^\d{1,7}\s+', linea_limpia):
                partes = linea_limpia.split()

                if len(partes) >= 14:
                    codigo = partes[0]
                    n = partes[-13:]  # Los 13 valores numericos finales

                    # Descartamos filas desalineadas (ej: celdas vacias o "**********"
                    # que corren el conteo y meten palabras del producto en los numeros)
                    if not all(token_numerico.match(x) for x in n):
                        continue

                    # Reconstruimos el producto uniendo lo que hay entre el codigo y los numeros
                    producto = " ".join(partes[1:-13])

                    # Limpieza de caracteres rotos (Ñ y acentos)
                    producto = producto.replace('袿', 'Ñ').replace('髇', 'Ó')

                    # Sacamos comas del nombre de producto (ej: "x4,5LT" -> "x4.5LT").
                    # No son necesarias y en Excel, segun la config regional, pueden
                    # confundirse con el separador de columnas y dejar todo lo de
                    # despues vacio o desordenado (esto NO afecta a pgAdmin/Postgres,
                    # que usa el ';' real del archivo, pero evita problemas al abrirlo
                    # en Excel para revisar).
                    producto = producto.replace(',', '.')

                    # Armamos la fila saltando la columna de ceros (n[5])
                    fila = [
                        codigo, producto, n[0], n[1], n[2], n[3], n[4],
                        n[6], n[7], n[8], n[9], n[10], n[11], n[12]
                    ]
                    nuevas_filas.append(fila)

    if not nuevas_filas:
        print("Error: No se detectaron filas. Revisá el formato del archivo.")
        return

    # Crear DataFrame
    df = pd.DataFrame(nuevas_filas, columns=columnas)

    # Limpieza numérica (puntos de miles y comas decimales)
    for col in columnas[2:]:
        df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Guardado para pgAdmin
    df.to_csv(archivo_salida, index=False, sep=';', encoding="utf-8-sig")

    print(f"¡LOGRADO! Se procesaron {len(df)} filas.")

if __name__ == "__main__":
    limpiar_archivo()