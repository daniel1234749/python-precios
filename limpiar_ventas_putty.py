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

    print(f"Analizando {archivo_entrada}...")
    nuevas_filas = []

    with open(archivo_entrada, "r", encoding="latin1") as f:
        for linea in f:
            # Limpiamos basura de PuTTY
            linea_limpia = re.sub(r'\[[0-9;]*[a-zA-Z]', '', linea).strip()
            
            # CAMBIO CLAVE: Ahora acepta códigos desde 1 dígito (\d{1,7})
            if re.match(r'^\d{1,7}\s+', linea_limpia):
                partes = linea_limpia.split()
                
                # Verificamos que tenga la estructura de números al final
                if len(partes) >= 14: 
                    codigo = partes[0]
                    n = partes[-13:] # Los 13 valores numéricos finales
                    
                    # Reconstruimos el producto uniendo lo que hay entre el código y los números
                    producto = " ".join(partes[1:-13])
                    
                    # Limpieza de caracteres rotos (Ñ y acentos)
                    producto = producto.replace('袿', 'Ñ').replace('髇', 'Ó')
                    
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
        # Quitamos puntos de miles y pasamos coma a punto
        df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Guardado para pgAdmin
    df.to_csv(archivo_salida, index=False, sep=';', encoding="utf-8-sig")
    
    print(f"¡LOGRADO! Se procesaron {len(df)} filas.")
    print(f"El producto de 2 dígitos ahora debería aparecer en el archivo.")

if __name__ == "__main__":
    limpiar_archivo()