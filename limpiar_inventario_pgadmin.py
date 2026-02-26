import pandas as pd
import re

def limpiar_inventario_definitivo():
    archivo_entrada = "../zinventa.txt"
    archivo_salida = "inventario_precios_final.csv"
    
    columnas = [
        "codigo", "descripcion", "uxb", 
        "fair_stock", "burzaco_stock", "akorn_stock", "tucuman_stock", 
        "cdistrib", "precio_vta"
    ]

    print(f"Procesando {archivo_entrada}...")
    nuevas_filas = []

    try:
        with open(archivo_entrada, "r", encoding="latin1") as f:
            for linea in f:
                # 1. Limpiamos la basura de PuTTY
                linea_limpia = re.sub(r'\[[0-9;]*[a-zA-Z]', '', linea).strip()
                
                # 2. Identificamos líneas con código (79, 4156154, etc.)
                if re.match(r'^\d{1,7}\s+', linea_limpia):
                    partes = linea_limpia.split()
                    
                    # Buscamos el bloque de números final. 
                    # En zinventa.txt, el patrón es: [UXB] [U/M] [Adminis] [Fair] [Burzaco] [AKorn] [Tucuman] [CDist] [Precio]
                    # Son exactamente 9 bloques de datos después de la descripción.
                    if len(partes) >= 10:
                        codigo = partes[0]
                        n = partes[-9:] # Los 9 datos finales fijos
                        
                        # Aquí está la magia: la descripción es todo lo que está 
                        # entre el código y los 9 bloques finales.
                        descripcion = " ".join(partes[1:-9])
                        descripcion = descripcion.replace('袿', 'Ñ').replace('髇', 'Ó').strip()
                        
                        fila = [
                            codigo,
                            descripcion,
                            n[0], # UXB
                            n[3], # Fair
                            n[4], # Burzaco
                            n[5], # A.Korn
                            n[6], # Tucuman
                            n[7], # CDist
                            n[8]  # Precio Venta
                        ]
                        nuevas_filas.append(fila)

        # 3. Crear DataFrame y Limpieza Numérica
        df = pd.DataFrame(nuevas_filas, columns=columnas)
        
        for col in columnas[2:]:
            # Limpiamos puntos de miles y comas decimales para Postgres
            df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 4. Guardado con ";" para pgAdmin
        df.to_csv(archivo_salida, index=False, sep=';', encoding="utf-8-sig")
        
        print(f"¡LOGRADO! El VINO ABEL con su 'L' y todos los productos están listos.")
        print(f"Archivo generado: {archivo_salida}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    limpiar_inventario_definitivo()