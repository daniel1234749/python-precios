import pandas as pd
import glob
import os

def procesar_lista():
    # Buscar automáticamente el CSV más reciente del proveedor
    archivos = glob.glob("dferreyrat*.csv")

    if not archivos:
        raise FileNotFoundError("No se encontró ningún archivo dferreyrat*.csv")

    input_file = max(archivos, key=os.path.getmtime)
    output_file = 'precios_listos.csv'

    print(f"Leyendo {input_file} con formato de tabuladores...")

    try:
        df = pd.read_csv(
            input_file,
            sep='\t',
            engine='python',
            header=None,
            encoding='latin-1'
        )

        columnas_indices = [2, 4, 7, 9, 10, 11, 12]
        df_final = df.iloc[:, columnas_indices].copy()
        df_final.columns = ['codigos', 'productos', 'precio_vta', 'of_1', 'of_2', 'of_3', 'of_5']

        def clean_num(x):
            if pd.isna(x) or str(x).strip() == "":
                return 0
            val = str(x).replace('.', '').replace(',', '.').strip()
            try:
                return int(float(val))
            except:
                return 0

        for col in ['precio_vta', 'of_1', 'of_2', 'of_3', 'of_5']:
            df_final[col] = df_final[col].apply(clean_num)

        df_final['codigos'] = df_final['codigos'].astype(str).str.strip()
        df_final['productos'] = df_final['productos'].astype(str).str.strip()

        df_final.to_csv(output_file, index=False, header=False, sep=';', encoding='utf-8')

        print(f"¡AHORA SÍ! Se procesaron {len(df_final)} filas.")
        print(f"Archivo generado listo para pgAdmin: {output_file}")

    except Exception as e:
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    procesar_lista()