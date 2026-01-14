import pandas as pd

# 1. Definir los nombres de las columnas en el orden correcto
column_names = [
    "Codigo", "DESCRIPCION", "UxB", "Fair", "Burzaco", "A.Korn",
    "Tucuman", "CDistrib.6", "PrecioVta"
]

# 2. Leer el CSV, saltando las filas de metadatos ( Daniel es para tabla de inventarios)
# Se salta las 16 primeras filas que contienen los encabezados y datos de informe
df = pd.read_csv("inventario.csv", skiprows=16, delimiter=';', encoding="latin1", names=column_names)

# 3. Eliminar filas con valores de encabezado o vacías
# Esto elimina filas como "-----------" o "Código" que no son datos
df = df[~df['Codigo'].str.contains('-----|Código', na=False)]

# 4. Filtrar solo filas con código numérico válido
# Primero, se eliminan los espacios en blanco
df['Codigo'] = df['Codigo'].astype(str).str.strip()
# Luego, se filtra para mantener solo los valores numéricos
df = df[df['Codigo'].str.isnumeric()]

# 5. Convertir las columnas de stock y precios a numérico
# Se reemplaza la ',' por '.' y se convierte a tipo float
cols_to_convert = ['Fair', 'Burzaco', 'A.Korn', 'Tucuman', 'CDistrib.6', 'PrecioVta']
for col in cols_to_convert:
    df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
    df[col] = pd.to_numeric(df[col], errors='coerce')

# 6. Guardar el archivo limpio
df.to_csv("inventario_limpio.csv", index=False, sep=';', encoding="utf-8")

print("Archivo limpio guardado como inventario_limpio.csv")