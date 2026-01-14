import pandas as pd

# Ruta a tu CSV original
archivo = "ventas.csv"

# Renombrar columnas para evitar problemas de formato
column_names = [
    "codigos",
    "productos",
    "UXB",
    "Fair_ventas",
    "Burzaco_ventas",
    "A.Korn_ventas",
    "Tucuman_ventas",
    "Fair_stock",
    "Burzaco_stock",
    "A.Korn_stock",
    "Tucuman_stock",
    "CDistrib",
    "Ventas",
    "Stock"
]

# Leer CSV, saltando las 16 primeras filas de encabezado,
# y usando el delimitador ';' y la codificación "latin1".
df = pd.read_csv(archivo, skiprows=16, encoding="latin1", delimiter=';', names=column_names)

# Filtrar solo filas con código numérico válido
# La columna 'codigos' puede tener espacios, así que los eliminamos primero con .str.strip()
df = df[df["codigos"].astype(str).str.strip().str.isnumeric()]

# Convertir las columnas de ventas y stock a numérico para poder operar con ellas.
# Reemplazamos ',' por '.' y convertimos a tipo float.
# Usaremos 'errors='coerce'' para que los valores que no puedan convertirse,
# se conviertan a NaN (valores nulos).
for col in df.columns[3:]:  # Desde 'Fair_ventas' hasta 'Stock'
    df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
    df[col] = pd.to_numeric(df[col], errors='coerce')


# Guardar archivo limpio usando punto y coma como delimitador
df.to_csv("ventas_limpio.csv", index=False, encoding="utf-8", sep=';')

print("Archivo limpio guardado como ventas_limpio.csv")