import pandas as pd
import psycopg2
from psycopg2 import sql
import io
import re

def limpiar_y_cargar_ventas():
    archivo_entrada = "../zventas.txt"
    
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
    
    print(f"Filas procesadas: {len(df)}")
    
    # Limpieza numérica (puntos de miles y comas decimales)
    for col in columnas[2:]:
        # Quitamos puntos de miles y pasamos coma a punto
        df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Eliminar duplicados por código (mantiene el último)
    df_limpio = df.drop_duplicates(subset=['codigos'], keep='last')
    duplicados_eliminados = len(df) - len(df_limpio)
    
    if duplicados_eliminados > 0:
        print(f"⚠️  Se eliminaron {duplicados_eliminados} códigos duplicados")
    
    print(f"Filas únicas: {len(df_limpio)}")

    # Conectar y cargar a PostgreSQL
    try:
        print("\nConectando a PostgreSQL...")
        conn = psycopg2.connect(
            host="100.121.113.108",
            port=5432,
            database="Precios",  # ⚠️ Cambia si tu base de datos tiene otro nombre
            user="postgres",
            password="central10"
        )
        
        cursor = conn.cursor()
        
        # Vaciar la tabla de ventas
        print("Vaciando tabla ventas...")
        cursor.execute("TRUNCATE TABLE public.ventas;")  # ⚠️ Ajusta el nombre si es diferente
        conn.commit()
        print("✅ Tabla vaciada")
        
        # Cargar datos usando COPY
        print(f"\nCargando {len(df_limpio)} filas a PostgreSQL...")
        
        buffer = io.StringIO()
        df_limpio.to_csv(buffer, index=False, header=False, sep=';')
        buffer.seek(0)
        
        cursor.copy_expert(
            sql.SQL("""
                COPY public.ventas(codigos, productos, uxb, fair_ventas, burzaco_ventas, 
                                   a_korn_ventas, tucuman_ventas, fair_stock, burzaco_stock, 
                                   a_korn_stock, tucuman_stock, cdistrib, ventas, stock)
                FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '"', ESCAPE '"')
            """),
            buffer
        )
        
        conn.commit()
        
        # Verificar
        cursor.execute("SELECT COUNT(*) FROM public.ventas;")
        total = cursor.fetchone()[0]
        
        print(f"\n✅ ¡ÉXITO! Se cargaron {total} productos con ventas a la base de datos.")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"\n❌ Error de PostgreSQL: {e}")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    limpiar_y_cargar_ventas()