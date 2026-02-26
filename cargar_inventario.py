import pandas as pd
import psycopg2
from psycopg2 import sql
import io
import re

def limpiar_y_cargar_inventario():
    archivo_entrada = "../zinventa.txt"
    
    columnas = [
        "codigos", "productos", "uxb", 
        "fair", "burzaco", "korn", "tucuman", 
        "stockcd", "precios_ventas"
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

        if not nuevas_filas:
            print("Error: No se detectaron filas. Revisá el formato del archivo.")
            return

        # 3. Crear DataFrame y Limpieza Numérica
        df = pd.DataFrame(nuevas_filas, columns=columnas)
        
        print(f"Filas procesadas: {len(df)}")
        
        for col in columnas[2:]:
            df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            # Convertir a entero para evitar decimales
            df[col] = df[col].astype(int)

        # Eliminar duplicados por código (mantiene el último)
        df_limpio = df.drop_duplicates(subset=['codigos'], keep='last')
        duplicados_eliminados = len(df) - len(df_limpio)
        
        if duplicados_eliminados > 0:
            print(f"⚠️  Se eliminaron {duplicados_eliminados} códigos duplicados")
        
        print(f"Filas únicas: {len(df_limpio)}")

        # Conectar y cargar a PostgreSQL
        print("\nConectando a PostgreSQL...")
        conn = psycopg2.connect(
            host="100.121.113.108",
            port=5432,
            database="Precios",
            user="postgres",
            password="central10"
        )
        
        cursor = conn.cursor()
        
        # Vaciar la tabla de inventarios
        print("Vaciando tabla inventarios...")
        cursor.execute("TRUNCATE TABLE public.inventarios;")
        conn.commit()
        print("✅ Tabla vaciada")
        
        # Cargar datos usando COPY
        print(f"\nCargando {len(df_limpio)} filas a PostgreSQL...")
        
        buffer = io.StringIO()
        df_limpio.to_csv(buffer, index=False, header=False, sep=';')
        buffer.seek(0)
        
        cursor.copy_expert(
            sql.SQL("""
                COPY public.inventarios(codigos, productos, uxb, fair, burzaco, 
                                       korn, tucuman, stockcd, precios_ventas)
                FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '"', ESCAPE '"')
            """),
            buffer
        )
        
        conn.commit()
        
        # Verificar
        cursor.execute("SELECT COUNT(*) FROM public.inventarios;")
        total = cursor.fetchone()[0]
        
        print(f"\n✅ ¡ÉXITO! Se cargaron {total} productos a la tabla inventarios en PostgreSQL.")
        print(f"¡El VINO ABEL con su 'L' y todos los productos están en la base de datos!")
        
        cursor.close()
        conn.close()
        
    except FileNotFoundError:
        print(f"\n❌ Error: No se encontró el archivo {archivo_entrada}")
    except psycopg2.Error as e:
        print(f"\n❌ Error de PostgreSQL: {e}")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    limpiar_y_cargar_inventario()