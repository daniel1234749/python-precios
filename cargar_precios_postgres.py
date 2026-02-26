import pandas as pd
import psycopg2
from psycopg2 import sql
import io

def cargar_precios_a_postgres():
    # 1. Leer y limpiar el CSV
    print("Leyendo precios_listos.csv...")
    df = pd.read_csv(
        'precios_listos.csv', 
        sep=';', 
        header=None,
        names=['codigos', 'productos', 'precio_vta', 'of_1', 'of_2', 'of_3', 'of_5'],
        dtype={'codigos': str}
    )
    
    print(f"Filas leídas: {len(df)}")
    
    # 2. Eliminar duplicados (mantiene el último)
    df_limpio = df.drop_duplicates(subset=['codigos'], keep='last')
    duplicados_eliminados = len(df) - len(df_limpio)
    
    if duplicados_eliminados > 0:
        print(f"⚠️  Se eliminaron {duplicados_eliminados} códigos duplicados")
    
    print(f"Filas únicas: {len(df_limpio)}")
    
    # 3. Conectar a PostgreSQL
    try:
        print("\nConectando a PostgreSQL...")
        conn = psycopg2.connect(
            host="100.121.113.108",
            port=5432,
            database="Precios",
            user="postgres",
            password="central10"  # ⚠️ CAMBIA ESTO
        )
        
        cursor = conn.cursor()
        
        # 4. Vaciar la tabla (TRUNCATE)
        print("Vaciando tabla precios_productos...")
        cursor.execute("TRUNCATE TABLE public.precios_productos;")
        conn.commit()
        print("✅ Tabla vaciada")
        
        # 5. Cargar datos usando COPY
        print(f"\nCargando {len(df_limpio)} filas a PostgreSQL...")
        
        buffer = io.StringIO()
        df_limpio.to_csv(buffer, index=False, header=False, sep=';')
        buffer.seek(0)
        
        cursor.copy_expert(
            sql.SQL("""
                COPY public.precios_productos(codigos, productos, precio_vta, of_1, of_2, of_3, of_5)
                FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '"', ESCAPE '"')
            """),
            buffer
        )
        
        conn.commit()
        
        # 6. Verificar
        cursor.execute("SELECT COUNT(*) FROM public.precios_productos;")
        total = cursor.fetchone()[0]
        
        print(f"\n✅ ¡ÉXITO! Se cargaron {total} productos a la base de datos.")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"\n❌ Error de PostgreSQL: {e}")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    cargar_precios_a_postgres()