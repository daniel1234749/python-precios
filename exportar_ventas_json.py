import psycopg2
import json
from datetime import datetime, timedelta

def exportar_ventas_a_json(dias_atras=90):
    """
    Exporta las ventas de los últimos X días desde PostgreSQL a JSON
    para usar en el dashboard web
    """
    try:
        print("🔌 Conectando a PostgreSQL...")
        conn = psycopg2.connect(
            host="100.121.113.108",
            port=5432,
            database="Precios",
            user="postgres",
            password="central10"
        )
        
        cursor = conn.cursor()
        
        # Calcular fecha desde
        fecha_desde = (datetime.now() - timedelta(days=dias_atras)).strftime('%Y-%m-%d')
        
        print(f"📊 Exportando ventas desde {fecha_desde}...")
        
        # Consultar ventas
        cursor.execute("""
            SELECT 
                fecha,
                sucursal_id,
                sucursal_nombre,
                codigo,
                descripcion,
                cantidad
            FROM ventas_diarias
            WHERE fecha >= %s
            ORDER BY fecha DESC, codigo
        """, (fecha_desde,))
        
        registros = cursor.fetchall()
        
        # Convertir a lista de diccionarios
        ventas = []
        for row in registros:
            ventas.append({
                'fecha': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'sucursal_id': row[1],
                'sucursal_nombre': row[2],
                'codigo': row[3],
                'descripcion': row[4],
                'cantidad': float(row[5]) if row[5] else 0
            })
        
        # Guardar a JSON
        with open('ventas_data.json', 'w', encoding='utf-8') as f:
            json.dump(ventas, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Exportados {len(ventas)} registros a ventas_data.json")
        
        # Estadísticas
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT fecha) as dias,
                COUNT(DISTINCT codigo) as productos,
                COUNT(DISTINCT sucursal_id) as sucursales,
                SUM(cantidad) as total_cantidad
            FROM ventas_diarias
            WHERE fecha >= %s
        """, (fecha_desde,))
        
        stats = cursor.fetchone()
        
        print(f"\n📈 Estadísticas:")
        print(f"   • Días con datos: {stats[0]}")
        print(f"   • Productos únicos: {stats[1]}")
        print(f"   • Sucursales: {stats[2]}")
        print(f"   • Total unidades vendidas: {stats[3]:,.0f}")
        
        cursor.close()
        conn.close()
        
        return len(ventas)
        
    except psycopg2.Error as e:
        print(f"❌ Error de PostgreSQL: {e}")
        return 0
    except Exception as e:
        print(f"❌ Error: {e}")
        return 0

if __name__ == "__main__":
    print("=" * 60)
    print("EXPORTADOR DE VENTAS PARA DASHBOARD")
    print("=" * 60)
    print()
    
    dias = input("¿Cuántos días hacia atrás quieres exportar? (default: 90): ").strip()
    dias = int(dias) if dias.isdigit() else 90
    
    total = exportar_ventas_a_json(dias)
    
    if total > 0:
        print(f"\n🎉 Archivo 'ventas_data.json' creado exitosamente")
        print(f"   Ahora puedes usar este archivo en tu dashboard web")
