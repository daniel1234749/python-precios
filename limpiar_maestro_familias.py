#!/usr/bin/env python3
"""
Limpia el export del sistema (TXT/CSV separado por TAB, cp1252, sin encabezado)
y genera un CSV UTF-8 listo para cargar en la tabla maestro_familias:

    familia | codigos (PK) | dum | productos

Uso:
    py limpiar_maestro_familias.py              -> toma el dferreyrat*.csv más reciente
    py limpiar_maestro_familias.py entrada.csv  -> usa ese archivo
"""
import csv
import glob
import os
import re
import sys

CARPETA = os.path.dirname(os.path.abspath(__file__))
PATRON = "dferreyrat*.csv"

if len(sys.argv) > 1:
    entrada = sys.argv[1]
else:
    candidatos = glob.glob(os.path.join(CARPETA, PATRON))
    if not candidatos:
        sys.exit(f"No encontré ningún archivo {PATRON} en {CARPETA}")
    # el nombre lleva fecha+hora (AAAAMMDDHHMM), así que el mayor es el último
    entrada = max(candidatos, key=os.path.basename)

print(f"Procesando: {os.path.basename(entrada)}")
salida = os.path.join(CARPETA, "maestro_familias_limpio.csv")

# Columnas del archivo origen (índice 0-based)
COL_FAMILIA, COL_CODIGO, COL_DUM, COL_PRODUCTO = 1, 2, 3, 4

# Familias que no se cargan (vacías o basura del sistema)
FAMILIAS_EXCLUIDAS = {"", "NO EXISTE TIENE VENTAS"}


def limpiar(txt: str) -> str:
    txt = txt.replace(" ", " ")          # espacios duros
    txt = re.sub(r"[\x00-\x1f\x7f]", "", txt)  # caracteres de control
    return re.sub(r"\s+", " ", txt).strip()    # espacios dobles / bordes


vistos = set()
ok, excluidos, duplicados, sin_dum = 0, [], [], 0

with open(entrada, encoding="cp1252", newline="") as fin, \
     open(salida, "w", encoding="utf-8", newline="") as fout:
    lector = csv.reader(fin, delimiter="\t")
    escritor = csv.writer(fout)
    escritor.writerow(["familia", "codigos", "dum", "productos"])

    for n, fila in enumerate(lector, 1):
        if len(fila) <= COL_PRODUCTO:
            excluidos.append((n, "fila incompleta"))
            continue

        familia = limpiar(fila[COL_FAMILIA]).upper()
        codigo = limpiar(fila[COL_CODIGO]).lstrip("0") or "0"
        dum = re.sub(r"\D", "", fila[COL_DUM])     # solo dígitos
        producto = limpiar(fila[COL_PRODUCTO])

        if familia in FAMILIAS_EXCLUIDAS:
            excluidos.append((n, f"familia '{familia}' - {codigo} {producto}"))
            continue
        if not codigo.isdigit() or not producto:
            excluidos.append((n, f"código/producto inválido: {fila[:5]}"))
            continue
        if codigo in vistos:                        # protege la PK
            duplicados.append((n, codigo))
            continue
        vistos.add(codigo)

        if not dum:
            sin_dum += 1

        escritor.writerow([familia, codigo, dum or None, producto])
        ok += 1

print(f"Filas cargables: {ok}")
print(f"Sin DUM (quedan NULL): {sin_dum}")
print(f"Duplicados de código descartados: {len(duplicados)}")
print(f"Excluidas: {len(excluidos)}")
for n, motivo in excluidos:
    print(f"  línea {n}: {motivo}")
print(f"\nArchivo generado: {salida}")