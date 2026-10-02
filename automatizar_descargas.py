import os
import time
from datetime import datetime
import paramiko
import pyte
from dotenv import load_dotenv

load_dotenv()

SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
REMOTE_DIR = os.getenv("REMOTE_DIR", "/u/public")
LOCAL_DIR = os.getenv("LOCAL_DIR", "./descargas")
NOMBRE_ARCHIVO_VENTAS_STOCK = "zventas.txt"

COLUMNAS = 80
FILAS = 24

DELAY_CORTO = 2
DELAY_MEDIO = 3
DELAY_LARGO = 6

screen = pyte.Screen(COLUMNAS, FILAS)
stream = pyte.Stream(screen)


class PantallaInesperada(Exception):
    pass


def texto_pantalla():
    return "\n".join(screen.display)


def mostrar_pantalla(paso):
    print(f"\n{'=' * 10} DESPUES DE: {paso} {'=' * 10}")
    print(texto_pantalla())
    print(f">>> CURSOR: fila {screen.cursor.y + 1}, columna {screen.cursor.x + 1} <<<")
    print("=" * 40)


def conectar_shell_interactivo():
    sock = paramiko.Transport((SSH_HOST, SSH_PORT))
    opts = sock.get_security_options()
    opts.key_types = ["ssh-rsa"]
    try:
        opts.ciphers = list(opts.ciphers) + ["aes128-cbc", "aes256-cbc", "3des-cbc"]
    except ValueError:
        pass
    sock.connect(username=SSH_USER, password=SSH_PASSWORD)
    canal = sock.open_session()
    canal.get_pty(term="linux", width=COLUMNAS, height=FILAS)
    canal.invoke_shell()
    time.sleep(DELAY_MEDIO)
    if canal.recv_ready():
        stream.feed(canal.recv(65535).decode(errors="ignore"))
    return sock, canal


def enviar(canal, texto, delay=DELAY_CORTO, paso=""):
    canal.send(texto)
    time.sleep(delay)
    datos = b""
    while canal.recv_ready():
        datos += canal.recv(65535)
    if datos:
        stream.feed(datos.decode(errors="ignore"))

    pantalla_actual = texto_pantalla()
    mostrar_pantalla(paso)

    if "no se dispone" in pantalla_actual.lower() or "linvalor.hlp" in pantalla_actual.lower():
        raise PantallaInesperada(f"Se disparo la ventana de ayuda en el paso: {paso}")

    return pantalla_actual


def enter(canal, delay=DELAY_CORTO, paso=""):
    return enviar(canal, "\r", delay, paso)


def seleccionar_impresora(canal):
    enviar(canal, "12", DELAY_CORTO, "Impresora: escribir 12")
    enter(canal, DELAY_MEDIO, "Impresora: Enter")


def generar_reporte_ventas_stock(canal):
    enviar(canal, "7", DELAY_MEDIO, "Menu: 7 (Stock)")
    enter(canal, DELAY_MEDIO, "Menu: Enter")

    enviar(canal, "1c", DELAY_MEDIO, "Submenu: 1c")
    enter(canal, DELAY_MEDIO, "Submenu: Enter")

    enter(canal, DELAY_MEDIO, "Ordenamiento (precargado)")
    enter(canal, DELAY_MEDIO, "Sucursal (precargado)")

    # OJO: Articulo es UN SOLO campo, no Desde/Hasta separados
    for campo in ["Proveedor", "Familia", "Departamento", "Seccion", "Grupo", "Articulo"]:
        enter(canal, DELAY_MEDIO, f"{campo} (vacio)")

    enviar(canal, "0", DELAY_CORTO, "Estado: escribir 0")
    enter(canal, DELAY_MEDIO, "Estado: Enter")

    enviar(canal, "S", DELAY_CORTO, "Incluir sin stock: escribir S")
    enter(canal, DELAY_MEDIO, "Incluir sin stock: Enter")

    enter(canal, DELAY_MEDIO, "Fechas (automatico)")

    f1 = "\x1b[11~"
    enviar(canal, f1, DELAY_MEDIO, "F1: tipo de salida")
    enviar(canal, "A", DELAY_CORTO, "Elegir Archivo")
    enter(canal, DELAY_MEDIO, "Confirmar Archivo")

    f5 = "\x1b[15~"
    pantalla = enviar(canal, f5, DELAY_LARGO, "F5: procesar")

    if "archivo a generar" in pantalla.lower() or "nombre del documento" in pantalla.lower():
        enviar(canal, "zventas", DELAY_CORTO, "Nombre: zventas")
        enter(canal, DELAY_LARGO, "Confirmar nombre")
    else:
        print("\n!!! ADVERTENCIA: no aparecio 'Archivo a Generar', revisar pantalla arriba !!!")


def descargar_archivos_sftp(sock, archivos_remotos):
    os.makedirs(LOCAL_DIR, exist_ok=True)
    sftp = paramiko.SFTPClient.from_transport(sock)
    try:
        for nombre_archivo in archivos_remotos:
            ruta_remota = REMOTE_DIR + "/" + nombre_archivo
            marca_tiempo = datetime.now().strftime("%Y%m%d_%H%M%S")
            nombre_local = marca_tiempo + "_" + nombre_archivo
            ruta_local = os.path.join(LOCAL_DIR, nombre_local)
            print(f"Descargando {ruta_remota} -> {ruta_local}")
            sftp.get(ruta_remota, ruta_local)
            print("OK:", nombre_local)
    finally:
        sftp.close()


def main():
    print("Conectando a", SSH_HOST, "...")
    sock, canal = conectar_shell_interactivo()
    try:
        seleccionar_impresora(canal)
        generar_reporte_ventas_stock(canal)
        time.sleep(DELAY_LARGO)
        descargar_archivos_sftp(sock, [NOMBRE_ARCHIVO_VENTAS_STOCK])
    except PantallaInesperada as e:
        print("\n>>> El script se detuvo:", e)
        print(">>> Revisa arriba la ULTIMA pantalla impresa antes de este mensaje.")
    finally:
        canal.close()
        sock.close()
        print("Conexion cerrada.")


if __name__ == "__main__":
    main()