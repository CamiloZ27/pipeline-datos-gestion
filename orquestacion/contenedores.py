"""
Control de contenedores Docker desde Airflow (encender, apagar y docker exec).
Permite gestionar la RAM encendiendo Kafka/HDFS solo en las tareas que los usan.
"""
from __future__ import annotations

import logging
import time
from typing import Iterable, List, Optional, Sequence

log = logging.getLogger(__name__)

ESPERA_MAXIMA_S = 300
INTERVALO_S = 5


class ErrorContenedor(RuntimeError):
    pass


def cliente():
    import docker

    return docker.from_env()


def _obtener(cli, nombre: str):
    import docker.errors

    try:
        return cli.containers.get(nombre)
    except docker.errors.NotFound:
        raise ErrorContenedor(
            f"No existe el contenedor '{nombre}'. Ejecute primero: "
            "docker compose --profile todo create"
        )


def estado(contenedor) -> str:
    contenedor.reload()
    st = contenedor.attrs.get("State", {})
    salud = st.get("Health", {}).get("Status")
    return salud or st.get("Status", "desconocido")


def listo(contenedor) -> bool:
    return estado(contenedor) in ("healthy", "running")


def encender(
    nombres: Iterable[str],
    cli=None,
    espera_maxima: float = ESPERA_MAXIMA_S,
    intervalo: float = INTERVALO_S,
    obtener=None,
) -> None:
    cli = cli or cliente()
    obtener = obtener or _obtener
    contenedores = [obtener(cli, n) for n in nombres]
    for c in contenedores:
        if estado(c) not in ("healthy", "running", "starting"):
            log.info("Encendiendo contenedor %s", c.name)
            c.start()

    limite = time.monotonic() + espera_maxima
    pendientes = list(contenedores)
    while pendientes:
        pendientes = [c for c in pendientes if not listo(c)]
        if not pendientes:
            break
        for c in pendientes:
            if estado(c) in ("exited", "dead", "unhealthy"):
                raise ErrorContenedor(f"Contenedor {c.name} terminó en estado '{estado(c)}'")
        if time.monotonic() > limite:
            raise ErrorContenedor(
                "Timeout esperando contenedores: "
                + ", ".join(f"{c.name}={estado(c)}" for c in pendientes)
            )
        time.sleep(intervalo)
    log.info("Contenedores listos: %s", ", ".join(c.name for c in contenedores))


def apagar(nombres: Iterable[str], cli=None, obtener=None) -> None:
    cli = cli or cliente()
    obtener = obtener or _obtener
    for n in nombres:
        try:
            c = obtener(cli, n)
        except ErrorContenedor as e:
            log.warning("%s", e)
            continue
        if estado(c) not in ("exited", "created"):
            log.info("Apagando contenedor %s", n)
            c.stop(timeout=30)


def ejecutar(
    nombre: str,
    comando: Sequence[str],
    cli=None,
    obtener=None,
    directorio: Optional[str] = None,
) -> List[str]:
    cli = cli or cliente()
    obtener = obtener or _obtener
    c = obtener(cli, nombre)
    if not listo(c):
        c.start()
    log.info("%s$ %s", nombre, " ".join(comando))
    exec_id = cli.api.exec_create(c.id, list(comando), workdir=directorio)["Id"]
    lineas, resto = [], b""
    for trozo in cli.api.exec_start(exec_id, stream=True):
        resto += trozo
        *completas, resto = resto.split(b"\n")
        for linea in completas:
            texto = linea.decode("utf-8", "replace").rstrip()
            lineas.append(texto)
            log.info("%s", texto)
    if resto:
        texto = resto.decode("utf-8", "replace")
        lineas.append(texto)
        log.info("%s", texto)
    codigo = cli.api.exec_inspect(exec_id)["ExitCode"]
    if codigo != 0:
        raise ErrorContenedor(f"Comando falló (exit {codigo}) en {nombre}")
    return lineas

