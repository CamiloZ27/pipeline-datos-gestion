"""
Cliente REST WebHDFS para operar HDFS sin instalar Java/Hadoop en el host.

Protocolo en dos pasos al escribir:
  1) PUT NameNode (/webhdfs/v1/<ruta>?op=CREATE&noredirect=true) -> devuelve Location del DataNode.
  2) PUT DataNode (traduciendo datanode:9864 -> localhost:9864)  -> envía los bytes.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlsplit, urlunsplit

import requests

from datalake import config

log = logging.getLogger("datalake.webhdfs")


class ErrorHDFS(Exception):
    def __init__(self, excepcion: str, mensaje: str, status: int):
        super().__init__(f"{excepcion}: {mensaje} (HTTP {status})")
        self.excepcion = excepcion
        self.status = status


class WebHDFS:
    def __init__(
        self,
        url: Optional[str] = None,
        usuario: Optional[str] = None,
        datanode_publico: Optional[str] = None,
        sesion=None,
    ):
        self.url = (url or config.WEBHDFS_URL).rstrip("/")
        self.usuario = usuario or config.HDFS_USUARIO
        self.datanode_publico = (
            config.DATANODE_PUBLICO if datanode_publico is None else datanode_publico
        )
        self.sesion = sesion or requests.Session()

    def _url(self, ruta: str) -> str:
        if not ruta.startswith("/"):
            raise ValueError(f"La ruta HDFS debe iniciar con '/': {ruta}")
        return f"{self.url}/webhdfs/v1{ruta}"

    def _params(self, op: str, **extra) -> dict:
        return {"op": op, "user.name": self.usuario, **extra}

    @staticmethod
    def _revisar(resp) -> None:
        if resp.status_code < 400:
            return
        try:
            remota = resp.json()["RemoteException"]
            raise ErrorHDFS(
                remota.get("exception", "Error"),
                remota.get("message", ""),
                resp.status_code,
            )
        except (ValueError, KeyError):
            raise ErrorHDFS("HTTPError", resp.text[:250], resp.status_code)

    def _traducir_datanode(self, ubicacion: str) -> str:
        if not self.datanode_publico:
            return ubicacion
        partes = urlsplit(ubicacion)
        return urlunsplit(partes._replace(netloc=self.datanode_publico))

    def _con_reintento_modo_seguro(self, funcion):
        for intento in range(config.REINTENTOS_MODO_SEGURO):
            try:
                return funcion()
            except ErrorHDFS as e:
                if e.excepcion not in ("SafeModeException", "RetriableException"):
                    raise
                log.warning(
                    "NameNode en modo seguro, reintento %d en %ds...",
                    intento + 1,
                    config.ESPERA_MODO_SEGURO_S,
                )
                time.sleep(config.ESPERA_MODO_SEGURO_S)
        return funcion()

    def mkdirs(self, ruta: str) -> bool:
        def hacer():
            r = self.sesion.put(
                self._url(ruta),
                params=self._params("MKDIRS"),
                timeout=config.HDFS_TIMEOUT,
            )
            self._revisar(r)
            return r.json().get("boolean", False)

        return self._con_reintento_modo_seguro(hacer)

    def escribir(self, ruta: str, contenido: bytes, sobrescribir: bool = True) -> None:
        def hacer():
            r = self.sesion.put(
                self._url(ruta),
                params=self._params(
                    "CREATE",
                    overwrite=str(sobrescribir).lower(),
                    noredirect="true",
                ),
                timeout=config.HDFS_TIMEOUT,
            )
            self._revisar(r)
            ubicacion = self._traducir_datanode(r.json()["Location"])
            r2 = self.sesion.put(
                ubicacion,
                data=contenido,
                headers={"Content-Type": "application/octet-stream"},
                timeout=config.HDFS_TIMEOUT,
            )
            self._revisar(r2)

        self._con_reintento_modo_seguro(hacer)

    def subir_archivo(self, local: str, ruta: str) -> int:
        contenido = Path(local).read_bytes()
        self.mkdirs(ruta.rsplit("/", 1)[0] or "/")
        self.escribir(ruta, contenido, sobrescribir=True)
        return len(contenido)

    def leer(self, ruta: str) -> bytes:
        r = self.sesion.get(
            self._url(ruta),
            params=self._params("OPEN", noredirect="true"),
            timeout=config.HDFS_TIMEOUT,
        )
        self._revisar(r)
        r2 = self.sesion.get(
            self._traducir_datanode(r.json()["Location"]),
            timeout=config.HDFS_TIMEOUT,
        )
        self._revisar(r2)
        return r2.content

    def listar(self, ruta: str) -> List[dict]:
        r = self.sesion.get(
            self._url(ruta),
            params=self._params("LISTSTATUS"),
            timeout=config.HDFS_TIMEOUT,
        )
        self._revisar(r)
        return r.json()["FileStatuses"]["FileStatus"]

    def resumen(self, ruta: str) -> dict:
        r = self.sesion.get(
            self._url(ruta),
            params=self._params("GETCONTENTSUMMARY"),
            timeout=config.HDFS_TIMEOUT,
        )
        self._revisar(r)
        return r.json()["ContentSummary"]

    def existe(self, ruta: str) -> bool:
        r = self.sesion.get(
            self._url(ruta),
            params=self._params("GETFILESTATUS"),
            timeout=config.HDFS_TIMEOUT,
        )
        if r.status_code == 404:
            return False
        self._revisar(r)
        return True

