"""Crea la base de datos 'airflow' dentro del contenedor PostgreSQL si no existe."""
from __future__ import annotations

import os
import time

import pg8000.native

for _ in range(30):
    try:
        con = pg8000.native.Connection(
            user=os.getenv("POSTGRES_USER", "pipeline"),
            password=os.getenv("POSTGRES_PASSWORD", "pipeline"),
            host=os.getenv("POSTGRES_HOST", "postgres"),
            database=os.getenv("POSTGRES_DB", "matricula_es"),
        )
        break
    except Exception as e:
        print(f"Esperando a PostgreSQL ({e})...")
        time.sleep(2)
else:
    raise SystemExit("No se pudo conectar a PostgreSQL para inicializar Airflow.")

if con.run("SELECT 1 FROM pg_database WHERE datname = 'airflow'"):
    print("Base de datos 'airflow' ya existe.")
else:
    con.run("CREATE DATABASE airflow")
    print("Base de datos 'airflow' creada.")
con.close()

