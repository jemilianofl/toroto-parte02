import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import date, datetime
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Cargar variables de entorno
load_dotenv()
CODA_API_TOKEN = os.getenv("CODA_API_TOKEN")
RAILWAY_URL = os.getenv("POSTGRES_URL")

# Headers de autenticación para Coda
headers = {
    "Authorization": f"Bearer {CODA_API_TOKEN}",
    "Content-Type": "application/json"
}

# Configuración ORM
Base = declarative_base()

class Proyecto(Base):
    __tablename__ = "proyectos"
    id = Column(Integer, primary_key=True)
    nombre = Column(String)

class Responsable(Base):
    __tablename__ = "responsables"
    id = Column(Integer, primary_key=True)
    nombre = Column(String)
    correo = Column(String)

class Obra(Base):
    __tablename__ = "obras"
    id = Column(Integer, primary_key=True)
    nombre_obra = Column(String)
    proyecto_id = Column(Integer, ForeignKey("proyectos.id"))
    estado = Column(String)
    fecha_inicio = Column(Date)
    fecha_fin = Column(Date)
    responsable_id = Column(Integer, ForeignKey("responsables.id"))
    fase = Column(String)

    proyecto = relationship("Proyecto")
    responsable = relationship("Responsable")


# Función para obtener los datos desde la base con ORM
def obtener_obras_orm():
    engine = create_engine(RAILWAY_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    obras = session.query(
        Obra.nombre_obra.label("Nombre de la obra"),
        Proyecto.nombre.label("Proyecto"),
        Obra.estado.label("Estado"),
        Obra.fecha_inicio.label("Fecha de inicio"),
        Obra.fecha_fin.label("Fecha de finalización"),
        Responsable.nombre.label("Responsable"),
        Responsable.correo.label("Correo"),
        Obra.fase.label("Fase")
    ).join(Proyecto, Obra.proyecto_id == Proyecto.id, isouter=True
    ).join(Responsable, Obra.responsable_id == Responsable.id, isouter=True
    ).all()

    # Convertir a DataFrame
    df = pd.DataFrame(obras, columns=[
        "Nombre de la obra", "Proyecto", "Estado",
        "Fecha de inicio", "Fecha de finalización",
        "Responsable", "Correo", "Fase"
    ])

    # Convertir fechas a datetime
    df["Fecha de inicio"] = pd.to_datetime(df["Fecha de inicio"], errors="coerce")
    df["Fecha de finalización"] = pd.to_datetime(df["Fecha de finalización"], errors="coerce")

    # Añadir columna Año
    df["Año"] = df["Fecha de inicio"].dt.year

    return df

# -------------------------- Coda API Helpers --------------------------

def obtener_documento_id_por_nombre(nombre):
    print("📄 Buscando documento...")
    url = "https://coda.io/apis/v1/docs"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    documentos = r.json().get("items", [])
    for doc in documentos:
        if doc["name"] == nombre:
            print(f"📄 Documento encontrado: {nombre} (ID: {doc['id']})")
            return doc["id"]
    raise ValueError(f"No se encontró el documento con nombre '{nombre}'")

def obtener_table_id(doc_id, nombre_tabla):
    print("🔍 Buscando tabla dentro del documento...")
    url = f"https://coda.io/apis/v1/docs/{doc_id}/tables"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    tablas = r.json().get("items", [])
    for tabla in tablas:
        if tabla["name"] == nombre_tabla:
            print(f"📊 Tabla encontrada: {nombre_tabla} (ID: {tabla['id']})")
            return tabla["id"]
    raise ValueError(f"No se encontró la tabla '{nombre_tabla}' en el documento.")

def eliminar_todas_las_filas(doc_id, table_id):
    print("🧹 Eliminando filas existentes en Coda...")
    url = f"https://coda.io/apis/v1/docs/{doc_id}/tables/{table_id}/rows"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    rows = r.json().get("items", [])
    ids = [row["id"] for row in rows]

    for i in range(0, len(ids), 50):
        batch = {"rowIds": ids[i:i+50]}
        r = requests.delete(f"https://coda.io/apis/v1/docs/{doc_id}/tables/{table_id}/rows", headers=headers, json=batch)
        if r.status_code == 202:
            print(f"🗑️ {len(batch['rowIds'])} filas eliminadas.")
        else:
            print(f"❌ Error al eliminar filas: {r.status_code} - {r.text}")

def insertar_filas_en_bloques(doc_id, table_id, filas, batch_size=10, max_reintentos=5):
    def serializar_fila(fila):
        return {
            "cells": [
                {"column": col, "value": (val.strftime("%Y-%m-%d") if isinstance(val, (date, datetime)) else val)}
                for col, val in fila.items()
            ]
        }

    for i in range(0, len(filas), batch_size):
        batch = filas[i:i+batch_size]
        payload = {"rows": [serializar_fila(f) for f in batch]}

        for intento in range(max_reintentos):
            r = requests.post(f"https://coda.io/apis/v1/docs/{doc_id}/tables/{table_id}/rows", headers=headers, json=payload)
            if r.status_code == 202:
                nombres = [f.get("Nombre de la obra", "Sin nombre") for f in batch]
                print(f"✅ {len(batch)} filas insertadas: {', '.join(nombres)}")
                break
            elif r.status_code == 429:
                espera = 2 ** intento
                print(f"⚠️ Límite alcanzado (429). Esperando {espera}s y reintentando...")
                time.sleep(espera)
            else:
                print(f"❌ Error al insertar batch (status {r.status_code}): {r.text}")
                break

# -------------------------- MAIN --------------------------

if __name__ == "__main__":
    nombre_documento = os.getenv("CODA_DOC_NAME")
    nombre_tabla = os.getenv("CODA_TABLE_NAME")

    doc_id = obtener_documento_id_por_nombre(nombre_documento)
    table_id = obtener_table_id(doc_id, nombre_tabla)

    df_obras = obtener_obras_orm()
    print(f"📦 Obras a sincronizar: {len(df_obras)}")

    filas_dicts = [row.dropna().to_dict() for _, row in df_obras.iterrows()]
    # Se limpian las filas antes de insertar
    eliminar_todas_las_filas(doc_id, table_id)
    insertar_filas_en_bloques(doc_id, table_id, filas_dicts)