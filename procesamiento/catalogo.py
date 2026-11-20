"""
Catálogos oficiales SNIES y DANE para normalización en la zona curada (Fase 5).

Resuelve problemas reales detectados al perfilar `5wck-szir`:
  - 90 variantes sucias de nombres de departamento (ej. 'BOGOTA D.C', 'Bogotá, D.C.',
    'NARINIO', 'ARCHIPIÉLAGO DE SA') -> 33 departamentos oficiales DANE.
  - Códigos numéricos SNIES (sector, carácter, nivel, metodología, área, género)
    -> etiquetas descriptivas legibles para analítica y tablero.
"""
from __future__ import annotations

# 33 departamentos oficiales de Colombia por código DANE de 2 dígitos
DEPARTAMENTOS_DANE = {
    "05": "Antioquia",
    "08": "Atlántico",
    "11": "Bogotá D.C.",
    "13": "Bolívar",
    "15": "Boyacá",
    "17": "Caldas",
    "18": "Caquetá",
    "19": "Cauca",
    "20": "Cesar",
    "23": "Córdoba",
    "25": "Cundinamarca",
    "27": "Chocó",
    "41": "Huila",
    "44": "La Guajira",
    "47": "Magdalena",
    "50": "Meta",
    "52": "Nariño",
    "54": "Norte de Santander",
    "63": "Quindío",
    "66": "Risaralda",
    "68": "Santander",
    "70": "Sucre",
    "73": "Tolima",
    "76": "Valle del Cauca",
    "81": "Arauca",
    "85": "Casanare",
    "86": "Putumayo",
    "88": "San Andrés y Providencia",
    "91": "Amazonas",
    "94": "Guainía",
    "95": "Guaviare",
    "97": "Vaupés",
    "99": "Vichada",
}

SECTORES = {
    1: "Oficial",
    2: "Privada",
}

CARACTERES = {
    1: "Institución Técnica Profesional",
    2: "Institución Tecnológica",
    3: "Institución Universitaria / Escuela Tecnológica",
    4: "Universidad",
}

NIVELES = {
    1: "Pregrado",
    2: "Posgrado",
}

NIVELES_FORMACION = {
    1: "Especialización Universitaria",
    2: "Maestría",
    3: "Doctorado",
    4: "Formación Técnica Profesional",
    5: "Tecnológica",
    6: "Universitaria",
    7: "Especialización Técnico Profesional",
    8: "Especialización Tecnológica",
    10: "Especialización Médico Quirúrgica",
}

METODOLOGIAS = {
    1: "Presencial",
    2: "Distancia (tradicional)",
    3: "Virtual",
    4: "Distancia (virtual)",
    5: "Dual",
    7: "Presencial-Virtual",
    9: "Presencial-Dual",
}

AREAS_CONOCIMIENTO = {
    0: "Sin clasificar",
    1: "Agronomía, Veterinaria y Afines",
    2: "Bellas Artes",
    3: "Ciencias de la Educación",
    4: "Ciencias de la Salud",
    5: "Ciencias Sociales y Humanas",
    6: "Economía, Administración, Contaduría y Afines",
    8: "Ingeniería, Arquitectura, Urbanismo y Afines",
    9: "Matemáticas y Ciencias Naturales",
}

GENEROS = {
    1: "Masculino",
    2: "Femenino",
}

FUENTE_OFICIAL_CITA = (
    "Ministerio de Educación Nacional (SNIES) — MEN_MATRICULA_ESTADISTICA_ES "
    "(datos.gov.co, dataset 5wck-szir)"
)

