"""
Rastreador de noticias del sector energético (multi-país).

Genera/actualiza `noticias_master.csv`, que GitHub Actions sube al repositorio
y Power BI lee desde la URL "raw" del archivo.

Columnas: "Términos del Sector" pasó a llamarse "Temática" y se agregó
"Linea de negocio" (valor fijo: Energía). El histórico se migra automáticamente.
"""
import logging
import os
import re
import sys
import threading
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, quote_plus, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
# PARÁMETROS (se pueden cambiar con variables de entorno)
# =========================================================
ARCHIVO_CSV = os.getenv("ARCHIVO_CSV", "noticias_master.csv")
DIAS_ATRAS = int(os.getenv("DIAS_ATRAS", "7"))
ZONA_HORARIA = ZoneInfo(os.getenv("ZONA_HORARIA", "America/Bogota"))
TIMEOUT_SEG = int(os.getenv("TIMEOUT_SEG", "20"))
MAX_HILOS = int(os.getenv("MAX_HILOS", "8"))
USER_AGENT = "Mozilla/5.0 (compatible; RastreadorNoticiasEnergia/2.0)"

COLUMNAS_BASE = [
    "País", "Fuente", "Entidades Coincidentes", "Temática", "Linea de negocio",
    "Palabras Clave (Keywords)", "Título", "Fecha Publicación",
    "URL Verificada", "Resumen",
]
COLUMNAS_PBI = ["Fecha_DT", "Año", "Mes_Num", "Mes", "Año-Mes"]

LINEA_NEGOCIO = os.getenv("LINEA_NEGOCIO", "Energía")
# Temáticas generales del sector, válidas para todos los países (se suman a las de
# cada país). Una noticia SOLO se guarda si su texto contiene al menos una temática.
TEMATICAS_GENERALES = [
    "energía", "energia", "energético", "energética", "energetico", "energetica",
    "electricidad", "eléctrico", "eléctrica", "electrico", "electrica", "eletricidade", "elétrica", "eletrica",
    "tarifa eléctrica", "tarifa de energía", "tarifa de energia", "tarifas de energía",
    "hidrógeno", "hidrogeno", "hidrógeno verde", "hidrogênio", "hidrogênio verde",
    "renovable", "renovables", "renovável", "renováveis", "renovaveis",
    "solar", "fotovoltaica", "fotovoltaico", "eólica", "eolica", "eólico", "eólico marino",
    "hidroeléctrica", "hidroelectrica", "hidroeléctricas", "hidrelétrica", "termoeléctrica", "termoelectrica", "termelétrica",
    "embalse", "embalses", "reservatório", "transmisión", "transmision", "transmissão",
    "generación", "generacion", "geração", "distribución eléctrica", "red eléctrica",
    "mercado eléctrico", "mercado mayorista", "apagón", "apagon", "apagones", "racionamiento",
    "gas natural", "petróleo", "petroleo", "combustibles", "transición energética", "transicion energetica",
    "descarbonización", "matriz energética", "litio", "baterías", "almacenamiento de energía",
]

# Nombres antiguos de columnas -> nombres nuevos (para no perder el histórico del CSV)
COLUMNAS_RENOMBRADAS = {"Términos del Sector": "Temática"}

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

# Lista general de stopwords (español y portugués) para extracción de Keywords
STOPWORDS = {
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por", "un", "para", "con", "no", "una", "su",
    "al", "lo", "como", "más", "mas", "pero", "sus", "le", "ya", "o", "este", "sí", "si", "porque", "esta", "entre",
    "cuando", "muy", "sin", "sobre", "también", "tambien", "me", "hasta", "hay", "donde", "quien", "desde", "todo",
    "nos", "durante", "todos", "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos", "e", "esto",
    "antes", "algunos", "qué", "unos", "otro", "otras", "otra", "él", "tanto", "esa", "estos", "mucho",
    "quienes", "nada", "muchos", "cual", "poco", "ella", "estar", "estas", "algunas", "algo", "nosotros", "mi", "mis",
    "uma", "com", "não", "nao", "mais", "dos", "foi", "ao", "ele", "das", "tem", "pela",
}

# Estructura modular multi-país e internacional (sin cambios de contenido)
CONFIGURACION_PAISES = {
    "Guatemala": {
        "feeds_rss": [
            "https://www.prensalibre.com/feed/",
            "https://guatemala.com/feed/",
            "https://lahora.gt/feed/",
            "https://elperiodico.com.gt/rss",
            "https://www.soy502.com/rss.xml",
            "https://www.publinews.gt/gt/rss",
            "https://emisorasunidas.com/feed/",
        ],
        "entidades": ["ANG", "CIE", "AGER", "AMM", "MEM", "CNEE", "Energia Estrategica", "Energía Estratégica"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno", "renovable", "solar", "eólica", "embalse", "transmisión", "generación"],
        "gl_code": "GT",
        "hl_code": "es-419",
    },
    "Panamá": {
        "feeds_rss": [
            "https://www.prensa.com/rss/",
            "https://www.panamaamerica.com.pa/rss",
            "https://www.critica.com.pa/rss",
            "https://www.laestrella.com.pa/rss",
            "https://newsroompanama.com/rss",
            "https://www.telemetro.com/rss",
            "https://www.tvn-2.com/rss",
        ],
        "entidades": ["ASEP", "ETESA", "SNE", "CNO", "ENSA", "Naturgy", "Canal de Panamá"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "mercado eléctrico"],
        "gl_code": "PA",
        "hl_code": "es-419",
    },
    "Chile": {
        "feeds_rss": [
            "https://www.df.cl/rss",
            "https://elsiglo.cl/feed",
            "https://www.latercera.com/feed/",
            "https://www.cooperativa.cl/rss/",
            "https://www.biobiochile.cl/rss.xml",
            "https://www.emol.com/rss.asp",
            "https://www.cnnchile.com/rss",
        ],
        "entidades": ["CNE", "SEC", "Coordinador Eléctrico", "Ministerio de Energía", "Enel", "Colbún", "Engie", "AES Andes"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno verde", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "descarbonización"],
        "gl_code": "CL",
        "hl_code": "es-419",
    },
    "Brasil": {
        "feeds_rss": [
            "https://agenciabrasil.ebc.com.br/rss/ultimasnoticias/feed.xml",
            "https://valor.globo.com/rss/g1/brasil/",
            "https://oglobo.globo.com/economia/rss.xml",
            "https://www.infomoney.com.br/feed/",
            "https://www.aneel.gov.br/rss",
            "https://www.epe.gov.br/rss",
            "https://www.bndes.gov.br/rss",
        ],
        "entidades": ["ANEEL", "EPE", "BNDES", "ONS", "MME", "Petrobras", "Eletrobras", "Agência Brasil", "Valor Econômico"],
        "terminos_sector": ["energia", "eletricidade", "tarifa de energia", "hidrogênio verde", "renovável", "renovaveis", "solar", "eólica", "eolica", "reservatório", "transmissão", "geração", "mercado livre", "bandeira tarifária"],
        "gl_code": "BR",
        "hl_code": "pt-BR",
    },
    "Perú": {
        "feeds_rss": [
            "https://elcomercio.pe/rss/",
            "https://gestion.pe/rss/",
            "https://larepublica.pe/rss/",
            "https://www.gob.pe/minem",
            "https://www.osinergmin.gob.pe/",
            "https://www.proinversion.gob.pe/",
        ],
        "entidades": ["OSINERGMIN", "MINEM", "COES", "ProInversión", "Proinversion", "Electroperú", "Luz del Sur", "Enel Perú"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno verde", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "canon energético"],
        "gl_code": "PE",
        "hl_code": "es-419",
    },
    "Costa Rica": {
        "feeds_rss": [
            "https://www.nacion.com/rss/",
            "https://www.crhoy.com/site/rss",
            "https://www.elfinancierocr.com/rss/",
            "https://www.grupoice.com/",
            "https://aresep.go.cr/",
            "https://www.minae.go.cr/",
        ],
        "entidades": ["ICE", "Grupo ICE", "ARESEP", "MINAE", "CNFL", "Recope"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno verde", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "matriz energética"],
        "gl_code": "CR",
        "hl_code": "es-419",
    },
    "Venezuela": {
        "feeds_rss": [
            "https://www.elnacional.com/feed/",
            "https://talcualdigital.com/feed/",
            "https://www.bancaynegocios.com/feed/",
        ],
        "entidades": ["CORPOELEC", "Corpoelec", "MPPEE", "PDVSA", "Guri", "SEN"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "apagón", "apagon", "sistema eléctrico", "racionamiento", "termoeléctrica", "hidroeléctrica", "embalse"],
        "gl_code": "VE",
        "hl_code": "es-419",
    },
    "Internacional": {
        "feeds_rss": [
            "https://www.reuters.com/markets/latam/",
            "https://www.bloomberg.com/latin-america",
            "https://www.cepal.org/es/rss.xml",
        ],
        "entidades": ["CEPAL", "Reuters", "Bloomberg", "OLADE", "BID", "Banco Mundial", "IEA"],
        "terminos_sector": ["energía", "energia", "electricidad", "transición energética", "transicion energetica", "hidrógeno verde", "renovables", "matriz energética", "mercado eléctrico"],
        "gl_code": "US",
        "hl_code": "es-419",
    },
    "Colombia": {
        "feeds_rss": [
            "https://www.eltiempo.com/rss",
            "https://www.elespectador.com/rss",
            "https://www.elcolombiano.com/rss",
            "https://www.larepublica.co/rss",
            "https://www.portafolio.co/rss",
            "https://www.semana.com/feed/",
            "https://www.bluradio.com/rss",
            "https://caracol.com.co/rss",
            "https://www.rcnradio.com/rss",
            "https://www.publimetro.co/rss",
            "https://www.elpais.com.co/rss",
            "https://www.eluniversal.com.co/rss",
            "https://www.vanguardia.com/rss",
            "https://occidente.co/feed",
            "https://kienyke.com/feed",
            "https://thecitypaperbogota.com/feed",
        ],
        "entidades": ["Grupo EPM", "EPM", "Afinia", "Energua", "CREG", "Naturgas", "XM"],
        "terminos_sector": ["Mercado Mayorista", "Cargo por Capacidad", "Cargo por Confiabilidad", "Formula Tarifaria", "Demanda Regulada", "Niveles de Embalses", "FAZNI", "FAER", "FOES"],
        "gl_code": "CO",
        "hl_code": "es-419",
    },
}

log = logging.getLogger("scraper")


# =========================================================
# RED: sesión HTTP con timeout, reintentos y User-Agent
# =========================================================
_hilo_local = threading.local()


def obtener_sesion():
    """Una sesión por hilo (requests.Session no es 100 % thread-safe)."""
    if not hasattr(_hilo_local, "sesion"):
        sesion = requests.Session()
        sesion.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        })
        reintentos = Retry(total=2, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        adaptador = HTTPAdapter(max_retries=reintentos)
        sesion.mount("https://", adaptador)
        sesion.mount("http://", adaptador)
        _hilo_local.sesion = sesion
    return _hilo_local.sesion


def descargar_feed(url):
    """Devuelve (feed_parseado | None, estado). Nunca se cuelga: siempre hay timeout."""
    try:
        respuesta = obtener_sesion().get(url, timeout=TIMEOUT_SEG)
        respuesta.raise_for_status()
    except requests.RequestException as e:
        return None, f"error HTTP: {type(e).__name__}"
    parsed = feedparser.parse(respuesta.content)
    if not parsed.entries:
        return None, "no es RSS/Atom (página web)" if parsed.bozo else "feed sin entradas"
    return parsed, "ok"


# =========================================================
# UTILIDADES DE TEXTO Y FECHAS
# =========================================================
def limpiar_texto(html_text):
    if not html_text:
        return ""
    texto = BeautifulSoup(html_text, "html.parser").get_text(" ")
    return re.sub(r"\s+", " ", texto).strip()


def parsear_fecha(entry):
    """Fecha en UTC, o None si la noticia no trae fecha (antes se usaba 'ahora')."""
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        st = entry.get(attr)
        if st:
            return datetime(*st[:6], tzinfo=timezone.utc)
    for attr in ("published", "updated", "created"):
        valor = entry.get(attr)
        if valor:
            try:
                dt = date_parser.parse(valor)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except (ValueError, OverflowError):
                continue
    return None


def extraer_palabras_clave(texto, top_n=5):
    palabras = re.findall(r"\b[a-záéíóúñãõâêôçü]{4,}\b", texto.lower())
    frecuentes = Counter(p for p in palabras if p not in STOPWORDS).most_common(top_n)
    return ", ".join(p.capitalize() for p, _ in frecuentes)


def normalizar_titulo(titulo):
    """Clave para detectar la misma noticia llegada por RSS y por Google News."""
    if not isinstance(titulo, str):
        return ""
    t = re.sub(r"\s+[-|]\s+[^-|]{2,60}$", "", titulo)  # quita " - Medio" al final
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", t.lower()).strip()


_PARAMS_RASTREO = re.compile(r"^(utm_|fbclid$|gclid$|mc_|ocid$)")


def normalizar_url(url):
    if not isinstance(url, str) or not url.strip():
        return ""
    p = urlparse(url.strip())
    query = urlencode([(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
                       if not _PARAMS_RASTREO.match(k)])
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/") or "/", p.params, query, ""))


# =========================================================
# COINCIDENCIAS (siglas sensibles a mayúsculas)
# =========================================================
def compilar_patron(terminos, ignorar_mayusculas):
    if not terminos:
        return None
    ordenados = sorted(set(terminos), key=len, reverse=True)  # frases largas primero
    flags = re.IGNORECASE if ignorar_mayusculas else 0
    return re.compile(r"\b(" + "|".join(map(re.escape, ordenados)) + r")\b", flags)


def es_sigla(termino):
    return termino.isupper() and " " not in termino


def compilar_patrones_pais(data):
    siglas = [e for e in data["entidades"] if es_sigla(e)]
    nombres = [e for e in data["entidades"] if not es_sigla(e)]
    return {
        # "ICE", "SEC", "SEN"... solo en mayúsculas para evitar falsos positivos
        "entidades": [compilar_patron(siglas, False), compilar_patron(nombres, True)],
        # Sector del país + temáticas generales (ver TEMATICAS_GENERALES)
        "terminos": [compilar_patron(list(data["terminos_sector"]) + TEMATICAS_GENERALES, True)],
    }


def formatear_tematica(terminos):
    return ", ".join(t.title() for t in terminos)


def completar_tematica(df, config):
    """Recalcula la Temática de filas vacías/N/A (histórico) y elimina las que no tienen ninguna."""
    vacias = df["Temática"].str.strip().str.upper().isin(["", "N/A", "NAN"])
    if vacias.any():
        patrones = {pais: compilar_patrones_pais(data)["terminos"] for pais, data in config.items()}
        generico = [compilar_patron(TEMATICAS_GENERALES, True)]
        def calcular(fila):
            texto = f"{fila['Título']} {fila['Resumen']}"
            return formatear_tematica(coincidencias(texto, patrones.get(fila["País"], generico)))
        df.loc[vacias, "Temática"] = df.loc[vacias].apply(calcular, axis=1)
    sin_tematica = df["Temática"].str.strip() == ""
    if sin_tematica.any():
        log.warning("Se eliminan %d noticias del histórico sin temática en su texto.", int(sin_tematica.sum()))
    return df[~sin_tematica]


def coincidencias(texto, patrones):
    vistos = {}
    for patron in patrones:
        if patron is not None:
            for m in patron.findall(texto):
                vistos.setdefault(m.lower(), m)
    return list(vistos.values())


# =========================================================
# RASTREO
# =========================================================
def url_google_news(entidad, pais, data, dias):
    ambito = "" if pais == "Internacional" else f" {pais}"
    consulta = quote_plus(f'"{entidad}"{ambito} when:{dias}d')
    gl, hl = data["gl_code"], data["hl_code"]
    return f"https://news.google.com/rss/search?q={consulta}&hl={hl}&gl={gl}&ceid={gl}:{hl}"


def generar_tareas(config, dias):
    for pais, data in config.items():
        for url in data["feeds_rss"]:
            yield {"pais": pais, "tipo": "rss", "url": url, "entidad": None}
        for entidad in data["entidades"]:
            yield {"pais": pais, "tipo": "google_news", "entidad": entidad,
                   "url": url_google_news(entidad, pais, data, dias)}


def procesar_tarea(tarea, patrones, fecha_limite):
    parsed, estado = descargar_feed(tarea["url"])
    if parsed is None:
        return [], estado

    es_rss = tarea["tipo"] == "rss"
    if es_rss:
        nombre_fuente = parsed.feed.get("title") or urlparse(tarea["url"]).netloc
    else:
        nombre_fuente = f"Búsqueda {tarea['entidad']} (Google News)"

    filas = []
    for entry in parsed.entries:
        fecha_pub = parsear_fecha(entry)
        if fecha_pub is None or fecha_pub < fecha_limite:
            continue
        titulo = limpiar_texto(entry.get("title", ""))
        resumen = limpiar_texto(entry.get("summary", entry.get("description", "")))
        texto = f"{titulo} {resumen}"
        terminos = coincidencias(texto, patrones["terminos"])
        if not terminos:
            continue  # Temática obligatoria: sin temática en el artículo, no se guarda

        if es_rss:
            entidades = coincidencias(texto, patrones["entidades"])
            entidades_txt = ", ".join(e.upper() for e in entidades) if entidades else "N/A"
        else:
            entidades_txt = tarea["entidad"]

        filas.append({
            "País": tarea["pais"],
            "Fuente": nombre_fuente,
            "Entidades Coincidentes": entidades_txt,
            "Temática": formatear_tematica(terminos),
            "Linea de negocio": LINEA_NEGOCIO,
            "Palabras Clave (Keywords)": extraer_palabras_clave(texto),
            "Título": titulo,
            "Fecha Publicación": fecha_pub.astimezone(ZONA_HORARIA).strftime("%Y-%m-%d %H:%M"),
            "URL Verificada": entry.get("link", ""),
            "Resumen": resumen,
            "_prioridad": 0 if es_rss else 1,  # ante duplicados, gana el enlace directo del medio
        })
    return filas, estado


def rastrear_noticias_multi_pais(config, dias_atras=DIAS_ATRAS):
    fecha_limite = datetime.now(timezone.utc) - timedelta(days=dias_atras)
    patrones = {pais: compilar_patrones_pais(data) for pais, data in config.items()}
    tareas = list(generar_tareas(config, dias_atras))
    log.info("Rastreando %d fuentes en %d países/ámbitos...", len(tareas), len(config))

    noticias, fallidas = [], []
    with ThreadPoolExecutor(max_workers=MAX_HILOS) as ejecutor:
        futuros = {ejecutor.submit(procesar_tarea, t, patrones[t["pais"]], fecha_limite): t for t in tareas}
        for futuro in as_completed(futuros):
            tarea = futuros[futuro]
            try:
                filas, estado = futuro.result()
            except Exception as e:  # red de seguridad: una fuente nunca tumba el proceso
                filas, estado = [], f"error inesperado: {e}"
            if estado != "ok":
                fallidas.append((tarea["pais"], tarea["url"], estado))
            noticias.extend(filas)

    for pais, url, estado in sorted(fallidas):
        log.warning("[%s] %s -> %s", pais, url, estado)
    log.info("Fuentes OK: %d / %d | Noticias encontradas: %d",
             len(tareas) - len(fallidas), len(tareas), len(noticias))

    df = pd.DataFrame(noticias, columns=COLUMNAS_BASE + ["_prioridad"])
    return df, len(fallidas), len(tareas)


# =========================================================
# ARCHIVO MAESTRO
# =========================================================
def construir_maestro(df_nuevas, archivo, config=None):
    df_nuevas = df_nuevas.sort_values("_prioridad", kind="stable").drop(columns="_prioridad")
    total_previo = 0
    if os.path.exists(archivo):
        df_existente = pd.read_csv(archivo, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        total_previo = len(df_existente)
        df_existente = df_existente.rename(columns=COLUMNAS_RENOMBRADAS)
        if "Linea de negocio" not in df_existente.columns:
            df_existente["Linea de negocio"] = LINEA_NEGOCIO
        df_existente = df_existente.reindex(columns=COLUMNAS_BASE, fill_value="")
        # Lo existente va primero: ante duplicados se conserva el registro histórico
        df = pd.concat([df_existente, df_nuevas], ignore_index=True)
    else:
        df = df_nuevas.reset_index(drop=True)

    df = df.fillna("")
    df["Linea de negocio"] = df["Linea de negocio"].replace("", LINEA_NEGOCIO)
    df = completar_tematica(df, config or CONFIGURACION_PAISES)
    titulos = df["País"] + "|" + df["Título"].map(normalizar_titulo)
    urls = df["URL Verificada"].map(normalizar_url)
    df["_url"] = urls.where(urls != "", titulos)
    df["_clave"] = titulos.where(df["Título"].str.strip() != "", df["_url"])
    df = (df.drop_duplicates("_url", keep="first")
            .drop_duplicates("_clave", keep="first")
            .drop(columns=["_url", "_clave"]))

    # Columnas temporales para Power BI (mismos nombres que antes)
    fechas = pd.to_datetime(df["Fecha Publicación"], errors="coerce")
    df["Fecha_DT"] = fechas
    df["Año"] = fechas.dt.year.astype("Int64")
    df["Mes_Num"] = fechas.dt.month.astype("Int64")
    df["Mes"] = fechas.dt.month.map(MESES_ES)  # en español, sin depender del idioma del servidor
    df["Año-Mes"] = fechas.dt.strftime("%Y-%m")

    df = (df.assign(_orden=fechas)
            .sort_values(["País", "_orden"], ascending=[True, False], na_position="last")
            .drop(columns="_orden")
            .reset_index(drop=True))
    log.info("Registros nuevos agregados: %d", len(df) - total_previo)
    return df[COLUMNAS_BASE + COLUMNAS_PBI]


def guardar_csv_atomico(df, archivo):
    """Escribe a un temporal y reemplaza: nunca queda un CSV a medio escribir."""
    temporal = f"{archivo}.tmp"
    df.to_csv(temporal, index=False, encoding="utf-8-sig", date_format="%Y-%m-%d %H:%M:%S")
    os.replace(temporal, archivo)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log.info("Iniciando rastreo de noticias multi-país (últimos %d días)", DIAS_ATRAS)

    df_nuevas, n_fallidas, n_total = rastrear_noticias_multi_pais(CONFIGURACION_PAISES, DIAS_ATRAS)

    if n_total and n_fallidas == n_total:
        log.error("Todas las fuentes fallaron (¿sin conexión?). No se modifica el archivo.")
        return 1  # GitHub Actions marca la ejecución como fallida
    if df_nuevas.empty:
        log.warning("No se encontraron noticias nuevas en el rastreo.")
        return 0

    df_final = construir_maestro(df_nuevas, ARCHIVO_CSV)
    guardar_csv_atomico(df_final, ARCHIVO_CSV)
    log.info("Reporte maestro actualizado en '%s' | Total noticias: %d", ARCHIVO_CSV, len(df_final))
    return 0


if __name__ == "__main__":
    sys.exit(main())
