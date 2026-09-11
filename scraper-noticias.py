import os
import re
import feedparser
import pandas as pd
from collections import Counter
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser
from bs4 import BeautifulSoup

# Lista general de stopwords (español y portugués) para extracción de Keywords
STOPWORDS = set([
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por", "un", "para", "con", "no", "una", "su",
    "al", "lo", "como", "más", "mas", "pero", "sus", "le", "ya", "o", "este", "sí", "si", "porque", "esta", "entre",
    "cuando", "muy", "sin", "sobre", "también", "tambien", "me", "hasta", "hay", "donde", "quien", "desde", "todo",
    "nos", "durante", "todos", "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos", "e", "esto",
    "antes", "algunos", "qué", "que", "unos", "otro", "otras", "otra", "él", "el", "tanto", "esa", "estos", "mucho",
    "quienes", "nada", "muchos", "cual", "poco", "ella", "estar", "estas", "algunas", "algo", "nosotros", "mi", "mis",
    "uma", "com", "não", "nao", "por", "mais", "dos", "como", "mas", "foi", "ao", "ele", "das", "tem", "para", "pela"
])

# Estructura modular multi-país e internacional
CONFIGURACION_PAISES = {
    "Guatemala": {
        "feeds_rss": [
            "https://www.prensalibre.com/feed/",
            "https://guatemala.com/feed/",
            "https://lahora.gt/feed/",
            "https://elperiodico.com.gt/rss",
            "https://www.soy502.com/rss.xml",
            "https://www.publinews.gt/gt/rss",
            "https://emisorasunidas.com/feed/"
        ],
        "entidades": ["ANG", "CIE", "AGER", "AMM", "MEM", "CNEE", "Energia Estrategica", "Energía Estratégica"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno", "renovable", "solar", "eólica", "embalse", "transmisión", "generación"],
        "gl_code": "GT",
        "hl_code": "es-419"
    },
    "Panamá": {
        "feeds_rss": [
            "https://www.prensa.com/rss/",
            "https://www.panamaamerica.com.pa/rss",
            "https://www.critica.com.pa/rss",
            "https://www.laestrella.com.pa/rss",
            "https://newsroompanama.com/rss",
            "https://www.telemetro.com/rss",
            "https://www.tvn-2.com/rss"
        ],
        "entidades": ["ASEP", "ETESA", "SNE", "CNO", "ENSA", "Naturgy", "Canal de Panamá"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "mercado eléctrico"],
        "gl_code": "PA",
        "hl_code": "es-419"
    },
    "Chile": {
        "feeds_rss": [
            "https://www.df.cl/rss",
            "https://elsiglo.cl/feed",
            "https://www.latercera.com/feed/",
            "https://www.cooperativa.cl/rss/",
            "https://www.biobiochile.cl/rss.xml",
            "https://www.emol.com/rss.asp",
            "https://www.cnnchile.com/rss"
        ],
        "entidades": ["CNE", "SEC", "Coordinador Eléctrico", "Ministerio de Energía", "Enel", "Colbún", "Engie", "AES Andes"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno verde", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "descarbonización"],
        "gl_code": "CL",
        "hl_code": "es-419"
    },
    "Brasil": {
        "feeds_rss": [
            "https://agenciabrasil.ebc.com.br/rss/ultimasnoticias/feed.xml",
            "https://valor.globo.com/rss/g1/brasil/",
            "https://oglobo.globo.com/economia/rss.xml",
            "https://www.infomoney.com.br/feed/",
            "https://www.aneel.gov.br/rss",
            "https://www.epe.gov.br/rss",
            "https://www.bndes.gov.br/rss"
        ],
        "entidades": ["ANEEL", "EPE", "BNDES", "ONS", "MME", "Petrobras", "Eletrobras", "Agência Brasil", "Valor Econômico"],
        "terminos_sector": ["energia", "eletricidade", "tarifa de energia", "hidrogênio verde", "renovável", "renovaveis", "solar", "eólica", "eolica", "reservatório", "transmissão", "geração", "mercado livre", "bandeira tarifária"],
        "gl_code": "BR",
        "hl_code": "pt-BR"
    },
    "Perú": {
        "feeds_rss": [
            "https://elcomercio.pe/rss/",
            "https://gestion.pe/rss/",
            "https://larepublica.pe/rss/",
            "https://www.gob.pe/minem",
            "https://www.osinergmin.gob.pe/",
            "https://www.proinversion.gob.pe/"
        ],
        "entidades": ["OSINERGMIN", "MINEM", "COES", "ProInversión", "Proinversion", "Electroperú", "Luz del Sur", "Enel Perú"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno verde", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "canon energético"],
        "gl_code": "PE",
        "hl_code": "es-419"
    },
    "Costa Rica": {
        "feeds_rss": [
            "https://www.nacion.com/rss/",
            "https://www.crhoy.com/site/rss",
            "https://www.elfinancierocr.com/rss/",
            "https://www.grupoice.com/",
            "https://aresep.go.cr/",
            "https://www.minae.go.cr/"
        ],
        "entidades": ["ICE", "Grupo ICE", "ARESEP", "MINAE", "CNFL", "Recope"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "hidrógeno verde", "renovable", "solar", "eólica", "embalse", "transmisión", "generación", "matriz energética"],
        "gl_code": "CR",
        "hl_code": "es-419"
    },
    "Venezuela": {
        "feeds_rss": [
            "https://www.elnacional.com/feed/",
            "https://talcualdigital.com/feed/",
            "https://www.bancaynegocios.com/feed/"
        ],
        "entidades": ["CORPOELEC", "Corpoelec", "MPPEE", "PDVSA", "Guri", "SEN"],
        "terminos_sector": ["energía", "energia", "electricidad", "tarifa eléctrica", "apagón", "apagon", "sistema eléctrico", "racionamiento", "termoeléctrica", "hidroeléctrica", "embalse"],
        "gl_code": "VE",
        "hl_code": "es-419"
    },
    "Internacional": {
        "feeds_rss": [
            "https://www.reuters.com/markets/latam/",
            "https://www.bloomberg.com/latin-america",
            "https://www.cepal.org/es/rss.xml"
        ],
        "entidades": ["CEPAL", "Reuters", "Bloomberg", "OLADE", "BID", "Banco Mundial", "IEA"],
        "terminos_sector": ["energía", "energia", "electricidad", "transición energética", "transicion energetica", "hidrógeno verde", "renovables", "matriz energética", "mercado eléctrico"],
        "gl_code": "US",
        "hl_code": "es-419"
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
            "https://thecitypaperbogota.com/feed"
        ],
        "entidades": ["Grupo EPM", "EPM", "Afinia", "Energua", "CREG", "Naturgas", "XM"],
        "terminos_sector": ["Mercado Mayorista", "Cargo por Capacidad", "Cargo por Confiabilidad", "Formula Tarifaria", "Demanda Regulada", "Niveles de Embalses", "FAZNI", "FAER", "FOES"],
        "gl_code": "CO",
        "hl_code": "es-419"
    }
}

def limpiar_texto(html_text):
    if not html_text:
        return ""
    text = BeautifulSoup(html_text, "html.parser").get_text()
    return re.sub(r'\s+', ' ', text).strip()

def parsear_fecha(entry):
    for attr in ['published', 'updated', 'created']:
        if hasattr(entry, attr):
            try:
                dt = date_parser.parse(getattr(entry, attr))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
    return datetime.now(timezone.utc)

def extraer_palabras_clave(texto, top_n=5):
    words = re.findall(r'\b[a-zA-ZáéíóúÁÉÍÓÚñÑãõÃÕçÇ]{4,}\b', texto.lower())
    words_filtradas = [w for w in words if w not in STOPWORDS]
    frecuentes = Counter(words_filtradas).most_common(top_n)
    return ", ".join([palabra.capitalize() for palabra, _ in frecuentes])

def rastrear_noticias_multi_pais(config, dias_atras=7):
    noticias = []
    fecha_limite = datetime.now(timezone.utc) - timedelta(days=dias_atras)

    for pais, data in config.items():
        print(f"--- Rastreando: {pais} ---")

        entidades = data["entidades"]
        terminos = data["terminos_sector"]
        gl_code = data["gl_code"]
        hl_code = data["hl_code"]

        patron_entidades = re.compile(r'\b(' + '|'.join(re.escape(k) for k in entidades) + r')\b', re.IGNORECASE)
        patron_terminos = re.compile(r'\b(' + '|'.join(re.escape(k) for k in terminos) + r')\b', re.IGNORECASE)

        # 1. Feeds RSS Directos
        for feed_url in data["feeds_rss"]:
            try:
                parsed = feedparser.parse(feed_url)
                nombre_fuente = parsed.feed.get('title', feed_url.split('/')[2] if '/' in feed_url else feed_url)

                for entry in parsed.entries:
                    fecha_pub = parsear_fecha(entry)
                    if fecha_pub >= fecha_limite:
                        titulo = entry.get('title', '')
                        resumen = limpiar_texto(entry.get('summary', entry.get('description', '')))
                        texto_completo = f"{titulo} {resumen}"

                        entidades_match = list(set(patron_entidades.findall(texto_completo)))
                        terminos_match = list(set(patron_terminos.findall(texto_completo)))

                        if entidades_match or terminos_match:
                            keywords = extraer_palabras_clave(texto_completo)
                            noticias.append({
                                'País': pais,
                                'Fuente': nombre_fuente,
                                'Entidades Coincidentes': ", ".join([e.upper() for e in entidades_match]) if entidades_match else "N/A",
                                'Términos del Sector': ", ".join([t.title() for t in terminos_match]) if terminos_match else "N/A",
                                'Palabras Clave (Keywords)': keywords,
                                'Título': titulo,
                                'Fecha Publicación': fecha_pub.strftime('%Y-%m-%d %H:%M'),
                                'URL Verificada': entry.get('link', ''),
                                'Resumen': resumen
                            })
            except Exception as e:
                print(f"Error procesando feed {feed_url}: {e}")

        # 2. Búsqueda por Entidades regulatorias en Google News
        for entidad in entidades:
            try:
                query = f"{entidad} {pais}".replace(' ', '+')
                gn_url = f"https://news.google.com/rss/search?q={query}&hl={hl_code}&gl={gl_code}&ceid={gl_code}:{hl_code}"
                parsed = feedparser.parse(gn_url)

                for entry in parsed.entries:
                    fecha_pub = parsear_fecha(entry)
                    if fecha_pub >= fecha_limite:
                        titulo = entry.get('title', '')
                        resumen = limpiar_texto(entry.get('summary', ''))
                        texto_completo = f"{titulo} {resumen}"

                        terminos_match = list(set(patron_terminos.findall(texto_completo)))
                        keywords = extraer_palabras_clave(texto_completo)

                        noticias.append({
                            'País': pais,
                            'Fuente': f'Búsqueda {entidad} (Google News)',
                            'Entidades Coincidentes': entidad,
                            'Términos del Sector': ", ".join([t.title() for t in terminos_match]) if terminos_match else "N/A",
                            'Palabras Clave (Keywords)': keywords,
                            'Título': titulo,
                            'Fecha Publicación': fecha_pub.strftime('%Y-%m-%d %H:%M'),
                            'URL Verificada': entry.get('link', ''),
                            'Resumen': resumen
                        })
            except Exception as e:
                print(f"Error en búsqueda de entidad {entidad}: {e}")

    df = pd.DataFrame(noticias)
    return df

if __name__ == "__main__":
    print("🚀 Iniciando rastreo de noticias multi-país...")
    df_nuevas = rastrear_noticias_multi_pais(CONFIGURACION_PAISES, dias_atras=7)

    archivo_csv = "noticias_master.csv"

    if not df_nuevas.empty:
        if os.path.exists(archivo_csv):
            df_existente = pd.read_csv(archivo_csv)
            df_final = (
                pd.concat([df_existente, df_nuevas])
                .drop_duplicates(subset=['URL Verificada'])
                .sort_values(by=['País', 'Fecha Publicación'], ascending=[True, False])
                .reset_index(drop=True)
            )
        else:
            df_final = df_nuevas.drop_duplicates(subset=['URL Verificada']).sort_values(
                by=['País', 'Fecha Publicación'], ascending=[True, False]
            ).reset_index(drop=True)

        # =========================================================
        # NUEVAS COLUMNAS TEMPORALES PARA POWER BI (Añadir aquí)
        # =========================================================
        df_final['Fecha_DT'] = pd.to_datetime(df_final['Fecha Publicación'], errors='coerce')
        df_final['Año'] = df_final['Fecha_DT'].dt.year
        df_final['Mes_Num'] = df_final['Fecha_DT'].dt.month
        df_final['Mes'] = df_final['Fecha_DT'].dt.strftime('%B')
        df_final['Año-Mes'] = df_final['Fecha_DT'].dt.strftime('%Y-%m')
        # =========================================================

        df_final.to_csv(archivo_csv, index=False, encoding='utf-8-sig')
        print(f"\n✅ Reporte maestro actualizado exitosamente en '{archivo_csv}'")
        print(f"📊 Total noticias almacenadas: {len(df_final)}")
    else:
        print("\n⚠️ No se encontraron noticias nuevas en el rastreo.")
