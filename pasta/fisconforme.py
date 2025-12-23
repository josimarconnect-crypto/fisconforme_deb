# fisconforme.py
import os
import re
import base64
import tempfile
import time
import subprocess
import zipfile
from datetime import date, timedelta
from typing import Dict, Any, Optional, List, Tuple

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import uvicorn

# =========================================================
# CONFIG SUPABASE
# =========================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://hysrxadnigzqadnlkynq.supabase.co")
SUPABASE_KEY = os.getenv(
    "SUPABASE_KEY",
    "COLOQUE_SUA_ANON_KEY_AQUI_SE_NAO_USAR_ENV",
)
TABELA_CERTS = os.getenv("TABELA_CERTS", "certifica_dfe")


def supabase_headers(is_json: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if is_json:
        h["Content-Type"] = "application/json"
    return h


# =========================================================
# BUSCA CERTIFICADOS (SUPABASE)
# =========================================================
def carregar_certificados_validos(user_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{TABELA_CERTS}"
    params: Dict[str, str] = {'select': 'id,pem,key,empresa,codi,user,vencimento,"cnpj/cpf"'}
    if user_filter:
        params["user"] = f"eq.{user_filter}"

    r = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json() or []


def criar_arquivos_cert_temp(cert_row: Dict[str, Any]) -> Tuple[str, str]:
    pem_b64 = cert_row.get("pem") or ""
    key_b64 = cert_row.get("key") or ""

    pem_bytes = base64.b64decode(pem_b64)
    key_bytes = base64.b64decode(key_b64)

    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".key")

    cert_file.write(pem_bytes)
    cert_file.flush()
    cert_file.close()

    key_file.write(key_bytes)
    key_file.flush()
    key_file.close()

    return cert_file.name, key_file.name


def criar_sessao(cert_path: str, key_path: str) -> requests.Session:
    s = requests.Session()
    s.cert = (cert_path, key_path)
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s


# =========================================================
# URLs DET / PORTAL / DARE
# =========================================================
URL_DET_HOME = "https://detsec.sefin.ro.gov.br/certificados"
URL_ENTRAR = "https://detsec.sefin.ro.gov.br/entrar"
URL_REDIRECT_PORTAL = "https://detsec.sefin.ro.gov.br/contribuinte/notificacoes/redirect_portal"
URL_PORTAL_HOME_DEFAULT = "https://portalcontribuinte.sefin.ro.gov.br/app/home/?exibir_modal=true"

URL_CONSULTA_DEBITOS = "https://portalcontribuinte.sefin.ro.gov.br/app/consultadebitos/"
URL_CONSULTA_DEBITOS_LISTA = "https://portalcontribuinte.sefin.ro.gov.br/app/consultadebitos/lista.jsp"

BASE_DARE = "https://dare.sefin.ro.gov.br/"
BASE_PORTAL = "https://portalcontribuinte.sefin.ro.gov.br/"


# =========================================================
# CHROME/EDGE PRINT-TO-PDF
# =========================================================
# Pode fixar via ENV: CHROME_PATH
POSSIVEIS_BROWSERS = [
    os.getenv("CHROME_PATH", "").strip(),
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
]


def encontrar_browser() -> Optional[str]:
    for p in POSSIVEIS_BROWSERS:
        if not p:
            continue
        if os.path.isfile(p):
            return p
    return None


BROWSER_EXE = encontrar_browser()


def html_para_pdf_chrome(html: str, pdf_path: str, timeout_sec: int = 180):
    if not BROWSER_EXE:
        raise RuntimeError(
            "Não encontrei Chrome/Edge/Chromium. "
            "Instale ou defina a variável de ambiente CHROME_PATH apontando para o executável."
        )

    pdf_path = os.path.abspath(pdf_path)
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode="w", encoding="utf-8") as f:
        f.write(html)
        html_path = f.name

    file_url = "file:///" + os.path.abspath(html_path).replace("\\", "/")
    pdf_flag_path = pdf_path.replace("\\", "/")

    cmd = [
        BROWSER_EXE,
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--allow-file-access-from-files",
        "--disable-web-security",
        "--run-all-compositor-stages-before-draw",
        "--virtual-time-budget=15000",
        "--print-to-pdf-no-header",
        f"--print-to-pdf={pdf_flag_path}",
        file_url,
    ]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
        if proc.returncode != 0:
            raise RuntimeError(
                f"Falha print-to-pdf (code={proc.returncode}).\nSTDERR:\n{proc.stderr}\nSTDOUT:\n{proc.stdout}"
            )
        if not os.path.isfile(pdf_path):
            raise RuntimeError("Chrome/Edge não gerou o PDF (arquivo não existe).")
        if os.path.getsize(pdf_path) < 2000:
            raise RuntimeError("PDF muito pequeno (provável renderização em branco).")
    finally:
        try:
            os.remove(html_path)
        except Exception:
            pass


# =========================================================
# PDF MERGE (opcional)
# =========================================================
try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None


def merge_pdfs(pdf_paths: List[str], output_path: str):
    if not PdfReader or not PdfWriter:
        raise RuntimeError("Biblioteca pypdf não instalada. Rode: pip install pypdf")

    writer = PdfWriter()
    for p in pdf_paths:
        if not os.path.exists(p):
            continue
        reader = PdfReader(p)
        for page in reader.pages:
            writer.add_page(page)

    with open(output_path, "wb") as f:
        writer.write(f)


# =========================================================
# UTILITÁRIOS
# =========================================================
def limpar_nome_arquivo(s: str) -> str:
    s = re.sub(r'[<>:"/\\|?*]+', "_", s)
    return s.replace("\n", " ").replace("\r", " ").strip()


def absolutizar_recursos(html_fragment: str, base_url: str) -> str:
    soup = BeautifulSoup(html_fragment, "lxml")

    for tag in soup.find_all(src=True):
        src = tag.get("src", "")
        if src and not src.startswith(("http://", "https://", "data:")):
            tag["src"] = requests.compat.urljoin(base_url, src)

    for tag in soup.find_all(href=True):
        href = tag.get("href", "")
        if href and not href.startswith(("http://", "https://", "data:", "javascript:", "#")):
            tag["href"] = requests.compat.urljoin(base_url, href)

    return str(soup)


def parse_data_br(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip().replace("-", "/")
    m = re.search(r"(\d{2})/(\d{2})/(\d{2,4})", s)
    if not m:
        return None
    dd, mm, yy = m.group(1), m.group(2), m.group(3)
    if len(yy) == 2:
        yy = "20" + yy
    try:
        return date(int(yy), int(mm), int(dd))
    except Exception:
        return None


# =========================================================
# DET / PORTAL (mesmo fluxo do seu /fisconforme)
# =========================================================
def abrir_acesso_digital_e_entrar(sess: requests.Session) -> bool:
    r = sess.get(URL_DET_HOME, timeout=30, allow_redirects=True)
    if r.status_code != 200:
        return False

    soup = BeautifulSoup(r.text, "lxml")
    form = soup.find("form")
    if not form:
        return False

    action = form.get("action") or URL_ENTRAR
    if not action.startswith("http"):
        action = requests.compat.urljoin(URL_DET_HOME, action)

    r_ent = sess.get(action, timeout=30, allow_redirects=True)

    if r_ent.status_code != 200 or "/certificado/acessos" not in r_ent.url:
        return False

    return True


def extrair_form_logintoken(html: str) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    soup = BeautifulSoup(html, "lxml")
    for form in soup.find_all("form"):
        action = form.get("action") or ""
        if "portalcontribuinte.sefin.ro.gov.br" in action or "LoginToken" in action:
            if not action.startswith("http"):
                action = requests.compat.urljoin(URL_REDIRECT_PORTAL, action)
            data: Dict[str, str] = {}
            for inp in form.find_all("input"):
                name = inp.get("name")
                if not name:
                    continue
                value = inp.get("value", "")
                data[name] = "" if value is None else value
            return action, data
    return None, None


def extrair_redirect_do_logintoken(html: str) -> Optional[str]:
    m = re.search(
        r"location\s*=\s*['\"](https://portalcontribuinte\.sefin\.ro\.gov\.br[^'\"]+)['\"]",
        html,
    )
    if m:
        return m.group(1)
    m = re.search(r"location\.href\s*=\s*['\"](/app/home[^'\"]*)['\"]", html)
    if m:
        return "https://portalcontribuinte.sefin.ro.gov.br" + m.group(1)
    return None


def ir_para_portal_e_carregar_home(sess: requests.Session) -> Optional[str]:
    r_red = sess.get(URL_REDIRECT_PORTAL, timeout=30, allow_redirects=True)
    if r_red.status_code != 200:
        return None

    action_form, data_form = extrair_form_logintoken(r_red.text)
    if action_form:
        r_login = sess.post(action_form, data=data_form, timeout=30, allow_redirects=True)

        if r_login.status_code == 200 and "portalcontribuinte.sefin.ro.gov.br" in r_login.url and "LoginToken" not in r_login.url:
            return r_login.text

        if r_login.status_code == 200 and "LoginToken" in r_login.url:
            next_url = extrair_redirect_do_logintoken(r_login.text) or URL_PORTAL_HOME_DEFAULT
            r_home = sess.get(next_url, timeout=30, allow_redirects=True)
            if r_home.status_code == 200 and "portalcontribuinte.sefin.ro.gov.br" in r_home.url:
                return r_home.text
            return None

    r_portal = sess.get(URL_PORTAL_HOME_DEFAULT, timeout=30, allow_redirects=True)
    if r_portal.status_code == 200 and "portalcontribuinte.sefin.ro.gov.br" in r_portal.url and "LoginToken" not in r_portal.url:
        return r_portal.text

    return None


# =========================================================
# FISCONFORME (igual seu código)
# =========================================================
def encontrar_form_fisconforme(html_portal: str) -> Optional[Tuple[str, str]]:
    soup = BeautifulSoup(html_portal, "lxml")
    forms = soup.find_all("form")
    for f in forms:
        action = f.get("action") or ""
        if "fisconforme" in action.lower():
            token_input = f.find("input", {"name": "token"})
            token_val = token_input.get("value") if token_input else None
            if not token_val:
                return None
            if not action.startswith("http"):
                action = requests.compat.urljoin(URL_PORTAL_HOME_DEFAULT, action)
            return action, token_val
    return None


def acessar_fisconforme(sess: requests.Session, action_url: str, token: str) -> Optional[str]:
    r = sess.post(action_url, data={"token": token}, timeout=30, allow_redirects=True)
    if r.status_code != 200:
        return None
    return r.text


def obter_pendencias_fisconforme(html_fis: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_fis, "lxml")
    tables = soup.find_all("table")
    if not tables:
        return []

    tabela_alvo = None
    for t in tables:
        thead = t.find("thead")
        if not thead:
            continue
        header_text = " ".join(thead.stripped_strings).upper()
        if "CÓDIGO" in header_text and "DESCRIÇÃO DA PENDÊNCIA" in header_text:
            tabela_alvo = t
            break
    if not tabela_alvo:
        return []

    tbody = tabela_alvo.find("tbody")
    if not tbody:
        return []

    pendencias: List[Dict[str, str]] = []
    for tr in tbody.find_all("tr"):
        cols = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cols) < 5:
            continue
        codigo, ie, nome, periodo, descricao = cols[:5]
        pendencias.append({
            "codigo": codigo,
            "ie": ie,
            "nome": nome,
            "periodo": periodo,
            "descricao": descricao,
        })
    return pendencias


# =========================================================
# CONSULTA DÉBITOS (ano atual/anter)
# =========================================================
def obter_debitos_inscricao_estadual(html_deb: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_deb, "lxml")

    tabela_alvo = None
    for tab in soup.find_all("table"):
        ths = tab.find_all("th")
        if not ths:
            continue
        first_th_text = ths[0].get_text(" ", strip=True).upper()
        if "DÉBITOS NA INSCRIÇÃO ESTADUAL" in first_th_text:
            tabela_alvo = tab
            break

    if not tabela_alvo:
        return []

    linhas = tabela_alvo.find_all("tr")
    if len(linhas) <= 2:
        return []

    debitos: List[Dict[str, str]] = []
    for tr in linhas[2:]:
        tds = tr.find_all("td")
        if len(tds) < 11:
            continue

        def txt(i: int) -> str:
            return tds[i].get_text(" ", strip=True) if i < len(tds) else ""

        link_dare = tr.find("a", href=re.compile(r"dare\.sefin\.ro\.gov\.br/adm"))
        link_extrato = tr.find("a", href=re.compile(r"extrato\.jsp"))

        def normalizar_url(href: Optional[str]) -> str:
            if not href:
                return ""
            href = href.replace("%22", "").strip('"')
            if href.startswith("http"):
                return href
            return requests.compat.urljoin(URL_CONSULTA_DEBITOS_LISTA, href)

        url_dare = normalizar_url(link_dare.get("href") if link_dare else "")
        url_extrato = normalizar_url(link_extrato.get("href") if link_extrato else "")

        debitos.append({
            "nr_lancamento": txt(2),
            "receita": txt(6),
            "data_vencimento": txt(8),
            "valor_lancamento": txt(9),
            "valor_atualizado": txt(10),
            "url_dare": url_dare,
            "url_extrato": url_extrato,
        })

    return debitos


def consultar_debitos_ano(sess: requests.Session, ano: int) -> Tuple[List[Dict[str, str]], Optional[str], Optional[str]]:
    r = sess.get(URL_CONSULTA_DEBITOS, timeout=30, allow_redirects=True)
    if r.status_code != 200:
        return [], f"Erro HTTP {r.status_code} ao abrir Consulta de Débitos", None

    soup = BeautifulSoup(r.text, "lxml")
    sel_ie = soup.find("select", {"name": "inscricaoEstadual"})
    if not sel_ie:
        return [], "Campo inscrição estadual não encontrado", None

    opt = sel_ie.find("option")
    if not opt or not opt.get("value"):
        return [], "Nenhuma inscrição estadual disponível", None

    ie_val = opt["value"].strip()
    input_tipo = soup.find("input", {"name": "tipoDevedor"})
    tipo_devedor = input_tipo.get("value", "1") if input_tipo else "1"

    payload = {
        "inscricaoEstadual": ie_val,
        "ano": str(ano),
        "tipoDevedor": tipo_devedor,
        "Submit": "Consultar Débitos",
    }

    r2 = sess.post(URL_CONSULTA_DEBITOS_LISTA, data=payload, timeout=30, allow_redirects=True)
    if r2.status_code != 200:
        return [], f"Erro HTTP {r2.status_code} ao consultar lista (ano {ano})", None

    debitos = obter_debitos_inscricao_estadual(r2.text)
    return debitos, None, r2.text


# =========================================================
# PDF CONSULTA (2 anos)
# =========================================================
MARGEM_CONSULTA_MM = int(os.getenv("MARGEM_CONSULTA_MM", "12"))


def gerar_pdf_consulta_dois_anos(html_atual: Optional[str], html_anterior: Optional[str],
                                empresa: str, codi: str, out_dir: str) -> Optional[str]:
    if not html_atual and not html_anterior:
        return None

    ano_atual = date.today().year
    ano_ant = ano_atual - 1
    data_consulta = date.today().strftime("%d-%m-%Y")

    pasta_saida = os.path.join(out_dir, data_consulta)
    os.makedirs(pasta_saida, exist_ok=True)

    nome_pdf = limpar_nome_arquivo(f"consulta_{str(codi).strip() or '0'}_{data_consulta}.pdf")
    caminho_pdf = os.path.join(pasta_saida, nome_pdf)

    def body_from(html: str) -> str:
        soup = BeautifulSoup(html, "lxml")
        body = soup.body.decode_contents() if soup.body else html
        return absolutizar_recursos(body, BASE_PORTAL)

    sec_atual = f"<h2>Consulta – Ano {ano_atual}</h2>{body_from(html_atual)}" if html_atual else ""
    sec_ant = f"<h2>Consulta – Ano {ano_ant}</h2>{body_from(html_anterior)}" if html_anterior else ""

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <base href="{BASE_PORTAL}">
  <style>
    @page {{ size: A4 portrait; margin: {MARGEM_CONSULTA_MM}mm; }}
    body {{ font-family: Arial, sans-serif; font-size: 12px; margin: 0; }}
    h1 {{ font-size: 16px; margin: 0 0 10px 0; }}
    h2 {{ font-size: 13px; margin: 14px 0 6px 0; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border: 1px solid #444; padding: 4px; }}
    .pagebreak {{ page-break-after: always; }}
  </style>
</head>
<body>
  <h1>Consulta de Débitos – {empresa} (codi {codi}) – {data_consulta}</h1>
  {sec_atual}
  {"<div class='pagebreak'></div>" if sec_atual and sec_ant else ""}
  {sec_ant}
</body>
</html>
"""
    html_para_pdf_chrome(html, caminho_pdf)
    return caminho_pdf


# =========================================================
# DARE (CAPTCHA) - AntiCaptcha opcional
# =========================================================
ANTICAPTCHA_KEY = os.getenv("ANTICAPTCHA_KEY", "60ce5191cf427863d4f3c79ee20e4afe")

try:
    from anticaptchaofficial.imagecaptcha import imagecaptcha
except Exception:
    imagecaptcha = None


DIAS_MAX_FUTURO_DARE = int(os.getenv("DIAS_MAX_FUTURO_DARE", "30"))

MARGEM_DARE_MM = int(os.getenv("MARGEM_DARE_MM", "6"))
ZOOM_DARE_2VIAS = float(os.getenv("ZOOM_DARE_2VIAS", "0.92"))
LOGO_MAX_WIDTH_PX = int(os.getenv("LOGO_MAX_WIDTH_PX", "110"))
LOGO_MAX_HEIGHT_PX = int(os.getenv("LOGO_MAX_HEIGHT_PX", "50"))

MARGEM_EXTRATO_MM = int(os.getenv("MARGEM_EXTRATO_MM", "10"))


def resolver_captcha_automatico(img_bytes: bytes) -> Optional[str]:
    if not imagecaptcha:
        return None

    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
        tmp_file.write(img_bytes)
        img_path = tmp_file.name

    try:
        solver = imagecaptcha()
        solver.set_key(ANTICAPTCHA_KEY)
        captcha_resp = solver.solve_and_return_solution(img_path)
        if captcha_resp != 0:
            return str(captcha_resp)
        return None
    finally:
        try:
            os.remove(img_path)
        except Exception:
            pass


def carregar_html_dare_final(sess: requests.Session, url_dare: str, max_tentativas: int = 3) -> str:
    for _ in range(max_tentativas):
        r = sess.get(url_dare, timeout=30, allow_redirects=True)
        if r.status_code != 200:
            raise RuntimeError(f"Falha ao abrir DARE (adm): HTTP {r.status_code}")

        # já é a guia final (sem captcha)
        if "copy-cb" in r.text:
            return r.text

        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form", id="adm_processar_form")
        if not form:
            return r.text

        img = soup.find("img", id="captcha-imagem")
        if not img:
            return r.text

        src = img.get("src", "")
        if not src.startswith("data:image"):
            return r.text

        try:
            _, b64_data = src.split(",", 1)
            img_bytes = base64.b64decode(b64_data)
        except Exception:
            return r.text

        captcha_resp = resolver_captcha_automatico(img_bytes)
        if not captcha_resp:
            time.sleep(1)
            continue

        data: Dict[str, str] = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if name:
                val = inp.get("value")
                data[name] = "" if val is None else val

        data["captcha[resposta]"] = captcha_resp

        for sel in form.find_all("select"):
            name = sel.get("name")
            if not name:
                continue
            opt = sel.find("option", selected=True) or sel.find("option")
            if opt:
                data[name] = opt.get("value", "")

        action = form.get("action") or ""
        if not action.startswith("http"):
            action = requests.compat.urljoin(BASE_DARE, action)

        r2 = sess.post(action, data=data, timeout=30, allow_redirects=True)
        if r2.status_code == 200 and "copy-cb" in r2.text:
            return r2.text

        time.sleep(1)

    raise RuntimeError("Não foi possível emitir o DARE (máx. tentativas atingido).")


def remover_elementos_de_menu(soup: BeautifulSoup):
    for txt in ["Voltar", "Imprimir", "COPIAR CÓDIGO DE BARRAS", "COPIAR QR CODE PIX"]:
        for node in soup.find_all(string=re.compile(re.escape(txt), re.I)):
            a = node.find_parent(["a", "button", "div", "span"])
            if a:
                try:
                    a.decompose()
                except Exception:
                    pass


def remover_rodape_sefin_geinf(soup: BeautifulSoup):
    for node in soup.find_all(string=re.compile(r"Desenvolvido\s+por\s+GEINF/SEFIN", re.I)):
        parent = node
        for _ in range(6):
            if parent and parent.parent:
                parent = parent.parent
        try:
            if parent:
                parent.decompose()
        except Exception:
            pass


def neutralizar_pagebreaks(soup: BeautifulSoup):
    for tag in soup.find_all(True):
        st = (tag.get("style") or "")
        cls = " ".join(tag.get("class") or [])
        if "page-break" in st.lower() or "break-after" in st.lower() or "pagebreak" in cls.lower():
            try:
                tag["style"] = re.sub(r"page-break-[^;]+;?", "", st, flags=re.I)
                tag["style"] = re.sub(r"break-[^;]+;?", "", tag.get("style", ""), flags=re.I)
            except Exception:
                pass


def marcar_logo_sefin(soup: BeautifulSoup):
    for img in soup.find_all("img"):
        src = (img.get("src") or "").strip()
        if not src or src.startswith("data:image"):
            continue
        classes = img.get("class") or []
        if "logo-sefin" not in classes:
            classes.append("logo-sefin")
        img["class"] = classes
        break


def centralizar_cod_barras(soup: BeautifulSoup):
    padrao = re.compile(r"\b\d{11}\s+\d{12}\s+\d{12}\s+\d{12}\b")
    for node in soup.find_all(string=padrao):
        parent = node.find_parent(["td", "div", "p", "span"])
        if parent:
            st = parent.get("style") or ""
            parent["style"] = (st + ";text-align:center;").strip(";")

    for tag in soup.find_all(["img", "svg"]):
        src = (tag.get("src") or "").lower()
        if tag.name == "svg" or ("barra" in src) or ("barcode" in src) or ("codigo" in src) or ("qrcode" in src):
            st = tag.get("style") or ""
            tag["style"] = (st + ";display:block;margin:0 auto;").strip(";")
            p = tag.find_parent(["td", "div", "p", "span"])
            if p:
                pst = p.get("style") or ""
                p["style"] = (pst + ";text-align:center;").strip(";")


def extrair_bloco_via(soup: BeautifulSoup, regex_alvo: str, regex_proibido: str) -> Optional[str]:
    alvo = re.compile(regex_alvo, re.I)
    proib = re.compile(regex_proibido, re.I)

    node = soup.find(string=alvo)
    if not node:
        return None

    cur = node
    candidates = []
    for _ in range(18):
        cur = cur.parent if hasattr(cur, "parent") else None
        if not cur or not getattr(cur, "name", None):
            break
        if cur.name in ("table", "div", "section", "article"):
            txt = cur.get_text(" ", strip=True)
            if len(txt) < 200:
                continue
            if proib.search(txt):
                continue
            candidates.append(cur)

    if not candidates:
        parent = node.find_parent(["table", "div", "section", "article"])
        if parent:
            txt = parent.get_text(" ", strip=True)
            if len(txt) >= 200 and not proib.search(txt):
                return str(parent)
        return None

    best = min(candidates, key=lambda t: len(t.get_text(" ", strip=True)))
    return str(best)


def preparar_dare_duas_vias(html_dare_final: str) -> str:
    soup = BeautifulSoup(html_dare_final, "lxml")
    remover_elementos_de_menu(soup)
    neutralizar_pagebreaks(soup)
    marcar_logo_sefin(soup)
    centralizar_cod_barras(soup)
    remover_rodape_sefin_geinf(soup)

    via_banco = extrair_bloco_via(
        soup,
        r"Autenticação\s*mecânica\s*/\s*Via\s*banco",
        r"Autenticação\s*mecânica\s*/\s*Via\s*Usu[aá]rio",
    )
    via_usuario = extrair_bloco_via(
        soup,
        r"Autenticação\s*mecânica\s*/\s*Via\s*Usu[aá]rio",
        r"Autenticação\s*mecânica\s*/\s*Via\s*banco",
    )

    if via_banco and via_usuario:
        body = f"""
<div class="vias">
  <div class="via">{via_banco}</div>
  <div class="corte">------------------------------------ corte aqui ------------------------------------</div>
  <div class="via">{via_usuario}</div>
</div>
"""
        return absolutizar_recursos(body, BASE_DARE)

    body = soup.body.decode_contents() if soup.body else str(soup)
    return absolutizar_recursos(body, BASE_DARE)


def preparar_extrato_normal_retrato(html_extrato: str) -> str:
    soup = BeautifulSoup(html_extrato, "lxml")

    for node in soup.find_all(string=re.compile(r"paisagem", re.I)):
        p = node.find_parent(["div", "p", "span", "td"])
        if p:
            try:
                p.decompose()
            except Exception:
                pass

    for tag in soup.find_all(True):
        st = (tag.get("style") or "")
        if "rotate" in st.lower():
            tag["style"] = re.sub(r"transform\s*:\s*rotate\([^)]+\)\s*;?", "", st, flags=re.I)

    body = soup.body.decode_contents() if soup.body else str(soup)
    return absolutizar_recursos(body, BASE_PORTAL)


def gerar_pdf_dare_e_extrato(sess: requests.Session, debito: Dict[str, str], codi: str, out_dir: str) -> Optional[str]:
    venc_txt = (debito.get("data_vencimento") or "").strip()
    venc_date = parse_data_br(venc_txt)
    if venc_date:
        limite = date.today() + timedelta(days=DIAS_MAX_FUTURO_DARE)
        if venc_date > limite:
            return None

    url_dare = (debito.get("url_dare") or "").strip()
    url_ext = (debito.get("url_extrato") or "").strip()
    if not url_dare:
        return None

    data_consulta = date.today().strftime("%d-%m-%Y")
    pasta_saida = os.path.join(out_dir, data_consulta)
    os.makedirs(pasta_saida, exist_ok=True)

    receita = (debito.get("receita") or "").strip() or "0"
    valor_txt = (debito.get("valor_atualizado") or "").strip() or (debito.get("valor_lancamento") or "").strip() or "0"
    valor_nome = valor_txt.replace("R$", "").replace(" ", "").replace(".", "").replace(",", ",")

    venc_nome = venc_txt.replace("/", "-") if venc_txt else date.today().strftime("%d-%m-%Y")
    codi_txt = str(codi).strip() or "0"
    nome_pdf = limpar_nome_arquivo(f"{codi_txt}_{venc_nome}_{receita}_{valor_nome}.pdf")
    caminho_pdf = os.path.join(pasta_saida, nome_pdf)

    html_dare_final = carregar_html_dare_final(sess, url_dare)
    body_dare_2vias = preparar_dare_duas_vias(html_dare_final)

    dare_html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <base href="{BASE_DARE}">
  <style>
    * {{ box-sizing: border-box; }}
    @page {{ size: A4 portrait; margin: {MARGEM_DARE_MM}mm; }}
    html, body {{ margin:0; padding:0; }}
    body {{ overflow: visible !important; }}

    body > *:first-child {{
      break-before: avoid !important;
      page-break-before: avoid !important;
    }}

    .via {{ zoom: {ZOOM_DARE_2VIAS}; transform-origin: top center; }}
    .corte {{
      text-align:center;
      font: 10px/1.2 Arial, sans-serif;
      opacity:.8;
      margin: 2mm 0;
      white-space: nowrap;
    }}

    img.logo-sefin {{
      max-width: {LOGO_MAX_WIDTH_PX}px !important;
      max-height: {LOGO_MAX_HEIGHT_PX}px !important;
      width: auto !important;
      height: auto !important;
      display: block;
      margin: 2px auto !important;
    }}

    img[src*="barra"], img[src*="barcode"], svg {{
      display:block;
      margin:0 auto;
    }}

    .via, .via * {{
      break-inside: avoid !important;
      page-break-inside: avoid !important;
    }}
  </style>
</head>
<body>
  {body_dare_2vias}
</body>
</html>
"""

    # Sem extrato: apenas DARE
    if not url_ext:
        html_para_pdf_chrome(dare_html, caminho_pdf)
        return caminho_pdf

    # Com extrato: DARE + extrato
    if PdfReader is None or PdfWriter is None:
        # Se não tiver pypdf, salva só DARE
        html_para_pdf_chrome(dare_html, caminho_pdf)
        return caminho_pdf

    tmp_dare = os.path.join(pasta_saida, "__tmp_dare.pdf")
    tmp_ext = os.path.join(pasta_saida, "__tmp_extrato.pdf")

    html_para_pdf_chrome(dare_html, tmp_dare)

    r_ext = sess.get(url_ext, timeout=30, allow_redirects=True)
    if r_ext.status_code != 200:
        os.replace(tmp_dare, caminho_pdf)
        return caminho_pdf

    body_ext = preparar_extrato_normal_retrato(r_ext.text)

    extrato_html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <base href="{BASE_PORTAL}">
  <style>
    * {{ box-sizing: border-box; }}
    @page {{ size: A4 portrait; margin: {MARGEM_EXTRATO_MM}mm; }}
    html, body {{ margin:0; padding:0; }}
    body {{ overflow: visible !important; }}
  </style>
</head>
<body>
  {body_ext}
</body>
</html>
"""
    html_para_pdf_chrome(extrato_html, tmp_ext)

    merge_pdfs([tmp_dare, tmp_ext], caminho_pdf)

    for p in (tmp_dare, tmp_ext):
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    return caminho_pdf


# =========================================================
# FLUXOS POR EMPRESA (API)
# =========================================================
def fluxo_fisconforme_para_empresa_api(cert_row: Dict[str, Any]) -> Dict[str, Any]:
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    venc = cert_row.get("vencimento")
    doc = cert_row.get("cnpj/cpf") or ""

    resultado: Dict[str, Any] = {
        "empresa": empresa,
        "user": user,
        "cnpj": doc,
        "vencimento": venc,
        "status": "erro",
        "situacao_geral": "erro",
        "qtd_pendencias": 0,
        "pendencias": [],
        "qtd_debitos": 0,
        "debitos": [],
        "erro_fisconforme": None,
        "erro_debitos": None,
        "erro": None,
    }

    cert_path, key_path = None, None
    try:
        cert_path, key_path = criar_arquivos_cert_temp(cert_row)
        sess = criar_sessao(cert_path, key_path)

        if not abrir_acesso_digital_e_entrar(sess):
            resultado["erro"] = "Falha ao entrar no Acesso Digital"
            return resultado

        html_portal = ir_para_portal_e_carregar_home(sess)
        if not html_portal:
            resultado["erro"] = "Falha ao abrir o Portal"
            return resultado

        # FisConforme
        pendencias: List[Dict[str, str]] = []
        try:
            res = encontrar_form_fisconforme(html_portal)
            if not res:
                resultado["erro_fisconforme"] = "Formulário do FisConforme não encontrado"
            else:
                action_url, token = res
                html_fis = acessar_fisconforme(sess, action_url, token)
                if not html_fis:
                    resultado["erro_fisconforme"] = "Erro ao abrir FisConforme"
                else:
                    pendencias = obter_pendencias_fisconforme(html_fis)
        except Exception as e_fis:
            resultado["erro_fisconforme"] = f"Exceção no FisConforme: {e_fis}"

        resultado["pendencias"] = pendencias
        resultado["qtd_pendencias"] = len(pendencias)

        # Débitos (ano atual)
        debitos: List[Dict[str, str]] = []
        try:
            debitos, err, _html = consultar_debitos_ano(sess, date.today().year)
            if err:
                resultado["erro_debitos"] = err
        except Exception as e_deb:
            resultado["erro_debitos"] = f"Exceção na Consulta de Débitos: {e_deb}"

        resultado["debitos"] = debitos
        resultado["qtd_debitos"] = len(debitos)

        tem_pend = len(pendencias) > 0
        tem_deb = len(debitos) > 0
        tem_erro = bool(resultado["erro_fisconforme"] or resultado["erro_debitos"])

        if tem_erro and not (tem_pend or tem_deb):
            situacao = "erro"
        elif tem_pend and tem_deb:
            situacao = "pendencia_fis_e_debitos"
        elif tem_pend:
            situacao = "pendencia_fis"
        elif tem_deb:
            situacao = "debitos"
        else:
            situacao = "regular"

        resultado["situacao_geral"] = situacao
        resultado["status"] = situacao
        return resultado

    except Exception as e:
        resultado["erro"] = str(e)
        resultado["situacao_geral"] = "erro"
        resultado["status"] = "erro"
        return resultado

    finally:
        try:
            if cert_path and os.path.exists(cert_path):
                os.remove(cert_path)
            if key_path and os.path.exists(key_path):
                os.remove(key_path)
        except Exception:
            pass


def fluxo_dare_para_empresa_api(cert_row: Dict[str, Any], out_dir: str) -> Dict[str, Any]:
    empresa = cert_row.get("empresa") or ""
    doc = cert_row.get("cnpj/cpf") or ""
    user = cert_row.get("user") or ""
    codi = str(cert_row.get("codi") or "").strip() or "0"

    resultado: Dict[str, Any] = {
        "empresa": empresa,
        "user": user,
        "cnpj": doc,
        "codi": codi,
        "situacao_geral": "erro",
        "qtd_debitos_ano_atual": 0,
        "qtd_debitos_ano_anterior": 0,
        "pdf_consulta": None,
        "pdfs_dare": [],
        "erro": None,
    }

    cert_path, key_path = None, None
    try:
        cert_path, key_path = criar_arquivos_cert_temp(cert_row)
        sess = criar_sessao(cert_path, key_path)

        if not abrir_acesso_digital_e_entrar(sess):
            resultado["erro"] = "Falha ao entrar no Acesso Digital"
            return resultado

        html_portal = ir_para_portal_e_carregar_home(sess)
        if not html_portal:
            resultado["erro"] = "Falha ao abrir o Portal"
            return resultado

        ano_atual = date.today().year
        ano_ant = ano_atual - 1

        debitos_atual, err1, html_lista_atual = consultar_debitos_ano(sess, ano_atual)
        if err1:
            resultado["erro"] = err1
            return resultado

        debitos_ant, err2, html_lista_ant = consultar_debitos_ano(sess, ano_ant)
        if err2:
            resultado["erro"] = err2
            return resultado

        resultado["qtd_debitos_ano_atual"] = len(debitos_atual)
        resultado["qtd_debitos_ano_anterior"] = len(debitos_ant)

        # PDF consulta (2 anos)
        try:
            pdf_consulta = gerar_pdf_consulta_dois_anos(html_lista_atual, html_lista_ant, empresa, codi, out_dir)
            resultado["pdf_consulta"] = pdf_consulta
        except Exception as e:
            # não trava o processo se falhar a consulta
            resultado["pdf_consulta"] = None

        # PDFs DARE/Extrato (com filtro vencimento)
        todos = debitos_atual + debitos_ant
        pdfs_gerados: List[str] = []

        for deb in todos:
            try:
                p = gerar_pdf_dare_e_extrato(sess, deb, codi, out_dir)
                if p:
                    pdfs_gerados.append(p)
            except Exception:
                pass

        resultado["pdfs_dare"] = pdfs_gerados
        resultado["situacao_geral"] = "debitos" if todos else "sem_debitos"
        return resultado

    except Exception as e:
        resultado["erro"] = str(e)
        resultado["situacao_geral"] = "erro"
        return resultado

    finally:
        try:
            if cert_path and os.path.exists(cert_path):
                os.remove(cert_path)
            if key_path and os.path.exists(key_path):
                os.remove(key_path)
        except Exception:
            pass


# =========================================================
# FASTAPI
# =========================================================
app = FastAPI(title="API DET/SEFIN-RO — FisConforme + Débitos + DARE")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/fisconforme")
def listar_fisconforme(user: str = Query(..., description="Campo user (e-mail) da tabela certifica_dfe")):
    certs = carregar_certificados_validos(user_filter=user)
    if not certs:
        return {"ok": True, "user": user, "total_empresas": 0, "results": []}

    results: List[Dict[str, Any]] = []
    for cert_row in certs:
        res = fluxo_fisconforme_para_empresa_api(cert_row)
        results.append(res)

    return {"ok": True, "user": user, "total_empresas": len(results), "results": results}


@app.get("/dare")
def gerar_dare(
    user: str = Query(..., description="Campo user (e-mail) da tabela certifica_dfe"),
    download: int = Query(1, description="1 = retorna ZIP com PDFs, 0 = retorna só JSON"),
):
    if not BROWSER_EXE:
        raise HTTPException(
            status_code=500,
            detail="Chrome/Edge/Chromium não encontrado. Instale ou defina CHROME_PATH.",
        )

    # pasta temporária do job (evita misturar com outros usuários)
    job_dir = tempfile.mkdtemp(prefix="dare_job_")

    certs = carregar_certificados_validos(user_filter=user)
    if not certs:
        return {"ok": True, "user": user, "total_empresas": 0, "results": [], "zip": None}

    results: List[Dict[str, Any]] = []
    for cert_row in certs:
        res = fluxo_dare_para_empresa_api(cert_row, job_dir)
        results.append(res)

    payload = {
        "ok": True,
        "user": user,
        "total_empresas": len(results),
        "job_dir": job_dir if download == 0 else None,  # só aparece se não baixar
        "results": results,
    }

    if download == 0:
        return JSONResponse(payload)

    # gera zip com tudo que foi criado (PDFs por data)
    zip_path = os.path.join(job_dir, f"DARE_{limpar_nome_arquivo(user)}_{date.today().strftime('%Y%m%d')}.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _dirs, files in os.walk(job_dir):
            for fn in files:
                if not fn.lower().endswith(".pdf"):
                    continue
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, job_dir)
                z.write(full, rel)

    # devolve o zip
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=os.path.basename(zip_path),
    )


if __name__ == "__main__":
    uvicorn.run(
        "fisconforme:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
