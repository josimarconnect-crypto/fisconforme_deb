import os
import re
import base64
import tempfile
import time
import zipfile
from datetime import date, timedelta
from typing import Dict, Any, Optional, List, Tuple

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import uvicorn

# PDF no Render via Playwright (Chromium headless)
from playwright.sync_api import sync_playwright

# Anti-Captcha
from anticaptchaofficial.imagecaptcha import imagecaptcha

# PDF merge (DARE + extrato)
from pypdf import PdfReader, PdfWriter


# =========================================================
# 🔐 CONFIG FIXA (COM KEYS NO CÓDIGO, COMO VOCÊ PEDIU)
# =========================================================
SUPABASE_URL = "https://hysrxadnigzqadnlkynq.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh5c3J4YWRuaWd6cWFkbmxreW5xIiw"
    "icm9sZSI6ImFub24iLCJpYXQiOjE3NDM3MTQwODAsImV4cCI6MjA1OTI5MDA4MH0."
    "RLcu44IvY4X8PLK5BOa_FL5WQ0vJA3p0t80YsGQjTrA"
)
TABELA_CERTS = "certifica_dfe"

ANTICAPTCHA_KEY = "60ce5191cf427863d4f3c79ee20e4afe"

# =========================================================
# URLs DET / PORTAL
# =========================================================
URL_DET_HOME = "https://detsec.sefin.ro.gov.br/certificados"
URL_ENTRAR = "https://detsec.sefin.ro.gov.br/entrar"
URL_REDIRECT_PORTAL = "https://detsec.sefin.ro.gov.br/contribuinte/notificacoes/redirect_portal"
URL_PORTAL_HOME_DEFAULT = "https://portalcontribuinte.sefin.ro.gov.br/app/home/?exibir_modal=true"

URL_CONSULTA_DEBITOS = "https://portalcontribuinte.sefin.ro.gov.br/app/consultadebitos/"
URL_CONSULTA_DEBITOS_LISTA = "https://portalcontribuinte.sefin.ro.gov.br/app/consultadebitos/lista.jsp"

BASE_DARE = "https://dare.sefin.ro.gov.br/"
BASE_PORTAL = "https://portalcontribuinte.sefin.ro.gov.br/"

# DARE: filtro vencimento até hoje+30
DIAS_MAX_FUTURO_DARE = 30


# =========================================================
# SUPABASE
# =========================================================
def supabase_headers(is_json: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if is_json:
        h["Content-Type"] = "application/json"
    return h


def carregar_certificados_validos(user_filter: str) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{TABELA_CERTS}"
    params: Dict[str, str] = {'select': 'id,pem,key,empresa,codi,user,vencimento,"cnpj/cpf"'}
    params["user"] = f"eq.{user_filter}"
    r = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json() or []


# =========================================================
# CERT TEMP + SESSION
# =========================================================
def criar_arquivos_cert_temp(cert_row: Dict[str, Any]) -> Tuple[str, str]:
    pem_bytes = base64.b64decode(cert_row.get("pem") or "")
    key_bytes = base64.b64decode(cert_row.get("key") or "")

    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
    cert_file.write(pem_bytes); cert_file.close()
    key_file.write(key_bytes); key_file.close()
    return cert_file.name, key_file.name


def criar_sessao(cert_path: str, key_path: str) -> requests.Session:
    s = requests.Session()
    s.cert = (cert_path, key_path)
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    })
    return s


# =========================================================
# DET / PORTAL
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
                data[name] = inp.get("value", "") or ""
            return action, data
    return None, None


def extrair_redirect_do_logintoken(html: str) -> Optional[str]:
    m = re.search(r"location\s*=\s*['\"](https://portalcontribuinte\.sefin\.ro\.gov\.br[^'\"]+)['\"]", html)
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
        if r_login.status_code == 200 and "LoginToken" not in r_login.url:
            return r_login.text
        if r_login.status_code == 200 and "LoginToken" in r_login.url:
            next_url = extrair_redirect_do_logintoken(r_login.text) or URL_PORTAL_HOME_DEFAULT
            r_home = sess.get(next_url, timeout=30, allow_redirects=True)
            if r_home.status_code == 200 and "portalcontribuinte.sefin.ro.gov.br" in r_home.url:
                return r_home.text

    r_portal = sess.get(URL_PORTAL_HOME_DEFAULT, timeout=30, allow_redirects=True)
    if r_portal.status_code == 200 and "LoginToken" not in r_portal.url:
        return r_portal.text

    return None


# =========================================================
# FISCONFORME (mantido simples: pega pendências)
# =========================================================
def encontrar_form_fisconforme(html_portal: str) -> Optional[Tuple[str, str]]:
    soup = BeautifulSoup(html_portal, "lxml")
    for f in soup.find_all("form"):
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
            "codigo": codigo, "ie": ie, "nome": nome, "periodo": periodo, "descricao": descricao
        })
    return pendencias


# =========================================================
# DÉBITOS (pega URLs de DARE/Extrato)
# =========================================================
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


def obter_debitos_inscricao_estadual(html_deb: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_deb, "lxml")
    tabela_alvo = None
    for tab in soup.find_all("table"):
        ths = tab.find_all("th")
        if not ths:
            continue
        if "DÉBITOS NA INSCRIÇÃO ESTADUAL" in ths[0].get_text(" ", strip=True).upper():
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

        # links (quando existirem)
        link_dare = tr.find("a", href=re.compile(r"dare\.sefin\.ro\.gov\.br/adm"))
        link_extrato = tr.find("a", href=re.compile(r"extrato\.jsp"))

        def norm_url(href: Optional[str]) -> str:
            if not href:
                return ""
            href = href.replace("%22", "").strip('"')
            if href.startswith("http"):
                return href
            return requests.compat.urljoin(URL_CONSULTA_DEBITOS_LISTA, href)

        debitos.append({
            "dare": txt(0),
            "extrato": txt(1),
            "nr_lancamento": txt(2),
            "parcela": txt(3),
            "referencia": txt(4),
            "complemento": txt(5),
            "receita": txt(6),
            "situacao": txt(7),
            "data_vencimento": txt(8),
            "valor_lancamento": txt(9),
            "valor_atualizado": txt(10),
            "url_dare": norm_url(link_dare.get("href") if link_dare else ""),
            "url_extrato": norm_url(link_extrato.get("href") if link_extrato else ""),
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
    tipo_devedor = (soup.find("input", {"name": "tipoDevedor"}) or {}).get("value", "1")

    payload = {
        "inscricaoEstadual": ie_val,
        "ano": str(ano),
        "tipoDevedor": tipo_devedor,
        "Submit": "Consultar Débitos",
    }

    r2 = sess.post(URL_CONSULTA_DEBITOS_LISTA, data=payload, timeout=30, allow_redirects=True)
    if r2.status_code != 200:
        return [], f"Erro HTTP {r2.status_code} ao consultar lista (ano {ano})", None

    return obter_debitos_inscricao_estadual(r2.text), None, r2.text


# =========================================================
# CAPTCHA DARE
# =========================================================
def resolver_captcha_automatico(img_bytes: bytes) -> Optional[str]:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
        tmp_file.write(img_bytes)
        img_path = tmp_file.name
    try:
        solver = imagecaptcha()
        solver.set_key(ANTICAPTCHA_KEY)
        resp = solver.solve_and_return_solution(img_path)
        if resp != 0:
            return str(resp)
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
            raise RuntimeError(f"Falha ao abrir DARE: HTTP {r.status_code}")

        # já é guia final?
        if "copy-cb" in r.text:
            return r.text

        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form", id="adm_processar_form")
        img = soup.find("img", id="captcha-imagem")
        if not form or not img:
            return r.text

        src = img.get("src", "")
        if not src.startswith("data:image"):
            return r.text

        _, b64_data = src.split(",", 1)
        img_bytes = base64.b64decode(b64_data)

        captcha_resp = resolver_captcha_automatico(img_bytes)
        if not captcha_resp:
            time.sleep(1.5)
            continue

        data: Dict[str, str] = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if name:
                data[name] = inp.get("value", "") or ""
        data["captcha[resposta]"] = captcha_resp

        for sel in form.find_all("select"):
            name = sel.get("name")
            if not name:
                continue
            opt = sel.find("option", selected=True) or sel.find("option")
            if opt:
                data[name] = opt.get("value", "") or ""

        action = form.get("action") or ""
        if not action.startswith("http"):
            action = requests.compat.urljoin(BASE_DARE, action)

        r2 = sess.post(action, data=data, timeout=30, allow_redirects=True)
        if r2.status_code == 200 and "copy-cb" in r2.text:
            return r2.text

        time.sleep(1.2)

    raise RuntimeError("Não foi possível emitir o DARE (CAPTCHA).")


# =========================================================
# PDF via Playwright (Render)
# =========================================================
def html_para_pdf_playwright(html: str, pdf_path: str, base_url: Optional[str] = None):
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        page = browser.new_page()
        page.set_content(html, wait_until="load", base_url=base_url)
        page.pdf(path=pdf_path, format="A4", print_background=True)
        browser.close()


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


def merge_pdfs(pdf_paths: List[str], output_path: str):
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
# GERA DARE+EXTRATO PDF
# =========================================================
def gerar_pdf_dare_e_extrato(sess: requests.Session, deb: Dict[str, str], pasta: str) -> Optional[str]:
    venc = (deb.get("data_vencimento") or "").strip()
    venc_date = parse_data_br(venc)
    if venc_date:
        limite = date.today() + timedelta(days=DIAS_MAX_FUTURO_DARE)
        if venc_date > limite:
            return None

    url_dare = (deb.get("url_dare") or "").strip()
    url_ext  = (deb.get("url_extrato") or "").strip()
    if not url_dare:
        return None

    receita = (deb.get("receita") or "0").strip()
    valor = (deb.get("valor_atualizado") or deb.get("valor_lancamento") or "0").strip()
    nome = re.sub(r'[<>:"/\\|?*]+', "_", f"DARE_{venc.replace('/','-')}_{receita}_{valor}.pdf")
    out_pdf = os.path.join(pasta, nome)

    # 1) HTML final do DARE (resolve captcha se tiver)
    html_dare_final = carregar_html_dare_final(sess, url_dare)

    # 2) PDF do DARE
    dare_body = BeautifulSoup(html_dare_final, "lxml").body
    dare_body_html = absolutizar_recursos(dare_body.decode_contents() if dare_body else html_dare_final, BASE_DARE)
    dare_html = f"""<!doctype html><html><head><meta charset="utf-8"><base href="{BASE_DARE}"></head>
    <body>{dare_body_html}</body></html>"""

    tmp_dare = os.path.join(pasta, "__tmp_dare.pdf")
    html_para_pdf_playwright(dare_html, tmp_dare, base_url=BASE_DARE)

    # sem extrato
    if not url_ext:
        os.replace(tmp_dare, out_pdf)
        return out_pdf

    # 3) PDF extrato
    r_ext = sess.get(url_ext, timeout=30, allow_redirects=True)
    if r_ext.status_code != 200:
        os.replace(tmp_dare, out_pdf)
        return out_pdf

    ext_body = BeautifulSoup(r_ext.text, "lxml").body
    ext_body_html = absolutizar_recursos(ext_body.decode_contents() if ext_body else r_ext.text, BASE_PORTAL)
    ext_html = f"""<!doctype html><html><head><meta charset="utf-8"><base href="{BASE_PORTAL}"></head>
    <body>{ext_body_html}</body></html>"""

    tmp_ext = os.path.join(pasta, "__tmp_ext.pdf")
    html_para_pdf_playwright(ext_html, tmp_ext, base_url=BASE_PORTAL)

    # 4) merge
    merge_pdfs([tmp_dare, tmp_ext], out_pdf)

    for p in (tmp_dare, tmp_ext):
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    return out_pdf


# =========================================================
# FLUXO (FISCONFORME + DÉBITOS) JSON
# =========================================================
def fluxo_fisconforme(cert_row: Dict[str, Any]) -> Dict[str, Any]:
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    venc = cert_row.get("vencimento")
    doc = cert_row.get("cnpj/cpf") or ""
    codi = cert_row.get("codi") or ""

    res: Dict[str, Any] = {
        "empresa": empresa,
        "user": user,
        "cnpj": doc,
        "codi": codi,
        "vencimento": venc,
        "situacao_geral": "erro",
        "pendencias": [],
        "qtd_pendencias": 0,
        "debitos": [],
        "qtd_debitos": 0,
        "erro_fisconforme": None,
        "erro_debitos": None,
        "erro": None,
    }

    cert_path = key_path = None
    try:
        cert_path, key_path = criar_arquivos_cert_temp(cert_row)
        sess = criar_sessao(cert_path, key_path)

        if not abrir_acesso_digital_e_entrar(sess):
            res["erro"] = "Falha ao entrar no Acesso Digital"
            return res

        html_portal = ir_para_portal_e_carregar_home(sess)
        if not html_portal:
            res["erro"] = "Falha ao abrir Portal"
            return res

        # FisConforme
        try:
            form = encontrar_form_fisconforme(html_portal)
            if form:
                action, token = form
                html_fis = acessar_fisconforme(sess, action, token)
                if html_fis:
                    pend = obter_pendencias_fisconforme(html_fis)
                    res["pendencias"] = pend
                    res["qtd_pendencias"] = len(pend)
                else:
                    res["erro_fisconforme"] = "Erro ao abrir FisConforme"
            else:
                res["erro_fisconforme"] = "Form FisConforme não encontrado"
        except Exception as e:
            res["erro_fisconforme"] = str(e)

        # Débitos (ano atual apenas no JSON)
        try:
            debitos, err, _html = consultar_debitos_ano(sess, date.today().year)
            if err:
                res["erro_debitos"] = err
            else:
                res["debitos"] = debitos
                res["qtd_debitos"] = len(debitos)
        except Exception as e:
            res["erro_debitos"] = str(e)

        tem_p = res["qtd_pendencias"] > 0
        tem_d = res["qtd_debitos"] > 0
        tem_e = bool(res["erro_fisconforme"] or res["erro_debitos"] or res["erro"])

        if tem_e and not (tem_p or tem_d):
            sit = "erro"
        elif tem_p and tem_d:
            sit = "pendencia_fis_e_debitos"
        elif tem_p:
            sit = "pendencia_fis"
        elif tem_d:
            sit = "debitos"
        else:
            sit = "regular"

        res["situacao_geral"] = sit
        return res

    except Exception as e:
        res["erro"] = str(e)
        res["situacao_geral"] = "erro"
        return res

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
app = FastAPI(title="API FisConforme + DARE (Render)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/fisconforme")
def route_fisconforme(user: str = Query(..., description="E-mail do campo user na certifica_dfe")):
    certs = carregar_certificados_validos(user)
    results = [fluxo_fisconforme(c) for c in certs]
    return {"ok": True, "user": user, "total_empresas": len(results), "results": results}


@app.get("/dare")
def route_dare(
    user: str = Query(..., description="E-mail do campo user na certifica_dfe"),
    download: int = Query(1, description="1=retorna ZIP para download"),
):
    certs = carregar_certificados_validos(user)
    if not certs:
        return JSONResponse({"ok": True, "user": user, "msg": "Nenhuma empresa para este user.", "zip": None})

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, f"dares_{re.sub(r'[^a-zA-Z0-9]+','_',user)}_{date.today().isoformat()}.zip")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for cert in certs:
                empresa = (cert.get("empresa") or "empresa").strip()
                codi = str(cert.get("codi") or "0").strip()

                cert_path = key_path = None
                try:
                    cert_path, key_path = criar_arquivos_cert_temp(cert)
                    sess = criar_sessao(cert_path, key_path)

                    if not abrir_acesso_digital_e_entrar(sess):
                        continue
                    html_portal = ir_para_portal_e_carregar_home(sess)
                    if not html_portal:
                        continue

                    # DARE: consulta 2 anos (atual + anterior)
                    ano_atual = date.today().year
                    ano_ant = ano_atual - 1
                    deb_a, err_a, _ = consultar_debitos_ano(sess, ano_atual)
                    deb_b, err_b, _ = consultar_debitos_ano(sess, ano_ant)

                    if err_a and err_b:
                        continue

                    todos = (deb_a or []) + (deb_b or [])

                    # pasta por empresa
                    pasta_emp = os.path.join(tmpdir, f"{codi}_{re.sub(r'[^a-zA-Z0-9]+','_',empresa)[:30]}")
                    os.makedirs(pasta_emp, exist_ok=True)

                    for deb in todos:
                        try:
                            pdf_path = gerar_pdf_dare_e_extrato(sess, deb, pasta_emp)
                            if pdf_path and os.path.exists(pdf_path):
                                arcname = os.path.join(os.path.basename(pasta_emp), os.path.basename(pdf_path))
                                zf.write(pdf_path, arcname=arcname)
                        except Exception:
                            pass

                finally:
                    try:
                        if cert_path and os.path.exists(cert_path):
                            os.remove(cert_path)
                        if key_path and os.path.exists(key_path):
                            os.remove(key_path)
                    except Exception:
                        pass

        if download == 1:
            # FileResponse precisa de um path real — aqui ele existe até sair do handler
            return FileResponse(
                zip_path,
                media_type="application/zip",
                filename=os.path.basename(zip_path),
            )

        return {"ok": True, "user": user, "zip": os.path.basename(zip_path)}


if __name__ == "__main__":
    uvicorn.run("fisconforme:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
