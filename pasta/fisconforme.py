import os          # <-- Faltando
import re
import base64
import tempfile
from datetime import date
from typing import Dict, Any, Optional, List, Tuple

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# =========================================================
# CONFIG SUPABASE
# =========================================================
SUPABASE_URL = "https://hysrxadnigzqadnlkynq.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh5c3J4YWRuaWd6cWFkbmxreW5xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM3MTQwODAsImV4cCI6MjA1OTI5MDA4MH0."
    "RLcu44IvY4X8PLK5BOa_FL5WQ0vJA3p0t80YsGQjTrA"
)

TABELA_CERTS = "certifica_dfe"


def supabase_headers(is_json: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if is_json:
        h["Content-Type"] = "application/json"
    return h


# =========================================================
# BUSCA CERTIFICADOS
# =========================================================
def carregar_certificados_validos(user_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Busca certificados na tabela certifica_dfe.
    Se user_filter for informado, filtra por campo "user" (e-mail).
    """
    url = f"{SUPABASE_URL}/rest/v1/{TABELA_CERTS}"
    params: Dict[str, str] = {
        'select': 'id,pem,key,empresa,codi,user,vencimento,"cnpj/cpf"'
    }
    if user_filter:
        params["user"] = f"eq.{user_filter}"

    print(f"🔎 Buscando certificados na certifica_dfe para user={user_filter}...")
    r = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    r.raise_for_status()
    certs = r.json() or []
    print(f"   ✔ {len(certs)} certificados encontrados.")
    return certs


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

    print(f"   ✔ Arquivos temporários de certificado criados: {cert_file.name}, {key_file.name}")
    return cert_file.name, key_file.name


# =========================================================
# SESSÃO HTTP COM CERTIFICADO
# =========================================================
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
# URLs DET / PORTAL
# =========================================================
URL_DET_HOME = "https://detsec.sefin.ro.gov.br/certificados"
URL_ENTRAR = "https://detsec.sefin.ro.gov.br/entrar"
URL_REDIRECT_PORTAL = "https://detsec.sefin.ro.gov.br/contribuinte/notificacoes/redirect_portal"
URL_PORTAL_HOME_DEFAULT = "https://portalcontribuinte.sefin.ro.gov.br/app/home/?exibir_modal=true"

# Consulta Débitos
URL_CONSULTA_DEBITOS = "https://portalcontribuinte.sefin.ro.gov.br/app/consultadebitos/"
URL_CONSULTA_DEBITOS_LISTA = "https://portalcontribuinte.sefin.ro.gov.br/app/consultadebitos/lista.jsp"


# =========================================================
# FLUXO: Acesso Digital -> Portal
# =========================================================
def abrir_acesso_digital_e_entrar(sess: requests.Session) -> bool:
    r = sess.get(URL_DET_HOME, timeout=30, allow_redirects=True)
    print(f"   [/certificados] {r.status_code} | {r.url}")
    if r.status_code != 200:
        return False

    soup = BeautifulSoup(r.text, "lxml")
    form = soup.find("form")
    if not form:
        print("   ❌ Form de Acesso Digital não encontrado.")
        return False

    action = form.get("action") or URL_ENTRAR
    if not action.startswith("http"):
        action = requests.compat.urljoin(URL_DET_HOME, action)

    print(f"   ➜ action Entrar: {action}")
    r_ent = sess.get(action, timeout=30, allow_redirects=True)
    print(f"   [/entrar] {r_ent.status_code} | {r_ent.url}")

    if r_ent.status_code != 200 or "/certificado/acessos" not in r_ent.url:
        print("   ❌ Não chegamos em /certificado/acessos.")
        return False

    print("   ✔ Tela de Outorgadores OK.")
    return True


def extrair_url_login_token(html: str) -> Optional[str]:
    m = re.search(r"https://portalcontribuinte\.sefin\.ro\.gov\.br[^\s\"']+", html)
    if m:
        return m.group(0)

    m = re.search(
        r"location\s*=\s*['\"](https://portalcontribuinte\.sefin\.ro\.gov\.br[^'\"]+)['\"]",
        html,
    )
    if m:
        return m.group(1)

    return None


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
                data[name] = value
            return action, data
    return None, None


def ir_para_portal_e_carregar_home(sess: requests.Session) -> Optional[str]:
    print("👉 redirect_portal ...")
    r_red = sess.get(URL_REDIRECT_PORTAL, timeout=30, allow_redirects=True)
    print(f"   [redirect_portal] {r_red.status_code} | {r_red.url}")
    if r_red.status_code != 200:
        return None

    # 1) tenta form auto-submit (LoginToken)
    action_form, data_form = extrair_form_logintoken(r_red.text)
    if action_form:
        print(f"   ✔ Form LoginToken encontrado: {action_form}")
        r_login = sess.post(action_form, data=data_form, timeout=30, allow_redirects=True)
        print(f"   [LoginToken_POST] {r_login.status_code} | {r_login.url}")

        if (
            r_login.status_code == 200
            and "portalcontribuinte.sefin.ro.gov.br" in r_login.url
            and "LoginToken" not in r_login.url
        ):
            print("   ✔ Home do Portal via POST LoginToken.")
            return r_login.text

        if (
            r_login.status_code == 200
            and "portalcontribuinte.sefin.ro.gov.br" in r_login.url
            and "LoginToken" in r_login.url
        ):
            print("   ℹ️ Ainda em LoginToken, tentando redirect JS...")
            next_url = extrair_redirect_do_logintoken(r_login.text) or URL_PORTAL_HOME_DEFAULT
            print(f"   ➜ Próxima URL Portal: {next_url}")
            r_home = sess.get(next_url, timeout=30, allow_redirects=True)
            print(f"   [Portal_2] {r_home.status_code} | {r_home.url}")
            if r_home.status_code == 200 and "portalcontribuinte.sefin.ro.gov.br" in r_home.url:
                print("   ✔ Home do Portal via JS LoginToken.")
                return r_home.text
            return None

    # 2) fallback: procurar URL direta no HTML
    portal_url = extrair_url_login_token(r_red.text) or URL_PORTAL_HOME_DEFAULT
    print(f"   ⚠️ Usando URL Portal: {portal_url}")
    r_portal = sess.get(portal_url, timeout=30, allow_redirects=True)
    print(f"   [Portal_1] {r_portal.status_code} | {r_portal.url}")

    if (
        r_portal.status_code == 200
        and "portalcontribuinte.sefin.ro.gov.br" in r_portal.url
        and "LoginToken" not in r_portal.url
    ):
        print("   ✔ Home do Portal (fallback direto).")
        return r_portal.text

    if (
        r_portal.status_code == 200
        and "portalcontribuinte.sefin.ro.gov.br" in r_portal.url
        and "LoginToken" in r_portal.url
    ):
        print("   ℹ️ Em LoginToken (fallback), tentando redirect JS...")
        next_url = extrair_redirect_do_logintoken(r_portal.text) or URL_PORTAL_HOME_DEFAULT
        print(f"   ➜ Próxima URL Portal: {next_url}")
        r_home = sess.get(next_url, timeout=30, allow_redirects=True)
        print(f"   [Portal_2] {r_home.status_code} | {r_home.url}")
        if r_home.status_code == 200 and "portalcontribuinte.sefin.ro.gov.br" in r_home.url:
            print("   ✔ Home do Portal via JS (fallback).")
            return r_home.text
        return None

    print("   ❌ Não foi possível chegar no Portal.")
    return None


# =========================================================
# FISCONFORME
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
                print("   ❌ Form FisConforme sem token.")
                return None
            if not action.startswith("http"):
                action = requests.compat.urljoin(URL_PORTAL_HOME_DEFAULT, action)
            print(f"   ✔ Form FisConforme: {action}")
            return action, token_val
    print("   ❌ Form FisConforme não encontrado no Portal.")
    return None


def acessar_fisconforme(sess: requests.Session, action_url: str, token: str) -> Optional[str]:
    data = {"token": token}
    r = sess.post(action_url, data=data, timeout=30, allow_redirects=True)
    print(f"   [FisConforme] {r.status_code} | {r.url}")
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

    rows = tbody.find_all("tr")
    pendencias: List[Dict[str, str]] = []

    for tr in rows:
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
# CONSULTA DÉBITOS – TABELA "DÉBITOS NA INSCRIÇÃO ESTADUAL"
# =========================================================
def obter_debitos_inscricao_estadual(html_deb: str) -> List[Dict[str, str]]:
    """
    Localiza a tabela 'Débitos na Inscrição Estadual' e retorna cada linha
    em um dicionário com chaves fixas.
    """
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
        print("   ❌ Tabela 'Débitos na Inscrição Estadual' não encontrada.")
        return []

    linhas = tabela_alvo.find_all("tr")
    # linha 0: título (colspan); linha 1: cabeçalhos; demais: dados
    if len(linhas) <= 2:
        return []

    debitos: List[Dict[str, str]] = []

    for tr in linhas[2:]:
        tds = tr.find_all("td")
        if len(tds) < 11:
            continue

        def txt(i: int) -> str:
            return tds[i].get_text(" ", strip=True) if i < len(tds) else ""

        debito = {
            "dare":             txt(0),
            "extrato":          txt(1),
            "nr_lancamento":    txt(2),
            "parcela":          txt(3),
            "referencia":       txt(4),
            "complemento":      txt(5),
            "receita":          txt(6),
            "situacao":         txt(7),
            "data_vencimento":  txt(8),
            "valor_lancamento": txt(9),
            "valor_atualizado": txt(10),
        }

        # ignora linha totalmente vazia
        if not any(debito.values()):
            continue

        debitos.append(debito)

    print(f"   ✔ {len(debitos)} débitos de inscrição estadual encontrados.")
    return debitos


def consultar_debitos(sess: requests.Session) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """
    Abre /app/consultadebitos/, escolhe a primeira IE e consulta o ano atual.
    Depois extrai a tabela 'Débitos na Inscrição Estadual'.
    """
    print("👉 Abrindo Consulta de Débitos...")
    r = sess.get(URL_CONSULTA_DEBITOS, timeout=30, allow_redirects=True)
    print(f"   [consultadebitos/] {r.status_code} | {r.url}")
    if r.status_code != 200:
        return [], f"Erro HTTP {r.status_code} ao abrir Consulta de Débitos"

    soup = BeautifulSoup(r.text, "lxml")
    sel_ie = soup.find("select", {"name": "inscricaoEstadual"})
    if not sel_ie:
        return [], "Campo inscrição estadual não encontrado na Consulta de Débitos"

    opt = sel_ie.find("option")
    if not opt or not opt.get("value"):
        return [], "Nenhuma inscrição estadual disponível na Consulta de Débitos"

    ie_val = opt["value"].strip()
    input_tipo = soup.find("input", {"name": "tipoDevedor"})
    tipo_devedor = input_tipo.get("value", "1") if input_tipo else "1"

    ano_atual = str(date.today().year)

    payload = {
        "inscricaoEstadual": ie_val,
        "ano": ano_atual,
        "tipoDevedor": tipo_devedor,
        "Submit": "Consultar Débitos",
    }

    print(f"   ➜ Consultando débitos IE={ie_val} ano={ano_atual} ...")
    r2 = sess.post(URL_CONSULTA_DEBITOS_LISTA, data=payload, timeout=30, allow_redirects=True)
    print(f"   [consultadebitos/lista.jsp] {r2.status_code} | {r2.url}")
    if r2.status_code != 200:
        return [], f"Erro HTTP {r2.status_code} ao consultar lista de débitos"

    debitos = obter_debitos_inscricao_estadual(r2.text)
    return debitos, None


# =========================================================
# FLUXO POR EMPRESA (API)
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

    print("\n========================================================")
    print(f"🏢 Iniciando FisConforme/Débitos para empresa: {empresa}")
    print(f"    user: {user} | doc: {doc} | venc: {venc}")
    print("========================================================")

    try:
        cert_path, key_path = criar_arquivos_cert_temp(cert_row)
        sess = criar_sessao(cert_path, key_path)

        # 1) Entrar no DET / Acesso Digital
        if not abrir_acesso_digital_e_entrar(sess):
            resultado["erro"] = "Falha ao entrar no Acesso Digital"
            return resultado

        # 2) Ir para o Portal
        html_portal = ir_para_portal_e_carregar_home(sess)
        if not html_portal:
            resultado["erro"] = "Falha ao abrir o Portal"
            return resultado

        # 3) FisConforme
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
        resultado["status_fisconforme"] = "com_pendencia" if pendencias else "sem_pendencia"

        # 4) Débitos
        debitos: List[Dict[str, str]] = []
        try:
            debitos, erro_deb = consultar_debitos(sess)
            if erro_deb:
                resultado["erro_debitos"] = erro_deb
        except Exception as e_deb:
            resultado["erro_debitos"] = f"Exceção na Consulta de Débitos: {e_deb}"

        resultado["debitos"] = debitos
        resultado["qtd_debitos"] = len(debitos)
        resultado["status_debitos"] = "com_debitos" if debitos else "sem_debitos"

        # 5) Situação combinada
        tem_pend_fis = len(pendencias) > 0
        tem_debitos = len(debitos) > 0
        tem_erro = bool(resultado["erro_fisconforme"] or resultado["erro_debitos"])

        if tem_erro and not (tem_pend_fis or tem_debitos):
            situacao = "erro"
        elif tem_pend_fis and tem_debitos:
            situacao = "pendencia_fis_e_debitos"
        elif tem_pend_fis:
            situacao = "pendencia_fis"
        elif tem_debitos:
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


# =========================================================
# FASTAPI
# =========================================================
app = FastAPI(title="API FisConforme + Débitos DET/SEFIN-RO")

# CORS para o HTML/JS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # depois você pode restringir para o domínio do Bubble
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/fisconforme")
def listar_fisconforme(
    user: str = Query(..., description="Campo user (e-mail) da tabela certifica_dfe")
):
    """
    Retorna a lista de empresas para o user informado, com:
    - pendências FisConforme
    - débitos em aberto
    - situação geral combinada
    """
    certs = carregar_certificados_validos(user_filter=user)
    if not certs:
        return {"ok": True, "user": user, "total_empresas": 0, "results": []}

    results: List[Dict[str, Any]] = []
    for cert_row in certs:
        res = fluxo_fisconforme_para_empresa_api(cert_row)
        results.append(res)

    return {
        "ok": True,
        "user": user,
        "total_empresas": len(results),
        "results": results,
    }

if __name__ == "__main__":
    uvicorn.run(
        "fisconforme:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,  # em produção deixa False
    )
