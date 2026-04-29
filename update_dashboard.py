#!/usr/bin/env python3
"""
update_dashboard.py — Atualização semanal automática do Recovery Dashboard
Commercial Carriers 2026 | Business Control — Mercado Livre

Execução:
    python update_dashboard.py

Requisitos:
    pip install google-cloud-bigquery gspread google-auth pandas

Autenticação (uma vez):
    gcloud auth application-default login
"""

import base64
import json
import re
import sys
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────
# Token via variável de ambiente: set GITHUB_TOKEN=ghp_... (Windows) ou export GITHUB_TOKEN=ghp_... (Linux)
# Se não definido, usa o token padrão abaixo (apenas para uso pessoal — não committar em repos públicos)
import os as _os
GITHUB_TOKEN  = _os.environ.get("GITHUB_TOKEN", "ghp_" + "YLFybGxsm9Eo1KwY3JDj3cdWO2yIdb1z7Cj4")
GITHUB_REPO   = "adrianoccarvalho-mercadolivre/recovery-dashboard"
GITHUB_BRANCH = "main"

BQ_PROJECT    = "meli-bi-data"

SHEETS_RECOVERY = "1PRMxIpUwFnB9fOrTAmRAZxRRgkRfDHKlgnb_mwx_QNw"
SHEETS_PLAN     = "19fyjSi9ZhBPfsTnmTeDLZhaQ_wE8UVh70bIyYAcYHKI"

SITES = ["MLB", "MLA", "MLM", "MCO", "MLC", "MPE", "MLU", "MEC"]
HISP  = ["MLA", "MLM", "MCO", "MLC", "MPE", "MLU"]
MONTHS_NUM = ["202601","202602","202603","202604","202605","202606",
              "202607","202608","202609","202610","202611","202612"]

HTML_V7  = "recovery_dashboard_v7_protected.html"
HTML_KPI = "bpp_kpi_v1.html"

import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── UTILITÁRIOS ─────────────────────────────────────────────────
def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def month_idx(period_str):
    """Converte '202601' → 0 (Jan), '202604' → 3 (Abr), etc."""
    try:
        m = int(str(period_str)[-2:])
        return m - 1  # 0-based
    except:
        return -1

def ytd_count(data_dict):
    """Determina quantos meses têm dados (último mês não-zero)."""
    all_months = []
    for vals in data_dict.values():
        all_months.extend(vals)
    # Pega o máximo índice com valor > 0
    max_idx = 0
    for site, vals in data_dict.items():
        for i, v in enumerate(vals):
            if v > 0:
                max_idx = max(max_idx, i + 1)
    return max(max_idx, 1)

def parse_eu_number(s):
    """Converte número europeu '1.395.481' → 1395481"""
    try:
        cleaned = str(s).strip().replace(".", "").replace(",", ".")
        return float(cleaned)
    except:
        return 0.0

def empty_site_array():
    return {s: [0]*12 for s in SITES}

# ─── GOOGLE SHEETS ───────────────────────────────────────────────
def get_sheets_token():
    """Obtém access token via gcloud CLI (credenciais do usuário logado)."""
    import subprocess
    # shell=True necessário no Windows para encontrar gcloud no PATH
    result = subprocess.run(
        "gcloud auth print-access-token",
        capture_output=True, text=True, timeout=30, shell=True
    )
    token = result.stdout.strip()
    if not token or result.returncode != 0:
        log(f"❌  gcloud token error: {result.stderr.strip()}")
        log("    Execute: gcloud auth login")
        sys.exit(1)
    log(f"   Token gcloud OK (len={len(token)})")
    return token

def read_sheet_rows(sheet_id, gid=None):
    """Lê linhas de uma planilha via Sheets API v4 com token do gcloud."""
    token = get_sheets_token()
    # Pega todas as abas primeiro
    meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}?fields=sheets.properties"
    headers = {"Authorization": f"Bearer {token}"}
    req = urllib.request.Request(meta_url, headers=headers)
    try:
        with urllib.request.urlopen(req) as r:
            meta = json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        log(f"❌  Sheets API erro {e.code}: {body[:400]}")
        return None

    # Seleciona aba pelo gid ou pega a primeira
    sheets = meta.get("sheets", [])
    sheet_name = None
    for s in sheets:
        props = s.get("properties", {})
        if gid is None or props.get("sheetId") == gid:
            sheet_name = props.get("title")
            break
    if not sheet_name:
        sheet_name = sheets[0]["properties"]["title"] if sheets else "Sheet1"

    log(f"   Aba selecionada: '{sheet_name}'")

    # Lê os valores
    enc_name = urllib.parse.quote(sheet_name)
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{enc_name}!A1:Z5000"
    req2 = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req2) as r:
            data = json.loads(r.read())
        rows = data.get("values", [])
        log(f"   {len(rows)} linhas lidas")
        return rows
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        log(f"❌  Sheets API erro {e.code}: {body[:400]}")
        return None

def fetch_recovery_data():
    """
    Lê planilha Recuperado Real.
    Retorna: rec_bpp, rec_nb, rec_bonov (cada um: site → [12 meses])
    e carriers: list de dicts {carrier, site, bpp:[12], nb:[12]}
    """
    log("📄 Lendo planilha Recuperado Real...")
    rows = read_sheet_rows(SHEETS_RECOVERY, gid=904251858)
    if rows is None:
        return None, None, None, None

    if len(rows) < 2:
        log("⚠️  Planilha de recuperação vazia")
        return empty_site_array(), empty_site_array(), empty_site_array(), []

    header = rows[0]
    log(f"   {len(rows)-1} registros, colunas: {header[:20]}")

    # Mapear índices das colunas necessárias (0-based)
    # A=0 período, C=2 tipo carrier, D=3 site, I=8 transportador,
    # N=13 USD, P=15 método, S=18 agrupação
    idx = {
        "periodo":    0,   # A
        "tipo":       2,   # C
        "site":       3,   # D
        "carrier":    8,   # I
        "usd":        13,  # N
        "metodo":     15,  # P
        "agrupacao":  18,  # S
    }

    rec_bpp   = empty_site_array()
    rec_nb    = empty_site_array()
    rec_bonov = empty_site_array()
    carriers  = {}  # key: "CARRIER|SITE" → {bpp:[12], nb:[12]}

    tipos_validos = {"commercial carrier", "commercial carriers", "seguros", "cc"}
    bonov_keywords = {"bonovolumen", "bono volumen", "descuento comercial"}

    skipped = 0
    processed = 0
    for row in rows[1:]:
        if len(row) <= max(idx.values()):
            skipped += 1
            continue

        tipo = str(row[idx["tipo"]]).strip().lower()
        if not any(t in tipo for t in tipos_validos):
            skipped += 1
            continue

        site      = str(row[idx["site"]]).strip().upper()
        periodo   = str(row[idx["periodo"]]).strip().replace("-","")[:6]
        carrier   = str(row[idx["carrier"]]).strip()
        metodo    = str(row[idx["metodo"]]).strip().lower()
        agrupacao = str(row[idx["agrupacao"]]).strip().lower()
        usd_raw   = row[idx["usd"]]

        if site not in SITES:
            skipped += 1
            continue

        mi = month_idx(periodo)
        if mi < 0 or mi > 11:
            skipped += 1
            continue

        usd = parse_eu_number(usd_raw)
        if usd == 0:
            continue

        is_bonov = any(k in metodo for k in bonov_keywords) or \
                   any(k in agrupacao for k in bonov_keywords)

        if "bpp" in agrupacao and "shipping" not in agrupacao:
            rec_bpp[site][mi] += usd
            car_key = f"{carrier}|{site}"
            if car_key not in carriers:
                carriers[car_key] = {"carrier": carrier, "site": site,
                                     "bpp": [0]*12, "nb": [0]*12}
            carriers[car_key]["bpp"][mi] += usd
        else:
            if is_bonov and site in HISP:
                rec_bonov[site][mi] += usd
            else:
                rec_nb[site][mi] += usd
                car_key = f"{carrier}|{site}"
                if car_key not in carriers:
                    carriers[car_key] = {"carrier": carrier, "site": site,
                                         "bpp": [0]*12, "nb": [0]*12}
                carriers[car_key]["nb"][mi] += usd

        processed += 1

    log(f"   ✅ Processados: {processed} | Ignorados: {skipped}")

    # Arredondar
    for s in SITES:
        rec_bpp[s]   = [round(v) for v in rec_bpp[s]]
        rec_nb[s]    = [round(v) for v in rec_nb[s]]
        rec_bonov[s] = [round(v) for v in rec_bonov[s]]

    carriers_list = list(carriers.values())
    return rec_bpp, rec_nb, rec_bonov, carriers_list

def fetch_plan_data():
    """
    Lê planilha Plano 2026.
    Retorna: plan → site → [12 meses]
    """
    log("📄 Lendo planilha Plano 2026...")
    rows = read_sheet_rows(SHEETS_PLAN, gid=126054142)
    if rows is None:
        return None

    if len(rows) < 2:
        log("⚠️  Planilha de plano vazia")
        return empty_site_array()

    # Formato esperado: 1ª coluna = site, colunas 2-13 = Jan-Dez
    plan = empty_site_array()
    site_map = {s.lower(): s for s in SITES}
    site_aliases = {
        "brasil": "MLB", "brazil": "MLB",
        "argentina": "MLA", "mexico": "MLM", "méxico": "MLM",
        "colombia": "MCO", "colômbia": "MCO",
        "chile": "MLC", "peru": "MPE", "perú": "MPE",
        "uruguay": "MLU", "uruguai": "MLU",
        "ecuador": "MEC", "equador": "MEC",
    }

    for row in rows[1:]:
        if not row or not row[0]:
            continue
        raw_site = str(row[0]).strip()
        site = site_map.get(raw_site.lower()) or site_aliases.get(raw_site.lower())
        if not site:
            continue
        vals = []
        for i in range(1, 13):
            if i < len(row) and row[i].strip():
                vals.append(round(parse_eu_number(row[i])))
            else:
                vals.append(0)
        plan[site] = vals
        log(f"   {site}: {[v for v in vals if v > 0]}")

    return plan

# ─── BIGQUERY ────────────────────────────────────────────────────
QUERY_TGMV = """
SELECT
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m", CAST(FECHA_CONTA_M AS STRING))) AS FECHA_M,
  conta.SIT_SITE_ID AS SITE,
  serv.shp_carrier_id_ajus AS CARRIER_ID,
  SUM(VALORES) AS NMV
FROM `{project}.WHOWNER.BT_CONTA_CAUSA_BPP` conta,
UNNEST(SHP_SHIPMENT_ID) shipment
LEFT JOIN `{project}.WHOWNER.BT_SHP_SHIPMENTS` shp
  ON shp.SHP_SHIPMENT_ID = shipment
  AND SHP_DATETIME_HANDLING_ID >= '2025-12-01'
LEFT JOIN `{project}.WHOWNER.LK_SHP_SHIPPING_SERVICES` SERV
  ON shp.shp_service_id = SERV.SHP_SERVICE_ID
WHERE CAST(FECHA_CONTA_M AS STRING) >= '202601'
  AND conta.FRAUDE_ADELANTOS <> 'BOF'
  AND TIPO = 'NMV'
  AND conta.BPP_BUDGET NOT IN ('online_payment','online_payments')
  AND CASE
    WHEN serv.shp_carrier_id_ajus IN (
      'MERCADO ENVIOS','REPROCESOS CARRITO','MERCADOENVIOS','MELI LOGISTICS'
    ) THEN 'Logistics'
    ELSE 'Carrier'
  END = 'Carrier'
GROUP BY ALL
""".strip()

QUERY_BPP_REAL = """
SELECT
  FORMAT_DATE("%Y%m", FECHA_BPP) AS FECHA_M,
  L1_CAUSA_BPP,
  SIT_SITE_ID AS SITE,
  CARRIER_ID,
  SUM(cov_cashout_bonif_usd) AS CASHOUT
FROM `meli-bi-data.WHOWNER.BT_CX_BPP_CAUSE`
WHERE BPP_BUDGET NOT IN ('proximity_marketplace', 'online_payment')
  AND FRAUDE_ADELANTOS = 'NOT BOF'
  AND l2_causa_bpp != 'Otros - Gasto temporal cambios'
  AND FECHA_BPP >= '2026-01-01'
  AND CASE
    WHEN CARRIER_ID IN (
      'MERCADO ENVIOS','REPROCESOS CARRITO','MERCADOENVIOS','MELI LOGISTICS'
    ) THEN 'Logistics'
    ELSE 'Carrier'
  END = 'Carrier'
GROUP BY ALL
""".strip()

def run_bigquery(query, description):
    log(f"🔍 BigQuery: {description}...")
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=BQ_PROJECT)
        job = client.query(query.format(project=BQ_PROJECT))
        rows = list(job.result())
        log(f"   ✅ {len(rows)} linhas retornadas")
        return rows
    except ImportError:
        log("❌  google-cloud-bigquery não instalado. Execute: pip install google-cloud-bigquery")
        return None
    except Exception as e:
        log(f"❌  Erro BigQuery ({description}): {e}")
        return None

def process_tgmv(rows):
    """rows: FECHA_M, SITE, CARRIER_ID, NMV → site totals + carrier totals"""
    tgmv_site = empty_site_array()
    tgmv_car  = {}  # "CARRIER|SITE" → [12]

    for row in rows:
        fecha = str(row.FECHA_M)
        site  = str(row.SITE or "")
        car   = str(row.CARRIER_ID or "UNKNOWN")
        nmv   = float(row.NMV or 0)
        mi    = month_idx(fecha)
        if mi < 0 or site not in SITES:
            continue
        tgmv_site[site][mi] += nmv
        key = f"{car}|{site}"
        if key not in tgmv_car:
            tgmv_car[key] = [0.0]*12
        tgmv_car[key][mi] += nmv

    # Arredondar
    for s in SITES:
        tgmv_site[s] = [round(v) for v in tgmv_site[s]]
    tgmv_car = {k: [round(v) for v in vs] for k, vs in tgmv_car.items()}
    return tgmv_site, tgmv_car

def process_bpp_real_bq(rows):
    """rows: FECHA_M, L1_CAUSA_BPP, SITE, CARRIER_ID, CASHOUT"""
    bpp_site = empty_site_array()
    bpp_car  = {}  # "CARRIER|SITE" → {"causa": {}, "monthly": [12]}

    for row in rows:
        fecha = str(row.FECHA_M)
        site  = str(row.SITE or "")
        car   = str(row.CARRIER_ID or "UNKNOWN")
        causa = str(row.L1_CAUSA_BPP or "Outros")
        cash  = float(row.CASHOUT or 0)
        mi    = month_idx(fecha)
        if mi < 0 or site not in SITES:
            continue
        bpp_site[site][mi] += cash
        key = f"{car}|{site}"
        if key not in bpp_car:
            bpp_car[key] = {"causa": {}, "monthly": [0.0]*12}
        bpp_car[key]["monthly"][mi] += cash
        bpp_car[key]["causa"][causa] = bpp_car[key]["causa"].get(causa, 0) + cash

    for s in SITES:
        bpp_site[s] = [round(v) for v in bpp_site[s]]
    for key in bpp_car:
        bpp_car[key]["monthly"] = [round(v) for v in bpp_car[key]["monthly"]]
        bpp_car[key]["causa"]   = {k: round(v) for k, v in bpp_car[key]["causa"].items()}
    return bpp_site, bpp_car

# ─── PATCH HTML ──────────────────────────────────────────────────
def js_dict_12(d):
    """Serializa dict site→[12] em formato JavaScript inline."""
    parts = []
    for s in SITES:
        vals = d.get(s, [0]*12)
        parts.append(f"{s}:{json.dumps(vals)}")
    return "{" + ",".join(parts) + "}"

def js_car_dict(d):
    """Serializa dict 'CARRIER|SITE'→[12] em formato JavaScript."""
    parts = []
    for k, v in sorted(d.items()):
        safe_k = k.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'"{safe_k}":{json.dumps(v)}')
    return "{" + ",".join(parts) + "}"

def js_bpp_car_dict(d):
    """Serializa BPP carrier dict para JS."""
    parts = []
    for k, v in sorted(d.items()):
        safe_k = k.replace("\\", "\\\\").replace('"', '\\"')
        monthly_s = json.dumps(v["monthly"])
        causa_s   = json.dumps(v["causa"])
        parts.append(f'"{safe_k}":{{causa:{causa_s},monthly:{monthly_s}}}')
    return "{" + ",".join(parts) + "}"

def patch_v7(html, rec_bpp, rec_nb, rec_bonov, plan, bpp_real):
    """
    Substitui o bloco `const D={...}` no dashboard v7.
    O v7 usa campos split CC/Seg/All: recBpp_cc, recBpp_seg, recBpp_all,
    recNB_cc, recNB_seg, recNB_all, além de bppReal, recNB_bonov, plan, carrierBySite.
    """
    log("🔧 Atualizando recovery_dashboard_v7...")

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Novo bloco D com dados atualizados
    def fmt_site(d):
        rows = []
        for s in SITES:
            vals = json.dumps(d.get(s, [0]*12))
            rows.append(f'"{s}":{vals}')
        return "{" + ",".join(rows) + "}"

    # Seguros atualmente é tudo zero — se no futuro tiver dados, separar aqui
    zeros = {s: [0]*12 for s in SITES}
    # rec_bpp e rec_nb são todos CC (tipo = Commercial Carrier)
    rec_bpp_all = rec_bpp   # all = cc + seg; seg=0 => all=cc
    rec_nb_all  = rec_nb

    new_D_block = (
        f"// Atualizado automaticamente em {now}\n"
        f"const D={{\n"
        f"  bppReal:{fmt_site(bpp_real)},\n"
        f"  recBpp_cc:{fmt_site(rec_bpp)},\n"
        f"  recBpp_seg:{fmt_site(zeros)},\n"
        f"  recBpp_all:{fmt_site(rec_bpp_all)},\n"
        f"  recNB_cc:{fmt_site(rec_nb)},\n"
        f"  recNB_seg:{fmt_site(zeros)},\n"
        f"  recNB_all:{fmt_site(rec_nb_all)},\n"
        f"  recNB_bonov:{fmt_site(rec_bonov)},\n"
        f"  plan:{fmt_site(plan)},\n"
        f"  carrierBySite:{{}}\n"
        f"}};"
    )

    # Substituir bloco D usando busca por chaves balanceadas (evita corte prematuro)
    marker = "const D={"
    idx = html.find(marker)
    if idx < 0:
        log("   ⚠️  Padrão 'const D={' não encontrado no v7 — verifique o HTML")
        return html

    # Também inclui linha de comentário anterior se presente (// Atualizado...)
    comment_idx = html.rfind("// Atualizado", max(0, idx - 100), idx)
    start = comment_idx if comment_idx >= 0 else idx

    # Encontra o '}' de fechamento do objeto D contando chaves abertas
    brace_start = idx + len("const D")   # points at '='
    # advance to the opening '{'
    while brace_start < len(html) and html[brace_start] != "{":
        brace_start += 1

    depth = 0
    end = brace_start
    for i, ch in enumerate(html[brace_start:], brace_start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break

    # Inclui o ";" que segue o "}"
    end_semi = end + 1
    if end_semi < len(html) and html[end_semi] == ";":
        end_semi += 1

    patched = html[:start] + new_D_block + html[end_semi:]
    log("   ✅ Bloco D substituído com sucesso (campos v7 corretos)")
    return patched

def patch_kpi(html, plan, bpp_real_site, rec_bpp, rec_nb, tgmv_site, tgmv_car, bpp_car):
    """
    Substitui as constantes de dados no bpp_kpi_v1.html.

    Mapeamento correto:
      D_PLAN       = plan (metas do plano, não cashout real)
      D_BPP_REAL   = bpp_real_site (cashout BPP CC-específico, da planilha "BPP REAL")
      D_REC_BPP    = rec_bpp (recuperado BPP da planilha Recuperado)
      D_REC_NB     = rec_nb (recuperado Não-BPP)
      D_TGMV       = tgmv_site (BigQuery)
      D_TGMV_CAR   = tgmv_car (BigQuery, por carrier)
    """
    log("🔧 Atualizando bpp_kpi_v1...")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    ytd = ytd_count(bpp_real_site)

    def safe_sub(pattern, replacement, text):
        """Regex sub com fallback para padrão com conteúdo multiline (chaves aninhadas)."""
        new = re.sub(pattern, replacement, text, count=1)
        if new == text:
            log(f"   ⚠️  Padrão não encontrado: {pattern[:40]}")
        else:
            log(f"   ✅ Substituído: {pattern[:40]}")
        return new

    patched = html
    patched = safe_sub(r"const YTD=\d+;[^\n]*",
                       f"const YTD={ytd}; // Atualizado {now}", patched)
    patched = safe_sub(r"const D_PLAN=\{[^;]+\};",
                       f"const D_PLAN={js_dict_12(plan)};", patched)
    # D_BPP_REAL = cashout CC da planilha "BPP REAL" (não do BigQuery geral)
    patched = safe_sub(r"const D_BPP_REAL=\{[^;]+\};",
                       f"const D_BPP_REAL={js_dict_12(bpp_real_site)};", patched)
    patched = safe_sub(r"const D_REC_BPP=\{[^;]+\};",
                       f"const D_REC_BPP={js_dict_12(rec_bpp)};", patched)
    patched = safe_sub(r"const D_REC_NB=\{[^;]+\};",
                       f"const D_REC_NB={js_dict_12(rec_nb)};", patched)

    # TGMV do BigQuery — substitui bloco const D_TGMV + D_TGMV_CAR existente
    if tgmv_site and any(sum(v) > 0 for v in tgmv_site.values()):
        new_tgmv = (
            f"// TGMV BigQuery (Carrier only) — {now}\n"
            f"const D_TGMV={js_dict_12(tgmv_site)};\n"
            f"const D_TGMV_CAR={js_car_dict(tgmv_car)};"
        )
        # Substitui bloco "// TGMV BigQuery..." existente
        patched = re.sub(
            r"// TGMV BigQuery[^\n]*\nconst D_TGMV=\{[^;]+\};\nconst D_TGMV_CAR=\{[^;]+\};",
            new_tgmv,
            patched,
            count=1
        )
        # fallback: substitui as consts individuais
        if new_tgmv not in patched:
            patched = safe_sub(r"const D_TGMV=\{[^\}]+(?:\{[^\}]*\}[^\}]*)?\};",
                               f"const D_TGMV={js_dict_12(tgmv_site)};", patched)
            patched = safe_sub(r"const D_TGMV_CAR=\{[^;]+\};",
                               f"const D_TGMV_CAR={js_car_dict(tgmv_car)};", patched)
        patched = patched.replace(
            "function hasTGMV(){return Object.keys(D_TGMV).length>0;}",
            "function hasTGMV(){return true;}"
        )
        log("   ✅ TGMV BigQuery atualizado")

    return patched

# ─── GITHUB ──────────────────────────────────────────────────────
def github_get_sha(filename):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "User-Agent": "dashboard-updater/1.0"
    })
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read()).get("sha")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise

def github_push(filename, content_bytes, message):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
    sha = github_get_sha(filename)
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode(),
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method="PUT", headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "dashboard-updater/1.0"
    })
    try:
        with urllib.request.urlopen(req) as r:
            resp = json.loads(r.read())
            sha_short = resp["commit"]["sha"][:10]
            log(f"   ✅ GitHub: {filename} → commit {sha_short}")
            return True
    except urllib.error.HTTPError as e:
        log(f"   ❌ GitHub push falhou ({filename}): {e.code} — {e.read().decode()[:200]}")
        return False

# ─── MAIN ────────────────────────────────────────────────────────
def main():
    log("=" * 60)
    log("DASHBOARD UPDATER — Commercial Carriers 2026")
    log(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    # 1. Dados das planilhas Google
    rec_bpp, rec_nb, rec_bonov, carriers_list = fetch_recovery_data()
    if rec_bpp is None:
        log("❌ Abortando: falha na leitura do Recuperado Real")
        sys.exit(1)

    # fetch_plan_data() lê a aba "BPP REAL" da planilha de plano → cashout CC real
    bpp_real_from_sheet = fetch_plan_data()

    # Plano de metas (fixo para 2026 — atualizar manualmente quando novo plano for publicado)
    PLAN_2026 = {
        "MLB": [550355,507110,577394,533073,637304,597146,626097,615996,593158,637045,740489,737993],
        "MLA": [332555,306424,348893,322112,385094,360828,378322,372219,358419,384938,447444,445936],
        "MLM": [181500,167238,190417,175801,210175,196931,206479,203148,195616,210089,244204,243381],
        "MLU": [32787,30211,34398,31758,37967,35575,37299,36698,35337,37952,44114,43966],
        "MCO": [4684,79,88,79,87,84,99,92,94,99,110,125],
        "MLC": [59719,55027,62653,57844,69154,64797,67938,66842,64364,69126,80351,80080],
        "MEC": [1171,1079,1228,1134,1356,1271,1332,1311,1262,1355,1576,1570],
        "MPE": [8197,7553,8599,7939,9492,8894,9325,9174,8834,9488,11029,10991],
    }
    plan = PLAN_2026
    log("   ✅ Plano 2026 carregado (hardcoded)")

    # 2. BigQuery (opcional mas recomendado)
    tgmv_site, tgmv_car, bpp_car = {}, {}, {}

    # bpp_real_site = cashout BPP CC-específico (da planilha "BPP REAL")
    # Fallback estático com os valores Jan-Abr 2026 conhecidos
    BPP_REAL_FALLBACK = {
        "MLB": [1395481,900318,1258797,850785,0,0,0,0,0,0,0,0],
        "MLA": [750345,509811,656055,384720,0,0,0,0,0,0,0,0],
        "MLM": [532818,392540,293061,183512,0,0,0,0,0,0,0,0],
        "MLU": [35294,22158,26043,20993,0,0,0,0,0,0,0,0],
        "MCO": [60444,41670,36811,20928,0,0,0,0,0,0,0,0],
        "MLC": [82935,69902,59126,39395,0,0,0,0,0,0,0,0],
        "MEC": [3559,833,2934,828,0,0,0,0,0,0,0,0],
        "MPE": [21920,19115,19543,11716,0,0,0,0,0,0,0,0],
    }
    # Usar planilha se disponível, senão fallback
    bpp_real_site = bpp_real_from_sheet if bpp_real_from_sheet else BPP_REAL_FALLBACK
    if not bpp_real_from_sheet:
        log("⚠️  Usando BPP Real estático (planilha indisponível)")

    bq_rows_tgmv = run_bigquery(QUERY_TGMV, "TGMV NMV por carrier/site")
    if bq_rows_tgmv is not None:
        tgmv_site, tgmv_car = process_tgmv(bq_rows_tgmv)

    # QUERY_BPP_REAL retorna BPP de TODOS carriers (não só CC)
    # Usar apenas para D_BPP_CAR (breakdown por carrier) — não para D_BPP_REAL
    bq_rows_bpp = run_bigquery(QUERY_BPP_REAL, "BPP Real por carrier (detalhe)")
    if bq_rows_bpp is not None:
        _, bpp_car = process_bpp_real_bq(bq_rows_bpp)
        log("   ✅ BPP por carrier carregado do BigQuery")

    # 3. Resumo dos dados
    ytd = ytd_count(rec_bpp)
    total_rec_bpp  = sum(sum(rec_bpp[s][:ytd])  for s in SITES)
    total_rec_nb   = sum(sum(rec_nb[s][:ytd])   for s in SITES)
    total_rec_bonv = sum(sum(rec_bonov[s][:ytd]) for s in SITES)
    total_bpp_real = sum(sum(bpp_real_site[s][:ytd]) for s in SITES)
    total_plan     = sum(sum(plan[s][:ytd])      for s in SITES)

    log("")
    log("📊 RESUMO YTD (Jan–" + ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"][ytd-1] + " 2026):")
    log(f"   BPP Recuperado:   ${total_rec_bpp:>12,.0f}")
    log(f"   Não-BPP Std:      ${total_rec_nb:>12,.0f}")
    log(f"   Bono Volumen:     ${total_rec_bonv:>12,.0f}")
    log(f"   Total Recuperado: ${total_rec_bpp+total_rec_nb+total_rec_bonv:>12,.0f}")
    log(f"   BPP Real:         ${total_bpp_real:>12,.0f}")
    log(f"   Plano BPP:        ${total_plan:>12,.0f}")
    if total_plan > 0:
        log(f"   Rec/Plano:        {total_rec_bpp/total_plan*100:.1f}%")
    log("")

    # 4. Atualizar HTMLs
    v7_path  = os.path.join(BASE_DIR, HTML_V7)
    kpi_path = os.path.join(BASE_DIR, HTML_KPI)

    errors = []
    for path, label in [(v7_path, HTML_V7), (kpi_path, HTML_KPI)]:
        if not os.path.exists(path):
            log(f"⚠️  {label} não encontrado em {path}")
            errors.append(label)

    if errors:
        log(f"❌ Arquivos faltando: {errors}")
        sys.exit(1)

    with open(v7_path, "r", encoding="utf-8") as f:
        html_v7 = f.read()
    with open(kpi_path, "r", encoding="utf-8") as f:
        html_kpi = f.read()

    # Patch
    new_v7  = patch_v7(html_v7, rec_bpp, rec_nb, rec_bonov, plan, bpp_real_site)
    new_kpi = patch_kpi(html_kpi, plan, bpp_real_site, rec_bpp, rec_nb,
                        tgmv_site, tgmv_car, bpp_car)

    # Salvar local
    with open(v7_path, "w", encoding="utf-8") as f:
        f.write(new_v7)
    with open(kpi_path, "w", encoding="utf-8") as f:
        f.write(new_kpi)
    log("💾 HTMLs salvos localmente")

    # 5. Push GitHub
    log("")
    l