"""
Passo 4: transforma os relatórios JSON (docs/dados/) em páginas HTML (docs/).

Gera:
  docs/index.html            -> relatório mais recente
  docs/dia/AAAA-MM-DD.html   -> uma página por dia
  docs/arquivo.html          -> lista de todos os dias
  docs/.nojekyll             -> evita processamento extra do GitHub Pages

Todo texto vindo do JSON é "escapado" (html.escape), então nenhum conteúdo externo
consegue injetar código na página.
"""

import html
import json
import re
import sys
from pathlib import Path

PASTA_DOCS = Path("docs")
PASTA_DADOS = PASTA_DOCS / "dados"
TITULO_SITE = "Conjuntura Dialética"
SUBTITULO_SITE = "Notícias do mundo lidas pela economia política e pelas relações de trabalho"

MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
         "agosto", "setembro", "outubro", "novembro", "dezembro"]

CSS = """
:root{--bg:#f6f4ef;--card:#ffffff;--texto:#1d1d1f;--suave:#5c5c63;--linha:#e3e0d8;--destaque:#a3191c;
--alta:#1f7a3d;--media:#a66a00;--baixa:#a3191c;--chip:#efece4}
@media (prefers-color-scheme:dark){:root{--bg:#141416;--card:#1d1d21;--texto:#ececef;--suave:#a0a0a8;
--linha:#2e2e34;--destaque:#ff6b6e;--alta:#5fd08a;--media:#f0b84d;--baixa:#ff6b6e;--chip:#2a2a30}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--texto);font:17px/1.6 Georgia,'Times New Roman',serif}
a{color:var(--destaque)}
.wrap{max-width:860px;margin:0 auto;padding:0 18px 60px}
header.topo{padding:28px 0 18px;border-bottom:3px double var(--linha);margin-bottom:22px}
header.topo h1{margin:0;font-size:1.9rem;letter-spacing:.5px}
header.topo p{margin:4px 0 0;color:var(--suave);font-size:.95rem}
nav.menu{margin-top:12px;font:600 .9rem system-ui,sans-serif}
nav.menu a{margin-right:16px}
.data{font:600 .85rem system-ui,sans-serif;color:var(--suave);text-transform:uppercase;letter-spacing:1px}
h2.manchete{font-size:1.7rem;line-height:1.25;margin:6px 0 10px}
.resumo{font-size:1.05rem}
.aviso{font:.82rem/1.5 system-ui,sans-serif;color:var(--suave);background:var(--chip);
border-radius:8px;padding:10px 14px;margin:18px 0}
.filtros{display:flex;flex-wrap:wrap;gap:8px;margin:18px 0}
.filtros button{font:600 .82rem system-ui,sans-serif;border:1px solid var(--linha);background:var(--card);
color:var(--texto);padding:6px 12px;border-radius:999px;cursor:pointer}
.filtros button.ativo{background:var(--destaque);border-color:var(--destaque);color:#fff}
article.item{background:var(--card);border:1px solid var(--linha);border-radius:12px;padding:20px 22px;margin:18px 0}
.meta{display:flex;flex-wrap:wrap;gap:8px;align-items:center;font:600 .78rem system-ui,sans-serif;margin-bottom:6px}
.tag{background:var(--chip);padding:3px 10px;border-radius:999px;text-transform:uppercase;letter-spacing:.6px}
.conf{padding:3px 10px;border-radius:999px;border:1px solid currentColor}
.conf.alta{color:var(--alta)}.conf.media{color:var(--media)}.conf.baixa{color:var(--baixa)}
article.item h3{margin:4px 0 12px;font-size:1.3rem;line-height:1.3}
h4{font:700 .8rem system-ui,sans-serif;text-transform:uppercase;letter-spacing:1px;color:var(--suave);margin:18px 0 4px}
ul{margin:4px 0 4px 20px;padding:0}
blockquote{margin:14px 0;padding:6px 16px;border-left:4px solid var(--destaque);font-style:italic}
blockquote small{display:block;font:normal .8rem system-ui,sans-serif;color:var(--suave);margin-top:4px}
details{border-top:1px solid var(--linha);margin-top:12px;padding-top:8px}
summary{cursor:pointer;font:700 .85rem system-ui,sans-serif;color:var(--suave);text-transform:uppercase;letter-spacing:.8px}
table{width:100%;border-collapse:collapse;font:.85rem system-ui,sans-serif;margin-top:8px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--linha);vertical-align:top}
.fonte{font:.85rem/1.5 system-ui,sans-serif;margin:6px 0}
.fonte span{color:var(--suave)}
.limite{font:.85rem/1.5 system-ui,sans-serif;color:var(--suave)}
.chk{padding:3px 10px;border-radius:999px;background:var(--chip)}
.chk.corroborado{color:var(--alta)}.chk.parcial{color:var(--media)}.chk.divergente{color:var(--baixa)}
.contradicao{border-left:4px solid var(--destaque);background:var(--chip);padding:8px 14px;border-radius:0 8px 8px 0;margin:14px 0}
.contradicao b{font:700 .75rem system-ui,sans-serif;text-transform:uppercase;letter-spacing:1px;color:var(--suave);display:block}
.teste{background:var(--baixa);color:#fff;font:600 .85rem system-ui,sans-serif;padding:8px 14px;border-radius:8px;margin:14px 0}
footer{font:.8rem/1.5 system-ui,sans-serif;color:var(--suave);border-top:1px solid var(--linha);padding-top:14px;margin-top:30px}
ul.arquivo{list-style:none;margin:0;padding:0}
ul.arquivo li{padding:12px 0;border-bottom:1px solid var(--linha)}
"""

JS_FILTRO = """
(function(){
  var cards=[].slice.call(document.querySelectorAll('article.item'));
  var box=document.getElementById('filtros');
  if(!box||cards.length<2){return;}
  var regs=[];
  cards.forEach(function(c){var r=c.getAttribute('data-regiao');if(regs.indexOf(r)<0){regs.push(r);}});
  function aplicar(r,btn){
    cards.forEach(function(c){c.style.display=(r===''||c.getAttribute('data-regiao')===r)?'':'none';});
    [].slice.call(box.children).forEach(function(b){b.className='';});
    btn.className='ativo';
  }
  function criar(txt,r){
    var b=document.createElement('button');b.textContent=txt;
    b.onclick=function(){aplicar(r,b);};box.appendChild(b);return b;
  }
  criar('Todas','').className='ativo';
  regs.forEach(function(r){criar(r,r);});
})();
"""


# ------------------------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------------------------
def e(valor):
    """Escapa texto para HTML."""
    return html.escape(str(valor if valor is not None else ""), quote=True)


def url_segura(url):
    url = str(url or "")
    return url if url.startswith(("http://", "https://")) else "#"


def como_lista(valor):
    if isinstance(valor, list):
        return [str(v) for v in valor if str(v).strip()]
    if valor:
        return [str(valor)]
    return []


def data_extenso(iso):
    try:
        ano, mes, dia = iso.split("-")
        return f"{int(dia)} de {MESES[int(mes) - 1]} de {ano}"
    except (ValueError, IndexError):
        return str(iso)


def fmt_num(v, casas=1):
    s = f"{v:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_dado(reg):
    """Formata o valor de um indicador do Banco Mundial (texto simples, sem escape)."""
    v, cod = reg.get("valor"), str(reg.get("indicador", ""))
    if not isinstance(v, (int, float)):
        return str(v)
    if cod == "NY.GDP.MKTP.CD":
        if abs(v) >= 1e12:
            return f"US$ {fmt_num(v / 1e12, 2)} tri"
        if abs(v) >= 1e9:
            return f"US$ {fmt_num(v / 1e9, 1)} bi"
        return f"US$ {fmt_num(v / 1e6, 1)} mi"
    if cod == "SL.TLF.TOTL.IN":
        return f"{fmt_num(v / 1e6, 1)} milhões" if v >= 1e6 else fmt_num(v, 0)
    if cod.endswith(".ZS"):
        return f"{fmt_num(v, 2)}%"
    return fmt_num(v, 1)


def secao(titulo, corpo, aberta=False):
    if not corpo:
        return ""
    if aberta:
        return f"<h4>{e(titulo)}</h4>{corpo}"
    return f"<details><summary>{e(titulo)}</summary>{corpo}</details>"


# ------------------------------------------------------------------------------
# Blocos da página
# ------------------------------------------------------------------------------
def render_item(item):
    conf = item.get("confianca", "baixa")
    classe_conf = {"alta": "alta", "média": "media"}.get(conf, "baixa")
    paises = ", ".join(como_lista(item.get("paises_iso3")))
    status_chk = item.get("status_checagem", "não verificado")
    classe_chk = {"corroborado": "corroborado", "parcialmente corroborado": "parcial", "divergente": "divergente"}.get(status_chk, "")

    partes = [
        f'<article class="item" data-regiao="{e(item.get("regiao", "Global"))}">',
        '<div class="meta">'
        f'<span class="tag">{e(item.get("regiao", "Global"))}</span>'
        f'<span class="conf {classe_conf}">Confiança {e(conf)}</span>'
        f'<span class="chk {classe_chk}">Checagem: {e(status_chk)}</span>'
        + ('<span class="chk">Fonte única</span>' if len(item.get("fontes", [])) == 1 else "")
        + (f'<span>{e(paises)}</span>' if paises else "")
        + "</div>",
        f'<h3>{e(item.get("titulo", "Sem título"))}</h3>',
    ]

    fatos = como_lista(item.get("fatos_verificaveis"))
    if fatos:
        partes.append(secao("Fatos", "<ul>" + "".join(f"<li>{e(f)}</li>" for f in fatos) + "</ul>", aberta=True))

    if item.get("checagem_resumo"):
        partes.append(secao("Checagem independente", f"<p>{e(item['checagem_resumo'])}</p>", aberta=True))

    if item.get("enquadramentos_comparados"):
        partes.append(secao("Como as fontes enquadram o fato", f"<p>{e(item['enquadramentos_comparados'])}</p>", aberta=True))

    if item.get("citacao"):
        fonte_cit = next((f for f in item.get("fontes", []) if f.get("id") == item.get("citacao_fonte_id")), None)
        selo = "✓ trecho conferido no texto original" if item.get("citacao_verificada") else \
               "⚠ trecho não localizado no texto original: leia a matéria"
        origem = ""
        if fonte_cit:
            origem = f' — <a href="{e(url_segura(fonte_cit.get("url")))}" target="_blank" rel="noopener noreferrer">{e(fonte_cit.get("nome"))}</a>'
        partes.append(f'<blockquote>“{e(item["citacao"])}”<small>{selo}{origem}</small></blockquote>')

    if item.get("contradicao_principal"):
        partes.append(f'<div class="contradicao"><b>Contradição principal</b>{e(item["contradicao_principal"])}</div>')

    if item.get("analise_materialista"):
        partes.append(secao("Análise materialista", f"<p>{e(item['analise_materialista'])}</p>", aberta=True))

    partes.append(secao("Contexto histórico", f"<p>{e(item['contexto_historico'])}</p>" if item.get("contexto_historico") else ""))
    partes.append(secao("Trabalho e relações de produção", f"<p>{e(item['relacoes_de_trabalho'])}</p>" if item.get("relacoes_de_trabalho") else ""))

    dados = item.get("dados", [])
    if dados:
        linhas = "".join(
            f"<tr><td>{e(d.get('pais'))}</td><td>{e(d.get('nome_indicador'))}</td>"
            f"<td><strong>{e(fmt_dado(d))}</strong></td><td>{e(d.get('ano'))}</td>"
            f"<td>{e(d.get('fonte'))}</td></tr>"
            for d in dados
        )
        partes.append(secao(
            "Dados oficiais",
            "<table><tr><th>País</th><th>Indicador</th><th>Valor</th><th>Ano</th><th>Fonte</th></tr>" + linhas + "</table>",
        ))

    partes.append(secao("Explicações alternativas", f"<p>{e(item['explicacoes_alternativas'])}</p>" if item.get("explicacoes_alternativas") else ""))

    nums = como_lista(item.get("numeros_nao_verificados"))
    if nums:
        partes.append('<div class="aviso">⚠ Números no texto sem origem localizada nas fontes nem nos dados oficiais: '
                      + e(", ".join(nums)) + '. Trate-os com cautela.</div>')

    hip = como_lista(item.get("hipoteses"))
    if hip:
        partes.append(secao("Hipóteses (sugeridas pelos fatos, não provadas)", "<ul>" + "".join(f"<li>{e(h)}</li>" for h in hip) + "</ul>"))

    if item.get("limites_da_evidencia"):
        partes.append(secao("Limites da evidência", f'<p class="limite">{e(item["limites_da_evidencia"])}</p>'))

    notas = como_lista(item.get("revisao_notas"))
    if notas:
        partes.append(secao("Notas da revisão crítica", "<ul>" + "".join(f"<li>{e(n)}</li>" for n in notas) + "</ul>"))

    fontes = item.get("fontes", [])
    if fontes:
        blocos = "".join(
            f'<div class="fonte"><a href="{e(url_segura(f.get("url")))}" target="_blank" rel="noopener noreferrer">'
            f'{e(f.get("titulo_original"))}</a><br>'
            f'<span><strong>{e(f.get("nome"))}</strong> · {e(f.get("campo"))}'
            + (f' · publicado em {e(f.get("publicado"))}' if f.get("publicado") else "")
            + f'<br>{e(f.get("perfil_editorial"))}</span></div>'
            for f in fontes
        )
        web = "".join(
            f'<li><a href="{e(url_segura(w.get("url")))}" target="_blank" rel="noopener noreferrer">{e(w.get("titulo"))}</a></li>'
            for w in item.get("pesquisa_web_fontes", []) if isinstance(w, dict)
        )
        if web:
            blocos += '<h4>Consultadas na verificação independente</h4><ul>' + web + '</ul>'
        partes.append(secao("Fontes e perfil editorial", blocos))

    partes.append("</article>")
    return "\n".join(p for p in partes if p)


def moldura(titulo_pagina, corpo, base, extra_js=""):
    tag_script = "<script>" + extra_js + "</script>" if extra_js else ""
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(titulo_pagina)}</title>
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
<header class="topo">
<h1>☭ {e(TITULO_SITE)}</h1>
<p>{e(SUBTITULO_SITE)}</p>
<nav class="menu"><a href="{base}index.html">Hoje</a><a href="{base}arquivo.html">Arquivo</a></nav>
</header>
{corpo}
</div>
{tag_script}
</body>
</html>"""


def render_relatorio(rel, base=""):
    itens = "\n".join(render_item(i) for i in rel.get("itens", []))
    aviso_teste = '<div class="teste">Relatório de TESTE (modo rápido): análise resumida, sem verificação na web nem revisão crítica.</div>' \
        if rel.get("modo") == "rapido" else ""
    avisos = como_lista(rel.get("avisos_execucao"))
    aviso_exec = ('<div class="teste">Limitações desta execução:<ul>' + "".join(f"<li>{e(a)}</li>" for a in avisos) + "</ul></div>") if avisos else ""
    detalhes = []
    if rel.get("duracao_minutos"):
        detalhes.append(f"Processamento: {rel['duracao_minutos']} min")
    if rel.get("textos_integrais_lidos"):
        detalhes.append(f"{rel['textos_integrais_lidos']} matérias lidas na íntegra")
    if rel.get("fatos_verificados_na_web"):
        detalhes.append(f"{rel['fatos_verificados_na_web']} fatos verificados na web")
    detalhes_txt = (" " + " · ".join(detalhes) + ".") if detalhes else ""
    corpo = f"""
<div class="data">{e(data_extenso(rel.get("data", "")))}</div>
<h2 class="manchete">{e(rel.get("manchete_do_dia", ""))}</h2>
<p class="resumo">{e(rel.get("resumo_executivo", ""))}</p>
{aviso_teste}
{aviso_exec}
<div class="aviso">{e(rel.get("aviso_metodologico", ""))}</div>
<div class="filtros" id="filtros"></div>
{itens}
<footer>Gerado automaticamente em {e(rel.get("gerado_em", ""))} com o modelo {e(rel.get("modelo", ""))}.
{e(rel.get("total_noticias_coletadas", ""))} notícias coletadas.{e(detalhes_txt)} Nenhuma fonte é neutra: confira sempre as matérias originais.</footer>
"""
    return moldura(f"{TITULO_SITE} — {data_extenso(rel.get('data', ''))}", corpo, base, JS_FILTRO)


def render_arquivo(relatorios):
    itens = "".join(
        f'<li><a href="dia/{e(r["data"])}.html"><strong>{e(data_extenso(r["data"]))}</strong></a><br>'
        f'{e(r.get("manchete_do_dia", ""))}</li>'
        for r in reversed(relatorios)
    )
    corpo = f'<h2 class="manchete">Arquivo</h2><ul class="arquivo">{itens}</ul>'
    return moldura(f"{TITULO_SITE} — Arquivo", corpo, "")


# ------------------------------------------------------------------------------
# Execução
# ------------------------------------------------------------------------------
def carregar_relatorios():
    relatorios = []
    for caminho in sorted(PASTA_DADOS.glob("*.json")):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.json", caminho.name):
            continue
        try:
            rel = json.loads(caminho.read_text(encoding="utf-8"))
            rel.setdefault("data", caminho.stem)
            relatorios.append(rel)
        except (json.JSONDecodeError, OSError):
            print(f"⚠️ Ignorando arquivo inválido: {caminho.name}")
    return relatorios


def main():
    relatorios = carregar_relatorios()
    if not relatorios:
        print("❌ Nenhum relatório encontrado em docs/dados/. Rode primeiro: python relatorio_geopolitico.py")
        return 1

    (PASTA_DOCS / "dia").mkdir(parents=True, exist_ok=True)
    (PASTA_DOCS / ".nojekyll").write_text("", encoding="utf-8")

    for rel in relatorios:
        (PASTA_DOCS / "dia" / f"{rel['data']}.html").write_text(render_relatorio(rel, base="../"), encoding="utf-8")

    (PASTA_DOCS / "index.html").write_text(render_relatorio(relatorios[-1], base=""), encoding="utf-8")
    (PASTA_DOCS / "arquivo.html").write_text(render_arquivo(relatorios), encoding="utf-8")

    print(f"✓ Site gerado: {len(relatorios)} dia(s). Abra docs/index.html no navegador.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
