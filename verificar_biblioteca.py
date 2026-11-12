"""
Confere a biblioteca de referencial teórico (referencial/fichas.json) contra as páginas do
Marxists Internet Archive e mostra quais fichas estão OK.

Uso:
    python verificar_biblioteca.py            (usa o cache de 7 dias)
    python verificar_biblioteca.py --forcar   (confere tudo de novo)
"""
import sys

import relatorio_geopolitico as r


def main():
    fichas = r.carregar_fichas()
    if not fichas:
        print("❌ Não encontrei referencial/fichas.json (rode este comando na pasta do projeto).")
        return 1
    print(f"Conferindo {len(fichas)} fichas nas páginas do MIA (leva alguns minutos na primeira vez)...\n")
    status = r.verificar_biblioteca(fichas, forcar="--forcar" in sys.argv)
    ok = 0
    for f in fichas:
        s = status[f["id"]]
        if s["ok"]:
            ok += 1
            print(f"✓ {f['id']}  {f['autor']} — {f['conceito']}")
        else:
            print(f"✗ {f['id']}  {f['autor']} — {f['conceito']}\n     {s['motivo']}\n     {f['url']}")
    print(f"\nResumo: {ok} de {len(fichas)} fichas conferidas.")
    if ok < len(fichas):
        print("As fichas marcadas com ✗ NÃO entram na análise. Cole esta lista na conversa para eu corrigir.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
