# comparador/relatorio_comparador.py
"""
Geração de relatórios e exibição de alertas do comparador NF-e vs SPED.
Saídas: terminal colorido + CSV detalhado + TXT orientações ao cliente.
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import List
from colorama import Fore, Style, init

from comparador.comparador_nfe_sped import ResultadoComparacao
from config import REPORTS_DIR

init(autoreset=True)

COR = {
    "CRITICA": Fore.RED + Style.BRIGHT,
    "ALTA":    Fore.YELLOW + Style.BRIGHT,
    "MEDIA":   Fore.YELLOW,
    "OK":      Fore.GREEN + Style.BRIGHT,
    "INFO":    Fore.CYAN,
    "HEADER":  Fore.WHITE + Style.BRIGHT,
    "RESET":   Style.RESET_ALL,
}

ICONE_STATUS = {
    "OK":               "✅",
    "CRITICA":          "🚨",
    "ALTA":             "⚠️ ",
    "MEDIA":            "📋",
    "NAO_ESCRITURADA":  "❌",
    "FORA_COMPETENCIA": "📅",
}


# ─────────────────────────────────────────────
# EXIBIÇÃO NO TERMINAL
# ─────────────────────────────────────────────

class AlertaComparador:

    def exibir_resultado(self, res: ResultadoComparacao):
        """Exibe o resultado de uma comparação no terminal."""
        icone = ICONE_STATUS.get(res.status, "❓")
        cor   = COR.get(res.gravidade_maxima if res.encontrada_no_sped else "CRITICA", COR["INFO"])

        print(f"\n{cor}{icone} NF {res.numero_nf}/{res.serie} "
              f"| Mod.{res.modelo} | {res.dt_emissao[:10]}{COR['RESET']}")
        print(f"   Chave: {res.chave_nfe[:20]}..." if res.chave_nfe else "   Chave: (sem chave)")

        if not res.encontrada_no_sped:
            if res.fora_competencia:
                print(f"   {COR['INFO']}📅 FORA DA COMPETÊNCIA DO SPED{COR['RESET']}")
                print(f"   Competência XML : {res.competencia_xml}")
                print(f"   Competência SPED: {res.competencia_sped}")
                print(f"   {Fore.CYAN}📌 Esta nota pertence a outro período. "
                      f"Verifique se foi escriturada no SPED correto ({res.competencia_xml}).{COR['RESET']}")
            else:
                print(f"   {COR['CRITICA']}❌ NÃO ENCONTRADA NO SPED{COR['RESET']}")
                print(f"   {Fore.CYAN}📌 Nota emitida/recebida na competência {res.competencia_xml} "
                      f"mas não escriturada no SPED do mesmo período. "
                      f"Inclua esta nota no SPED via retificação ou escrituração extemporânea "
                      f"conforme prazo da SEFAZ-{res.competencia_sped[-4:]}.{COR['RESET']}")
            return

        if not res.divergencias:
            print(f"   {COR['OK']}✅ Sem divergências — conforme{COR['RESET']}")
            return

        print(f"   {len(res.divergencias)} divergência(s) encontrada(s):")
        for div in res.divergencias:
            cor_div = COR.get(div.gravidade, COR["INFO"])
            print(f"\n   {cor_div}[{div.gravidade}] {div.campo}{COR['RESET']}")
            print(f"     XML  : {Fore.RED}{div.valor_xml}{COR['RESET']}")
            print(f"     SPED : {Fore.GREEN}{div.valor_sped}{COR['RESET']}")
            print(f"     {Fore.CYAN}📌 {div.orientacao}{COR['RESET']}")

    def exibir_resumo(self, resultados: List[ResultadoComparacao],
                      dt_ini_sped: str, dt_fin_sped: str, cnpj: str):
        """Exibe resumo consolidado no terminal."""
        sep = "=" * 70
        print(f"\n{sep}")
        print(f"{COR['HEADER']}RESUMO — COMPARAÇÃO NF-e XML vs SPED FISCAL{COR['RESET']}")
        print(f"Período SPED : {dt_ini_sped} a {dt_fin_sped}")
        print(f"Contribuinte : {cnpj}")
        print(f"Total XMLs   : {len(resultados)}")
        print(sep)

        ok              = [r for r in resultados if r.status == "OK"]
        com_div         = [r for r in resultados if r.encontrada_no_sped and r.tem_divergencia]
        nao_escrituradas= [r for r in resultados if not r.encontrada_no_sped and not r.fora_competencia]
        fora_comp       = [r for r in resultados if r.fora_competencia]
        criticas        = [r for r in com_div if r.gravidade_maxima == "CRITICA"]

        print(f"\n  {COR['OK']}✅ Conformes           : {len(ok):>4}{COR['RESET']}")
        print(f"  {COR['CRITICA']}🚨 Com divergência CRÍTICA: {len(criticas):>4}{COR['RESET']}")
        print(f"  {COR['ALTA']}⚠️  Com divergência     : {len(com_div):>4}{COR['RESET']}")
        print(f"  {COR['CRITICA']}❌ Não escrituradas    : {len(nao_escrituradas):>4}{COR['RESET']}")
        print(f"  {COR['INFO']}📅 Fora da competência : {len(fora_comp):>4}{COR['RESET']}")

        total_divs = sum(len(r.divergencias) for r in resultados)
        dif_icms   = sum(abs(r.vl_icms_xml - r.vl_icms_sped) for r in com_div)
        dif_doc    = sum(abs(r.vl_doc_xml  - r.vl_doc_sped)  for r in com_div)
        print(f"\n  Total divergências   : {total_divs}")
        print(f"  Diferença total ICMS : R$ {dif_icms:,.2f}")
        print(f"  Diferença total NF   : R$ {dif_doc:,.2f}")

        # Top tipos de divergência
        tipos: dict = {}
        for r in resultados:
            for d in r.divergencias:
                tipos[d.campo] = tipos.get(d.campo, 0) + 1
        if tipos:
            print(f"\n{COR['HEADER']}  Top divergências mais frequentes:{COR['RESET']}")
            for campo, cnt in sorted(tipos.items(), key=lambda x: x[1], reverse=True)[:6]:
                print(f"    {campo}: {cnt}x")

        print(sep)


# ─────────────────────────────────────────────
# RELATÓRIOS EM ARQUIVO
# ─────────────────────────────────────────────

class RelatorioComparador:

    def __init__(self, uf: str):
        self.uf = uf
        self.ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    def gerar_todos(self, resultados: List[ResultadoComparacao]) -> List[Path]:
        arquivos = []
        arquivos.append(self._csv_divergencias(resultados))
        arquivos.append(self._csv_resumo(resultados))
        arquivos.append(self._csv_nao_escrituradas(resultados))
        arquivos.append(self._txt_orientacoes(resultados))
        return [a for a in arquivos if a]

    def _csv_divergencias(self, resultados: List[ResultadoComparacao]) -> Path:
        path = REPORTS_DIR / f"comparador_divergencias_{self.uf}_{self.ts}.csv"
        cab = [
            "chave_nfe", "numero_nf", "serie", "modelo", "dt_emissao",
            "status", "gravidade_maxima", "encontrada_sped",
            "competencia_xml", "competencia_sped",
            "campo", "valor_xml", "valor_sped", "gravidade_campo", "orientacao"
        ]
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(cab)
            for r in resultados:
                if not r.encontrada_no_sped or not r.divergencias:
                    w.writerow([
                        r.chave_nfe, r.numero_nf, r.serie, r.modelo,
                        r.dt_emissao[:10], r.status, r.gravidade_maxima,
                        "SIM" if r.encontrada_no_sped else "NÃO",
                        r.competencia_xml, r.competencia_sped,
                        "", "", "", "", ""
                    ])
                for div in r.divergencias:
                    w.writerow([
                        r.chave_nfe, r.numero_nf, r.serie, r.modelo,
                        r.dt_emissao[:10], r.status, r.gravidade_maxima,
                        "SIM",
                        r.competencia_xml, r.competencia_sped,
                        div.campo, div.valor_xml, div.valor_sped,
                        div.gravidade, div.orientacao
                    ])
        return path

    def _csv_resumo(self, resultados: List[ResultadoComparacao]) -> Path:
        path = REPORTS_DIR / f"comparador_resumo_{self.uf}_{self.ts}.csv"
        cab = [
            "chave_nfe", "numero_nf", "serie", "modelo", "dt_emissao",
            "status", "encontrada_sped", "fora_competencia",
            "total_divergencias", "gravidade_maxima",
            "vl_icms_xml", "vl_icms_sped", "dif_icms",
            "vl_doc_xml", "vl_doc_sped", "dif_doc"
        ]
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(cab)
            for r in resultados:
                w.writerow([
                    r.chave_nfe, r.numero_nf, r.serie, r.modelo,
                    r.dt_emissao[:10], r.status,
                    "SIM" if r.encontrada_no_sped else "NÃO",
                    "SIM" if r.fora_competencia else "NÃO",
                    len(r.divergencias), r.gravidade_maxima,
                    f"{r.vl_icms_xml:.2f}", f"{r.vl_icms_sped:.2f}",
                    f"{abs(r.vl_icms_xml - r.vl_icms_sped):.2f}",
                    f"{r.vl_doc_xml:.2f}", f"{r.vl_doc_sped:.2f}",
                    f"{abs(r.vl_doc_xml - r.vl_doc_sped):.2f}",
                ])
        return path

    def _csv_nao_escrituradas(self, resultados: List[ResultadoComparacao]) -> Path:
        path = REPORTS_DIR / f"comparador_nao_escrituradas_{self.uf}_{self.ts}.csv"
        nao_esc = [r for r in resultados if not r.encontrada_no_sped]
        cab = [
            "chave_nfe", "numero_nf", "serie", "modelo", "dt_emissao",
            "status", "competencia_xml", "competencia_sped", "caminho_xml"
        ]
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(cab)
            for r in nao_esc:
                w.writerow([
                    r.chave_nfe, r.numero_nf, r.serie, r.modelo,
                    r.dt_emissao[:10], r.status,
                    r.competencia_xml, r.competencia_sped, r.caminho_xml
                ])
        return path

    def _txt_orientacoes(self, resultados: List[ResultadoComparacao]) -> Path:
        path = REPORTS_DIR / f"comparador_orientacoes_{self.uf}_{self.ts}.txt"
        nao_esc   = [r for r in resultados if not r.encontrada_no_sped and not r.fora_competencia]
        fora_comp = [r for r in resultados if r.fora_competencia]
        com_div   = [r for r in resultados if r.encontrada_no_sped and r.tem_divergencia]

        with open(path, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write("RELATÓRIO DE COMPARAÇÃO NF-e XML vs SPED FISCAL\n")
            f.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"UF: {self.uf} | Total XMLs: {len(resultados)}\n")
            f.write("=" * 70 + "\n\n")

            if nao_esc:
                f.write(f"❌ NOTAS NÃO ESCRITURADAS NO SPED ({len(nao_esc)})\n")
                f.write("-" * 50 + "\n")
                f.write("ATENÇÃO: As notas abaixo foram emitidas/recebidas na competência\n"
                        "do SPED mas NÃO foram escrituradas. Isso pode gerar autuação fiscal.\n\n")
                for r in nao_esc:
                    f.write(f"  NF {r.numero_nf}/{r.serie} | {r.dt_emissao[:10]}\n")
                    f.write(f"  Chave: {r.chave_nfe}\n")
                    f.write(f"  Ação: Inclua esta nota no SPED por retificação.\n\n")

            if fora_comp:
                f.write(f"\n📅 NOTAS FORA DA COMPETÊNCIA DO SPED ({len(fora_comp)})\n")
                f.write("-" * 50 + "\n")
                for r in fora_comp:
                    f.write(f"  NF {r.numero_nf}/{r.serie} | {r.dt_emissao[:10]}\n")
                    f.write(f"  Competência XML: {r.competencia_xml} | SPED: {r.competencia_sped}\n")
                    f.write(f"  Ação: Verifique o SPED do período {r.competencia_xml}.\n\n")

            if com_div:
                f.write(f"\n⚠️  NOTAS COM DIVERGÊNCIAS ({len(com_div)})\n")
                f.write("-" * 50 + "\n")
                for r in com_div:
                    f.write(f"\nNF {r.numero_nf}/{r.serie} | {r.dt_emissao[:10]} "
                            f"| Chave: {r.chave_nfe[:20]}...\n")
                    for i, div in enumerate(r.divergencias, 1):
                        f.write(f"  {i}. [{div.gravidade}] {div.campo}\n")
                        f.write(f"     XML : {div.valor_xml}\n")
                        f.write(f"     SPED: {div.valor_sped}\n")
                        f.write(f"     Ação: {div.orientacao}\n")

            if not nao_esc and not com_div:
                f.write("✅ Todos os documentos estão em conformidade com o SPED!\n")

            f.write("\n" + "=" * 70 + "\n")
            f.write("Dúvidas? Consulte a SEFAZ ou seu contador.\n")

        return path
