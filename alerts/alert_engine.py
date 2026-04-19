# alerts/alert_engine.py
"""
Engine de alertas: formata divergências em mensagens claras
para o cliente entender o problema e como corrigir.
Saída: texto colorido no terminal (CLI).
"""

from colorama import Fore, Style, init
from typing import List
from analyzers.icms_analyzer import ResultadoAnaliseICMS, Divergencia
from analyzers.icms_st_analyzer import ResultadoST
from analyzers.ncm_analyzer import ResultadoNCM

init(autoreset=True)

# ─────────────────────────────────────────────
# CORES POR GRAVIDADE
# ─────────────────────────────────────────────
COR = {
    "CRITICA": Fore.RED + Style.BRIGHT,
    "ALTA":    Fore.YELLOW + Style.BRIGHT,
    "MEDIA":   Fore.YELLOW,
    "BAIXA":   Fore.CYAN,
    "OK":      Fore.GREEN + Style.BRIGHT,
    "INFO":    Fore.CYAN,
    "HEADER":  Fore.WHITE + Style.BRIGHT,
    "RESET":   Style.RESET_ALL,
}

EMOJI_RISCO = {
    "ALTO":  "🔴",
    "MEDIO": "🟡",
    "BAIXO": "🟢",
}

EMOJI_GRAV = {
    "CRITICA": "🚨",
    "ALTA":    "⚠️ ",
    "MEDIA":   "📋",
    "BAIXA":   "ℹ️ ",
}


class AlertEngine:
    """Formata e exibe alertas fiscais no terminal."""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose

    # ─────────────────────────────────────────
    # EXIBIÇÃO DE RESULTADO ICMS
    # ─────────────────────────────────────────

    def exibir_resultado_icms(
        self,
        resultado: ResultadoAnaliseICMS,
        classificacao_ml: dict,
        resultados_st: List[ResultadoST] = None,
        resultados_ncm: List[ResultadoNCM] = None,
    ):
        risco = classificacao_ml.get("risco", resultado.risco_calculado)
        metodo = classificacao_ml.get("metodo", "")

        self._linha_separadora("=")
        print(f"{COR['HEADER']}ANÁLISE FISCAL - {resultado.uf}{COR['RESET']}")
        print(f"Documento : {resultado.identificador}")
        print(f"ICMS Doc  : R$ {resultado.total_icms_documento:>12.2f}")
        if resultado.total_icms_esperado > 0:
            print(f"ICMS Calc : R$ {resultado.total_icms_esperado:>12.2f}")
            print(f"Diferença : R$ {resultado.diferenca_icms:>12.2f}")

        cor_risco = COR["CRITICA"] if risco == "ALTO" else (COR["ALTA"] if risco == "MEDIO" else COR["OK"])
        print(f"\nRISCO FISCAL: {cor_risco}{EMOJI_RISCO.get(risco, '')} {risco}{COR['RESET']}")
        print(f"Método    : {metodo}")

        if classificacao_ml.get("probabilidades"):
            probs = classificacao_ml["probabilidades"]
            print(f"Prob.     : BAIXO={probs.get('BAIXO', 0):.0%} "
                  f"MEDIO={probs.get('MEDIO', 0):.0%} "
                  f"ALTO={probs.get('ALTO', 0):.0%}")

        # Divergências ICMS
        if resultado.divergencias:
            self._linha_separadora("-")
            print(f"{COR['HEADER']}DIVERGÊNCIAS ICMS ({len(resultado.divergencias)} encontrada(s)){COR['RESET']}")
            for i, div in enumerate(resultado.divergencias, 1):
                self._exibir_divergencia(i, div)
        else:
            print(f"\n{COR['OK']}✅ Nenhuma divergência ICMS encontrada.{COR['RESET']}")

        # Divergências ST
        if resultados_st:
            divs_st = [r for r in resultados_st if r.divergencias_st]
            if divs_st:
                self._linha_separadora("-")
                print(f"{COR['HEADER']}DIVERGÊNCIAS ICMS-ST ({len(divs_st)} item(ns)){COR['RESET']}")
                for res_st in divs_st:
                    self._exibir_st(res_st)
            else:
                print(f"\n{COR['OK']}✅ ICMS-ST: sem divergências.{COR['RESET']}")

        # Divergências NCM
        if resultados_ncm:
            divs_ncm = [r for r in resultados_ncm if r.divergencias]
            if divs_ncm:
                self._linha_separadora("-")
                print(f"{COR['HEADER']}DIVERGÊNCIAS NCM ({len(divs_ncm)} item(ns)){COR['RESET']}")
                for res_ncm in divs_ncm:
                    self._exibir_ncm(res_ncm)
            else:
                print(f"\n{COR['OK']}✅ NCM: todos válidos.{COR['RESET']}")

        self._linha_separadora("=")

    def _exibir_divergencia(self, num: int, div: Divergencia):
        cor = COR.get(div.gravidade, COR["INFO"])
        emoji = EMOJI_GRAV.get(div.gravidade, "")
        print(f"\n{cor}{emoji} [{div.gravidade}] #{num}: {div.tipo}{COR['RESET']}")
        print(f"  Problema  : {div.descricao}")
        if div.valor_encontrado:
            print(f"  Encontrado: {Fore.RED}{div.valor_encontrado}{COR['RESET']}")
        if div.valor_esperado:
            print(f"  Esperado  : {Fore.GREEN}{div.valor_esperado}{COR['RESET']}")
        print(f"  {Fore.CYAN}📌 Orientação: {div.orientacao}{COR['RESET']}")
        if div.referencia_legal:
            print(f"  Base Legal: {div.referencia_legal}")

    def _exibir_st(self, res: ResultadoST):
        print(f"\n  {Fore.YELLOW}Item {res.num_item}: {res.descricao[:45]}{COR['RESET']}")
        print(f"  NCM: {res.ncm} | CFOP: {res.cfop}")
        if res.mva_esperado > 0:
            print(f"  MVA esperado: {res.mva_esperado:.2f}% | informado: {res.mva_informado:.2f}%")
        for i, div in enumerate(res.divergencias_st, 1):
            print(f"  {Fore.YELLOW}⚠️  {div}{COR['RESET']}")
        for ori in res.orientacoes_st:
            print(f"  {Fore.CYAN}📌 {ori}{COR['RESET']}")

    def _exibir_ncm(self, res: ResultadoNCM):
        status = f"{COR['OK']}✅" if res.valido else f"{COR['CRITICA']}❌"
        print(f"\n  {status} NCM {res.ncm}: {res.descricao_produto[:40]}{COR['RESET']}")
        if res.descricao_encontrada:
            print(f"  Descrição tabela: {res.descricao_encontrada}")
        for div in res.divergencias:
            print(f"  {Fore.YELLOW}⚠️  {div}{COR['RESET']}")
        for ori in res.orientacoes:
            print(f"  {Fore.CYAN}📌 {ori}{COR['RESET']}")
        if res.sugestao_ncm:
            print(f"  {Fore.CYAN}💡 NCM similar encontrado: {res.sugestao_ncm}{COR['RESET']}")

    # ─────────────────────────────────────────
    # RESUMO CONSOLIDADO (vários documentos)
    # ─────────────────────────────────────────

    def exibir_resumo_lote(self, resultados: List[ResultadoAnaliseICMS], classificacoes: List[dict]):
        self._linha_separadora("=")
        print(f"{COR['HEADER']}RESUMO CONSOLIDADO - {len(resultados)} DOCUMENTO(S){COR['RESET']}")
        self._linha_separadora("-")

        alto  = sum(1 for c in classificacoes if c.get("risco") == "ALTO")
        medio = sum(1 for c in classificacoes if c.get("risco") == "MEDIO")
        baixo = sum(1 for c in classificacoes if c.get("risco") == "BAIXO")

        print(f"  {COR['CRITICA']}🔴 Alto risco : {alto:>3} documento(s){COR['RESET']}")
        print(f"  {COR['ALTA']}🟡 Médio risco: {medio:>3} documento(s){COR['RESET']}")
        print(f"  {COR['OK']}🟢 Baixo risco: {baixo:>3} documento(s){COR['RESET']}")

        total_divs = sum(len(r.divergencias) for r in resultados)
        total_icms = sum(r.total_icms_documento for r in resultados)
        total_dif  = sum(r.diferenca_icms for r in resultados)

        print(f"\n  Total divergências : {total_divs}")
        print(f"  Total ICMS docs    : R$ {total_icms:,.2f}")
        print(f"  Total diferença    : R$ {total_dif:,.2f}")

        # Top divergências mais comuns
        tipos_count = {}
        for r in resultados:
            for d in r.divergencias:
                tipos_count[d.tipo] = tipos_count.get(d.tipo, 0) + 1
        if tipos_count:
            print(f"\n{COR['HEADER']}  Top divergências mais frequentes:{COR['RESET']}")
            for tipo, count in sorted(tipos_count.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"    {tipo}: {count}x")

        self._linha_separadora("=")

    # ─────────────────────────────────────────
    # ORIENTAÇÃO GERAL AO CLIENTE
    # ─────────────────────────────────────────

    def orientacao_geral(self, uf: str):
        self._linha_separadora("-")
        print(f"{COR['HEADER']}ORIENTAÇÕES GERAIS - CONFORMIDADE FISCAL {uf}{COR['RESET']}")
        orientacoes = [
            f"1. Mantenha a tabela NCM atualizada em data/ncm_aliquotas_{uf.lower()}.csv",
            "2. Verifique mensalmente portarias SEFAZ com novos MVAs de ST",
            "3. Confira sempre se o CST está compatível com a operação realizada",
            "4. Para dúvidas específicas, consulte o RICMS vigente ou a SEFAZ",
            f"5. Documentos com RISCO ALTO devem ser revisados ANTES do envio ao SPED",
            "6. Guarde os XMLs das NF-e por no mínimo 5 anos (Art. 195 CTN)",
        ]
        if uf == "MT":
            orientacoes.append("7. Verifique TARE (Termo de Acordo Regime Especial) se aplicável")
            orientacoes.append("8. Atenção ao ICMS Estimativa Simplificado (Simples MT)")
        if uf == "MS":
            orientacoes.append("7. Verifique o Programa Estadual de Desenvolvimento (PRÓ-MS)")
        for ori in orientacoes:
            print(f"  {Fore.CYAN}{ori}{COR['RESET']}")
        self._linha_separadora("-")

    def _linha_separadora(self, char: str = "-", tamanho: int = 70):
        print(char * tamanho)
