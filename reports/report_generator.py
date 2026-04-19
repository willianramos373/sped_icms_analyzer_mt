# reports/report_generator.py
"""
Gera relatórios em CSV e TXT com os resultados das análises.
Os arquivos são salvos na pasta /relatorios com timestamp.
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from config import REPORTS_DIR
from analyzers.icms_analyzer import ResultadoAnaliseICMS
from analyzers.icms_st_analyzer import ResultadoST
from analyzers.ncm_analyzer import ResultadoNCM


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


class ReportGenerator:
    """Gera relatórios de análise fiscal."""

    def __init__(self, uf: str, prefixo: str = "analise"):
        self.uf = uf
        self.prefixo = prefixo
        self.ts = _timestamp()

    def gerar_relatorio_completo(
        self,
        resultados: List[ResultadoAnaliseICMS],
        classificacoes: List[dict],
        resultados_st: List[List[ResultadoST]] = None,
        resultados_ncm: List[List[ResultadoNCM]] = None,
    ) -> List[Path]:
        """Gera todos os relatórios e retorna lista de caminhos."""
        arquivos = []
        arquivos.append(self._gerar_csv_divergencias(resultados, classificacoes))
        arquivos.append(self._gerar_csv_resumo(resultados, classificacoes))
        if resultados_st:
            arquivos.append(self._gerar_csv_st(resultados_st))
        if resultados_ncm:
            arquivos.append(self._gerar_csv_ncm(resultados_ncm))
        arquivos.append(self._gerar_txt_orientacoes(resultados, classificacoes))
        return [a for a in arquivos if a is not None]

    def _gerar_csv_divergencias(
        self,
        resultados: List[ResultadoAnaliseICMS],
        classificacoes: List[dict],
    ) -> Path:
        caminho = REPORTS_DIR / f"{self.prefixo}_divergencias_{self.uf}_{self.ts}.csv"
        cabecalho = [
            "documento", "uf", "risco", "confianca_ml", "metodo_ml",
            "tipo_divergencia", "gravidade", "descricao",
            "valor_encontrado", "valor_esperado", "orientacao", "base_legal"
        ]
        with open(caminho, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)
            for resultado, classif in zip(resultados, classificacoes):
                risco = classif.get("risco", "")
                conf = f"{classif.get('confianca', 0):.0%}"
                metodo = classif.get("metodo", "")
                if not resultado.divergencias:
                    writer.writerow([
                        resultado.identificador, self.uf, risco, conf, metodo,
                        "NENHUMA", "", "Sem divergências", "", "", "", ""
                    ])
                for div in resultado.divergencias:
                    writer.writerow([
                        resultado.identificador, self.uf, risco, conf, metodo,
                        div.tipo, div.gravidade, div.descricao,
                        div.valor_encontrado, div.valor_esperado,
                        div.orientacao, div.referencia_legal
                    ])
        return caminho

    def _gerar_csv_resumo(
        self,
        resultados: List[ResultadoAnaliseICMS],
        classificacoes: List[dict],
    ) -> Path:
        caminho = REPORTS_DIR / f"{self.prefixo}_resumo_{self.uf}_{self.ts}.csv"
        cabecalho = [
            "documento", "uf", "risco", "confianca_ml",
            "total_divergencias", "criticas", "altas", "medias", "baixas",
            "icms_documento", "icms_calculado", "diferenca_icms"
        ]
        with open(caminho, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)
            for resultado, classif in zip(resultados, classificacoes):
                divs = resultado.divergencias
                writer.writerow([
                    resultado.identificador,
                    self.uf,
                    classif.get("risco", ""),
                    f"{classif.get('confianca', 0):.0%}",
                    len(divs),
                    sum(1 for d in divs if d.gravidade == "CRITICA"),
                    sum(1 for d in divs if d.gravidade == "ALTA"),
                    sum(1 for d in divs if d.gravidade == "MEDIA"),
                    sum(1 for d in divs if d.gravidade == "BAIXA"),
                    f"{resultado.total_icms_documento:.2f}",
                    f"{resultado.total_icms_esperado:.2f}",
                    f"{resultado.diferenca_icms:.2f}",
                ])
        return caminho

    def _gerar_csv_st(self, todos_resultados_st: List[List[ResultadoST]]) -> Path:
        caminho = REPORTS_DIR / f"{self.prefixo}_icms_st_{self.uf}_{self.ts}.csv"
        cabecalho = [
            "num_item", "descricao", "ncm", "cfop",
            "tem_st", "mva_informado", "mva_esperado",
            "bc_st_informado", "bc_st_calculado",
            "vl_st_informado", "vl_st_calculado", "divergencias"
        ]
        with open(caminho, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)
            for lista_st in todos_resultados_st:
                for res in lista_st:
                    if res.divergencias_st:
                        writer.writerow([
                            res.num_item, res.descricao, res.ncm, res.cfop,
                            "SIM" if res.tem_st else "NÃO",
                            f"{res.mva_informado:.2f}", f"{res.mva_esperado:.2f}",
                            f"{res.vl_bc_st_informado:.2f}", f"{res.vl_bc_st_calculado:.2f}",
                            f"{res.vl_st_informado:.2f}", f"{res.vl_st_calculado:.2f}",
                            " | ".join(res.divergencias_st)
                        ])
        return caminho

    def _gerar_csv_ncm(self, todos_resultados_ncm: List[List[ResultadoNCM]]) -> Path:
        caminho = REPORTS_DIR / f"{self.prefixo}_ncm_{self.uf}_{self.ts}.csv"
        cabecalho = [
            "ncm", "valido", "descricao_produto", "descricao_tabela",
            "aliq_informada", "aliq_esperada", "sugestao_ncm", "divergencias", "orientacoes"
        ]
        with open(caminho, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)
            for lista_ncm in todos_resultados_ncm:
                for res in lista_ncm:
                    if res.divergencias:
                        writer.writerow([
                            res.ncm,
                            "SIM" if res.valido else "NÃO",
                            res.descricao_produto,
                            res.descricao_encontrada,
                            f"{res.aliq_informada:.2f}",
                            f"{res.aliq_esperada:.2f}",
                            res.sugestao_ncm or "",
                            " | ".join(res.divergencias),
                            " | ".join(res.orientacoes),
                        ])
        return caminho

    def _gerar_txt_orientacoes(
        self,
        resultados: List[ResultadoAnaliseICMS],
        classificacoes: List[dict],
    ) -> Path:
        """Relatório TXT em linguagem natural para o cliente."""
        caminho = REPORTS_DIR / f"{self.prefixo}_orientacoes_{self.uf}_{self.ts}.txt"

        with open(caminho, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write(f"RELATÓRIO DE CONFORMIDADE FISCAL - {self.uf}\n")
            f.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"Total de documentos analisados: {len(resultados)}\n")
            f.write("=" * 70 + "\n\n")

            alto  = sum(1 for c in classificacoes if c.get("risco") == "ALTO")
            medio = sum(1 for c in classificacoes if c.get("risco") == "MEDIO")
            baixo = sum(1 for c in classificacoes if c.get("risco") == "BAIXO")

            f.write("RESUMO EXECUTIVO\n")
            f.write("-" * 40 + "\n")
            f.write(f"  🔴 Alto risco : {alto} documento(s)\n")
            f.write(f"  🟡 Médio risco: {medio} documento(s)\n")
            f.write(f"  🟢 Baixo risco: {baixo} documento(s)\n\n")

            # Apenas documentos com divergência
            docs_com_div = [
                (r, c) for r, c in zip(resultados, classificacoes)
                if r.divergencias
            ]

            if not docs_com_div:
                f.write("✅ Todos os documentos analisados estão em conformidade!\n")
            else:
                f.write("DETALHAMENTO DAS CORREÇÕES NECESSÁRIAS\n")
                f.write("-" * 40 + "\n\n")
                for resultado, classif in docs_com_div:
                    risco = classif.get("risco", "")
                    f.write(f"Documento: {resultado.identificador}\n")
                    f.write(f"Risco    : {risco}\n")
                    f.write(f"ICMS Doc : R$ {resultado.total_icms_documento:.2f}\n")
                    if resultado.diferenca_icms > 0:
                        f.write(f"Diferença: R$ {resultado.diferenca_icms:.2f}\n")
                    f.write("\nO que precisa corrigir:\n")
                    for i, div in enumerate(resultado.divergencias, 1):
                        f.write(f"\n  {i}. [{div.gravidade}] {div.descricao}\n")
                        if div.valor_encontrado:
                            f.write(f"     Encontrado: {div.valor_encontrado}\n")
                        if div.valor_esperado:
                            f.write(f"     Correto   : {div.valor_esperado}\n")
                        f.write(f"     Como corrigir: {div.orientacao}\n")
                        if div.referencia_legal:
                            f.write(f"     Base legal: {div.referencia_legal}\n")
                    f.write("\n" + "-" * 50 + "\n\n")

            f.write("\nIMPORTANTE:\n")
            f.write("As orientações acima são baseadas nas regras do RICMS vigente.\n")
            f.write("Para situações específicas, consulte um contador ou a SEFAZ.\n")
            f.write("=" * 70 + "\n")

        return caminho
