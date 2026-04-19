# pipeline.py
"""
Pipeline central do SPED ICMS Analyzer - MT.

Este modulo e o unico ponto de entrada para analise de documentos.
Resolve os problemas estruturais identificados:

  1. PARALELISMO         ProcessPoolExecutor para lotes de XML
  2. REUSO DE OBJETOS    ICMSAnalyzer, NCMAnalyzer, RiskClassifier instanciados 1x
  3. NORMALIZACAO        Entrada sempre via DadoFiscalNormalizado
  4. RETORNO PADRONIZADO Sempre ResultadoFinal ou None (nunca None, None / [], [])
  5. MEMORIA             Escrita incremental de CSV; sem acumulo de lote inteiro em RAM
  6. IMPORTS NO TOPO     Sem imports dentro de funcoes (exceto guard de processo filho)

Fluxo por documento:
  Arquivo
    -> Parser (NFeParser | SpedParser)
    -> Normalizador (normalizar_nfe | normalizar_sped)
    -> ICMSAnalyzer.analisar(dado)
    -> ICMSSTAnalyzer.analisar_itens(dado)
    -> NCMAnalyzer.validar_itens(dado)
    -> RiskClassifier.classificar(resultado)
    -> ResultadoFinal
"""

from __future__ import annotations

import csv
import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional

from config import (
    UF_PADRAO,
    EXTENSOES_XML,
    EXTENSOES_SPED,
    MAX_WORKERS,
    MIN_ARQUIVOS_PARA_PARALELO,
    REPORTS_DIR,
    _garantir_diretorios,
)
from normalizer import DadoFiscalNormalizado, normalizar_nfe, normalizar_sped
from analyzers.icms_analyzer import ICMSAnalyzer, ResultadoAnaliseICMS
from analyzers.icms_st_analyzer import ICMSSTAnalyzer, ResultadoST
from analyzers.ncm_analyzer import NCMAnalyzer, ResultadoNCM
from ml.risk_classifier import RiskClassifier

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# RETORNO PADRONIZADO
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ResultadoFinal:
    """
    Resultado unico e padronizado por documento fiscal.
    Substitui os retornos inconsistentes anteriores
    (None,None) / ([],[]) / (resultado, classif).
    """
    dado:           DadoFiscalNormalizado
    icms:           ResultadoAnaliseICMS
    st:             List[ResultadoST]     = field(default_factory=list)
    ncm:            List[ResultadoNCM]    = field(default_factory=list)
    classificacao:  dict                  = field(default_factory=dict)
    erro:           Optional[str]         = None

    @property
    def sucesso(self) -> bool:
        return self.erro is None

    @property
    def risco(self) -> str:
        return self.classificacao.get("risco", self.icms.risco_calculado if self.icms else "BAIXO")


# ─────────────────────────────────────────────────────────────────────────────
# CONTEXTO DE ANALISE (objetos pesados instanciados 1x)
# ─────────────────────────────────────────────────────────────────────────────

class ContextoAnalise:
    """
    Agrupa os objetos pesados que devem ser instanciados apenas UMA vez
    por processo, nao dentro de loops.

    Uso:
        ctx = ContextoAnalise()
        for dado in dados:
            resultado = ctx.analisar(dado)
    """

    def __init__(self, uf: str = UF_PADRAO):
        self.uf            = uf
        self.icms_analyzer = ICMSAnalyzer(uf)
        self.st_analyzer   = ICMSSTAnalyzer(uf)
        self.ncm_analyzer  = NCMAnalyzer(uf)
        self.classifier    = RiskClassifier()   # carrega modelo .pkl 1x
        log.debug("ContextoAnalise inicializado para UF=%s | ML=%s",
                  uf, "sim" if self.classifier.usa_ml else "regras")

    def analisar(self, dado: DadoFiscalNormalizado) -> ResultadoFinal:
        """Executa o pipeline completo para um dado normalizado."""
        try:
            # 1. ICMS
            resultado_icms = self.icms_analyzer.analisar(dado)

            # 2. ST
            resultados_st = self.st_analyzer.analisar_itens(dado)

            # 3. NCM
            resultados_ncm = self.ncm_analyzer.validar_itens(dado)

            # 4. ML
            classif = self.classifier.classificar(resultado_icms)

            return ResultadoFinal(
                dado          = dado,
                icms          = resultado_icms,
                st            = resultados_st,
                ncm           = resultados_ncm,
                classificacao = classif,
            )
        except Exception as exc:
            log.error("Erro ao analisar %s: %s", dado.chave or dado.numero, exc)
            return ResultadoFinal(
                dado  = dado,
                icms  = ResultadoAnaliseICMS(
                    identificador=dado.chave or dado.numero,
                    uf=self.uf,
                ),
                erro  = str(exc),
            )


# ─────────────────────────────────────────────────────────────────────────────
# FUNCOES DE PARSING (top-level para uso com ProcessPoolExecutor)
# ─────────────────────────────────────────────────────────────────────────────
# IMPORTANTE: funcoes passadas ao ProcessPoolExecutor precisam ser picklable,
# ou seja, definidas no nivel do modulo, nao como lambdas ou closures.

def _parsear_xml(caminho: str) -> Optional[DadoFiscalNormalizado]:
    """Parseia um arquivo XML (NF-e ou NFC-e). Executado em processo filho."""
    # Import local: processo filho nao herda estado do processo pai
    from parsers.nfe_parser import NFeParser
    try:
        nfe = NFeParser(caminho).parse()
        if nfe.erros_leitura and not nfe.numero:
            log.warning("XML ignorado (%s): %s", caminho, nfe.erros_leitura[0])
            return None
        return normalizar_nfe(nfe, caminho)
    except Exception as exc:
        log.error("Falha ao parsear XML %s: %s", caminho, exc)
        return None


def _analisar_dado(args: tuple) -> Optional[ResultadoFinal]:
    """
    Wrapper picklable para uso no ProcessPoolExecutor.
    Recebe (DadoFiscalNormalizado, uf) e retorna ResultadoFinal.
    Cria ContextoAnalise localmente no processo filho.
    """
    dado, uf = args
    if dado is None:
        return None
    ctx = ContextoAnalise(uf)
    return ctx.analisar(dado)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PUBLICO
# ─────────────────────────────────────────────────────────────────────────────

class Pipeline:
    """
    API publica do sistema. Ponto de entrada para main.py e testes.

    Exemplos:
        pipe = Pipeline()

        # Documento unico
        resultado = pipe.processar_xml("nota.xml")
        resultado = pipe.processar_sped("EFD.txt")

        # Lote com paralelismo automatico
        for resultado in pipe.processar_pasta("./xmls/"):
            ...

        # Comparador
        pipe.comparar(pasta_xmls="./xmls/", caminho_sped="EFD.txt")
    """

    def __init__(self, uf: str = UF_PADRAO):
        _garantir_diretorios()
        self.uf  = uf
        self._ctx = ContextoAnalise(uf)   # instancia unica para uso sequencial

    # ── Documento unico ──────────────────────────────────────────────────────

    def processar_xml(self, caminho: str) -> Optional[ResultadoFinal]:
        """Processa um arquivo XML (NF-e ou NFC-e). Retorna None em caso de erro."""
        dado = _parsear_xml(caminho)
        if dado is None:
            return None
        return self._ctx.analisar(dado)

    def processar_sped(self, caminho: str) -> List[ResultadoFinal]:
        """
        Processa um arquivo SPED Fiscal (.txt).
        Retorna lista (pode ser vazia). Nunca retorna None.
        """
        from parsers.sped_parser import SpedParser
        sped = SpedParser(caminho).parse()

        if sped.erros_leitura:
            log.warning("%d erro(s) na leitura do SPED %s",
                        len(sped.erros_leitura), caminho)
            for e in sped.erros_leitura[:3]:
                log.warning("  %s", e)

        if not sped.notas:
            log.error("Nenhuma nota encontrada no SPED: %s", caminho)
            return []

        log.info("%d nota(s) encontrada(s) no SPED", len(sped.notas))
        resultados = []
        for nota in sped.notas:
            dado = normalizar_sped(nota, sped.produtos, caminho)
            resultados.append(self._ctx.analisar(dado))
        return resultados

    # ── Lote com paralelismo ─────────────────────────────────────────────────

    def processar_pasta(self, pasta: str) -> Iterator[ResultadoFinal]:
        """
        Gera ResultadoFinal para cada documento na pasta.
        Usa ProcessPoolExecutor para XMLs quando o lote e grande o suficiente.
        SPEDs sao sempre processados sequencialmente (ja que o SPED e 1 arquivo
        com N notas e o paralelismo e feito internamente por nota).
        Usa yield para controle de memoria - nao acumula tudo em RAM.
        """
        path = Path(pasta)
        if not path.exists():
            raise FileNotFoundError(f"Pasta nao encontrada: {pasta}")

        xmls  = [str(p) for ext in EXTENSOES_XML  for p in path.glob(f"*{ext}")]
        speds = [str(p) for ext in EXTENSOES_SPED for p in path.glob(f"*{ext}")]

        log.info("Pasta: %d XML(s), %d SPED(s)", len(xmls), len(speds))

        # XMLs: paralelo se lote grande o suficiente
        yield from self._processar_xmls_paralelo(xmls)

        # SPEDs: sequencial
        for sped_path in speds:
            for res in self.processar_sped(sped_path):
                yield res

    def _processar_xmls_paralelo(self, caminhos: List[str]) -> Iterator[ResultadoFinal]:
        """
        Processa XMLs em paralelo com ProcessPoolExecutor.
        Fallback sequencial para lotes pequenos ou quando paralelismo e desativado.
        """
        if not caminhos:
            return

        usar_paralelo = (
            len(caminhos) >= MIN_ARQUIVOS_PARA_PARALELO
            and MAX_WORKERS != 1
        )

        if not usar_paralelo:
            log.debug("Processamento sequencial (%d arquivo(s))", len(caminhos))
            for caminho in caminhos:
                res = self.processar_xml(caminho)
                if res is not None:
                    yield res
            return

        workers = MAX_WORKERS or os.cpu_count() or 1
        log.info("Paralelismo ativo: %d worker(s) para %d XML(s)",
                 workers, len(caminhos))

        # Fase 1: parse em paralelo (I/O bound — beneficia de multiprocessing)
        dados: List[Optional[DadoFiscalNormalizado]] = [None] * len(caminhos)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_parsear_xml, c): i
                       for i, c in enumerate(caminhos)}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    dados[idx] = future.result()
                except Exception as exc:
                    log.error("Erro no parse de %s: %s", caminhos[idx], exc)

        # Fase 2: analise em paralelo (CPU bound — usa todos os cores)
        args = [(d, self.uf) for d in dados if d is not None]
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures_analise = {executor.submit(_analisar_dado, a): a
                               for a in args}
            for future in as_completed(futures_analise):
                try:
                    res = future.result()
                    if res is not None:
                        yield res
                except Exception as exc:
                    log.error("Erro na analise: %s", exc)

    # ── Escrita incremental de CSV ────────────────────────────────────────────

    def processar_pasta_para_csv(self, pasta: str, arquivo_saida: str) -> Path:
        """
        Processa pasta e escreve divergencias em CSV incrementalmente.
        Nao acumula todos os resultados em RAM - ideal para grandes volumes.
        Retorna o caminho do arquivo gerado.
        """
        saida = REPORTS_DIR / arquivo_saida
        cabecalho = [
            "arquivo", "chave", "numero", "serie", "modelo",
            "risco", "confianca", "tipo_divergencia", "gravidade",
            "descricao", "valor_encontrado", "valor_esperado", "orientacao"
        ]

        total = 0
        with open(saida, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)

            for res in self.processar_pasta(pasta):
                total += 1
                risco     = res.risco
                confianca = f"{res.classificacao.get('confianca', 0):.0%}"
                arq       = Path(res.dado.caminho_arquivo).name

                if not res.icms.divergencias:
                    writer.writerow([
                        arq, res.dado.chave, res.dado.numero,
                        res.dado.serie, res.dado.modelo,
                        risco, confianca, "NENHUMA", "", "", "", "", ""
                    ])
                else:
                    for div in res.icms.divergencias:
                        writer.writerow([
                            arq, res.dado.chave, res.dado.numero,
                            res.dado.serie, res.dado.modelo,
                            risco, confianca,
                            div.tipo, div.gravidade, div.descricao,
                            div.valor_encontrado, div.valor_esperado,
                            div.orientacao,
                        ])
                f.flush()   # garante escrita a cada nota

        log.info("CSV incremental: %d documento(s) -> %s", total, saida)
        return saida

    # ── Comparador ───────────────────────────────────────────────────────────

    def comparar(self, pasta_xmls: str, caminho_sped: str) -> list:
        """
        Compara XMLs de uma pasta com um arquivo SPED.
        Delega para o modulo comparador (sem acoplamento direto).
        """
        from comparador.comparador_nfe_sped import ComparadorNFeSped
        comp = ComparadorNFeSped(caminho_sped, pasta_xmls, self.uf)
        return comp.comparar()