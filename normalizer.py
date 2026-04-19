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
    dado: DadoFiscalNormalizado
    icms: ResultadoAnaliseICMS
    st: List[ResultadoST] = field(default_factory=list)
    ncm: List[ResultadoNCM] = field(default_factory=list)
    classificacao: dict = field(default_factory=dict)
    erro: Optional[str] = None

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
        self.uf = uf
        self.icms_analyzer = ICMSAnalyzer(uf)
        self.st_analyzer = ICMSSTAnalyzer(uf)
        self.ncm_analyzer = NCMAnalyzer(uf)
        self.classifier = RiskClassifier()  # carrega modelo .pkl 1x
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
                dado=dado,
                icms=resultado_icms,
                st=resultados_st,
                ncm=resultados_ncm,
                classificacao=classif,
            )
        except Exception as exc:
            log.error("Erro ao analisar %s: %s", dado.chave or dado.numero, exc)
            return ResultadoFinal(
                dado=dado,
                icms=ResultadoAnaliseICMS(
                    identificador=dado.chave or dado.numero,
                    uf=self.uf,
                ),
                erro=str(exc),
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
        self.uf = uf
        self._ctx = ContextoAnalise(uf)  # instancia unica para uso sequencial

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

        xmls = [str(p) for ext in EXTENSOES_XML for p in path.glob(f"*{ext}")]
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
                risco = res.risco
                confianca = f"{res.classificacao.get('confianca', 0):.0%}"
                arq = Path(res.dado.caminho_arquivo).name

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
                f.flush()  # garante escrita a cada nota

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


# normalizer.py
"""
Camada de normalizacao de dados fiscais.

Problema resolvido:
  Antes, o ICMSAnalyzer tinha dois metodos diferentes:
    - analisar_nfe(nfe: NFe)
    - analisar_nota_sped(nota: SpedNotaFiscal, produtos: dict)

  Isso criava acoplamento entre o parser e o analyzer, retornos
  inconsistentes no main.py, e impossibilitava o paralelismo limpo.

Solucao:
  Este modulo define um DadoFiscalNormalizado (dict tipado) e duas
  funcoes de conversao. O pipeline.py sempre passa o dado normalizado
  para os analyzers, que agora recebem a mesma estrutura independente
  da origem (NF-e XML ou SPED .txt).

Fluxo:
  NFeParser.parse()    -> NFe            -> normalizar_nfe()   -> DadoFiscalNormalizado
  SpedParser.parse()   -> SpedEFD        -> normalizar_sped()  -> [DadoFiscalNormalizado]
                                                                     |
                                                              pipeline.analisar(dado)
"""

from dataclasses import dataclass, field
from typing import List, Optional


# ─────────────────────────────────────────────────────────────────────────────
# ESTRUTURA NORMALIZADA
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ItemNormalizado:
    """Representa um item de nota fiscal de forma uniforme."""
    num_item:     str   = ""
    cod_item:     str   = ""
    descricao:    str   = ""
    ncm:          str   = ""
    cfop:         str   = ""
    unid:         str   = ""
    qtd:          float = 0.0
    vl_item:      float = 0.0
    vl_desc:      float = 0.0
    cst_icms:     str   = ""
    vl_bc_icms:   float = 0.0
    aliq_icms:    float = 0.0
    vl_icms:      float = 0.0
    vl_bc_st:     float = 0.0
    aliq_st:      float = 0.0
    vl_icms_st:   float = 0.0
    p_mva_st:     float = 0.0
    p_red_bc:     float = 0.0
    cst_ipi:      str   = ""
    aliq_ipi:     float = 0.0
    vl_ipi:       float = 0.0
    orig:         str   = ""      # 0=Nacional, 1+=Importado
    vl_bc_fcp:    float = 0.0
    p_fcp:        float = 0.0
    vl_fcp:       float = 0.0


@dataclass
class TotaisNormalizados:
    """Totais financeiros normalizados da nota."""
    vl_doc:      float = 0.0
    vl_merc:     float = 0.0
    vl_frete:    float = 0.0
    vl_seg:      float = 0.0
    vl_out_da:   float = 0.0
    vl_bc_icms:  float = 0.0
    vl_icms:     float = 0.0
    vl_bc_st:    float = 0.0
    vl_icms_st:  float = 0.0
    vl_ipi:      float = 0.0
    vl_pis:      float = 0.0
    vl_cofins:   float = 0.0


@dataclass
class ParticipanteNormalizado:
    """Emitente ou destinatario de forma uniforme."""
    cnpj: str = ""
    cpf:  str = ""
    nome: str = ""
    ie:   str = ""
    uf:   str = ""
    crt:  str = ""   # 1=SN, 3=Lucro Real/Presumido (so disponivel na NF-e)


@dataclass
class DadoFiscalNormalizado:
    """
    Representacao uniforme de um documento fiscal.
    Produzida por normalizar_nfe() ou normalizar_sped().
    Consumida por ICMSAnalyzer.analisar(), ICMSSTAnalyzer e NCMAnalyzer.
    """
    # Identificacao
    origem:         str = ""   # "nfe" | "nfce" | "sped"
    chave:          str = ""
    numero:         str = ""
    serie:          str = ""
    modelo:         str = ""
    nat_op:         str = ""
    dt_emissao:     str = ""
    dt_saida_entrada: str = ""
    tipo_nf:        str = ""   # "0"=entrada, "1"=saida
    cod_sit:        str = ""   # Situacao: 00=regular, 02=cancelada etc.
    caminho_arquivo: str = ""

    # Participantes
    emitente:      ParticipanteNormalizado = field(default_factory=ParticipanteNormalizado)
    destinatario:  ParticipanteNormalizado = field(default_factory=ParticipanteNormalizado)

    # Conteudo
    itens:   List[ItemNormalizado]  = field(default_factory=list)
    totais:  TotaisNormalizados     = field(default_factory=TotaisNormalizados)

    # Resumo por CFOP+CST+Aliquota (disponivel no SPED via C190, opcional na NF-e)
    resumo_c190: List[dict] = field(default_factory=list)

    # Erros de leitura repassados do parser
    erros_leitura: List[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# CONVERSORES
# ─────────────────────────────────────────────────────────────────────────────

def normalizar_nfe(nfe, caminho: str = "") -> DadoFiscalNormalizado:
    """
    Converte um objeto NFe (parsers/nfe_parser.py) para DadoFiscalNormalizado.
    Funciona para NF-e (mod.55) e NFC-e (mod.65).
    """
    dado = DadoFiscalNormalizado(
        origem          = "nfe" if nfe.modelo != "65" else "nfce",
        chave           = nfe.chave,
        numero          = nfe.numero,
        serie           = nfe.serie,
        modelo          = nfe.modelo,
        nat_op          = nfe.nat_op,
        dt_emissao      = nfe.dt_emis,
        dt_saida_entrada= nfe.dt_saida_entrada,
        tipo_nf         = nfe.tipo_nf,
        caminho_arquivo = caminho,
        erros_leitura   = nfe.erros_leitura,
    )

    dado.emitente = ParticipanteNormalizado(
        cnpj = nfe.emitente.cnpj,
        nome = nfe.emitente.nome,
        ie   = nfe.emitente.ie,
        uf   = nfe.emitente.endereco.uf,
        crt  = nfe.emitente.crt,
    )
    dado.destinatario = ParticipanteNormalizado(
        cnpj = nfe.destinatario.cnpj,
        nome = nfe.destinatario.nome,
        ie   = nfe.destinatario.ie,
        uf   = nfe.destinatario.endereco.uf,
    )

    for item in nfe.itens:
        dado.itens.append(ItemNormalizado(
            num_item   = item.num_item,
            cod_item   = item.cod_prod,
            descricao  = item.descricao,
            ncm        = item.ncm,
            cfop       = item.cfop,
            unid       = item.unid,
            qtd        = item.qtd,
            vl_item    = item.vl_total_bruto,
            vl_desc    = item.vl_desc,
            cst_icms   = item.icms.cst,
            vl_bc_icms = item.icms.vl_bc,
            aliq_icms  = item.icms.aliq,
            vl_icms    = item.icms.vl_icms,
            vl_bc_st   = item.icms.vl_bc_st,
            aliq_st    = item.icms.aliq_st,
            vl_icms_st = item.icms.vl_icms_st,
            p_mva_st   = item.icms.p_mva_st,
            p_red_bc   = item.icms.p_red_bc,
            cst_ipi    = item.cst_ipi,
            aliq_ipi   = item.aliq_ipi,
            vl_ipi     = item.vl_ipi,
            orig       = item.icms.orig,
            vl_bc_fcp  = item.icms.vl_bc_fcp,
            p_fcp      = item.icms.p_fcp,
            vl_fcp     = item.icms.vl_fcp,
        ))

    t = nfe.totais
    dado.totais = TotaisNormalizados(
        vl_doc     = t.vl_nf,
        vl_merc    = t.vl_prod,
        vl_frete   = t.vl_frete,
        vl_seg     = t.vl_seg,
        vl_bc_icms = t.vl_bc_icms,
        vl_icms    = t.vl_icms,
        vl_bc_st   = t.vl_bc_icms_st,
        vl_icms_st = t.vl_icms_st,
        vl_ipi     = t.vl_ipi,
    )

    return dado


def normalizar_sped(nota, produtos: dict, caminho: str = "") -> DadoFiscalNormalizado:
    """
    Converte um SpedNotaFiscal (parsers/sped_parser.py) para DadoFiscalNormalizado.
    produtos: dict cod_item -> SpedProduto (do SpedEFD.produtos)
    """
    dado = DadoFiscalNormalizado(
        origem           = "sped",
        chave            = nota.chv_nfe,
        numero           = nota.num_doc,
        serie            = nota.ser,
        modelo           = nota.cod_mod,
        dt_emissao       = nota.dt_doc,
        dt_saida_entrada = nota.dt_e_s,
        tipo_nf          = nota.ind_oper,   # "0"=entrada, "1"=saida no SPED
        cod_sit          = nota.cod_sit,
        caminho_arquivo  = caminho,
    )

    # Emitente/Destinatario: no SPED ind_emit=0 (propria) vs 1 (terceiro)
    # Apenas indicamos o participante; CNPJ real esta no 0150
    dado.emitente    = ParticipanteNormalizado()
    dado.destinatario = ParticipanteNormalizado()

    for item in nota.itens:
        prod = produtos.get(item.cod_item)
        ncm  = prod.cod_ncm if prod else ""
        dado.itens.append(ItemNormalizado(
            num_item   = item.num_item,
            cod_item   = item.cod_item,
            descricao  = item.descr_compl or (prod.descr_item if prod else ""),
            ncm        = ncm,
            cfop       = item.cfop,
            unid       = item.unid,
            qtd        = item.qtd,
            vl_item    = item.vl_item,
            vl_desc    = item.vl_desc,
            cst_icms   = item.cst_icms,
            vl_bc_icms = item.vl_bc_icms,
            aliq_icms  = item.aliq_icms,
            vl_icms    = item.vl_icms,
            vl_bc_st   = item.vl_bc_icms_st,
            aliq_st    = item.aliq_st,
            vl_icms_st = item.vl_icms_st,
        ))

    dado.totais = TotaisNormalizados(
        vl_doc     = nota.vl_doc,
        vl_frete   = nota.vl_frt,
        vl_seg     = nota.vl_seg,
        vl_bc_icms = nota.vl_bc_icms,
        vl_icms    = nota.vl_icms,
        vl_bc_st   = nota.vl_bc_icms_st,
        vl_icms_st = nota.vl_icms_st,
        vl_ipi     = nota.vl_ipi,
    )

    dado.resumo_c190 = nota.totais_c190

    return dado
