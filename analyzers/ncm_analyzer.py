# analyzers/icms_st_analyzer.py
"""
Analise de ICMS por Substituicao Tributaria (ICMS-ST) - MT.

MVAs carregados de data/mva_mt.csv (versionavel, editavel sem alterar codigo).
Formato do CSV:
  ncm_prefixo, descricao, mva_interno, mva_ajustado_12,
  aliq_interna, ativo, fonte

Para atualizar MVAs: edite data/mva_mt.csv e reinicie o sistema.
Nao e necessario alterar este arquivo.

Referencias:
  - Protocolo ICMS 41/2008 (combustiveis)
  - Convenio ICMS 142/2018 (bebidas)
  - Protocolo ICMS 76/2014 (medicamentos)
  - TARE-MT (Termo de Acordo de Regime Especial)
"""

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import TOLERANCIA_PERCENTUAL, CST_COM_ST, DATA_DIR

log = logging.getLogger(__name__)

# Caminho do CSV de MVAs (versionavel)
MVA_CSV_PATH: Path = DATA_DIR / "mva_mt.csv"


@dataclass
class ResultadoST:
    """Resultado da analise de ST de um item."""
    num_item: str
    descricao: str
    ncm: str
    cfop: str
    tem_st: bool = False
    divergencias_st: List[str] = field(default_factory=list)
    orientacoes_st: List[str] = field(default_factory=list)
    mva_informado: float = 0.0
    mva_esperado: float = 0.0
    vl_bc_st_calculado: float = 0.0
    vl_bc_st_informado: float = 0.0
    vl_st_calculado: float = 0.0
    vl_st_informado: float = 0.0


def _carregar_mva_csv(csv_path: Path) -> Dict[str, Tuple[float, float, float, str]]:
    """
    Carrega tabela MVA do CSV.
    Retorna: {ncm_prefixo: (mva_interno, mva_ajustado_12, aliq_interna, descricao)}
    Ignora linhas com ativo != "S".
    """
    tabela: Dict[str, Tuple[float, float, float, str]] = {}
    if not csv_path.exists():
        log.warning("Arquivo MVA nao encontrado: %s", csv_path)
        return tabela
    try:
        with open(csv_path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("ativo", "S").upper() != "S":
                    continue
                ncm = row["ncm_prefixo"].strip()
                tabela[ncm] = (
                    float(row["mva_interno"]),
                    float(row["mva_ajustado_12"]),
                    float(row.get("aliq_interna", 17.0)),
                    row.get("descricao", ""),
                )
        log.debug("MVA-MT carregado: %d registros de %s", len(tabela), csv_path)
    except Exception as exc:
        log.error("Erro ao carregar MVA CSV: %s", exc)
    return tabela


class ICMSSTAnalyzer:
    """
    Analisa ICMS-ST em NF-e e SPED.
    Verifica MVA, base de cálculo e valor do ST.
    """

    def __init__(self, uf: str):
        self.uf = "MT"  # sistema opera apenas com MT
        self.tabela_mva = _carregar_mva_csv(MVA_CSV_PATH)
        # Alíquota interna padrão para cálculo do ST
        self.aliq_interna = 17.0

    # ─────────────────────────────────────────
    # ANÁLISE NF-e
    # ─────────────────────────────────────────

    def analisar_itens_nfe(self, nfe) -> List[ResultadoST]:
        """Analisa todos os itens de uma NF-e para ICMS-ST."""
        resultados = []
        uf_emit = nfe.emitente.endereco.uf.upper()
        is_interestadual = uf_emit != self.uf

        for item in nfe.itens:
            res = self._analisar_item(
                num_item=item.num_item,
                descricao=item.descricao,
                ncm=item.ncm,
                cfop=item.cfop,
                cst=item.icms.cst,
                vl_prod=item.vl_total_bruto,
                vl_frete=0.0,
                vl_outros=0.0,
                aliq_interna=self.aliq_interna,
                aliq_origem=item.icms.aliq,
                mva_informado=item.icms.p_mva_st,
                vl_bc_st_informado=item.icms.vl_bc_st,
                aliq_st_informado=item.icms.aliq_st,
                vl_st_informado=item.icms.vl_icms_st,
                is_interestadual=is_interestadual,
            )
            resultados.append(res)

        return resultados

    # ─────────────────────────────────────────
    # ANÁLISE SPED
    # ─────────────────────────────────────────

    def analisar_item_sped(self, item, is_interestadual: bool,
                           vl_prod: float, aliq_icms_origem: float) -> ResultadoST:
        """Analisa um SpedItemNota para ICMS-ST."""
        ncm = ""  # Buscar do 0200 se necessário
        return self._analisar_item(
            num_item=item.num_item,
            descricao=item.descr_compl or item.cod_item,
            ncm=ncm,
            cfop=item.cfop,
            cst=item.cst_icms,
            vl_prod=vl_prod,
            vl_frete=0.0,
            vl_outros=0.0,
            aliq_interna=self.aliq_interna,
            aliq_origem=aliq_icms_origem,
            mva_informado=item.aliq_st,
            vl_bc_st_informado=item.vl_bc_icms_st,
            aliq_st_informado=item.aliq_st,
            vl_st_informado=item.vl_icms_st,
            is_interestadual=is_interestadual,
        )

    # ─────────────────────────────────────────
    # ANÁLISE CORE
    # ─────────────────────────────────────────


    def analisar_itens(self, dado) -> list:
        """
        Metodo unificado para DadoFiscalNormalizado.
        Substitui analisar_itens_nfe() e analisar_item_sped() no pipeline.
        """
        from normalizer import DadoFiscalNormalizado
        resultados = []
        uf_emit = dado.emitente.uf.upper() if dado.emitente.uf else ""
        is_interestadual = bool(uf_emit and uf_emit != self.uf)
        for item in dado.itens:
            res = self._analisar_item(
                num_item=item.num_item,
                descricao=item.descricao,
                ncm=item.ncm,
                cfop=item.cfop,
                cst=item.cst_icms,
                vl_prod=item.vl_item,
                vl_frete=0.0,
                vl_outros=0.0,
                aliq_interna=self.aliq_interna,
                aliq_origem=item.aliq_icms,
                mva_informado=item.p_mva_st,
                vl_bc_st_informado=item.vl_bc_st,
                aliq_st_informado=item.aliq_st,
                vl_st_informado=item.vl_icms_st,
                is_interestadual=is_interestadual,
            )
            resultados.append(res)
        return resultados

    def _analisar_item(
        self,
        num_item: str,
        descricao: str,
        ncm: str,
        cfop: str,
        cst: str,
        vl_prod: float,
        vl_frete: float,
        vl_outros: float,
        aliq_interna: float,
        aliq_origem: float,
        mva_informado: float,
        vl_bc_st_informado: float,
        aliq_st_informado: float,
        vl_st_informado: float,
        is_interestadual: bool,
    ) -> ResultadoST:

        res = ResultadoST(
            num_item=num_item,
            descricao=descricao[:50],
            ncm=ncm,
            cfop=cfop,
            mva_informado=mva_informado,
            vl_bc_st_informado=vl_bc_st_informado,
            vl_st_informado=vl_st_informado,
        )

        ncm_prefixo = self._ncm_prefixo(ncm)
        deve_ter_st = ncm_prefixo in self.tabela_mva or cst in CST_COM_ST

        # 1) Produto sujeito a ST mas CST não indica ST
        if ncm_prefixo in self.tabela_mva and cst and cst not in CST_COM_ST and cst not in ("60",):
            res.tem_st = True
            res.divergencias_st.append(
                f"NCM {ncm} ({self.tabela_mva[ncm_prefixo][2]}) está sujeito a ST "
                f"em {self.uf}, mas CST informado é {cst}."
            )
            res.orientacoes_st.append(
                f"Para este produto em {self.uf}, utilize CST 10 (saída com retenção ST) "
                f"ou CST 60 (ICMS-ST já recolhido) conforme posição na cadeia."
            )

        # 2) Produto com ST: verifica MVA e cálculo
        if cst in CST_COM_ST and ncm_prefixo in self.tabela_mva:
            res.tem_st = True
            mva_interno, mva_ajustado, _aliq_csv, descr_ncm = self.tabela_mva[ncm_prefixo]
            mva_usar = mva_ajustado if is_interestadual else mva_interno
            res.mva_esperado = mva_usar

            # MVA informado muito diferente do esperado
            if mva_informado > 0 and abs(mva_informado - mva_usar) > 2.0:
                res.divergencias_st.append(
                    f"MVA informado ({mva_informado:.2f}%) difere do esperado "
                    f"({mva_usar:.2f}%) para {descr_ncm} em {self.uf}."
                )
                res.orientacoes_st.append(
                    f"Utilize MVA {'ajustado' if is_interestadual else 'interno'} "
                    f"de {mva_usar:.2f}% conforme tabela SEFAZ-{self.uf} para NCM {ncm}. "
                    f"Verifique portaria vigente pois MVAs podem ser atualizados."
                )

            # Calcula BC-ST esperada
            bc_proprio = vl_prod + vl_frete + vl_outros
            bc_st_calc = round(bc_proprio * (1 + mva_usar / 100), 2)
            res.vl_bc_st_calculado = bc_st_calc

            # Calcula ST esperado
            icms_proprio = round(bc_proprio * aliq_origem / 100, 2) if aliq_origem > 0 else 0.0
            st_calc = round(bc_st_calc * aliq_interna / 100 - icms_proprio, 2)
            res.vl_st_calculado = max(st_calc, 0.0)

            # Verifica BC-ST informada
            if vl_bc_st_informado > 0:
                if abs(vl_bc_st_informado - bc_st_calc) > 1.0:
                    res.divergencias_st.append(
                        f"BC-ST informada (R$ {vl_bc_st_informado:.2f}) difere "
                        f"da calculada (R$ {bc_st_calc:.2f}) usando MVA {mva_usar:.2f}%."
                    )
                    res.orientacoes_st.append(
                        f"Recalcule: BC-ST = (Vl.Prod + Frete + Outros) × (1 + MVA/100). "
                        f"BC-ST esperada: R$ {bc_st_calc:.2f}."
                    )

            # Verifica valor ST informado
            if vl_st_informado > 0:
                if abs(vl_st_informado - res.vl_st_calculado) > 1.0:
                    res.divergencias_st.append(
                        f"ICMS-ST informado (R$ {vl_st_informado:.2f}) difere "
                        f"do calculado (R$ {res.vl_st_calculado:.2f})."
                    )
                    res.orientacoes_st.append(
                        f"ST = (BC-ST × Alíq. interna {aliq_interna}%) - ICMS próprio. "
                        f"ST esperado: R$ {res.vl_st_calculado:.2f}."
                    )

        # 3) ST informado mas NCM não está na tabela do estado
        if vl_st_informado > 0 and ncm_prefixo not in self.tabela_mva:
            res.divergencias_st.append(
                f"ICMS-ST informado (R$ {vl_st_informado:.2f}) mas NCM {ncm} "
                f"não consta na tabela ST de {self.uf}."
            )
            res.orientacoes_st.append(
                f"Verifique se o produto NCM {ncm} possui protocolo ou convênio ST "
                f"específico para {self.uf}. Consulte a SEFAZ-{self.uf} ou o RICMS vigente. "
                f"Se não houver ST para este NCM, zere os campos de ST."
            )

        return res

    def _ncm_prefixo(self, ncm: str) -> str:
        """Retorna os 4 primeiros dígitos do NCM para busca na tabela."""
        ncm_limpo = ncm.replace(".", "").replace("-", "").strip()
        return ncm_limpo[:4] if len(ncm_limpo) >= 4 else ncm_limpo

    def resumo_st(self, resultados: List[ResultadoST]) -> Dict:
        """Gera resumo consolidado da análise ST."""
        total_st_doc = sum(r.vl_st_informado for r in resultados)
        total_st_calc = sum(r.vl_st_calculado for r in resultados)
        itens_com_diverg = [r for r in resultados if r.divergencias_st]
        return {
            "total_itens": len(resultados),
            "itens_com_st": sum(1 for r in resultados if r.tem_st),
            "itens_com_divergencia": len(itens_com_diverg),
            "total_st_documento": total_st_doc,
            "total_st_calculado": total_st_calc,
            "diferenca_st": abs(total_st_doc - total_st_calc),
        }