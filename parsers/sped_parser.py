# parsers/sped_parser.py
"""
Lê e estrutura arquivos SPED Fiscal (EFD ICMS/IPI) .txt
Registros suportados: 0000, 0150, 0190, 0200, C100, C170, C190,
                      E110, E111, E116, H005, H010
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional
from config import ENCODING_SPED


# ─────────────────────────────────────────────
# DATACLASSES - estruturas de dados do SPED
# ─────────────────────────────────────────────

@dataclass
class SpedAbertura:
    """Registro 0000 - Abertura do arquivo"""
    cod_ver: str = ""
    tipo_escrituracao: str = ""
    ind_sit_esp: str = ""
    num_rec_anterior: str = ""
    dt_ini: str = ""
    dt_fin: str = ""
    nome: str = ""
    cnpj: str = ""
    cpf: str = ""
    uf: str = ""
    ie: str = ""
    cod_mun: str = ""
    im: str = ""
    suframa: str = ""
    ind_perfil: str = ""
    ind_ativ: str = ""


@dataclass
class SpedParticipante:
    """Registro 0150 - Participantes"""
    cod_part: str = ""
    nome: str = ""
    cod_pais: str = ""
    cnpj: str = ""
    cpf: str = ""
    ie: str = ""
    cod_mun: str = ""
    suframa: str = ""
    end: str = ""
    num: str = ""
    compl: str = ""
    bairro: str = ""


@dataclass
class SpedUnidade:
    """Registro 0190 - Unidades de medida"""
    unid: str = ""
    descr: str = ""


@dataclass
class SpedProduto:
    """Registro 0200 - Produtos"""
    cod_item: str = ""
    descr_item: str = ""
    cod_barra: str = ""
    cod_ant_item: str = ""
    unid_inv: str = ""
    tipo_item: str = ""
    cod_ncm: str = ""
    ex_ipi: str = ""
    cod_gen: str = ""
    cod_lst: str = ""
    aliq_icms: float = 0.0


@dataclass
class SpedItemNota:
    """Registro C170 - Itens da nota fiscal"""
    num_item: str = ""
    cod_item: str = ""
    descr_compl: str = ""
    qtd: float = 0.0
    unid: str = ""
    vl_item: float = 0.0
    vl_desc: float = 0.0
    ind_mov: str = ""
    cst_icms: str = ""
    cfop: str = ""
    cod_nat: str = ""
    vl_bc_icms: float = 0.0
    aliq_icms: float = 0.0
    vl_icms: float = 0.0
    vl_bc_icms_st: float = 0.0
    aliq_st: float = 0.0
    vl_icms_st: float = 0.0
    ind_apur: str = ""
    cst_ipi: str = ""
    cod_enq: str = ""
    vl_bc_ipi: float = 0.0
    aliq_ipi: float = 0.0
    vl_ipi: float = 0.0
    cst_pis: str = ""
    vl_bc_pis: float = 0.0
    aliq_pis_perc: float = 0.0
    quant_bc_pis: float = 0.0
    aliq_pis_r: float = 0.0
    vl_pis: float = 0.0
    cst_cofins: str = ""
    vl_bc_cofins: float = 0.0
    aliq_cofins_perc: float = 0.0
    quant_bc_cofins: float = 0.0
    aliq_cofins_r: float = 0.0
    vl_cofins: float = 0.0
    cod_cta: str = ""


@dataclass
class SpedNotaFiscal:
    """Registro C100 - Nota Fiscal"""
    ind_oper: str = ""          # 0=Entrada, 1=Saída
    ind_emit: str = ""          # 0=Emissão própria, 1=Terceiros
    cod_part: str = ""
    cod_mod: str = ""
    cod_sit: str = ""
    ser: str = ""
    num_doc: str = ""
    chv_nfe: str = ""
    dt_doc: str = ""
    dt_e_s: str = ""
    vl_doc: float = 0.0
    ind_pgto: str = ""
    vl_desc: float = 0.0
    vl_abat_nt: float = 0.0
    vl_merc: float = 0.0
    ind_frt: str = ""
    vl_frt: float = 0.0
    vl_seg: float = 0.0
    vl_out_da: float = 0.0
    vl_bc_icms: float = 0.0
    vl_icms: float = 0.0
    vl_bc_icms_st: float = 0.0
    vl_icms_st: float = 0.0
    vl_ipi: float = 0.0
    vl_pis: float = 0.0
    vl_cofins: float = 0.0
    vl_pis_st: float = 0.0
    vl_cofins_st: float = 0.0
    itens: List[SpedItemNota] = field(default_factory=list)
    totais_c190: List[Dict] = field(default_factory=list)


@dataclass
class SpedEFD:
    """Estrutura completa do arquivo SPED"""
    caminho: str = ""
    abertura: Optional[SpedAbertura] = None
    participantes: Dict[str, SpedParticipante] = field(default_factory=dict)
    unidades: Dict[str, SpedUnidade] = field(default_factory=dict)
    produtos: Dict[str, SpedProduto] = field(default_factory=dict)
    notas: List[SpedNotaFiscal] = field(default_factory=list)
    erros_leitura: List[str] = field(default_factory=list)
    total_linhas: int = 0


# ─────────────────────────────────────────────
# FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────

def _float(valor: str) -> float:
    """Converte string SPED para float, tratando vazio e vírgula."""
    try:
        return float(valor.replace(",", ".").strip()) if valor.strip() else 0.0
    except ValueError:
        return 0.0


def _campos(linha: str) -> List[str]:
    """Divide linha SPED pelo pipe, removendo o primeiro e último vazio."""
    partes = linha.strip().split("|")
    return partes[1:-1] if len(partes) > 2 else partes


# ─────────────────────────────────────────────
# PARSER PRINCIPAL
# ─────────────────────────────────────────────

class SpedParser:
    """
    Parser para EFD ICMS/IPI (SPED Fiscal).
    Lê o arquivo linha a linha mantendo contexto do registro atual.
    """

    def __init__(self, caminho: str):
        self.caminho = caminho
        self.efd = SpedEFD(caminho=caminho)
        self._nota_atual: Optional[SpedNotaFiscal] = None

    def parse(self) -> SpedEFD:
        """Lê o arquivo SPED e retorna estrutura populada."""
        path = Path(self.caminho)
        if not path.exists():
            self.efd.erros_leitura.append(f"Arquivo não encontrado: {self.caminho}")
            return self.efd

        try:
            with open(path, encoding=ENCODING_SPED, errors="replace") as f:
                linhas = f.readlines()
        except Exception as e:
            self.efd.erros_leitura.append(f"Erro ao abrir arquivo: {e}")
            return self.efd

        self.efd.total_linhas = len(linhas)

        for num_linha, linha in enumerate(linhas, 1):
            linha = linha.strip()
            if not linha:
                continue

            c = _campos(linha)
            if not c:
                continue

            registro = c[0].upper()

            try:
                if registro == "0000":
                    self._parse_0000(c)
                elif registro == "0150":
                    self._parse_0150(c)
                elif registro == "0190":
                    self._parse_0190(c)
                elif registro == "0200":
                    self._parse_0200(c)
                elif registro == "C100":
                    self._parse_c100(c)
                elif registro == "C170":
                    self._parse_c170(c)
                elif registro == "C190":
                    self._parse_c190(c)
            except Exception as e:
                self.efd.erros_leitura.append(
                    f"Linha {num_linha} ({registro}): {e}"
                )

        # Fecha última nota pendente
        if self._nota_atual:
            self.efd.notas.append(self._nota_atual)

        return self.efd

    def _parse_0000(self, c: List[str]):
        ab = SpedAbertura()
        ab.cod_ver           = c[1] if len(c) > 1 else ""
        ab.tipo_escrituracao = c[2] if len(c) > 2 else ""
        ab.dt_ini            = c[4] if len(c) > 4 else ""
        ab.dt_fin            = c[5] if len(c) > 5 else ""
        ab.nome              = c[6] if len(c) > 6 else ""
        ab.cnpj              = c[7] if len(c) > 7 else ""
        ab.uf                = c[9] if len(c) > 9 else ""
        ab.ie                = c[10] if len(c) > 10 else ""
        self.efd.abertura = ab

    def _parse_0150(self, c: List[str]):
        p = SpedParticipante()
        p.cod_part = c[1] if len(c) > 1 else ""
        p.nome     = c[2] if len(c) > 2 else ""
        p.cod_pais = c[3] if len(c) > 3 else ""
        p.cnpj     = c[4] if len(c) > 4 else ""
        p.cpf      = c[5] if len(c) > 5 else ""
        p.ie       = c[6] if len(c) > 6 else ""
        p.cod_mun  = c[7] if len(c) > 7 else ""
        self.efd.participantes[p.cod_part] = p

    def _parse_0190(self, c: List[str]):
        u = SpedUnidade()
        u.unid  = c[1] if len(c) > 1 else ""
        u.descr = c[2] if len(c) > 2 else ""
        self.efd.unidades[u.unid] = u

    def _parse_0200(self, c: List[str]):
        p = SpedProduto()
        p.cod_item   = c[1] if len(c) > 1 else ""
        p.descr_item = c[2] if len(c) > 2 else ""
        p.unid_inv   = c[5] if len(c) > 5 else ""
        p.tipo_item  = c[6] if len(c) > 6 else ""
        p.cod_ncm    = c[7] if len(c) > 7 else ""
        p.aliq_icms  = _float(c[12]) if len(c) > 12 else 0.0
        self.efd.produtos[p.cod_item] = p

    def _parse_c100(self, c: List[str]):
        # Salva nota anterior se existir
        if self._nota_atual:
            self.efd.notas.append(self._nota_atual)

        n = SpedNotaFiscal()
        n.ind_oper      = c[1]  if len(c) > 1  else ""
        n.ind_emit      = c[2]  if len(c) > 2  else ""
        n.cod_part      = c[3]  if len(c) > 3  else ""
        n.cod_mod       = c[4]  if len(c) > 4  else ""
        n.cod_sit       = c[5]  if len(c) > 5  else ""
        n.ser           = c[6]  if len(c) > 6  else ""
        n.num_doc       = c[7]  if len(c) > 7  else ""
        n.chv_nfe       = c[8]  if len(c) > 8  else ""
        n.dt_doc        = c[9]  if len(c) > 9  else ""
        n.dt_e_s        = c[10] if len(c) > 10 else ""
        n.vl_doc        = _float(c[11]) if len(c) > 11 else 0.0
        n.vl_bc_icms    = _float(c[19]) if len(c) > 19 else 0.0
        n.vl_icms       = _float(c[20]) if len(c) > 20 else 0.0
        n.vl_bc_icms_st = _float(c[21]) if len(c) > 21 else 0.0
        n.vl_icms_st    = _float(c[22]) if len(c) > 22 else 0.0
        n.vl_ipi        = _float(c[23]) if len(c) > 23 else 0.0
        self._nota_atual = n

    def _parse_c170(self, c: List[str]):
        if not self._nota_atual:
            return

        i = SpedItemNota()
        i.num_item       = c[1]  if len(c) > 1  else ""
        i.cod_item       = c[2]  if len(c) > 2  else ""
        i.descr_compl    = c[3]  if len(c) > 3  else ""
        i.qtd            = _float(c[4])  if len(c) > 4  else 0.0
        i.unid           = c[5]  if len(c) > 5  else ""
        i.vl_item        = _float(c[6])  if len(c) > 6  else 0.0
        i.vl_desc        = _float(c[7])  if len(c) > 7  else 0.0
        i.cst_icms       = c[9]  if len(c) > 9  else ""
        i.cfop           = c[10] if len(c) > 10 else ""
        i.vl_bc_icms     = _float(c[11]) if len(c) > 11 else 0.0
        i.aliq_icms      = _float(c[12]) if len(c) > 12 else 0.0
        i.vl_icms        = _float(c[13]) if len(c) > 13 else 0.0
        i.vl_bc_icms_st  = _float(c[14]) if len(c) > 14 else 0.0
        i.aliq_st        = _float(c[15]) if len(c) > 15 else 0.0
        i.vl_icms_st     = _float(c[16]) if len(c) > 16 else 0.0
        self._nota_atual.itens.append(i)

    def _parse_c190(self, c: List[str]):
        if not self._nota_atual:
            return
        total = {
            "cst_icms":      c[1]  if len(c) > 1  else "",
            "cfop":          c[2]  if len(c) > 2  else "",
            "aliq_icms":     _float(c[3]) if len(c) > 3 else 0.0,
            "vl_opr":        _float(c[4]) if len(c) > 4 else 0.0,
            "vl_bc_icms":    _float(c[5]) if len(c) > 5 else 0.0,
            "vl_icms":       _float(c[6]) if len(c) > 6 else 0.0,
            "vl_bc_icms_st": _float(c[7]) if len(c) > 7 else 0.0,
            "vl_icms_st":    _float(c[8]) if len(c) > 8 else 0.0,
            "vl_red_bc":     _float(c[9]) if len(c) > 9 else 0.0,
            "cod_obs":       c[10] if len(c) > 10 else "",
        }
        self._nota_atual.totais_c190.append(total)
