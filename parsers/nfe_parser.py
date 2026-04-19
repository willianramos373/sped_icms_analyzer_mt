# parsers/nfe_parser.py
"""
Lê NF-e (modelo 55) e NFC-e (modelo 65) em XML.
Suporta versões 4.00 e 3.10 do layout.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from lxml import etree
from config import ENCODING_XML


# ─────────────────────────────────────────────
# DATACLASSES
# ─────────────────────────────────────────────

@dataclass
class NFeEndereco:
    logradouro: str = ""
    numero: str = ""
    bairro: str = ""
    cod_mun: str = ""
    mun: str = ""
    uf: str = ""
    cep: str = ""
    cod_pais: str = ""
    fone: str = ""


@dataclass
class NFeEmitente:
    cnpj: str = ""
    cpf: str = ""
    nome: str = ""
    nome_fantasia: str = ""
    ie: str = ""
    crt: str = ""           # 1=SN, 3=LP
    endereco: NFeEndereco = field(default_factory=NFeEndereco)


@dataclass
class NFeDestinatario:
    cnpj: str = ""
    cpf: str = ""
    nome: str = ""
    ie: str = ""
    ind_ie_dest: str = ""   # 1=contribuinte, 2=isento, 9=não contribuinte
    endereco: NFeEndereco = field(default_factory=NFeEndereco)


@dataclass
class NFeICMS:
    cst: str = ""           # CST ou CSOSN (SN)
    orig: str = ""          # 0=Nacional, 1=Estrangeiro
    mod_bc: str = ""
    vl_bc: float = 0.0
    aliq: float = 0.0
    vl_icms: float = 0.0
    # ST
    mod_bc_st: str = ""
    p_mva_st: float = 0.0
    p_red_bc_st: float = 0.0
    vl_bc_st: float = 0.0
    aliq_st: float = 0.0
    vl_icms_st: float = 0.0
    # Redução BC
    p_red_bc: float = 0.0
    # DIFAL
    vl_bc_fcp: float = 0.0
    p_fcp: float = 0.0
    vl_fcp: float = 0.0


@dataclass
class NFeItem:
    num_item: str = ""
    cod_prod: str = ""
    cod_ean: str = ""
    descricao: str = ""
    ncm: str = ""
    cfop: str = ""
    unid: str = ""
    qtd: float = 0.0
    vl_unit: float = 0.0
    vl_total_bruto: float = 0.0
    vl_desc: float = 0.0
    vl_total: float = 0.0
    ind_tot: str = ""
    icms: NFeICMS = field(default_factory=NFeICMS)
    # IPI
    cst_ipi: str = ""
    vl_ipi: float = 0.0
    aliq_ipi: float = 0.0


@dataclass
class NFeTotais:
    vl_bc_icms: float = 0.0
    vl_icms: float = 0.0
    vl_icms_deson: float = 0.0
    vl_bc_icms_st: float = 0.0
    vl_icms_st: float = 0.0
    vl_prod: float = 0.0
    vl_frete: float = 0.0
    vl_seg: float = 0.0
    vl_desc: float = 0.0
    vl_ipi: float = 0.0
    vl_nf: float = 0.0


@dataclass
class NFe:
    """Estrutura de uma NF-e ou NFC-e"""
    caminho: str = ""
    chave: str = ""
    modelo: str = ""        # "55" ou "65"
    serie: str = ""
    numero: str = ""
    nat_op: str = ""
    dt_emis: str = ""
    dt_saida_entrada: str = ""
    tipo_nf: str = ""       # "0"=entrada, "1"=saída
    uf_emissao: str = ""
    versao: str = ""
    emitente: NFeEmitente = field(default_factory=NFeEmitente)
    destinatario: NFeDestinatario = field(default_factory=NFeDestinatario)
    itens: List[NFeItem] = field(default_factory=list)
    totais: NFeTotais = field(default_factory=NFeTotais)
    erros_leitura: List[str] = field(default_factory=list)


# ─────────────────────────────────────────────
# NAMESPACES NFe
# ─────────────────────────────────────────────
NS = {
    "nfe": "http://www.portalfiscal.inf.br/nfe"
}


def _txt(elemento, xpath: str, ns=NS) -> str:
    """Busca texto em xpath, retorna '' se não encontrar."""
    resultado = elemento.find(xpath, ns)
    return resultado.text.strip() if resultado is not None and resultado.text else ""


def _flt(elemento, xpath: str, ns=NS) -> float:
    """Busca float em xpath, retorna 0.0 se não encontrar."""
    try:
        return float(_txt(elemento, xpath, ns))
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────
# PARSER
# ─────────────────────────────────────────────

class NFeParser:
    """
    Parser para NF-e (mod 55) e NFC-e (mod 65) em XML.
    Detecta automaticamente o modelo pelo conteúdo.
    """

    def __init__(self, caminho: str):
        self.caminho = caminho

    def parse(self) -> NFe:
        """Lê o XML e retorna estrutura NFe populada."""
        nfe = NFe(caminho=self.caminho)
        path = Path(self.caminho)

        if not path.exists():
            nfe.erros_leitura.append(f"Arquivo não encontrado: {self.caminho}")
            return nfe

        try:
            tree = etree.parse(str(path))
            root = tree.getroot()
        except etree.XMLSyntaxError as e:
            nfe.erros_leitura.append(f"XML inválido: {e}")
            return nfe

        # Remove namespace para facilitar busca, ou usa o ns definido
        # Tenta localizar o elemento infNFe
        inf_nfe = (
            root.find(".//nfe:infNFe", NS) or
            root.find(".//{http://www.portalfiscal.inf.br/nfe}infNFe")
        )
        if inf_nfe is None:
            nfe.erros_leitura.append("Elemento infNFe não encontrado no XML.")
            return nfe

        nfe.versao = inf_nfe.get("versao", "")
        nfe.chave = inf_nfe.get("Id", "").replace("NFe", "")

        # IDE
        ide = inf_nfe.find("nfe:ide", NS)
        if ide is not None:
            nfe.modelo           = _txt(ide, "nfe:mod")
            nfe.serie            = _txt(ide, "nfe:serie")
            nfe.numero           = _txt(ide, "nfe:nNF")
            nfe.nat_op           = _txt(ide, "nfe:natOp")
            nfe.dt_emis          = _txt(ide, "nfe:dhEmi") or _txt(ide, "nfe:dEmi")
            nfe.dt_saida_entrada = _txt(ide, "nfe:dhSaiEnt") or _txt(ide, "nfe:dSaiEnt")
            nfe.tipo_nf          = _txt(ide, "nfe:tpNF")
            nfe.uf_emissao       = _txt(ide, "nfe:cUF")

        # EMITENTE
        emit = inf_nfe.find("nfe:emit", NS)
        if emit is not None:
            nfe.emitente.cnpj         = _txt(emit, "nfe:CNPJ")
            nfe.emitente.cpf          = _txt(emit, "nfe:CPF")
            nfe.emitente.nome         = _txt(emit, "nfe:xNome")
            nfe.emitente.nome_fantasia = _txt(emit, "nfe:xFant")
            nfe.emitente.ie           = _txt(emit, "nfe:IE")
            nfe.emitente.crt          = _txt(emit, "nfe:CRT")
            end = emit.find("nfe:enderEmit", NS)
            if end is not None:
                nfe.emitente.endereco.uf     = _txt(end, "nfe:UF")
                nfe.emitente.endereco.mun    = _txt(end, "nfe:xMun")
                nfe.emitente.endereco.cod_mun = _txt(end, "nfe:cMun")

        # DESTINATÁRIO
        dest = inf_nfe.find("nfe:dest", NS)
        if dest is not None:
            nfe.destinatario.cnpj        = _txt(dest, "nfe:CNPJ")
            nfe.destinatario.cpf         = _txt(dest, "nfe:CPF")
            nfe.destinatario.nome        = _txt(dest, "nfe:xNome")
            nfe.destinatario.ie          = _txt(dest, "nfe:IE")
            nfe.destinatario.ind_ie_dest = _txt(dest, "nfe:indIEDest")
            end = dest.find("nfe:enderDest", NS)
            if end is not None:
                nfe.destinatario.endereco.uf     = _txt(end, "nfe:UF")
                nfe.destinatario.endereco.mun    = _txt(end, "nfe:xMun")
                nfe.destinatario.endereco.cod_mun = _txt(end, "nfe:cMun")

        # ITENS
        for det in inf_nfe.findall("nfe:det", NS):
            item = self._parse_item(det)
            nfe.itens.append(item)

        # TOTAIS
        total_el = inf_nfe.find("nfe:total/nfe:ICMSTot", NS)
        if total_el is not None:
            nfe.totais.vl_bc_icms    = _flt(total_el, "nfe:vBC")
            nfe.totais.vl_icms       = _flt(total_el, "nfe:vICMS")
            nfe.totais.vl_bc_icms_st = _flt(total_el, "nfe:vBCST")
            nfe.totais.vl_icms_st    = _flt(total_el, "nfe:vST")
            nfe.totais.vl_prod       = _flt(total_el, "nfe:vProd")
            nfe.totais.vl_frete      = _flt(total_el, "nfe:vFrete")
            nfe.totais.vl_desc       = _flt(total_el, "nfe:vDesc")
            nfe.totais.vl_ipi        = _flt(total_el, "nfe:vIPI")
            nfe.totais.vl_nf         = _flt(total_el, "nfe:vNF")

        return nfe

    def _parse_item(self, det) -> NFeItem:
        item = NFeItem()
        item.num_item = det.get("nItem", "")

        prod = det.find("nfe:prod", NS)
        if prod is not None:
            item.cod_prod        = _txt(prod, "nfe:cProd")
            item.cod_ean         = _txt(prod, "nfe:cEAN")
            item.descricao       = _txt(prod, "nfe:xProd")
            item.ncm             = _txt(prod, "nfe:NCM")
            item.cfop            = _txt(prod, "nfe:CFOP")
            item.unid            = _txt(prod, "nfe:uCom")
            item.qtd             = _flt(prod, "nfe:qCom")
            item.vl_unit         = _flt(prod, "nfe:vUnCom")
            item.vl_total_bruto  = _flt(prod, "nfe:vProd")
            item.vl_desc         = _flt(prod, "nfe:vDesc")
            item.ind_tot         = _txt(prod, "nfe:indTot")

        # ICMS - vários grupos possíveis
        imposto = det.find("nfe:imposto", NS)
        if imposto is not None:
            icms_group = imposto.find("nfe:ICMS", NS)
            if icms_group is not None:
                item.icms = self._parse_icms(icms_group)
            # IPI
            ipi_group = imposto.find("nfe:IPI", NS)
            if ipi_group is not None:
                for ipi_cst in ["nfe:IPITrib", "nfe:IPINT"]:
                    ipi_el = ipi_group.find(ipi_cst, NS)
                    if ipi_el is not None:
                        item.cst_ipi  = _txt(ipi_el, "nfe:CST")
                        item.aliq_ipi = _flt(ipi_el, "nfe:pIPI")
                        item.vl_ipi   = _flt(ipi_el, "nfe:vIPI")
                        break

        return item

    def _parse_icms(self, icms_group) -> NFeICMS:
        """Trata todos os grupos ICMS (ICMS00, ICMS10, ICMS20 ... ICMSSN)."""
        icms = NFeICMS()
        # Lista de todos os grupos ICMS possíveis
        grupos = [
            "nfe:ICMS00", "nfe:ICMS10", "nfe:ICMS20", "nfe:ICMS30",
            "nfe:ICMS40", "nfe:ICMS41", "nfe:ICMS50", "nfe:ICMS51",
            "nfe:ICMS60", "nfe:ICMS70", "nfe:ICMS90",
            "nfe:ICMSSN101", "nfe:ICMSSN102", "nfe:ICMSSN201",
            "nfe:ICMSSN202", "nfe:ICMSSN500", "nfe:ICMSSN900",
        ]
        for grupo in grupos:
            el = icms_group.find(grupo, NS)
            if el is not None:
                icms.orig      = _txt(el, "nfe:orig")
                icms.cst       = _txt(el, "nfe:CST") or _txt(el, "nfe:CSOSN")
                icms.mod_bc    = _txt(el, "nfe:modBC")
                icms.vl_bc     = _flt(el, "nfe:vBC")
                icms.aliq      = _flt(el, "nfe:pICMS")
                icms.vl_icms   = _flt(el, "nfe:vICMS")
                icms.p_red_bc  = _flt(el, "nfe:pRedBC")
                # ST
                icms.mod_bc_st  = _txt(el, "nfe:modBCST")
                icms.p_mva_st   = _flt(el, "nfe:pMVAST")
                icms.p_red_bc_st = _flt(el, "nfe:pRedBCST")
                icms.vl_bc_st   = _flt(el, "nfe:vBCST")
                icms.aliq_st    = _flt(el, "nfe:pICMSST")
                icms.vl_icms_st = _flt(el, "nfe:vICMSST")
                # FCP
                icms.vl_bc_fcp  = _flt(el, "nfe:vBCFCP")
                icms.p_fcp      = _flt(el, "nfe:pFCP")
                icms.vl_fcp     = _flt(el, "nfe:vFCP")
                break

        return icms
