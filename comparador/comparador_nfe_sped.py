# comparador/comparador_nfe_sped.py
"""
Comparador NF-e XML vs SPED Fiscal (EFD ICMS/IPI).

Cruza todos os XMLs de uma pasta com o arquivo SPED .txt do mesmo período,
verificando:
  - Presença da chave NF-e no SPED (C100)
  - Emitente: CNPJ, Razão Social, IE
  - Destinatário: CNPJ, Razão Social, IE
  - Número, Série e Modelo
  - Data de emissão e data entrada/saída
  - Totais: vl_doc, vl_merc, vl_frete, vl_seg
  - ICMS: BC, alíquota, valor
  - ICMS-ST: BC-ST, valor ST
  - IPI: valor
  - CFOP e CST (via C190)
  - Competência da nota vs competência do SPED

Tolerância: R$ 0,01 (qualquer diferença é reportada).
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, date
from pathlib import Path

TOLERANCIA = 0.01   # R$ 0,01


# ─────────────────────────────────────────────
# ESTRUTURAS DE RESULTADO
# ─────────────────────────────────────────────

@dataclass
class CampoDivergente:
    campo: str
    valor_xml: str
    valor_sped: str
    gravidade: str          # CRITICA / ALTA / MEDIA
    orientacao: str


@dataclass
class ResultadoComparacao:
    # Identificação
    chave_nfe: str
    numero_nf: str
    serie: str
    modelo: str
    dt_emissao: str
    caminho_xml: str

    # Status geral
    encontrada_no_sped: bool = False
    fora_competencia: bool = False
    competencia_xml: str = ""       # "MM/AAAA"
    competencia_sped: str = ""      # "MM/AAAA"

    # Divergências encontradas
    divergencias: List[CampoDivergente] = field(default_factory=list)

    # Totais para resumo
    vl_icms_xml: float = 0.0
    vl_icms_sped: float = 0.0
    vl_doc_xml: float = 0.0
    vl_doc_sped: float = 0.0

    @property
    def tem_divergencia(self) -> bool:
        return bool(self.divergencias)

    @property
    def gravidade_maxima(self) -> str:
        if not self.divergencias:
            return "OK"
        ordem = {"CRITICA": 0, "ALTA": 1, "MEDIA": 2}
        return min(self.divergencias, key=lambda d: ordem.get(d.gravidade, 9)).gravidade

    @property
    def status(self) -> str:
        if not self.encontrada_no_sped:
            return "FORA_COMPETENCIA" if self.fora_competencia else "NAO_ESCRITURADA"
        if not self.divergencias:
            return "OK"
        return self.gravidade_maxima


# ─────────────────────────────────────────────
# FUNÇÕES AUXILIARES DE DATA
# ─────────────────────────────────────────────

def _parse_data(valor: str) -> Optional[date]:
    """Tenta parsear data nos formatos usados no SPED (DDMMAAAA) e XML (ISO 8601)."""
    valor = valor.strip()
    if not valor:
        return None
    # DDMMAAAA — formato nativo do SPED
    if len(valor) == 8 and valor.isdigit():
        try:
            return datetime.strptime(valor, "%d%m%Y").date()
        except ValueError:
            pass
    # ISO 8601 (XML NF-e): 2024-01-15T10:00:00-03:00
    try:
        return datetime.fromisoformat(valor[:19]).date()
    except Exception:
        pass
    # Demais formatos
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(valor[:len(fmt)], fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def _competencia(valor: str) -> str:
    """Retorna 'MM/AAAA' de uma data qualquer, ou '' se inválida."""
    d = _parse_data(valor)
    return d.strftime("%m/%Y") if d else ""


def _mesmo_mes(data1: str, data2_ini: str, data2_fin: str) -> Tuple[bool, bool]:
    """
    Verifica se data1 está dentro do período [data2_ini, data2_fin].
    Retorna (dentro_do_periodo, data_valida).
    """
    d1 = _parse_data(data1)
    d_ini = _parse_data(data2_ini)
    d_fin = _parse_data(data2_fin)
    if not all([d1, d_ini, d_fin]):
        return False, False
    return d_ini <= d1 <= d_fin, True


# ─────────────────────────────────────────────
# ÍNDICE DO SPED (estrutura de busca rápida)
# ─────────────────────────────────────────────

@dataclass
class IndiceSpedNota:
    """Dados do C100 + C190 indexados para comparação rápida."""
    chave: str
    num_doc: str
    ser: str
    cod_mod: str
    ind_oper: str           # 0=entrada, 1=saída
    ind_emit: str           # 0=própria, 1=terceiro
    cod_part: str
    dt_doc: str
    dt_e_s: str
    cod_sit: str
    # Totais C100
    vl_doc: float
    vl_merc: float
    vl_frete: float
    vl_seg: float
    vl_out_da: float
    vl_bc_icms: float
    vl_icms: float
    vl_bc_icms_st: float
    vl_icms_st: float
    vl_ipi: float
    # Dados do participante 0150
    part_cnpj: str = ""
    part_nome: str = ""
    part_ie: str = ""
    # Totais C190 (agrupados por CFOP+CST+Alíq)
    cfop_cst_resumo: List[Dict] = field(default_factory=list)


def _cnpj_limpo(v: str) -> str:
    return "".join(c for c in (v or "") if c.isdigit())


def _normalizar(v: str) -> str:
    """Remove espaços extras e converte para maiúsculo."""
    return " ".join((v or "").upper().split())


# ─────────────────────────────────────────────
# COMPARADOR PRINCIPAL
# ─────────────────────────────────────────────

class ComparadorNFeSped:
    """
    Compara XMLs de NF-e/NFC-e com o SPED Fiscal (.txt).
    Uso:
        comp = ComparadorNFeSped(caminho_sped, pasta_xmls, uf)
        resultados = comp.comparar()
    """

    def __init__(self, caminho_sped: str, pasta_xmls: str, uf: str = "MT"):
        self.caminho_sped = caminho_sped
        self.pasta_xmls = Path(pasta_xmls)
        self.uf = uf.upper()
        self._indice_sped: Dict[str, IndiceSpedNota] = {}   # chave → nota
        self._indice_numero: Dict[str, IndiceSpedNota] = {} # "num|ser|mod" → nota
        self._participantes: Dict[str, dict] = {}
        self._dt_ini_sped: str = ""
        self._dt_fin_sped: str = ""
        self._cnpj_contribuinte: str = ""
        self._uf_contribuinte: str = ""

    # ─────────────────────────────────────────
    # INDEXAÇÃO DO SPED
    # ─────────────────────────────────────────

    def _indexar_sped(self):
        """Lê o SPED e monta índice de busca em memória."""
        from config import ENCODING_SPED

        path = Path(self.caminho_sped)
        if not path.exists():
            raise FileNotFoundError(f"SPED não encontrado: {self.caminho_sped}")

        nota_atual: Optional[IndiceSpedNota] = None

        with open(path, encoding=ENCODING_SPED, errors="replace") as f:
            for linha in f:
                linha = linha.strip()
                if not linha:
                    continue
                partes = linha.split("|")
                if len(partes) < 3:
                    continue
                reg = partes[1].upper()

                if reg == "0000":
                    # |0000|versao|tipo|ind_sit|num_rec|dt_ini|dt_fin|nome|cnpj|...|uf|
                    if len(partes) > 10:
                        self._dt_ini_sped = partes[5] if len(partes) > 5 else ""
                        self._dt_fin_sped = partes[6] if len(partes) > 6 else ""
                        self._cnpj_contribuinte = _cnpj_limpo(partes[8]) if len(partes) > 8 else ""
                        self._uf_contribuinte = partes[10] if len(partes) > 10 else ""

                elif reg == "0150":
                    # |0150|cod_part|nome|cod_pais|cnpj|cpf|ie|cod_mun|suframa|end|...
                    if len(partes) > 7:
                        cod = partes[2]
                        self._participantes[cod] = {
                            "nome": partes[3],
                            "cnpj": _cnpj_limpo(partes[5]),
                            "ie":   partes[7],
                        }

                elif reg == "C100":
                    # Salva nota anterior
                    if nota_atual:
                        self._salvar_no_indice(nota_atual)

                    if len(partes) < 27:
                        nota_atual = None
                        continue

                    def _f(i): return float(partes[i].replace(",", ".").strip()) if len(partes) > i and partes[i].strip() else 0.0

                    nota_atual = IndiceSpedNota(
                        chave=partes[9].strip()       if len(partes) > 9  else "",
                        num_doc=partes[8].strip()     if len(partes) > 8  else "",
                        ser=partes[7].strip()         if len(partes) > 7  else "",
                        cod_mod=partes[5].strip()     if len(partes) > 5  else "",
                        ind_oper=partes[2].strip()    if len(partes) > 2  else "",
                        ind_emit=partes[3].strip()    if len(partes) > 3  else "",
                        cod_part=partes[4].strip()    if len(partes) > 4  else "",
                        cod_sit=partes[6].strip()     if len(partes) > 6  else "",
                        dt_doc=partes[10].strip()     if len(partes) > 10 else "",
                        dt_e_s=partes[11].strip()     if len(partes) > 11 else "",
                        vl_doc=_f(12),
                        vl_merc=_f(16),
                        vl_frete=_f(18),
                        vl_seg=_f(19),
                        vl_out_da=_f(20),
                        vl_bc_icms=_f(21),
                        vl_icms=_f(22),
                        vl_bc_icms_st=_f(23),
                        vl_icms_st=_f(24),
                        vl_ipi=_f(25),
                    )

                elif reg == "C190" and nota_atual:
                    if len(partes) > 9:
                        def _f2(i): return float(partes[i].replace(",", ".").strip()) if len(partes) > i and partes[i].strip() else 0.0
                        nota_atual.cfop_cst_resumo.append({
                            "cst_icms":      partes[2],
                            "cfop":          partes[3],
                            "aliq_icms":     _f2(4),
                            "vl_opr":        _f2(5),
                            "vl_bc_icms":    _f2(6),
                            "vl_icms":       _f2(7),
                            "vl_bc_icms_st": _f2(8),
                            "vl_icms_st":    _f2(9),
                        })

        # Salva última nota
        if nota_atual:
            self._salvar_no_indice(nota_atual)

        # Enriquece com dados do participante
        for nota in self._indice_sped.values():
            part = self._participantes.get(nota.cod_part, {})
            nota.part_cnpj = part.get("cnpj", "")
            nota.part_nome = part.get("nome", "")
            nota.part_ie   = part.get("ie", "")

    def _salvar_no_indice(self, nota: IndiceSpedNota):
        if nota.chave:
            self._indice_sped[nota.chave] = nota
        # Índice secundário por número+série+modelo (para notas sem chave no SPED)
        chave_num = f"{nota.num_doc}|{nota.ser}|{nota.cod_mod}"
        self._indice_numero[chave_num] = nota

    # ─────────────────────────────────────────
    # COMPARAÇÃO PRINCIPAL
    # ─────────────────────────────────────────

    def comparar(self) -> List[ResultadoComparacao]:
        """Executa a comparação completa e retorna lista de resultados."""
        from parsers.nfe_parser import NFeParser
        from tqdm import tqdm

        print(f"\n[INFO] Indexando SPED: {self.caminho_sped}")
        self._indexar_sped()
        print(f"[INFO] SPED indexado: {len(self._indice_sped)} nota(s) encontrada(s)")
        print(f"[INFO] Período SPED : {self._dt_ini_sped} a {self._dt_fin_sped}")
        print(f"[INFO] Contribuinte : {self._cnpj_contribuinte} / {self._uf_contribuinte}")

        xmls = sorted(
            list(self.pasta_xmls.glob("*.xml")) +
            list(self.pasta_xmls.glob("*.XML"))
        )
        print(f"[INFO] XMLs na pasta: {len(xmls)}\n")

        if not xmls:
            print("[AVISO] Nenhum XML encontrado na pasta informada.")
            return []

        resultados = []
        for xml_path in tqdm(xmls, desc="Comparando XMLs", unit="NF"):
            nfe = NFeParser(str(xml_path)).parse()
            if nfe.erros_leitura:
                continue
            res = self._comparar_nfe(nfe, str(xml_path))
            resultados.append(res)

        return resultados

    def _comparar_nfe(self, nfe, caminho_xml: str) -> ResultadoComparacao:
        """Compara uma NF-e com o índice do SPED."""
        chave = nfe.chave
        num   = nfe.numero
        serie = nfe.serie
        modelo = nfe.modelo
        dt_emis = nfe.dt_emis

        resultado = ResultadoComparacao(
            chave_nfe=chave,
            numero_nf=num,
            serie=serie,
            modelo=modelo,
            dt_emissao=dt_emis,
            caminho_xml=caminho_xml,
            competencia_xml=_competencia(dt_emis),
            competencia_sped=_competencia(self._dt_ini_sped),
            vl_icms_xml=nfe.totais.vl_icms,
            vl_doc_xml=nfe.totais.vl_nf,
        )

        # Busca no SPED: primeiro por chave, depois por número+série+modelo
        nota_sped = (
            self._indice_sped.get(chave) or
            self._indice_numero.get(f"{num}|{serie}|{modelo}")
        )

        if nota_sped is None:
            # Verifica se nota é da competência do SPED
            dentro, valida = _mesmo_mes(dt_emis, self._dt_ini_sped, self._dt_fin_sped)
            if valida and not dentro:
                resultado.fora_competencia = True
            resultado.encontrada_no_sped = False
            return resultado

        resultado.encontrada_no_sped = True
        resultado.vl_icms_sped = nota_sped.vl_icms
        resultado.vl_doc_sped  = nota_sped.vl_doc

        # ── Executa todas as comparações ──
        divs = []
        divs += self._cmp_identificacao(nfe, nota_sped)
        divs += self._cmp_participantes(nfe, nota_sped)
        divs += self._cmp_datas(nfe, nota_sped)
        divs += self._cmp_totais(nfe, nota_sped)
        divs += self._cmp_icms(nfe, nota_sped)
        divs += self._cmp_icms_st(nfe, nota_sped)
        divs += self._cmp_ipi(nfe, nota_sped)
        divs += self._cmp_cfop_cst(nfe, nota_sped)
        resultado.divergencias = divs

        return resultado

    # ─────────────────────────────────────────
    # BLOCOS DE COMPARAÇÃO
    # ─────────────────────────────────────────

    def _cmp_identificacao(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        divs = []
        pares = [
            ("Número NF",  nfe.numero,  sped.num_doc, "ALTA"),
            ("Série NF",   nfe.serie,   sped.ser,     "ALTA"),
            ("Modelo NF",  nfe.modelo,  sped.cod_mod, "MEDIA"),
        ]
        for campo, v_xml, v_sped, grav in pares:
            if str(v_xml).strip() != str(v_sped).strip():
                divs.append(CampoDivergente(
                    campo=campo,
                    valor_xml=str(v_xml),
                    valor_sped=str(v_sped),
                    gravidade=grav,
                    orientacao=f"Verifique se o {campo} foi escriturado corretamente no SPED (C100). "
                               "Pode ser erro na importação do XML pelo sistema contábil."
                ))
        return divs

    def _cmp_participantes(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        """Compara emitente e destinatário entre XML e participante do SPED (0150+C100)."""
        divs = []

        # Determina quem é o "participante" no SPED:
        # ind_emit=0 (emissão própria) → participante é o destinatário
        # ind_emit=1 (terceiro)        → participante é o emitente
        if sped.ind_emit == "0":
            # Nota própria: compara destinatário
            cnpj_xml  = _cnpj_limpo(nfe.destinatario.cnpj)
            nome_xml  = _normalizar(nfe.destinatario.nome)
            ie_xml    = nfe.destinatario.ie.strip()
            rotulo    = "Destinatário"
        else:
            # Nota de terceiro: compara emitente
            cnpj_xml  = _cnpj_limpo(nfe.emitente.cnpj)
            nome_xml  = _normalizar(nfe.emitente.nome)
            ie_xml    = nfe.emitente.ie.strip()
            rotulo    = "Emitente"

        cnpj_sped = sped.part_cnpj
        nome_sped = _normalizar(sped.part_nome)
        ie_sped   = sped.part_ie.strip()

        if cnpj_xml and cnpj_sped and cnpj_xml != cnpj_sped:
            divs.append(CampoDivergente(
                campo=f"{rotulo} CNPJ",
                valor_xml=cnpj_xml,
                valor_sped=cnpj_sped,
                gravidade="CRITICA",
                orientacao=f"O CNPJ do {rotulo} no XML ({cnpj_xml}) diverge do cadastrado "
                           f"no registro 0150 do SPED ({cnpj_sped}). "
                           "Corrija o cadastro do participante no sistema contábil."
            ))

        if nome_xml and nome_sped and nome_xml != nome_sped:
            divs.append(CampoDivergente(
                campo=f"{rotulo} Nome/Razão Social",
                valor_xml=nome_xml,
                valor_sped=nome_sped,
                gravidade="MEDIA",
                orientacao=f"A razão social do {rotulo} diverge entre XML e SPED. "
                           "Atualize o cadastro do participante (0150) para manter consistência."
            ))

        if ie_xml and ie_sped and ie_xml != ie_sped:
            divs.append(CampoDivergente(
                campo=f"{rotulo} IE",
                valor_xml=ie_xml,
                valor_sped=ie_sped,
                gravidade="ALTA",
                orientacao=f"A Inscrição Estadual do {rotulo} diverge. "
                           "Verifique o cadastro no SPED (registro 0150) e corrija."
            ))

        # Sempre compara emitente também (CNPJ do contribuinte vs XML)
        cnpj_emit_xml = _cnpj_limpo(nfe.emitente.cnpj)
        if sped.ind_emit == "0" and self._cnpj_contribuinte:
            if cnpj_emit_xml and cnpj_emit_xml != self._cnpj_contribuinte:
                divs.append(CampoDivergente(
                    campo="Emitente CNPJ (vs contribuinte SPED)",
                    valor_xml=cnpj_emit_xml,
                    valor_sped=self._cnpj_contribuinte,
                    gravidade="CRITICA",
                    orientacao="O CNPJ emitente do XML não corresponde ao contribuinte "
                               "do arquivo SPED. Verifique se o XML pertence a este SPED."
                ))

        return divs

    def _cmp_datas(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        divs = []
        dt_emis_xml  = _competencia(nfe.dt_emis)
        dt_emis_sped = _competencia(sped.dt_doc)

        if dt_emis_xml and dt_emis_sped and dt_emis_xml != dt_emis_sped:
            divs.append(CampoDivergente(
                campo="Data Emissão (competência)",
                valor_xml=nfe.dt_emis[:10],
                valor_sped=sped.dt_doc,
                gravidade="ALTA",
                orientacao="A competência da data de emissão diverge entre XML e SPED. "
                           "Verifique se a nota foi escriturada no período correto. "
                           "Notas devem ser escrituradas no mês de emissão (saída) "
                           "ou de recebimento (entrada)."
            ))

        dt_es_xml  = nfe.dt_saida_entrada[:10] if nfe.dt_saida_entrada else ""
        dt_es_sped = sped.dt_e_s

        if dt_es_xml and dt_es_sped:
            d_xml  = _parse_data(dt_es_xml)
            d_sped = _parse_data(dt_es_sped)
            if d_xml and d_sped and d_xml != d_sped:
                divs.append(CampoDivergente(
                    campo="Data Saída/Entrada",
                    valor_xml=dt_es_xml,
                    valor_sped=dt_es_sped,
                    gravidade="MEDIA",
                    orientacao="A data de saída/entrada diverge entre XML e SPED. "
                               "Corrija o campo dt_E_S no C100 para refletir a data correta."
                ))
        return divs

    def _cmp_totais(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        divs = []
        pares = [
            ("Valor Total NF (vl_doc)",  nfe.totais.vl_nf,    sped.vl_doc,   "CRITICA"),
            ("Valor Mercadorias (vl_merc)", nfe.totais.vl_prod, sped.vl_merc, "ALTA"),
            ("Valor Frete",              nfe.totais.vl_frete, sped.vl_frete,  "MEDIA"),
            ("Valor Seguro",             nfe.totais.vl_seg,   sped.vl_seg,    "MEDIA"),
        ]
        for campo, v_xml, v_sped, grav in pares:
            if abs(v_xml - v_sped) > TOLERANCIA:
                divs.append(CampoDivergente(
                    campo=campo,
                    valor_xml=f"R$ {v_xml:.2f}",
                    valor_sped=f"R$ {v_sped:.2f}",
                    gravidade=grav,
                    orientacao=f"O campo '{campo}' difere em R$ {abs(v_xml - v_sped):.2f}. "
                               "Verifique se o XML foi importado corretamente pelo sistema contábil. "
                               "O C100 deve refletir exatamente os valores do XML autorizado."
                ))
        return divs

    def _cmp_icms(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        divs = []
        pares = [
            ("ICMS Base de Cálculo (vl_bc_icms)", nfe.totais.vl_bc_icms, sped.vl_bc_icms, "CRITICA"),
            ("ICMS Valor (vl_icms)",               nfe.totais.vl_icms,    sped.vl_icms,    "CRITICA"),
        ]
        for campo, v_xml, v_sped, grav in pares:
            if abs(v_xml - v_sped) > TOLERANCIA:
                dif = abs(v_xml - v_sped)
                divs.append(CampoDivergente(
                    campo=campo,
                    valor_xml=f"R$ {v_xml:.2f}",
                    valor_sped=f"R$ {v_sped:.2f}",
                    gravidade=grav,
                    orientacao=f"Diferença de R$ {dif:.2f} no {campo}. "
                               "Esta divergência pode indicar: (1) importação incorreta do XML, "
                               "(2) lançamento manual diferente do XML, ou "
                               "(3) escrituração de outro documento no lugar. "
                               f"O valor correto é o do XML: R$ {v_xml:.2f}."
                ))
        return divs

    def _cmp_icms_st(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        divs = []
        pares = [
            ("ICMS-ST Base de Cálculo", nfe.totais.vl_bc_icms_st, sped.vl_bc_icms_st, "CRITICA"),
            ("ICMS-ST Valor",           nfe.totais.vl_icms_st,    sped.vl_icms_st,    "CRITICA"),
        ]
        for campo, v_xml, v_sped, grav in pares:
            if abs(v_xml - v_sped) > TOLERANCIA:
                divs.append(CampoDivergente(
                    campo=campo,
                    valor_xml=f"R$ {v_xml:.2f}",
                    valor_sped=f"R$ {v_sped:.2f}",
                    gravidade=grav,
                    orientacao=f"Divergência no {campo}: XML=R$ {v_xml:.2f} vs SPED=R$ {v_sped:.2f}. "
                               "Verifique se o ICMS-ST foi escriturado corretamente no C100. "
                               "Em MT/MS, o ICMS-ST retido deve ser idêntico ao valor do XML."
                ))
        return divs

    def _cmp_ipi(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        divs = []
        if abs(nfe.totais.vl_ipi - sped.vl_ipi) > TOLERANCIA:
            divs.append(CampoDivergente(
                campo="IPI Valor",
                valor_xml=f"R$ {nfe.totais.vl_ipi:.2f}",
                valor_sped=f"R$ {sped.vl_ipi:.2f}",
                gravidade="ALTA",
                orientacao=f"Valor do IPI diverge em R$ {abs(nfe.totais.vl_ipi - sped.vl_ipi):.2f}. "
                           "Verifique o campo vl_ipi no C100 do SPED."
            ))
        return divs

    def _cmp_cfop_cst(self, nfe, sped: IndiceSpedNota) -> List[CampoDivergente]:
        """
        Compara CFOPs e CSTs usados nos itens do XML com o resumo C190 do SPED.
        Verifica se todos os CFOPs do XML estão presentes no C190.
        """
        divs = []
        if not sped.cfop_cst_resumo:
            return divs

        # CFOPs presentes no XML
        cfops_xml = set(item.cfop for item in nfe.itens if item.cfop)
        csts_xml  = set(item.icms.cst for item in nfe.itens if item.icms.cst)

        # CFOPs presentes no C190 do SPED
        cfops_sped = set(r["cfop"] for r in sped.cfop_cst_resumo if r.get("cfop"))
        csts_sped  = set(r["cst_icms"] for r in sped.cfop_cst_resumo if r.get("cst_icms"))

        cfops_ausentes = cfops_xml - cfops_sped
        csts_ausentes  = csts_xml - csts_sped

        if cfops_ausentes:
            divs.append(CampoDivergente(
                campo="CFOP ausente no C190",
                valor_xml=", ".join(sorted(cfops_ausentes)),
                valor_sped=", ".join(sorted(cfops_sped)),
                gravidade="ALTA",
                orientacao=f"Os CFOPs {', '.join(sorted(cfops_ausentes))} existem nos itens do XML "
                           "mas não aparecem no registro C190 do SPED. "
                           "O C190 deve conter um registro para cada combinação CST/CFOP/Alíquota. "
                           "Verifique a importação do XML no sistema contábil."
            ))

        if csts_ausentes:
            divs.append(CampoDivergente(
                campo="CST ausente no C190",
                valor_xml=", ".join(sorted(csts_ausentes)),
                valor_sped=", ".join(sorted(csts_sped)),
                gravidade="ALTA",
                orientacao=f"Os CSTs {', '.join(sorted(csts_ausentes))} existem nos itens do XML "
                           "mas não foram encontrados no C190 do SPED. "
                           "Corrija o agrupamento no C190 para incluir todos os CSTs presentes."
            ))

        return divs