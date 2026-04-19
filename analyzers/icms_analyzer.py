# analyzers/icms_analyzer.py
"""
Analisa divergências de ICMS em NF-e e SPED Fiscal.
Foco: Mato Grosso (MT) e Mato Grosso do Sul (MS).
Regras baseadas no RICMS-MT (Dec. 2.212/2014) e RICMS-MS (Dec. 15.093/2018).
"""

from dataclasses import dataclass, field
from typing import List, Optional
from config import (
    ALIQUOTAS_INTERNAS, ALIQUOTAS_INTERESTADUAIS,
    CST_COM_ICMS, CST_SEM_ICMS, CST_COM_ST,
    TOLERANCIA_PERCENTUAL, TOLERANCIA_ALIQUOTA
)


# ─────────────────────────────────────────────
# DATACLASS DE DIVERGÊNCIA
# ─────────────────────────────────────────────

@dataclass
class Divergencia:
    tipo: str                    # Código do tipo de divergência
    gravidade: str               # "CRITICA", "ALTA", "MEDIA", "BAIXA"
    descricao: str               # Descrição humana do problema
    orientacao: str              # Como corrigir
    valor_encontrado: str = ""   # Valor que está no documento
    valor_esperado: str = ""     # Valor que deveria estar
    referencia_legal: str = ""   # Base legal


@dataclass
class ResultadoAnaliseICMS:
    """Resultado da análise de um documento fiscal"""
    identificador: str           # Chave NF-e, número SPED etc.
    uf: str
    divergencias: List[Divergencia] = field(default_factory=list)
    risco_calculado: str = "BAIXO"   # ALTO / MEDIO / BAIXO (override pelo ML)
    total_icms_documento: float = 0.0
    total_icms_esperado: float = 0.0
    diferenca_icms: float = 0.0

    @property
    def tem_divergencia(self) -> bool:
        return len(self.divergencias) > 0

    @property
    def tem_critica(self) -> bool:
        return any(d.gravidade == "CRITICA" for d in self.divergencias)


# ─────────────────────────────────────────────
# ANALISADOR PRINCIPAL
# ─────────────────────────────────────────────

class ICMSAnalyzer:
    """
    Analisa documentos fiscais em busca de divergências de ICMS.
    Recebe objetos já parseados (NFe ou SpedNotaFiscal).
    """

    def __init__(self, uf: str):
        self.uf = uf.upper()
        self.aliq_interna = ALIQUOTAS_INTERNAS.get(self.uf, ALIQUOTAS_INTERNAS["MT"])

    # ─────────────────────────────────────────
    # ANÁLISE DE NF-e / NFC-e
    # ─────────────────────────────────────────

    def analisar_nfe(self, nfe) -> ResultadoAnaliseICMS:
        """Analisa uma NF-e ou NFC-e já parseada."""
        from parsers.nfe_parser import NFe
        resultado = ResultadoAnaliseICMS(
            identificador=nfe.chave or f"NF-{nfe.numero}",
            uf=self.uf,
            total_icms_documento=nfe.totais.vl_icms,
        )

        # Determina UF de destino/origem
        uf_emit = nfe.emitente.endereco.uf.upper()
        uf_dest = nfe.destinatario.endereco.uf.upper()
        is_interestadual = (uf_emit != uf_dest) and uf_dest != ""
        is_consumidor_final = nfe.destinatario.ind_ie_dest in ("9", "2", "")
        crt_emitente = nfe.emitente.crt  # 1=SN, 3=LP

        for item in nfe.itens:
            divs = self._analisar_item_nfe(
                item, uf_emit, uf_dest, is_interestadual,
                is_consumidor_final, crt_emitente, nfe.tipo_nf
            )
            resultado.divergencias.extend(divs)

        # Verifica totais da NF-e vs soma dos itens
        divs_totais = self._verificar_totais_nfe(nfe)
        resultado.divergencias.extend(divs_totais)

        resultado.total_icms_esperado = self._calcular_icms_esperado_nfe(nfe)
        resultado.diferenca_icms = abs(
            resultado.total_icms_documento - resultado.total_icms_esperado
        )
        resultado.risco_calculado = self._risco_simples(resultado)
        return resultado

    def _analisar_item_nfe(
        self, item, uf_emit, uf_dest, is_interestadual,
        is_consumidor_final, crt_emitente, tipo_nf
    ) -> List[Divergencia]:
        divs = []

        cst = item.icms.cst
        aliq = item.icms.aliq
        vl_bc = item.icms.vl_bc
        vl_icms = item.icms.vl_icms
        cfop = item.cfop

        # 1) CST em branco
        if not cst:
            divs.append(Divergencia(
                tipo="ICMS_CST_VAZIO",
                gravidade="CRITICA",
                descricao=f"Item {item.num_item} ({item.descricao[:40]}): CST/CSOSN não informado.",
                orientacao="Preencha o CST correto no item. Para optantes SN use CSOSN (101, 102, 500 etc.).",
                referencia_legal="Art. 12 Ajuste SINIEF 07/05"
            ))
            return divs  # Sem CST não tem como analisar mais

        # 2) Alíquota zerada em CST tributado
        if cst in CST_COM_ICMS and aliq == 0.0 and vl_bc > 0:
            divs.append(Divergencia(
                tipo="ICMS_ALIQ_ZERADA_CST_TRIBUTADO",
                gravidade="CRITICA",
                descricao=f"Item {item.num_item}: CST {cst} exige ICMS, mas alíquota está zerada.",
                orientacao=f"Verifique a alíquota aplicável. Para operações internas em {self.uf}, "
                           f"a alíquota padrão é {self.aliq_interna['padrao']}%. "
                           "Verifique se há benefício fiscal específico para este produto.",
                valor_encontrado=f"{aliq}%",
                valor_esperado=f"{self.aliq_interna['padrao']}% (padrão interno)",
                referencia_legal=f"RICMS-{self.uf}"
            ))

        # 3) ICMS informado em CST isento
        if cst in CST_SEM_ICMS and vl_icms > 0:
            divs.append(Divergencia(
                tipo="ICMS_INDEVIDO_CST_ISENTO",
                gravidade="ALTA",
                descricao=f"Item {item.num_item}: CST {cst} não deve ter ICMS, mas vl_icms = R$ {vl_icms:.2f}.",
                orientacao="Revise o CST. Se a operação é isenta/não tributada, zere o valor de ICMS "
                           "e verifique se o CST está correto para esta operação.",
                valor_encontrado=f"R$ {vl_icms:.2f}",
                valor_esperado="R$ 0,00",
                referencia_legal=f"RICMS-{self.uf}"
            ))

        # 4) Alíquota interestadual incorreta
        if is_interestadual and cst in CST_COM_ICMS and aliq > 0:
            aliq_esperada = self._aliq_interestadual_esperada(uf_emit, uf_dest)
            if abs(aliq - aliq_esperada) > TOLERANCIA_ALIQUOTA:
                divs.append(Divergencia(
                    tipo="ICMS_ALIQ_INTERESTADUAL_INCORRETA",
                    gravidade="ALTA",
                    descricao=f"Item {item.num_item}: Operação interestadual {uf_emit}→{uf_dest}. "
                              f"Alíquota {aliq}% pode estar incorreta.",
                    orientacao=f"Para operações entre {uf_emit} e {uf_dest}, a alíquota interestadual "
                               f"esperada é {aliq_esperada}%. Verifique a tabela CONFAZ e o CFOP {cfop}.",
                    valor_encontrado=f"{aliq}%",
                    valor_esperado=f"{aliq_esperada}%",
                    referencia_legal="Resolução SF nº 22/1989 e EC 87/2015"
                ))

        # 5) Base de cálculo do ICMS (verificação aritmética)
        if cst in CST_COM_ICMS and aliq > 0 and vl_bc > 0:
            vl_icms_calc = round(vl_bc * aliq / 100, 2)
            if abs(vl_icms_calc - vl_icms) > TOLERANCIA_PERCENTUAL:
                divs.append(Divergencia(
                    tipo="ICMS_VALOR_DIVERGENTE_BC",
                    gravidade="ALTA",
                    descricao=f"Item {item.num_item}: ICMS calculado (BC×Alíq) difere do informado.",
                    orientacao="Verifique se a base de cálculo está correta. "
                               "Pode haver redução de BC não declarada ou erro de arredondamento superior ao permitido.",
                    valor_encontrado=f"R$ {vl_icms:.2f}",
                    valor_esperado=f"R$ {vl_icms_calc:.2f} ({vl_bc:.2f} × {aliq}%)",
                    referencia_legal=f"RICMS-{self.uf}"
                ))

        # 6) DIFAL em operação interestadual para consumidor final (EC 87/2015)
        if (is_interestadual and is_consumidor_final and
                tipo_nf == "1" and cst in CST_COM_ICMS):
            if self.uf in (uf_dest,) and item.icms.vl_bc_fcp == 0.0:
                divs.append(Divergencia(
                    tipo="ICMS_DIFAL_FCP_NAO_RECOLHIDO",
                    gravidade="MEDIA",
                    descricao=f"Item {item.num_item}: Operação interestadual para consumidor final "
                              f"em {uf_dest}. Verificar necessidade de DIFAL e FCP.",
                    orientacao="Operações interestaduais para consumidor final não contribuinte "
                               "exigem recolhimento do DIFAL (EC 87/2015). Em MT e MS, "
                               "verifique também FCP (Fundo de Combate à Pobreza). "
                               "Utilize o GNRE ou apuração própria conforme protocolo.",
                    referencia_legal="EC 87/2015 / Convênio ICMS 93/2015"
                ))

        # 7) CFOP incompatível com tipo de operação
        if cfop:
            divs_cfop = self._verificar_cfop_operacao(item, tipo_nf, is_interestadual)
            divs.extend(divs_cfop)

        return divs

    def _verificar_totais_nfe(self, nfe) -> List[Divergencia]:
        """Verifica se soma dos itens bate com os totais da NF-e."""
        divs = []
        soma_icms_itens = sum(i.icms.vl_icms for i in nfe.itens)
        soma_bc_itens = sum(i.icms.vl_bc for i in nfe.itens)

        if abs(soma_icms_itens - nfe.totais.vl_icms) > 0.05:
            divs.append(Divergencia(
                tipo="ICMS_TOTAL_DIVERGE_ITENS",
                gravidade="CRITICA",
                descricao=f"Total ICMS da NF-e (R$ {nfe.totais.vl_icms:.2f}) "
                          f"difere da soma dos itens (R$ {soma_icms_itens:.2f}).",
                orientacao="Recalcule o ICMS por item e atualize o total da NF-e. "
                           "Esta divergência pode indicar erro no sistema emissor.",
                valor_encontrado=f"R$ {nfe.totais.vl_icms:.2f}",
                valor_esperado=f"R$ {soma_icms_itens:.2f}",
                referencia_legal="NT 2014/002 SEFAZ"
            ))
        return divs

    def _verificar_cfop_operacao(self, item, tipo_nf, is_interestadual) -> List[Divergencia]:
        divs = []
        cfop = item.cfop
        if not cfop or len(cfop) < 1:
            return divs

        primeiro_digito = cfop[0]

        # Saída (tipo_nf=1): CFOP deve começar com 5,6,7
        if tipo_nf == "1" and primeiro_digito not in ("5", "6", "7"):
            divs.append(Divergencia(
                tipo="CFOP_SAIDA_INCORRETO",
                gravidade="ALTA",
                descricao=f"Item {item.num_item}: NF de saída com CFOP {cfop} (inicia com {primeiro_digito}). "
                          "CFOPs de saída devem iniciar com 5, 6 ou 7.",
                orientacao="Corrija o CFOP. Saídas estaduais: 5.xxx, saídas interestaduais: 6.xxx, "
                           "exportações: 7.xxx.",
                valor_encontrado=cfop,
                referencia_legal="Ajuste SINIEF 07/05 - Tabela de CFOP"
            ))

        # Entrada (tipo_nf=0): CFOP deve começar com 1,2,3
        if tipo_nf == "0" and primeiro_digito not in ("1", "2", "3"):
            divs.append(Divergencia(
                tipo="CFOP_ENTRADA_INCORRETO",
                gravidade="ALTA",
                descricao=f"Item {item.num_item}: NF de entrada com CFOP {cfop} (inicia com {primeiro_digito}). "
                          "CFOPs de entrada devem iniciar com 1, 2 ou 3.",
                orientacao="Corrija o CFOP. Entradas estaduais: 1.xxx, interestaduais: 2.xxx, "
                           "importações: 3.xxx.",
                valor_encontrado=cfop,
                referencia_legal="Ajuste SINIEF 07/05 - Tabela de CFOP"
            ))

        # Interestadual mas CFOP estadual
        if is_interestadual and primeiro_digito in ("5", "1"):
            divs.append(Divergencia(
                tipo="CFOP_ESTADUAL_EM_INTERESTADUAL",
                gravidade="MEDIA",
                descricao=f"Item {item.num_item}: Operação interestadual com CFOP estadual {cfop}.",
                orientacao=f"Em operações interestaduais, use CFOP iniciado em 6 (saída) ou 2 (entrada). "
                           f"Verifique se UF emitente e destinatário estão corretos.",
                valor_encontrado=cfop,
                referencia_legal="Ajuste SINIEF 07/05"
            ))

        return divs

    # ─────────────────────────────────────────
    # ANÁLISE DE SPED
    # ─────────────────────────────────────────

    def analisar_nota_sped(self, nota, produtos: dict) -> ResultadoAnaliseICMS:
        """Analisa um SpedNotaFiscal."""
        resultado = ResultadoAnaliseICMS(
            identificador=nota.chv_nfe or f"NF-{nota.num_doc}",
            uf=self.uf,
            total_icms_documento=nota.vl_icms,
        )

        is_interestadual = nota.ind_oper in ("0", "1")  # refinado por CFOP

        for item in nota.itens:
            cfop = item.cfop
            is_inter = cfop and cfop[0] in ("2", "6")
            is_saida = nota.ind_oper == "1"

            divs = self._analisar_item_sped(item, is_inter, is_saida, produtos)
            resultado.divergencias.extend(divs)

        # Verificação totais C100 vs C190
        divs_c190 = self._verificar_totais_sped(nota)
        resultado.divergencias.extend(divs_c190)

        resultado.risco_calculado = self._risco_simples(resultado)
        return resultado

    def _analisar_item_sped(self, item, is_inter, is_saida, produtos: dict) -> List[Divergencia]:
        divs = []
        cst = item.cst_icms
        aliq = item.aliq_icms
        vl_bc = item.vl_bc_icms
        vl_icms = item.vl_icms

        if not cst:
            divs.append(Divergencia(
                tipo="SPED_CST_VAZIO",
                gravidade="CRITICA",
                descricao=f"Item {item.num_item} (Cód: {item.cod_item}): CST ICMS não informado no C170.",
                orientacao="Preencha o campo CST no registro C170. "
                           "Verifique o cadastro do produto no sistema ERP.",
                referencia_legal="Guia Prático EFD ICMS/IPI"
            ))
            return divs

        # Alíquota x BC x Valor
        if cst in CST_COM_ICMS and aliq > 0 and vl_bc > 0:
            vl_calc = round(vl_bc * aliq / 100, 2)
            if abs(vl_calc - vl_icms) > TOLERANCIA_PERCENTUAL:
                divs.append(Divergencia(
                    tipo="SPED_ICMS_ARITMETICO",
                    gravidade="ALTA",
                    descricao=f"Item {item.num_item}: ICMS = BC({vl_bc:.2f}) × Alíq({aliq}%) "
                              f"= {vl_calc:.2f}, mas C170 informa {vl_icms:.2f}.",
                    orientacao="Corrija o valor do ICMS ou a alíquota/base de cálculo no C170. "
                               "Verifique se há redução de BC não declarada.",
                    valor_encontrado=f"R$ {vl_icms:.2f}",
                    valor_esperado=f"R$ {vl_calc:.2f}",
                    referencia_legal=f"RICMS-{self.uf}"
                ))

        # CST sem ICMS mas tem valor
        if cst in CST_SEM_ICMS and vl_icms > 0:
            divs.append(Divergencia(
                tipo="SPED_ICMS_CST_ISENTO",
                gravidade="ALTA",
                descricao=f"Item {item.num_item}: CST {cst} (isento/NT) mas ICMS = R$ {vl_icms:.2f}.",
                orientacao="Zere o valor de ICMS para este item ou corrija o CST.",
                referencia_legal=f"RICMS-{self.uf}"
            ))

        return divs

    def _verificar_totais_sped(self, nota) -> List[Divergencia]:
        """Compara C100 com C190 (totais por CST/CFOP/Alíquota)."""
        divs = []
        soma_c190_icms = sum(t["vl_icms"] for t in nota.totais_c190)
        soma_c190_bc = sum(t["vl_bc_icms"] for t in nota.totais_c190)
        soma_c190_st = sum(t["vl_icms_st"] for t in nota.totais_c190)

        if abs(soma_c190_icms - nota.vl_icms) > 0.05 and soma_c190_icms > 0:
            divs.append(Divergencia(
                tipo="SPED_C100_C190_DIVERGE_ICMS",
                gravidade="CRITICA",
                descricao=f"NF {nota.num_doc}: ICMS no C100 (R$ {nota.vl_icms:.2f}) "
                          f"difere do total C190 (R$ {soma_c190_icms:.2f}).",
                orientacao="O registro C190 deve somar exatamente o mesmo ICMS do C100. "
                           "Revise os agrupamentos por CST/CFOP/Alíquota no C190.",
                valor_encontrado=f"C100: R$ {nota.vl_icms:.2f}",
                valor_esperado=f"C190: R$ {soma_c190_icms:.2f}",
                referencia_legal="Guia Prático EFD ICMS/IPI - Registro C190"
            ))

        if abs(soma_c190_st - nota.vl_icms_st) > 0.05 and soma_c190_st > 0:
            divs.append(Divergencia(
                tipo="SPED_C100_C190_DIVERGE_ST",
                gravidade="CRITICA",
                descricao=f"NF {nota.num_doc}: ICMS-ST no C100 (R$ {nota.vl_icms_st:.2f}) "
                          f"difere do total C190 (R$ {soma_c190_st:.2f}).",
                orientacao="Revise os registros C190 para ICMS-ST. "
                           "Verifique se todos os itens com ST estão corretamente agrupados.",
                referencia_legal="Guia Prático EFD ICMS/IPI - Registro C190"
            ))

        return divs

    # ─────────────────────────────────────────
    # AUXILIARES
    # ─────────────────────────────────────────

    def _aliq_interestadual_esperada(self, uf_orig: str, uf_dest: str) -> float:
        """Retorna alíquota interestadual esperada pela tabela CONFAZ."""
        regioes_sul_sudeste = {"SP", "RJ", "MG", "ES", "RS", "SC", "PR"}
        if uf_orig in regioes_sul_sudeste and uf_dest not in regioes_sul_sudeste:
            return 7.0
        return 12.0

    def _calcular_icms_esperado_nfe(self, nfe) -> float:
        total = 0.0
        for item in nfe.itens:
            if item.icms.cst in CST_COM_ICMS and item.icms.aliq > 0:
                total += round(item.icms.vl_bc * item.icms.aliq / 100, 2)
        return total

    def _risco_simples(self, resultado: ResultadoAnaliseICMS) -> str:
        """Classificação de risco simples (sem ML) como fallback."""
        if resultado.tem_critica:
            return "ALTO"
        n_divs = len(resultado.divergencias)
        if n_divs >= 3:
            return "ALTO"
        if n_divs >= 1:
            return "MEDIO"
        return "BAIXO"
