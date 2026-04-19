# config.py
"""
Configuracoes globais do SPED ICMS Analyzer - MT
=================================================
UF alvo: apenas Mato Grosso (MT).

Execute o sistema como modulo Python:
    python -m sped_icms_analyzer.main --help

Modulos que importam este arquivo:
  parsers/sped_parser.py          -> ENCODING_SPED
  parsers/nfe_parser.py           -> ENCODING_XML
  analyzers/icms_analyzer.py      -> ALIQUOTAS_INTERNAS, ALIQUOTAS_INTERESTADUAIS,
                                     CST_COM_ICMS, CST_SEM_ICMS, CST_COM_ST,
                                     TOLERANCIA_PERCENTUAL, TOLERANCIA_ALIQUOTA
  analyzers/ncm_analyzer.py       -> NCM_MT_CSV
  pipeline.py                     -> todas as constantes de caminho e UF
  comparador/comparador_nfe_sped.py -> ENCODING_SPED, TOLERANCIA_COMPARADOR,
                                       CAMPOS_COMPARADOR, GRAVIDADE_NAO_ESCRITURADA
  ml/risk_classifier.py           -> MODEL_PATH, SCALER_PATH,
                                     RISCO_ALTO_THRESHOLD, RISCO_MEDIO_THRESHOLD
  ml/model_trainer.py             -> MODEL_PATH, SCALER_PATH, DATA_DIR
  reports/report_generator.py     -> REPORTS_DIR
  comparador/relatorio_comparador.py -> REPORTS_DIR
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# UF ALVO
# ─────────────────────────────────────────────────────────────────────────────
# Este sistema opera exclusivamente com as regras do Mato Grosso (MT).
UF_PADRAO = "MT"

# ─────────────────────────────────────────────────────────────────────────────
# DIRETORIOS DO PROJETO
# ─────────────────────────────────────────────────────────────────────────────
# resolve() garante caminho absoluto independente de onde o modulo e importado.
# Funciona corretamente tanto com:
#   python -m sped_icms_analyzer.main   (recomendado)
#   python main.py                      (dentro da pasta do projeto)
BASE_DIR    = Path(__file__).resolve().parent
DATA_DIR    = BASE_DIR / "data"
MODEL_DIR   = DATA_DIR / "modelos"
REPORTS_DIR = BASE_DIR / "relatorios"


def _garantir_diretorios() -> None:
    """
    Cria os diretorios necessarios com log explicito.
    Chamado UMA vez em pipeline.py na inicializacao do sistema.
    Nao e chamado silenciosamente ao importar o modulo.
    """
    for _dir in [DATA_DIR, MODEL_DIR, REPORTS_DIR]:
        if not _dir.exists():
            _dir.mkdir(parents=True, exist_ok=True)
            log.info("Diretorio criado: %s", _dir)
        else:
            log.debug("Diretorio OK: %s", _dir)


# ─────────────────────────────────────────────────────────────────────────────
# CAMINHOS DE ARQUIVOS DE DADOS
# ─────────────────────────────────────────────────────────────────────────────
NCM_MT_CSV           = DATA_DIR / "ncm_aliquotas_mt.csv"
MODEL_PATH           = MODEL_DIR / "risk_classifier.pkl"
SCALER_PATH          = MODEL_DIR / "scaler.pkl"
HISTORICO_TREINO_CSV = DATA_DIR  / "historico_treino.csv"

# ─────────────────────────────────────────────────────────────────────────────
# ALIQUOTAS ICMS INTERNAS - MT
# Fonte: RICMS-MT (Dec. 2.212/2014) e atualizacoes
# ATENCAO: Verifique portarias SEFAZ-MT vigentes antes de cada competencia.
# ─────────────────────────────────────────────────────────────────────────────
ALIQUOTAS_INTERNAS = {
    "MT": {
        "padrao":           17.0,   # Aliquota geral para mercadorias
        "essenciais":       12.0,   # Alimentos basicos, medicamentos
        "energia_eletrica": 25.0,   # Energia eletrica residencial/comercial
        "comunicacao":      25.0,   # Servicos de telecomunicacao
        "combustiveis":     16.0,   # Combustiveis automotivos
        "luxo":             25.0,   # Produtos superfluos
        "simples_nacional":  0.0,   # Conforme faixa do Simples Nacional
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# ALIQUOTAS INTERESTADUAIS - CONFAZ
# Fonte: Resolucao Senado Federal n. 22/1989
# ─────────────────────────────────────────────────────────────────────────────
ALIQUOTAS_INTERESTADUAIS = {
    # Sul/Sudeste -> Norte/Nordeste/CO/ES: 7%
    # Demais combinacoes: 12%
    "sul_sudeste_para_norte_nordeste_co_es": 7.0,
    "demais_operacoes": 12.0,
    # EC 87/2015: DIFAL ativo para operacoes a consumidor final nao contribuinte
    "difal_ativo": True,
}

# UFs da regiao Sul e Sudeste (usadas em icms_analyzer.py)
UFS_SUL_SUDESTE = {"SP", "RJ", "MG", "ES", "RS", "SC", "PR"}

# ─────────────────────────────────────────────────────────────────────────────
# CFOP - CLASSIFICACAO POR TIPO DE OPERACAO
# Fonte: Ajuste SINIEF 07/05
# ─────────────────────────────────────────────────────────────────────────────
CFOP_SAIDA_ESTADUAL        = "5"
CFOP_SAIDA_INTERESTADUAL   = "6"
CFOP_SAIDA_EXTERIOR        = "7"
CFOP_ENTRADA_ESTADUAL      = "1"
CFOP_ENTRADA_INTERESTADUAL = "2"
CFOP_ENTRADA_EXTERIOR      = "3"

CFOP_COM_ST = [
    "1401", "1403",
    "2401", "2403",
    "5401", "5403",
    "6401", "6403",
    "5402", "6402",
]

CFOP_ISENTOS_COMUNS = [
    "5109", "6109",
    "1201", "2201",
    "5152", "6152",
    "5153", "6153",
]

# ─────────────────────────────────────────────────────────────────────────────
# CST ICMS - CODIGO DE SITUACAO TRIBUTARIA
# Fonte: Tabela A + Tabela B do ICMS (Ajuste SINIEF 07/05)
# ─────────────────────────────────────────────────────────────────────────────
CST_ICMS = {
    "00": "Tributada integralmente",
    "10": "Tributada e com cobranca do ICMS por substituicao tributaria",
    "20": "Com reducao de base de calculo",
    "30": "Isenta ou nao tributada e com cobranca do ICMS por ST",
    "40": "Isenta",
    "41": "Nao tributada",
    "50": "Suspensao",
    "51": "Diferimento",
    "60": "ICMS cobrado anteriormente por substituicao tributaria",
    "70": "Com reducao de BC e cobranca do ICMS por ST",
    "90": "Outras",
}

CSOSN_ICMS = {
    "101": "Tributada pelo SN com permissao de credito",
    "102": "Tributada pelo SN sem permissao de credito",
    "103": "Isencao do ICMS no SN para faixa de receita bruta",
    "201": "Tributada pelo SN com permissao de credito e com cobranca de ST",
    "202": "Tributada pelo SN sem permissao de credito e com cobranca de ST",
    "203": "Isencao do ICMS no SN e com cobranca de ST",
    "300": "Imune",
    "400": "Nao tributada pelo SN",
    "500": "ICMS cobrado anteriormente por ST ou por antecipacao",
    "900": "Outros",
}

# Grupos por comportamento fiscal
CST_COM_ICMS  = ["00", "10", "20", "70"]
CST_COM_ST    = ["10", "30", "70"]
CST_SEM_ICMS  = ["40", "41", "50", "51", "60"]

# ─────────────────────────────────────────────────────────────────────────────
# TOLERANCIAS DE DIVERGENCIA
# ─────────────────────────────────────────────────────────────────────────────
TOLERANCIA_PERCENTUAL = 0.01   # R$ 0,01 - analisador ICMS/ST
TOLERANCIA_ALIQUOTA   = 0.5    # 0,5 pontos percentuais
TOLERANCIA_COMPARADOR = 0.01   # R$ 0,01 - comparador NF-e vs SPED

# ─────────────────────────────────────────────────────────────────────────────
# MACHINE LEARNING
# ─────────────────────────────────────────────────────────────────────────────
RISCO_ALTO_THRESHOLD  = 0.65
RISCO_MEDIO_THRESHOLD = 0.35
ML_MIN_AMOSTRAS_REAIS = 10

# ─────────────────────────────────────────────────────────────────────────────
# ENCODINGS
# ─────────────────────────────────────────────────────────────────────────────
ENCODING_SPED = "latin-1"   # ISO-8859-1 - especificado no Guia Pratico EFD
ENCODING_XML  = "utf-8"

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACOES DO COMPARADOR NF-e vs SPED
# ─────────────────────────────────────────────────────────────────────────────
EXTENSOES_XML  = [".xml", ".XML"]
EXTENSOES_SPED = [".txt", ".TXT"]

# Campos comparados (True = ativo | False = desativado sem alterar codigo)
CAMPOS_COMPARADOR = {
    "numero_serie_modelo":  True,
    "emitente_cnpj":        True,
    "emitente_nome":        True,
    "emitente_ie":          True,
    "destinatario_cnpj":    True,
    "destinatario_nome":    True,
    "destinatario_ie":      True,
    "data_emissao":         True,
    "data_saida_entrada":   True,
    "vl_doc":               True,
    "vl_merc":              True,
    "vl_frete":             True,
    "vl_seg":               True,
    "vl_bc_icms":           True,
    "vl_icms":              True,
    "vl_bc_icms_st":        True,
    "vl_icms_st":           True,
    "vl_ipi":               True,
    "cfop_cst_c190":        True,
}

GRAVIDADE_NAO_ESCRITURADA = "CRITICA"

# ─────────────────────────────────────────────────────────────────────────────
# PARALELISMO
# ─────────────────────────────────────────────────────────────────────────────
# Numero de processos paralelos para analise em lote.
# None = usa todos os CPUs disponiveis (recomendado).
# 1    = desativa paralelismo (util para debug).
MAX_WORKERS = None

# Tamanho minimo de lote para ativar paralelismo.
# Lotes menores que este valor processam sequencialmente.
MIN_ARQUIVOS_PARA_PARALELO = 4

# ─────────────────────────────────────────────────────────────────────────────
# CORES NO TERMINAL (colorama)
# ─────────────────────────────────────────────────────────────────────────────
COR_CRITICA = "RED"
COR_ALTA    = "YELLOW"
COR_MEDIA   = "YELLOW"
COR_OK      = "GREEN"
COR_INFO    = "CYAN"
COR_HEADER  = "WHITE"