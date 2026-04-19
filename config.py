# config.py
"""
Configurações globais do SPED ICMS Analyzer
Edite este arquivo para ajustar parâmetros do sistema.
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────
# DIRETÓRIOS
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = DATA_DIR / "modelos"
REPORTS_DIR = BASE_DIR / "relatorios"

# Garante que diretórios existam
for _dir in [DATA_DIR, MODEL_DIR, REPORTS_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# ARQUIVOS DE DADOS
# ─────────────────────────────────────────────
NCM_MT_CSV = DATA_DIR / "ncm_aliquotas_mt.csv"
MODEL_PATH = MODEL_DIR / "risk_classifier.pkl"
SCALER_PATH = MODEL_DIR / "scaler.pkl"
ENCODER_PATH = MODEL_DIR / "label_encoder.pkl"

# ─────────────────────────────────────────────
# ALÍQUOTAS ICMS MT/MS (referência base)
# Fonte: RICMS-MT (Dec. 2.212/2014) e RICMS-MS (Dec. 15.093/2018)
# ─────────────────────────────────────────────
ALIQUOTAS_INTERNAS = {
    "MT": {
        "padrao": 17.0,
        "essenciais": 12.0,       # alimentos básicos, medicamentos
        "energia_eletrica": 25.0,
        "comunicacao": 25.0,
        "combustiveis": 16.0,
        "luxo": 25.0,
        "simples_nacional": 0.0,  # conforme faixa
    },
    "MS": {
        "padrao": 17.0,
        "essenciais": 12.0,
        "energia_eletrica": 17.0,
        "comunicacao": 25.0,
        "combustiveis": 17.0,
        "luxo": 25.0,
        "simples_nacional": 0.0,
    }
}

# Alíquotas interestaduais (CONFAZ)
ALIQUOTAS_INTERESTADUAIS = {
    "sul_sudeste_para_outros": 12.0,
    "norte_nordeste_co_para_outros": 12.0,
    "qualquer_para_norte_nordeste_co_es": 7.0,
    # A partir de 2016 (EC 87/2015) - DIFAL
    "difal_ativo": True,
}

# ─────────────────────────────────────────────
# CFOP - Classificação por tipo de operação
# ─────────────────────────────────────────────
CFOP_SAIDA_ESTADUAL = ["5", ]       # inicia com 5
CFOP_SAIDA_INTERESTADUAL = ["6", ]  # inicia com 6
CFOP_SAIDA_EXTERIOR = ["7", ]       # inicia com 7
CFOP_ENTRADA_ESTADUAL = ["1", ]     # inicia com 1
CFOP_ENTRADA_INTERESTADUAL = ["2", ]
CFOP_ENTRADA_EXTERIOR = ["3", ]

# CFOPs que tipicamente têm ICMS-ST
CFOP_COM_ST = [
    "1401", "1403", "2401", "2403",  # Compras com ST
    "5401", "5403", "6401", "6403",  # Vendas com ST
    "5402", "6402",                   # Vendas ST já recolhido
]

# CFOPs isentos / não tributados comuns
CFOP_ISENTOS = [
    "5101", "6101",  # pode ter isenção dependendo do produto
    "5109", "6109",  # devoluções
    "5152", "6152",  # transferências
]

# ─────────────────────────────────────────────
# CST ICMS - Situações Tributárias
# ─────────────────────────────────────────────
CST_ICMS = {
    "00": "Tributada integralmente",
    "10": "Tributada e com cobrança do ICMS por substituição tributária",
    "20": "Com redução de base de cálculo",
    "30": "Isenta ou não tributada e com cobrança do ICMS por ST",
    "40": "Isenta",
    "41": "Não tributada",
    "50": "Suspensão",
    "51": "Diferimento",
    "60": "ICMS cobrado anteriormente por substituição tributária",
    "70": "Com redução de BC e cobrança do ICMS por ST",
    "90": "Outras",
}

# CST que exigem ICMS preenchido
CST_COM_ICMS = ["00", "10", "20", "70"]
# CST que exigem ST preenchido
CST_COM_ST = ["10", "30", "70"]
# CST que não devem ter ICMS
CST_SEM_ICMS = ["40", "41", "50", "51", "60"]

# ─────────────────────────────────────────────
# LIMIARES DE RISCO ML
# ─────────────────────────────────────────────
RISCO_ALTO_THRESHOLD = 0.65
RISCO_MEDIO_THRESHOLD = 0.35

# ─────────────────────────────────────────────
# TOLERÂNCIAS DE DIVERGÊNCIA (%)
# ─────────────────────────────────────────────
TOLERANCIA_PERCENTUAL = 0.01   # 1 centavo - divergência de arredondamento
TOLERANCIA_ALIQUOTA = 0.5      # 0.5% de tolerância na alíquota

# ─────────────────────────────────────────────
# CONFIGURAÇÕES DE SAÍDA
# ─────────────────────────────────────────────
ENCODING_SPED = "latin-1"   # Encoding padrão dos arquivos SPED
ENCODING_XML = "utf-8"

# Cores no terminal (via colorama)
COR_ERRO = "RED"
COR_ALERTA = "YELLOW"
COR_OK = "GREEN"
COR_INFO = "CYAN"