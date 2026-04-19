# SPED ICMS Analyzer - MT/MS
Sistema de análise fiscal para EFD ICMS/IPI, NF-e e NFC-e XML
com classificação de risco por Machine Learning.

## Estrutura do Projeto
```
sped_icms_analyzer/
├── main.py                  # Ponto de entrada CLI
├── config.py                # Configurações globais
├── Readme.md                # Leia-me
├── parsers/
│   ├── sped_parser.py       # Lê EFD ICMS/IPI (.txt)
│   ├── nfe_parser.py        # Lê NF-e XML
│   └── nfce_parser.py       # Lê NFC-e XML
├── analyzers/
│   ├── icms_analyzer.py     # Regras ICMS MT/MS
│   ├── icms_st_analyzer.py  # Regras ICMS-ST MT/MS
│   └── ncm_analyzer.py      # Validação NCM
├── ml/
│   ├── risk_classifier.py   # Random Forest - classifica risco
│   └── model_trainer.py     # Treina/retreina modelo
├── alerts/
│   └── alert_engine.py      # Gera alertas e orientações
├── reports/
│   └── report_generator.py  # Gera relatórios CSV/TXT
├── data/
│   ├── ncm_aliquotas_mt.csv # Tabela NCM x alíquota MT
│   ├── ncm_aliquotas_ms.csv # Tabela NCM x alíquota MS
│   └── modelos/             # Modelos ML treinados (.pkl)
└── requirements.txt
```

## Instalação
```bash
pip install -r requirements.txt
```

## Uso
```bash
# Analisar um SPED
python main.py --tipo sped --arquivo c:/arquivo.txt --uf MT

# Analisar NF-e XML
python main.py --tipo nfe --arquivo caminho/nota.xml --uf MT

# Analisar pasta inteira
python main.py --tipo pasta --pasta caminho/pasta/ --uf MS

# Treinar/atualizar modelo ML
python main.py --treinar
```