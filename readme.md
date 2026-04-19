# SPED ICMS Analyzer — MT/MS

Sistema de análise fiscal desenvolvido em Python para **Mato Grosso (MT)** e
**Mato Grosso do Sul (MS)**. Lê arquivos EFD ICMS/IPI (SPED Fiscal), NF-e XML e
NFC-e XML, detecta divergências de ICMS, ICMS-ST e NCM, classifica o risco fiscal
de cada documento usando **Machine Learning (Random Forest)** e gera alertas e
orientações de correção ao cliente.

> Desenvolvido para uso no PyCharm. Não requer GPU — funciona integralmente em CPU.

---

## Funcionalidades

| Módulo | O que faz |
|---|---|
| **Análise ICMS** | 8 tipos de divergência: CST vazio, alíquota zerada, BC incorreta, CFOP errado, totais divergentes, DIFAL (EC 87/2015) |
| **Análise ICMS-ST** | Verifica MVA (interno e ajustado), BC-ST, valor ST para 30+ NCMs mapeados em MT/MS |
| **Validação NCM** | Valida estrutura (8 dígitos), alíquota x tabela CSV, isenção, ST obrigatório, sugestão de NCM similar |
| **Comparador NF-e vs SPED** | Cruza XMLs de uma pasta com o SPED do mesmo período em 17 pontos de verificação |
| **Classificação ML** | Random Forest classifica risco como BAIXO / MÉDIO / ALTO; fallback por regras determinísticas |
| **Alertas CLI** | Terminal colorido com problema, valor encontrado, valor esperado e orientação de correção |
| **Relatórios** | CSV e TXT com divergências, resumo, notas não escrituradas e orientações ao cliente |

---

## Estrutura do Projeto

```
sped_icms_analyzer/
│
├── main.py                          # Ponto de entrada — CLI com todos os comandos
├── config.py                        # Configurações globais (alíquotas, CSTs, CFOPs,
│                                    # tolerâncias, encodings, caminhos, comparador)
├── requirements.txt                 # Dependências Python
│
├── parsers/                         # Leitura e estruturação dos arquivos fiscais
│   ├── sped_parser.py               # EFD ICMS/IPI (.txt): reg. 0000, 0150, 0190,
│   │                                #   0200, C100, C170, C190
│   └── nfe_parser.py                # NF-e mod.55 e NFC-e mod.65 (XML): todos os
│                                    #   grupos ICMS (ICMS00..ICMS90, ICMSSNxxx)
│
├── analyzers/                       # Regras e verificações fiscais
│   ├── icms_analyzer.py             # Divergências ICMS: CST, alíquota, BC, CFOP,
│   │                                #   totais C100 vs C190, DIFAL (EC 87/2015)
│   ├── icms_st_analyzer.py          # ICMS-ST: MVA interno/ajustado MT/MS,
│   │                                #   BC-ST, valor ST, 30+ NCMs mapeados
│   └── ncm_analyzer.py              # Valida NCM (8 dígitos), alíquota x tabela CSV,
│                                    #   isenção, ST obrigatório, sugestão de similar
│
├── comparador/                      # Comparação NF-e XML vs SPED Fiscal
│   ├── comparador_nfe_sped.py       # Motor: indexa SPED em memória, cruza 17 campos
│   │                                #   por nota; busca por chave e por número+série
│   └── relatorio_comparador.py      # Alertas no terminal + 4 relatórios CSV/TXT
│
├── ml/                              # Machine Learning — classificação de risco
│   ├── risk_classifier.py           # Random Forest: 16 features, BAIXO/MÉDIO/ALTO;
│   │                                #   fallback por regras se modelo não treinado
│   └── model_trainer.py             # Treina/retreina; gera modelo sintético inicial
│
├── alerts/
│   └── alert_engine.py              # Terminal colorido: gravidade, valores, orientação
│
├── reports/
│   └── report_generator.py          # Relatórios análise ICMS/ST/NCM: 5 arquivos
│
├── data/
│   ├── ncm_aliquotas_mt.csv         # Tabela NCM x alíquota x ST para MT
│   │                                #   (criada automaticamente na 1ª execução)
│   ├── ncm_aliquotas_ms.csv         # Idem para MS
│   ├── historico_treino.csv         # Amostras rotuladas para treino ML (gerado com o uso)
│   └── modelos/
│       ├── risk_classifier.pkl      # Modelo Random Forest serializado (joblib)
│       └── scaler.pkl               # StandardScaler serializado
│
└── relatorios/                      # Todos os relatórios gerados aqui (com timestamp)
    ├── analise_divergencias_*.csv
    ├── analise_resumo_*.csv
    ├── analise_icms_st_*.csv
    ├── analise_ncm_*.csv
    ├── analise_orientacoes_*.txt
    ├── comparador_divergencias_*.csv
    ├── comparador_resumo_*.csv
    ├── comparador_nao_escrituradas_*.csv
    └── comparador_orientacoes_*.txt
```

---

## Instalação

**Requisito:** Python 3.10 ou superior

```bash
# No terminal integrado do PyCharm:
pip install -r requirements.txt
```

| Pacote | Finalidade |
|---|---|
| `scikit-learn` | Random Forest, StandardScaler, validação cruzada |
| `pandas` | Manipulação de dados nos relatórios |
| `numpy` | Vetores de features para o ML |
| `lxml` | Parser XML de alta performance para NF-e/NFC-e |
| `joblib` | Serialização do modelo ML em disco |
| `colorama` | Cores no terminal (Windows/Linux/Mac) |
| `tqdm` | Barra de progresso no processamento em lote |
| `openpyxl` | Suporte a arquivos Excel nos relatórios |

---

## Comandos CLI

### Análise de arquivo único

```bash
# NF-e ou NFC-e XML
python main.py --tipo nfe  --arquivo nota.xml      --uf MT
python main.py --tipo nfce --arquivo cupom.xml     --uf MS

# SPED Fiscal (EFD ICMS/IPI)
python main.py --tipo sped --arquivo EFD_01_2024.txt --uf MT
```

### Análise em lote

```bash
# Analisa todos os XMLs e SPEDs de uma pasta
python main.py --tipo pasta --pasta ./documentos/ --uf MT

# Sem gerar relatórios
python main.py --tipo pasta --pasta ./docs/ --uf MS --sem-relatorio
```

### Comparador NF-e vs SPED

```bash
# Cruza XMLs de uma pasta com o SPED do mesmo período
python main.py --comparar --pasta ./xmls_jan/ --sped EFD_01_2024.txt --uf MT

# Exemplo MS sem relatórios
python main.py --comparar --pasta ./notas/ --sped EFD_MS_01_2024.txt --uf MS --sem-relatorio
```

### Machine Learning

```bash
# Treinar ou retreinar o modelo com histórico acumulado
python main.py --treinar

# Ver status do modelo e importância das features
python main.py --status-ml
```

---

## Comparador NF-e vs SPED — 17 Pontos de Verificação

| # | Bloco | Campos | Gravidade máxima |
|---|---|---|---|
| 1 | Identificação | Número NF, Série, Modelo | ALTA |
| 2 | Emitente | CNPJ, Razão Social, IE (vs 0150 + CNPJ contribuinte) | CRÍTICA |
| 3 | Destinatário | CNPJ, Razão Social, IE (vs registro 0150) | CRÍTICA |
| 4 | Datas | Competência da emissão, Data saída/entrada | ALTA |
| 5 | Totais | vl_doc, vl_merc, vl_frete, vl_seg | CRÍTICA |
| 6 | ICMS | Base de cálculo ICMS, Valor ICMS | CRÍTICA |
| 7 | ICMS-ST | Base de cálculo ST, Valor ST | CRÍTICA |
| 8 | IPI | Valor IPI | ALTA |
| 9 | CFOP/CST | CFOPs e CSTs nos itens XML vs agrupamentos C190 | ALTA |

**Lógica de competência:**

- Nota da competência do SPED **não encontrada no C100** → `🚨 CRÍTICO`
  — orientação: incluir via retificação do SPED
- Nota de **outra competência** → `📅 FORA DA COMPETÊNCIA`
  — orientação: verificar no SPED do período correspondente

**Tolerância:** R$ 0,01 — qualquer diferença acima é reportada.

**Busca:** por chave NF-e (44 dígitos) como prioridade; por número+série+modelo como índice secundário.

**Relatórios gerados:**

| Arquivo | Conteúdo |
|---|---|
| `comparador_divergencias_UF_*.csv` | Uma linha por divergência: campo, valor XML, valor SPED, orientação |
| `comparador_resumo_UF_*.csv` | Uma linha por nota: status, total divergências, diferenças financeiras |
| `comparador_nao_escrituradas_UF_*.csv` | XMLs não encontrados no SPED com competência e caminho |
| `comparador_orientacoes_UF_*.txt` | Texto em linguagem natural para entregar ao cliente |

---

## Machine Learning — Como Funciona

**Algoritmo:** Random Forest (scikit-learn)

**Por que Random Forest:**
- Excelente para dados tabulares fiscais (sem necessidade de GPU)
- Treinamento em segundos para centenas de notas
- Interpretável via importância das features
- Funciona bem com poucos dados iniciais

**16 features extraídas por documento:**

```
n_criticas, n_altas, n_medias, n_baixas, n_total,
flag_cst_vazio, flag_aliq_zerada, flag_total_diverge,
flag_cfop_errado, flag_st_diverge, flag_ncm_invalido,
flag_difal, flag_interestadual,
vl_icms_documento, diferenca_icms, pct_diferenca
```

**Ciclo de melhoria:**

```
1. Analisa documentos → classifica com modelo atual
2. Usuário confirma ou corrige o risco
3. Amostra salva em data/historico_treino.csv
4. python main.py --treinar → modelo retreinado
5. Cada retreino melhora a precisão
```

Na primeira execução sem histórico real, o sistema gera automaticamente um modelo
inicial com 300 amostras sintéticas baseadas nas regras fiscais.

---

## Tabela NCM — Como Manter

Os arquivos `data/ncm_aliquotas_mt.csv` e `ncm_aliquotas_ms.csv` são criados
automaticamente na primeira execução com os NCMs mais comuns para MT/MS.

**Para expandir a tabela:**

1. Abra o CSV no Excel ou qualquer editor de texto
2. Adicione linhas seguindo o cabeçalho:
   ```
   ncm, descricao, aliq_icms_mt, aliq_icms_ms,
   tem_st_mt, tem_st_ms, reducao_bc_mt, reducao_bc_ms,
   isento_mt, isento_ms, observacoes
   ```
3. Use `S` para Sim e `N` para Não nos campos booleanos
4. Salve em UTF-8

**Fontes para atualização:**
- TIPI: https://www.receita.fazenda.gov.br (Decreto 10.923/2021)
- RICMS-MT: Decreto 2.212/2014 e portarias SEFAZ-MT
- RICMS-MS: Decreto 15.093/2018 e portarias SEFAZ-MS
- MVAs de ST: Portarias SEFAZ-MT/MS (verificar mensalmente)

---

## Bases Legais

| Norma | Conteúdo |
|---|---|
| RICMS-MT (Dec. 2.212/2014) | Alíquotas internas, isenções e ST para MT |
| RICMS-MS (Dec. 15.093/2018) | Alíquotas internas, isenções e ST para MS |
| Resolução SF nº 22/1989 | Alíquotas interestaduais (7% e 12%) |
| EC 87/2015 / Conv. ICMS 93/2015 | DIFAL para operações interestaduais a consumidor final |
| Ajuste SINIEF 07/05 | Tabela de CFOP e regras de escrituração |
| Guia Prático EFD ICMS/IPI (SEFAZ) | Estrutura dos registros do SPED Fiscal |
| NT 2014/002 SEFAZ | Validações de totais da NF-e |
| Decreto 10.923/2021 | TIPI — NCM e alíquotas IPI |
| Art. 195 CTN | Prazo de guarda de documentos fiscais (5 anos) |

---

## Roadmap

- [x] Parser SPED Fiscal (EFD ICMS/IPI) — registros 0000, 0150, 0190, 0200, C100, C170, C190
- [x] Parser NF-e XML (mod. 55) e NFC-e (mod. 65) — todos os grupos ICMS
- [x] Análise de divergências ICMS (8 tipos)
- [x] Análise ICMS-ST com MVA MT/MS (30+ NCMs)
- [x] Validação de NCM com tabela CSV local
- [x] Classificação de risco ML (Random Forest)
- [x] Alertas coloridos no terminal (CLI)
- [x] Relatórios CSV e TXT (5 arquivos por execução)
- [x] Comparador NF-e XML vs SPED Fiscal (17 pontos)
- [ ] Análise blocos E110/E111 (apuração ICMS do período)
- [ ] Cálculo automático do DIFAL (EC 87/2015)
- [ ] Interface de confirmação de risco para treino ML com dados reais
- [ ] EFD Contribuições PIS/COFINS — módulo separado (planejado)

---

## Observações

- Relatórios salvos com timestamp para não sobrescrever execuções anteriores
- Encoding do SPED: `latin-1` por padrão (ajustável em `config.py` → `ENCODING_SPED`)
- Todas as orientações são baseadas na legislação vigente; para situações específicas
  consulte um contador ou a SEFAZ-MT/MS
- Mantenha a tabela NCM atualizada, especialmente os MVAs de ICMS-ST que mudam com portarias
- 