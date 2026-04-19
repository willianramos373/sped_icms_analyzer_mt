# main.py
"""
SPED ICMS Analyzer - Ponto de entrada CLI
Uso: python main.py --help

Exemplos:
  python main.py --tipo sped --arquivo minha_efd.txt --uf MT
  python main.py --tipo nfe --arquivo nota.xml --uf MS
  python main.py --tipo nfce --arquivo cupom.xml --uf MT
  python main.py --tipo pasta --pasta ./xmls/ --uf MT
  python main.py --treinar
  python main.py --status-ml
"""

import argparse
import sys
import os
from pathlib import Path
from tqdm import tqdm

# Garante que o diretório do projeto está no PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from colorama import Fore, Style, init
init(autoreset=True)


def _banner():
    print(Fore.CYAN + Style.BRIGHT + """
╔══════════════════════════════════════════════════════════╗
║        SPED ICMS ANALYZER - MT/MS                        ║
║        Análise Fiscal com Machine Learning               ║
╚══════════════════════════════════════════════════════════╝
""" + Style.RESET_ALL)


def analisar_nfe(caminho: str, uf: str, verbose: bool = True):
    """Pipeline completo: parse → análise → ML → alerta → relatório."""
    from parsers.nfe_parser import NFeParser
    from analyzers.icms_analyzer import ICMSAnalyzer
    from analyzers.icms_st_analyzer import ICMSSTAnalyzer
    from analyzers.ncm_analyzer import NCMAnalyzer
    from ml.risk_classifier import RiskClassifier
    from alerts.alert_engine import AlertEngine
    from reports.report_generator import ReportGenerator

    # 1. Parse
    nfe = NFeParser(caminho).parse()
    if nfe.erros_leitura:
        for erro in nfe.erros_leitura:
            print(Fore.RED + f"[ERRO PARSE] {erro}")
        return None, None

    # 2. Análise ICMS
    icms = ICMSAnalyzer(uf)
    resultado = icms.analisar_nfe(nfe)

    # 3. Análise ST
    st_analyzer = ICMSSTAnalyzer(uf)
    resultados_st = st_analyzer.analisar_itens_nfe(nfe)

    # 4. Análise NCM
    ncm_analyzer = NCMAnalyzer(uf)
    itens_ncm = [
        (item.ncm, item.descricao, item.icms.aliq, item.icms.cst)
        for item in nfe.itens
    ]
    resultados_ncm = ncm_analyzer.validar_lista(itens_ncm)

    # 5. Classificação ML
    classifier = RiskClassifier()
    classif = classifier.classificar(resultado)

    # 6. Alertas no terminal
    if verbose:
        alert = AlertEngine()
        alert.exibir_resultado_icms(resultado, classif, resultados_st, resultados_ncm)

    return resultado, classif


def analisar_sped(caminho: str, uf: str, verbose: bool = True):
    """Pipeline SPED Fiscal."""
    from parsers.sped_parser import SpedParser
    from analyzers.icms_analyzer import ICMSAnalyzer
    from analyzers.icms_st_analyzer import ICMSSTAnalyzer
    from analyzers.ncm_analyzer import NCMAnalyzer
    from ml.risk_classifier import RiskClassifier
    from alerts.alert_engine import AlertEngine

    sped = SpedParser(caminho).parse()
    if sped.erros_leitura and verbose:
        print(Fore.YELLOW + f"[AVISO] {len(sped.erros_leitura)} erro(s) na leitura do SPED:")
        for e in sped.erros_leitura[:5]:
            print(f"  {e}")

    if not sped.notas:
        print(Fore.RED + "[ERRO] Nenhuma nota fiscal encontrada no SPED.")
        return [], []

    print(f"\n{Fore.CYAN}[INFO] {len(sped.notas)} nota(s) encontrada(s) no SPED.{Style.RESET_ALL}")

    icms = ICMSAnalyzer(uf)
    st_analyzer = ICMSSTAnalyzer(uf)
    ncm_analyzer = NCMAnalyzer(uf)
    classifier = RiskClassifier()

    todos_resultados = []
    todas_classifs = []

    for nota in tqdm(sped.notas, desc="Analisando notas", unit="NF"):
        resultado = icms.analisar_nota_sped(nota, sped.produtos)
        classif = classifier.classificar(resultado)
        todos_resultados.append(resultado)
        todas_classifs.append(classif)

    if verbose:
        alert = AlertEngine()
        # Exibe detalhes apenas das notas com risco alto/médio
        for resultado, classif in zip(todos_resultados, todas_classifs):
            if classif.get("risco") in ("ALTO", "MEDIO"):
                alert.exibir_resultado_icms(resultado, classif)
        alert.exibir_resumo_lote(todos_resultados, todas_classifs)

    return todos_resultados, todas_classifs


def analisar_pasta(pasta: str, uf: str):
    """Analisa todos os XMLs e SPEDs de uma pasta."""
    path = Path(pasta)
    if not path.exists():
        print(Fore.RED + f"[ERRO] Pasta não encontrada: {pasta}")
        sys.exit(1)

    xmls = list(path.glob("*.xml")) + list(path.glob("*.XML"))
    speds = list(path.glob("*.txt")) + list(path.glob("*.TXT"))

    print(f"[INFO] Encontrados: {len(xmls)} XML(s), {len(speds)} SPED(s)")

    todos_resultados = []
    todas_classifs = []

    # Processa XMLs
    for xml_path in tqdm(xmls, desc="XMLs NF-e/NFC-e"):
        res, clf = analisar_nfe(str(xml_path), uf, verbose=False)
        if res:
            todos_resultados.append(res)
            todas_classifs.append(clf)

    # Processa SPEDs
    for sped_path in tqdm(speds, desc="SPEDs"):
        resu, clfs = analisar_sped(str(sped_path), uf, verbose=False)
        todos_resultados.extend(resu)
        todas_classifs.extend(clfs)

    if not todos_resultados:
        print(Fore.YELLOW + "[AVISO] Nenhum documento processado com sucesso.")
        return

    # Resumo
    from alerts.alert_engine import AlertEngine
    from reports.report_generator import ReportGenerator
    alert = AlertEngine()
    alert.exibir_resumo_lote(todos_resultados, todas_classifs)
    alert.orientacao_geral(uf)

    # Relatórios
    rpt = ReportGenerator(uf, prefixo="lote")
    arquivos = rpt.gerar_relatorio_completo(todos_resultados, todas_classifs)
    print(f"\n{Fore.GREEN}[OK] Relatórios gerados:{Style.RESET_ALL}")
    for arq in arquivos:
        print(f"  📄 {arq}")


def gerar_relatorio_unico(resultado, classif, uf: str):
    """Gera relatório para análise de documento único."""
    from reports.report_generator import ReportGenerator
    rpt = ReportGenerator(uf)
    arquivos = rpt.gerar_relatorio_completo([resultado], [classif])
    print(f"\n{Fore.GREEN}[OK] Relatórios gerados:{Style.RESET_ALL}")
    for arq in arquivos:
        print(f"  📄 {arq}")


def cmd_treinar():
    """Treina ou retreina o modelo ML."""
    from ml.model_trainer import treinar_modelo, status_modelo
    status = status_modelo()
    print(f"\n[INFO] Status do modelo:")
    print(f"  Modelo existe      : {'Sim' if status['modelo_existe'] else 'Não'}")
    print(f"  Amostras históricas: {status['amostras_historico']}")
    print(f"  Pronto (≥10 reais) : {'Sim' if status['pronto_para_treino_real'] else 'Não'}\n")
    treinar_modelo(verbose=True)


def cmd_status_ml():
    """Exibe status do modelo ML e importância das features."""
    from ml.model_trainer import status_modelo
    from ml.risk_classifier import RiskClassifier
    status = status_modelo()
    print(f"\n{Fore.CYAN}=== STATUS ML ==={Style.RESET_ALL}")
    print(f"Modelo       : {'✅ Carregado' if status['modelo_existe'] else '❌ Não encontrado'}")
    print(f"Amostras     : {status['amostras_historico']}")
    print(f"Caminho      : {status['caminho_modelo']}")
    print(f"Histórico    : {status['caminho_historico']}")

    clf = RiskClassifier()
    if clf.usa_ml:
        print(f"\nUsando ML    : Sim (Random Forest)")
        imp = clf.importancia_features()
        if imp:
            print(f"\n{Fore.CYAN}Top 8 features:{Style.RESET_ALL}")
            for feat, val in list(imp.items())[:8]:
                barra = "█" * int(val * 40)
                print(f"  {feat:<30} {barra} {val:.3f}")
    else:
        print(f"\nUsando ML    : Não (modo regras)")
        print("Execute: python main.py --treinar")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    _banner()

    parser = argparse.ArgumentParser(
        description="SPED ICMS Analyzer - Análise fiscal MT/MS",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--tipo", choices=["sped", "nfe", "nfce", "pasta"],
                        help="Tipo de arquivo a analisar")
    parser.add_argument("--arquivo", help="Caminho do arquivo a analisar")
    parser.add_argument("--pasta", help="Caminho da pasta com múltiplos arquivos")
    parser.add_argument("--uf", choices=["MT", "MS"], default="MT",
                        help="UF para regras ICMS (padrão: MT)")
    parser.add_argument("--treinar", action="store_true",
                        help="Treina/retreina o modelo ML")
    parser.add_argument("--status-ml", action="store_true",
                        help="Exibe status do modelo ML")
    parser.add_argument("--relatorio", action="store_true", default=True,
                        help="Gera relatórios CSV/TXT (padrão: ativo)")
    parser.add_argument("--sem-relatorio", action="store_true",
                        help="Desativa geração de relatórios")

    args = parser.parse_args()

    if args.treinar:
        cmd_treinar()
        return

    if args.status_ml:
        cmd_status_ml()
        return

    if args.tipo == "pasta":
        if not args.pasta:
            print(Fore.RED + "[ERRO] Informe --pasta para análise em lote.")
            sys.exit(1)
        analisar_pasta(args.pasta, args.uf)
        return

    if not args.tipo or not args.arquivo:
        parser.print_help()
        print(f"\n{Fore.YELLOW}Exemplos de uso:{Style.RESET_ALL}")
        print("  python main.py --tipo sped --arquivo EFD_2024_01.txt --uf MT")
        print("  python main.py --tipo nfe  --arquivo nota_fiscal.xml --uf MS")
        print("  python main.py --tipo pasta --pasta ./documentos/ --uf MT")
        print("  python main.py --treinar")
        return

    if args.tipo in ("nfe", "nfce"):
        resultado, classif = analisar_nfe(args.arquivo, args.uf)
        if resultado and classif and not args.sem_relatorio:
            gerar_relatorio_unico(resultado, classif, args.uf)

    elif args.tipo == "sped":
        resultados, classifs = analisar_sped(args.arquivo, args.uf)
        if resultados and not args.sem_relatorio:
            from reports.report_generator import ReportGenerator
            rpt = ReportGenerator(args.uf, prefixo="sped")
            arquivos = rpt.gerar_relatorio_completo(resultados, classifs)
            print(f"\n{Fore.GREEN}[OK] Relatórios gerados:{Style.RESET_ALL}")
            for arq in arquivos:
                print(f"  📄 {arq}")

    from alerts.alert_engine import AlertEngine
    AlertEngine().orientacao_geral(args.uf)


if __name__ == "__main__":
    main()
