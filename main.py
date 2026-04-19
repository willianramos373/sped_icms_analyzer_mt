# main.py  (execute como: python -m sped_icms_analyzer.main)
"""
SPED ICMS Analyzer - MT
Ponto de entrada CLI.

Uso recomendado (a partir da pasta PAI do projeto):
    python -m sped_icms_analyzer.main --help

Exemplos:
    python -m sped_icms_analyzer.main --tipo nfe   --arquivo nota.xml
    python -m sped_icms_analyzer.main --tipo sped  --arquivo EFD_01_2024.txt
    python -m sped_icms_analyzer.main --tipo pasta --pasta ./xmls/
    python -m sped_icms_analyzer.main --comparar   --pasta ./xmls/ --sped EFD.txt
    python -m sped_icms_analyzer.main --treinar
    python -m sped_icms_analyzer.main --status-ml
"""

import argparse
import logging
import sys
from pathlib import Path

from colorama import Fore, Style, init

from config import UF_PADRAO, REPORTS_DIR
from pipeline import Pipeline, ResultadoFinal
from alerts.alert_engine import AlertEngine
from reports.report_generator import ReportGenerator

init(autoreset=True)
log = logging.getLogger(__name__)

# UF fixo: apenas Mato Grosso
UF = UF_PADRAO


# ─────────────────────────────────────────────────────────────────────────────
# BANNER
# ─────────────────────────────────────────────────────────────────────────────

def _banner() -> None:
    print(Fore.CYAN + Style.BRIGHT + """
+----------------------------------------------------------+
|        SPED ICMS ANALYZER - MT                           |
|        Analise Fiscal com Machine Learning               |
+----------------------------------------------------------+
""" + Style.RESET_ALL)


# ─────────────────────────────────────────────────────────────────────────────
# HANDLERS DE COMANDO
# ─────────────────────────────────────────────────────────────────────────────

def _exibir_resultado(res: ResultadoFinal, alert: AlertEngine) -> None:
    """Exibe resultado de um documento no terminal."""
    if not res.sucesso:
        print(Fore.RED + f"[ERRO] {res.dado.caminho_arquivo}: {res.erro}")
        return
    alert.exibir_resultado_icms(
        resultado=res.icms,
        classificacao_ml=res.classificacao,
        resultados_st=res.st,
        resultados_ncm=res.ncm,
    )


def cmd_nfe(caminho: str, sem_relatorio: bool) -> None:
    """Processa um arquivo XML (NF-e ou NFC-e)."""
    pipe  = Pipeline(UF)
    alert = AlertEngine()

    res = pipe.processar_xml(caminho)
    if res is None:
        print(Fore.RED + f"[ERRO] Nao foi possivel processar: {caminho}")
        sys.exit(1)

    _exibir_resultado(res, alert)
    alert.orientacao_geral(UF)

    if not sem_relatorio and res.sucesso:
        rpt = ReportGenerator(UF)
        arquivos = rpt.gerar_relatorio_completo(
            [res.icms], [res.classificacao],
            resultados_st=[[res.st]],
            resultados_ncm=[[res.ncm]],
        )
        _imprimir_arquivos(arquivos)


def cmd_sped(caminho: str, sem_relatorio: bool) -> None:
    """Processa um arquivo SPED Fiscal (.txt)."""
    pipe  = Pipeline(UF)
    alert = AlertEngine()

    print(Fore.CYAN + f"[INFO] Processando SPED: {caminho}" + Style.RESET_ALL)
    resultados = pipe.processar_sped(caminho)

    if not resultados:
        print(Fore.RED + "[ERRO] Nenhuma nota processada no SPED.")
        sys.exit(1)

    icms_list  = [r.icms for r in resultados]
    clf_list   = [r.classificacao for r in resultados]

    for res in resultados:
        if res.risco in ("ALTO", "MEDIO"):
            _exibir_resultado(res, alert)

    alert.exibir_resumo_lote(icms_list, clf_list)
    alert.orientacao_geral(UF)

    if not sem_relatorio:
        rpt = ReportGenerator(UF, prefixo="sped")
        arquivos = rpt.gerar_relatorio_completo(icms_list, clf_list)
        _imprimir_arquivos(arquivos)


def cmd_pasta(pasta: str, sem_relatorio: bool) -> None:
    """Processa todos os documentos de uma pasta com paralelismo."""
    pipe  = Pipeline(UF)
    alert = AlertEngine()

    if sem_relatorio:
        # Streaming: exibe e descarta
        icms_list, clf_list = [], []
        for res in pipe.processar_pasta(pasta):
            if res.risco in ("ALTO", "MEDIO"):
                _exibir_resultado(res, alert)
            icms_list.append(res.icms)
            clf_list.append(res.classificacao)
        alert.exibir_resumo_lote(icms_list, clf_list)
        alert.orientacao_geral(UF)
    else:
        # Escrita incremental: nao acumula em RAM
        from datetime import datetime
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        arq = pipe.processar_pasta_para_csv(pasta, f"lote_{UF}_{ts}.csv")
        print(Fore.GREEN + f"[OK] CSV gerado: {arq}" + Style.RESET_ALL)
        alert.orientacao_geral(UF)


def cmd_comparar(pasta: str, caminho_sped: str, sem_relatorio: bool) -> None:
    """Cruza XMLs de uma pasta com um arquivo SPED."""
    from comparador.relatorio_comparador import AlertaComparador, RelatorioComparador
    from comparador.comparador_nfe_sped import ComparadorNFeSped

    comp = ComparadorNFeSped(caminho_sped, pasta, UF)
    resultados = comp.comparar()

    if not resultados:
        print(Fore.YELLOW + "[AVISO] Nenhum resultado gerado.")
        return

    alerta = AlertaComparador()
    for res in resultados:
        if res.status != "OK":
            alerta.exibir_resultado(res)

    alerta.exibir_resumo(
        resultados,
        comp._dt_ini_sped,
        comp._dt_fin_sped,
        comp._cnpj_contribuinte,
    )

    if not sem_relatorio:
        rpt = RelatorioComparador(UF)
        _imprimir_arquivos(rpt.gerar_todos(resultados))


def cmd_treinar() -> None:
    """Treina ou retreina o modelo ML."""
    from ml.model_trainer import treinar_modelo, status_modelo
    st = status_modelo()
    print(f"
[INFO] Status do modelo:")
    print(f"  Modelo existe      : {'Sim' if st['modelo_existe'] else 'Nao'}")
    print(f"  Amostras historicas: {st['amostras_historico']}")
    print(f"  Pronto (>=10 reais): {'Sim' if st['pronto_para_treino_real'] else 'Nao'}
")
    treinar_modelo(verbose=True)


def cmd_status_ml() -> None:
    """Exibe status do modelo ML e importancia das features."""
    from ml.model_trainer import status_modelo
    from ml.risk_classifier import RiskClassifier
    st  = status_modelo()
    clf = RiskClassifier()
    print(Fore.CYAN + "=== STATUS ML ===" + Style.RESET_ALL)
    print(f"Modelo    : {'Carregado' if st['modelo_existe'] else 'Nao encontrado'}")
    print(f"Amostras  : {st['amostras_historico']}")
    print(f"Caminho   : {st['caminho_modelo']}")
    print(f"Usando ML : {'Sim (Random Forest)' if clf.usa_ml else 'Nao (regras)'}")
    if clf.usa_ml:
        imp = clf.importancia_features()
        if imp:
            print(Fore.CYAN + "
Top 8 features:" + Style.RESET_ALL)
            for feat, val in list(imp.items())[:8]:
                barra = "#" * int(val * 40)
                print(f"  {feat:<32} {barra} {val:.3f}")
    else:
        print("Execute: python -m sped_icms_analyzer.main --treinar")


def _imprimir_arquivos(arquivos) -> None:
    print(Fore.GREEN + "[OK] Relatorios gerados:" + Style.RESET_ALL)
    for arq in arquivos:
        if arq:
            print(f"  {arq}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _configurar_log(debug: bool) -> None:
    nivel = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=nivel,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def main() -> None:
    _banner()

    parser = argparse.ArgumentParser(
        prog="python -m sped_icms_analyzer.main",
        description="SPED ICMS Analyzer - Analise fiscal MT",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--tipo", choices=["sped", "nfe", "nfce", "pasta"],
        help="Tipo de arquivo a analisar"
    )
    parser.add_argument("--arquivo", help="Caminho do arquivo a analisar")
    parser.add_argument("--pasta",   help="Caminho da pasta com XMLs ou documentos")
    parser.add_argument("--sped",    help="Caminho do SPED .txt (usado com --comparar)")

    parser.add_argument(
        "--comparar", action="store_true",
        help="Cruza XMLs de --pasta com o SPED de --sped"
    )
    parser.add_argument(
        "--treinar", action="store_true",
        help="Treina/retreina o modelo ML com historico acumulado"
    )
    parser.add_argument(
        "--status-ml", action="store_true",
        help="Exibe status do modelo ML e importancia das features"
    )

    # CLI corrigido: dois flags independentes sem conflito
    parser.add_argument(
        "--sem-relatorio", action="store_true",
        help="Nao gera arquivos de relatorio (apenas exibe no terminal)"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Ativa log detalhado (DEBUG)"
    )

    args = parser.parse_args()
    _configurar_log(args.debug)

    if args.treinar:
        cmd_treinar()
        return

    if args.status_ml:
        cmd_status_ml()
        return

    if args.comparar:
        if not args.pasta or not args.sped:
            parser.error("--comparar requer --pasta e --sped")
        cmd_comparar(args.pasta, args.sped, args.sem_relatorio)
        return

    if args.tipo == "pasta":
        if not args.pasta:
            parser.error("--tipo pasta requer --pasta")
        cmd_pasta(args.pasta, args.sem_relatorio)
        return

    if args.tipo in ("nfe", "nfce"):
        if not args.arquivo:
            parser.error(f"--tipo {args.tipo} requer --arquivo")
        cmd_nfe(args.arquivo, args.sem_relatorio)
        return

    if args.tipo == "sped":
        if not args.arquivo:
            parser.error("--tipo sped requer --arquivo")
        cmd_sped(args.arquivo, args.sem_relatorio)
        return

    # Nenhum comando reconhecido
    parser.print_help()
    print(Fore.YELLOW + "\nExemplos:" + Style.RESET_ALL)
    print("  python -m sped_icms_analyzer.main --tipo nfe   --arquivo nota.xml")
    print("  python -m sped_icms_analyzer.main --tipo sped  --arquivo EFD_01_2024.txt")
    print("  python -m sped_icms_analyzer.main --tipo pasta --pasta ./xmls/")
    print("  python -m sped_icms_analyzer.main --comparar   --pasta ./xmls/ --sped EFD.txt")
    print("  python -m sped_icms_analyzer.main --treinar")


if __name__ == "__main__":
    main()