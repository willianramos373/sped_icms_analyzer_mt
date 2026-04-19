# analyzers/ncm_analyzer.py
"""
Validação de NCM (Nomenclatura Comum do Mercosul).
Usa tabela local CSV para MT/MS.
A tabela pode ser expandida manualmente ou importada da Receita Federal.
"""

import csv
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from config import NCM_MT_CSV, NCM_MS_CSV


@dataclass
class RegrasNCM:
    """Regras fiscais para um NCM específico"""
    ncm: str
    descricao: str
    aliq_icms_mt: float = 17.0
    aliq_icms_ms: float = 17.0
    tem_st_mt: bool = False
    tem_st_ms: bool = False
    reducao_bc_mt: float = 0.0
    reducao_bc_ms: float = 0.0
    isento_mt: bool = False
    isento_ms: bool = False
    observacoes: str = ""


@dataclass
class ResultadoNCM:
    ncm: str
    valido: bool
    descricao_encontrada: str = ""
    descricao_produto: str = ""
    aliq_esperada: float = 0.0
    aliq_informada: float = 0.0
    divergencias: List[str] = field(default_factory=list)
    orientacoes: List[str] = field(default_factory=list)
    sugestao_ncm: Optional[str] = None


class NCMAnalyzer:
    """
    Valida NCM e verifica alíquotas de ICMS por produto.
    Usa tabela CSV local. Se o NCM não for encontrado, orienta.
    """

    def __init__(self, uf: str):
        self.uf = uf.upper()
        self._tabela: Dict[str, RegrasNCM] = {}
        self._tabela_descricoes: Dict[str, str] = {}  # ncm -> descricao (para sugestão)
        self._carregar_tabela()

    def _carregar_tabela(self):
        """Carrega tabela NCM do CSV. Cria CSV padrão se não existir."""
        csv_path = NCM_MT_CSV if self.uf == "MT" else NCM_MS_CSV

        if not csv_path.exists():
            self._criar_csv_padrao(csv_path)

        try:
            with open(csv_path, encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ncm = row.get("ncm", "").strip().replace(".", "").replace("-", "")
                    if not ncm:
                        continue
                    regra = RegrasNCM(
                        ncm=ncm,
                        descricao=row.get("descricao", ""),
                        aliq_icms_mt=float(row.get("aliq_icms_mt", 17.0) or 17.0),
                        aliq_icms_ms=float(row.get("aliq_icms_ms", 17.0) or 17.0),
                        tem_st_mt=row.get("tem_st_mt", "").upper() in ("S", "SIM", "1", "TRUE"),
                        tem_st_ms=row.get("tem_st_ms", "").upper() in ("S", "SIM", "1", "TRUE"),
                        reducao_bc_mt=float(row.get("reducao_bc_mt", 0) or 0),
                        reducao_bc_ms=float(row.get("reducao_bc_ms", 0) or 0),
                        isento_mt=row.get("isento_mt", "").upper() in ("S", "SIM", "1", "TRUE"),
                        isento_ms=row.get("isento_ms", "").upper() in ("S", "SIM", "1", "TRUE"),
                        observacoes=row.get("observacoes", ""),
                    )
                    self._tabela[ncm] = regra
                    # Índice para sugestão por prefixo
                    self._tabela_descricoes[ncm] = regra.descricao
        except Exception as e:
            print(f"[AVISO] Erro ao carregar tabela NCM: {e}")

    def _criar_csv_padrao(self, path: Path):
        """Cria CSV base com NCMs mais comuns em MT/MS."""
        cabecalho = [
            "ncm", "descricao", "aliq_icms_mt", "aliq_icms_ms",
            "tem_st_mt", "tem_st_ms", "reducao_bc_mt", "reducao_bc_ms",
            "isento_mt", "isento_ms", "observacoes"
        ]
        # Base inicial com NCMs comuns
        dados = [
            # NCM, Descricao, aliq_mt, aliq_ms, st_mt, st_ms, red_mt, red_ms, is_mt, is_ms, obs
            ["01012100", "Cavalos reprodutores raça pura", "12", "12", "N", "N", "0", "0", "N", "N", ""],
            ["02013000", "Carnes bovinas frescas desossadas", "7", "7", "N", "N", "0", "0", "S", "S", "Isenção ICMS alimentos básicos"],
            ["02023000", "Carnes bovinas congeladas desossadas", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["02071100", "Frangos inteiros", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["04011000", "Leite fluido", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["04021000", "Leite em pó", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["07019000", "Batatas frescas", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["07031000", "Cebolas frescas", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["07061000", "Cenouras e nabos frescos", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["08051000", "Laranjas frescas", "7", "7", "N", "N", "0", "0", "S", "S", ""],
            ["10011000", "Trigo duro", "12", "12", "N", "N", "0", "0", "N", "N", ""],
            ["10051000", "Milho semente", "12", "12", "N", "N", "0", "0", "N", "N", ""],
            ["10059010", "Milho grão exceto semente", "12", "12", "N", "N", "0", "0", "N", "N", ""],
            ["12010090", "Soja em grão", "12", "12", "N", "N", "0", "0", "N", "N", "Isenção ICMS exportação"],
            ["15071100", "Óleo de soja bruto", "17", "17", "N", "N", "0", "0", "N", "N", ""],
            ["22021000", "Água mineral", "17", "17", "S", "S", "0", "0", "N", "N", "ST"],
            ["22030000", "Cerveja de malte", "17", "17", "S", "S", "0", "0", "N", "N", "ST obrigatório"],
            ["27101259", "Gasolina automotiva", "17", "17", "S", "S", "0", "0", "N", "N", "ST monofásico"],
            ["27102000", "Óleo diesel", "17", "17", "S", "S", "0", "0", "N", "N", "ST monofásico"],
            ["30021200", "Vacinas veterinárias", "12", "12", "N", "N", "0", "0", "N", "N", ""],
            ["30049099", "Medicamentos uso humano", "12", "12", "S", "S", "0", "0", "N", "N", "ST medicamentos"],
            ["33049900", "Cosméticos e perfumaria", "17", "17", "S", "S", "0", "0", "N", "N", "ST cosméticos"],
            ["40111000", "Pneumáticos automóveis", "17", "17", "S", "S", "0", "0", "N", "N", "ST pneus"],
            ["40119900", "Pneumáticos outros", "17", "17", "S", "S", "0", "0", "N", "N", "ST pneus"],
            ["84713000", "Computadores portáteis", "12", "12", "N", "N", "0", "0", "N", "N", ""],
            ["85171211", "Telefones celulares", "17", "17", "S", "S", "0", "0", "N", "N", "ST eletrônicos"],
            ["85176292", "Smartphones", "17", "17", "S", "S", "0", "0", "N", "N", "ST eletrônicos"],
            ["87032190", "Automóveis passageiros", "17", "17", "S", "S", "0", "0", "N", "N", "ST veículos"],
            ["87089900", "Autopeças em geral", "17", "17", "S", "S", "0", "0", "N", "N", "ST autopeças"],
        ]

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)
            writer.writerows(dados)

        print(f"[INFO] Tabela NCM criada em: {path}")
        print("[INFO] Edite o arquivo CSV para adicionar mais NCMs conforme necessário.")

    # ─────────────────────────────────────────
    # VALIDAÇÃO
    # ─────────────────────────────────────────

    def validar_ncm(
        self,
        ncm: str,
        descricao_produto: str = "",
        aliq_informada: float = 0.0,
        cst: str = "",
    ) -> ResultadoNCM:
        """Valida um NCM e verifica alíquota/ST."""
        ncm_limpo = ncm.replace(".", "").replace("-", "").strip()

        resultado = ResultadoNCM(
            ncm=ncm_limpo,
            valido=False,
            descricao_produto=descricao_produto,
            aliq_informada=aliq_informada,
        )

        # 1) NCM com 8 dígitos?
        if len(ncm_limpo) != 8:
            resultado.divergencias.append(
                f"NCM '{ncm}' tem {len(ncm_limpo)} dígitos. NCM deve ter exatamente 8 dígitos."
            )
            resultado.orientacoes.append(
                "Verifique o NCM do produto na Tabela TIPI (Decreto 10.923/2021). "
                "Consulte: https://www.receita.fazenda.gov.br/aliquotas/tipi.htm"
            )
            return resultado

        # 2) Busca na tabela (exata, depois prefixo 6, 4 dígitos)
        regra = (
            self._tabela.get(ncm_limpo) or
            self._buscar_por_prefixo(ncm_limpo, 6) or
            self._buscar_por_prefixo(ncm_limpo, 4)
        )

        if regra is None:
            resultado.valido = True  # Estruturalmente ok, não na tabela local
            resultado.divergencias.append(
                f"NCM {ncm_limpo} não encontrado na tabela local de {self.uf}. "
                "Verifique se o NCM está correto e se há regras específicas."
            )
            resultado.orientacoes.append(
                "Consulte a TIPI na Receita Federal para confirmar o NCM correto. "
                "Adicione o NCM ao arquivo CSV da tabela local para análises futuras. "
                f"Arquivo: data/ncm_aliquotas_{self.uf.lower()}.csv"
            )
            resultado.sugestao_ncm = self._sugerir_ncm(ncm_limpo, descricao_produto)
            return resultado

        resultado.valido = True
        resultado.descricao_encontrada = regra.descricao
        aliq_esperada = regra.aliq_icms_mt if self.uf == "MT" else regra.aliq_icms_ms
        resultado.aliq_esperada = aliq_esperada
        tem_st = regra.tem_st_mt if self.uf == "MT" else regra.tem_st_ms
        isento = regra.isento_mt if self.uf == "MT" else regra.isento_ms

        # 3) Verifica alíquota informada vs esperada
        if aliq_informada > 0 and abs(aliq_informada - aliq_esperada) > 0.5:
            if not isento:
                resultado.divergencias.append(
                    f"Alíquota ICMS {aliq_informada}% difere da esperada "
                    f"{aliq_esperada}% para NCM {ncm_limpo} ({regra.descricao}) em {self.uf}."
                )
                resultado.orientacoes.append(
                    f"A alíquota correta para NCM {ncm_limpo} em {self.uf} é {aliq_esperada}%. "
                    f"Corrija o cadastro do produto no sistema. {regra.observacoes}"
                )

        # 4) Produto isento mas com ICMS
        if isento and aliq_informada > 0:
            resultado.divergencias.append(
                f"NCM {ncm_limpo} ({regra.descricao}) é isento de ICMS em {self.uf}, "
                f"mas alíquota {aliq_informada}% foi informada."
            )
            resultado.orientacoes.append(
                f"Utilize CST 40 (Isenta) para este produto em {self.uf} e "
                "zere a alíquota/valor de ICMS. Verifique o benefício fiscal aplicável."
            )

        # 5) Produto tem ST mas CST não indica ST
        if tem_st and cst and cst not in ("10", "30", "60", "70"):
            resultado.divergencias.append(
                f"NCM {ncm_limpo} está sujeito a ICMS-ST em {self.uf}, "
                f"mas CST {cst} não indica substituição tributária."
            )
            resultado.orientacoes.append(
                f"Produtos com NCM {ncm_limpo} em {self.uf} devem usar CST 10 "
                "(se o contribuinte é substituto) ou CST 60 "
                "(se o ICMS-ST já foi retido). Ajuste o CST no sistema."
            )

        return resultado

    def _buscar_por_prefixo(self, ncm: str, tamanho: int) -> Optional[RegrasNCM]:
        """Busca NCM por prefixo de n dígitos."""
        prefixo = ncm[:tamanho]
        for ncm_tab, regra in self._tabela.items():
            if ncm_tab.startswith(prefixo):
                return regra
        return None

    def _sugerir_ncm(self, ncm: str, descricao: str) -> Optional[str]:
        """Sugere NCM similar pelo prefixo de 4 dígitos."""
        if not self._tabela:
            return None
        prefixo = ncm[:4]
        candidatos = [n for n in self._tabela if n.startswith(prefixo)]
        if candidatos:
            return candidatos[0]
        return None

    def validar_lista(self, itens: List[Tuple]) -> List[ResultadoNCM]:
        """
        Valida uma lista de itens.
        itens: [(ncm, descricao, aliq, cst), ...]
        """
        return [
            self.validar_ncm(ncm, desc, aliq, cst)
            for ncm, desc, aliq, cst in itens
        ]

    def resumo(self, resultados: List[ResultadoNCM]) -> Dict:
        return {
            "total": len(resultados),
            "validos": sum(1 for r in resultados if r.valido),
            "com_divergencia": sum(1 for r in resultados if r.divergencias),
            "invalidos": sum(1 for r in resultados if not r.valido),
        }
