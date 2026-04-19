# ml/risk_classifier.py
"""
Classificador de Risco Fiscal usando Random Forest (scikit-learn).
Classifica documentos como: BAIXO / MEDIO / ALTO risco.

Escolha técnica: scikit-learn Random Forest
  - Funciona excelente em CPU (sem GPU necessária)
  - Ótimo para dados tabulares fiscais
  - Treinamento rápido (segundos para centenas de notas)
  - Modelos interpretáveis (importância das features)
  - Requer poucos dados para começar a funcionar
"""

import numpy as np
import joblib
from pathlib import Path
from typing import Optional, List, Dict
from config import MODEL_PATH, SCALER_PATH, ENCODER_PATH, RISCO_ALTO_THRESHOLD, RISCO_MEDIO_THRESHOLD


# ─────────────────────────────────────────────
# EXTRAÇÃO DE FEATURES
# ─────────────────────────────────────────────

def extrair_features_resultado(resultado) -> List[float]:
    """
    Extrai vetor de features numérico de um ResultadoAnaliseICMS.
    Deve ser chamado APÓS análise ICMS, ST e NCM.
    """
    divs = resultado.divergencias

    # Contagem por gravidade
    n_criticas = sum(1 for d in divs if d.gravidade == "CRITICA")
    n_altas    = sum(1 for d in divs if d.gravidade == "ALTA")
    n_medias   = sum(1 for d in divs if d.gravidade == "MEDIA")
    n_baixas   = sum(1 for d in divs if d.gravidade == "BAIXA")
    n_total    = len(divs)

    # Tipos específicos de divergência (flags 0/1)
    tipos = {d.tipo for d in divs}
    flag_cst_vazio          = 1 if any("CST_VAZIO" in t for t in tipos) else 0
    flag_aliq_zerada        = 1 if any("ALIQ_ZERADA" in t for t in tipos) else 0
    flag_total_diverge      = 1 if any("TOTAL_DIVERGE" in t for t in tipos) else 0
    flag_cfop_errado        = 1 if any("CFOP" in t for t in tipos) else 0
    flag_st_diverge         = 1 if any("ST" in t for t in tipos) else 0
    flag_ncm_invalido       = 1 if any("NCM" in t for t in tipos) else 0
    flag_difal              = 1 if any("DIFAL" in t for t in tipos) else 0
    flag_interestadual      = 1 if any("INTERESTADUAL" in t for t in tipos) else 0

    # Valores financeiros
    vl_icms_doc      = getattr(resultado, "total_icms_documento", 0.0)
    diferenca_icms   = getattr(resultado, "diferenca_icms", 0.0)
    pct_diferenca    = (diferenca_icms / vl_icms_doc) if vl_icms_doc > 0 else 0.0

    return [
        n_criticas,
        n_altas,
        n_medias,
        n_baixas,
        n_total,
        flag_cst_vazio,
        flag_aliq_zerada,
        flag_total_diverge,
        flag_cfop_errado,
        flag_st_diverge,
        flag_ncm_invalido,
        flag_difal,
        flag_interestadual,
        min(vl_icms_doc, 1_000_000),   # cap para evitar outliers extremos
        min(diferenca_icms, 100_000),
        min(pct_diferenca, 1.0),       # cap em 100%
    ]


FEATURE_NAMES = [
    "n_criticas", "n_altas", "n_medias", "n_baixas", "n_total",
    "flag_cst_vazio", "flag_aliq_zerada", "flag_total_diverge",
    "flag_cfop_errado", "flag_st_diverge", "flag_ncm_invalido",
    "flag_difal", "flag_interestadual",
    "vl_icms_documento", "diferenca_icms", "pct_diferenca",
]

CLASSES = ["BAIXO", "MEDIO", "ALTO"]


# ─────────────────────────────────────────────
# CLASSIFICADOR
# ─────────────────────────────────────────────

class RiskClassifier:
    """
    Classificador de risco fiscal.
    Se o modelo treinado existir, usa ML.
    Caso contrário, usa regras determinísticas como fallback.
    """

    def __init__(self):
        self.modelo = None
        self.scaler = None
        self._modelo_carregado = False
        self._tentar_carregar()

    def _tentar_carregar(self):
        """Tenta carregar modelo salvo em disco."""
        if MODEL_PATH.exists() and SCALER_PATH.exists():
            try:
                self.modelo = joblib.load(MODEL_PATH)
                self.scaler = joblib.load(SCALER_PATH)
                self._modelo_carregado = True
            except Exception as e:
                print(f"[AVISO] Não foi possível carregar modelo ML: {e}")
                self._modelo_carregado = False

    @property
    def usa_ml(self) -> bool:
        return self._modelo_carregado and self.modelo is not None

    def classificar(self, resultado) -> Dict:
        """
        Classifica risco de um ResultadoAnaliseICMS.
        Retorna dict com: risco, probabilidades, metodo.
        """
        features = extrair_features_resultado(resultado)

        if self.usa_ml:
            return self._classificar_ml(features)
        else:
            return self._classificar_regras(resultado)

    def _classificar_ml(self, features: List[float]) -> Dict:
        """Classificação via Random Forest treinado."""
        X = np.array(features).reshape(1, -1)
        X_scaled = self.scaler.transform(X)
        pred = self.modelo.predict(X_scaled)[0]
        proba = self.modelo.predict_proba(X_scaled)[0]

        classes_modelo = list(self.modelo.classes_)
        probabilidades = {cls: float(p) for cls, p in zip(classes_modelo, proba)}

        return {
            "risco": pred,
            "probabilidades": probabilidades,
            "confianca": float(max(proba)),
            "metodo": "Random Forest (ML)",
        }

    def _classificar_regras(self, resultado) -> Dict:
        """Fallback: classificação por regras determinísticas."""
        divs = resultado.divergencias
        n_criticas = sum(1 for d in divs if d.gravidade == "CRITICA")
        n_altas    = sum(1 for d in divs if d.gravidade == "ALTA")
        n_total    = len(divs)
        dif_icms   = getattr(resultado, "diferenca_icms", 0.0)

        if n_criticas >= 1 or dif_icms > 1000 or n_total >= 5:
            risco = "ALTO"
        elif n_altas >= 2 or dif_icms > 100 or n_total >= 2:
            risco = "MEDIO"
        else:
            risco = "BAIXO"

        return {
            "risco": risco,
            "probabilidades": {"BAIXO": 0.0, "MEDIO": 0.0, "ALTO": 0.0, risco: 1.0},
            "confianca": 1.0,
            "metodo": "Regras determinísticas (sem modelo ML)",
        }

    def importancia_features(self) -> Optional[Dict[str, float]]:
        """Retorna importância das features (só disponível com ML)."""
        if not self.usa_ml:
            return None
        importancias = self.modelo.feature_importances_
        return dict(sorted(
            zip(FEATURE_NAMES, importancias),
            key=lambda x: x[1], reverse=True
        ))
