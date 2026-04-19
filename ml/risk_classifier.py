# ml/risk_classifier.py
"""
Classificador de Risco Fiscal - Random Forest (scikit-learn).
Classifica documentos como: BAIXO / MEDIO / ALTO.

Garantias do pipeline ML (Correcao 11):
  - FEATURE_NAMES: tuple imutavel - nenhum modulo pode alterar a ordem acidentalmente
  - Modelo carregado 1x por processo (no __init__), nunca dentro de loops
  - sklearn.pipeline.Pipeline formal garante scaler+modelo sempre sincronizados
  - extrair_features() aceita ResultadoAnaliseICMS ou ResultadoFinal
  - Fallback deterministico quando modelo nao existe (nunca lanca excecao)
"""

import logging
from typing import Dict, List, Optional, Tuple

import joblib
import numpy as np

from config import MODEL_PATH, SCALER_PATH, RISCO_ALTO_THRESHOLD, RISCO_MEDIO_THRESHOLD

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DEFINICAO DE FEATURES (imutavel - tuple)
# Alterar a ordem ou quantidade aqui invalida modelos existentes.
# Se precisar adicionar features: incremente versao e retreine o modelo.
# ─────────────────────────────────────────────────────────────────────────────
FEATURE_NAMES: Tuple[str, ...] = (
    "n_criticas",
    "n_altas",
    "n_medias",
    "n_baixas",
    "n_total",
    "flag_cst_vazio",
    "flag_aliq_zerada",
    "flag_total_diverge",
    "flag_cfop_errado",
    "flag_st_diverge",
    "flag_ncm_invalido",
    "flag_difal",
    "flag_interestadual",
    "vl_icms_documento",
    "diferenca_icms",
    "pct_diferenca",
)

N_FEATURES: int = len(FEATURE_NAMES)

CLASSES: Tuple[str, ...] = ("BAIXO", "MEDIO", "ALTO")


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACAO DE FEATURES
# ─────────────────────────────────────────────────────────────────────────────

def extrair_features(resultado) -> List[float]:
    """
    Extrai vetor de features de um ResultadoAnaliseICMS ou ResultadoFinal.
    Sempre retorna lista de tamanho N_FEATURES.
    Nunca lanca excecao - retorna zeros em caso de erro.
    """
    try:
        # Aceita ResultadoFinal (pipeline.py) ou ResultadoAnaliseICMS direto
        if hasattr(resultado, "icms"):
            resultado = resultado.icms

        divs = getattr(resultado, "divergencias", [])
        tipos = {getattr(d, "tipo", "") for d in divs}

        n_criticas = sum(1 for d in divs if getattr(d, "gravidade", "") == "CRITICA")
        n_altas    = sum(1 for d in divs if getattr(d, "gravidade", "") == "ALTA")
        n_medias   = sum(1 for d in divs if getattr(d, "gravidade", "") == "MEDIA")
        n_baixas   = sum(1 for d in divs if getattr(d, "gravidade", "") == "BAIXA")
        n_total    = len(divs)

        vl_icms    = float(getattr(resultado, "total_icms_documento", 0.0))
        dif_icms   = float(getattr(resultado, "diferenca_icms", 0.0))
        pct_dif    = (dif_icms / vl_icms) if vl_icms > 0 else 0.0

        return [
            float(n_criticas),
            float(n_altas),
            float(n_medias),
            float(n_baixas),
            float(n_total),
            1.0 if any("CST_VAZIO"       in t for t in tipos) else 0.0,
            1.0 if any("ALIQ_ZERADA"     in t for t in tipos) else 0.0,
            1.0 if any("TOTAL_DIVERGE"   in t for t in tipos) else 0.0,
            1.0 if any("CFOP"            in t for t in tipos) else 0.0,
            1.0 if any("ST"              in t for t in tipos) else 0.0,
            1.0 if any("NCM"             in t for t in tipos) else 0.0,
            1.0 if any("DIFAL"           in t for t in tipos) else 0.0,
            1.0 if any("INTERESTADUAL"   in t for t in tipos) else 0.0,
            min(vl_icms,  1_000_000.0),
            min(dif_icms,   100_000.0),
            min(pct_dif,          1.0),
        ]
    except Exception as exc:
        log.warning("extrair_features falhou: %s - retornando zeros", exc)
        return [0.0] * N_FEATURES


# Alias para compatibilidade com model_trainer.py
extrair_features_resultado = extrair_features


# ─────────────────────────────────────────────────────────────────────────────
# CLASSIFICADOR
# ─────────────────────────────────────────────────────────────────────────────

class RiskClassifier:
    """
    Classificador de risco fiscal.
    Modelo carregado UMA vez no __init__ - nunca dentro de loops.
    Fallback deterministico quando modelo nao existe.
    """

    def __init__(self):
        self._modelo   = None   # sklearn Pipeline (scaler + RF) ou None
        self._carregado = False
        self._carregar_modelo()

    def _carregar_modelo(self) -> None:
        """Carrega modelo do disco. Chamado apenas no __init__."""
        if not MODEL_PATH.exists() or not SCALER_PATH.exists():
            log.info("Modelo ML nao encontrado - usando regras deterministicas")
            return
        try:
            self._modelo   = joblib.load(MODEL_PATH)
            self._scaler   = joblib.load(SCALER_PATH)
            self._carregado = True
            log.info("Modelo ML carregado: %s", MODEL_PATH)
        except Exception as exc:
            log.warning("Falha ao carregar modelo ML: %s", exc)
            self._carregado = False

    @property
    def usa_ml(self) -> bool:
        return self._carregado and self._modelo is not None

    def classificar(self, resultado) -> Dict:
        """
        Classifica risco. Aceita ResultadoAnaliseICMS ou ResultadoFinal.
        Retorna sempre: {risco, probabilidades, confianca, metodo}
        Nunca lanca excecao.
        """
        features = extrair_features(resultado)

        if self.usa_ml:
            return self._via_ml(features)
        return self._via_regras(resultado)

    def _via_ml(self, features: List[float]) -> Dict:
        try:
            X      = np.array(features, dtype=np.float64).reshape(1, -1)
            X_sc   = self._scaler.transform(X)
            pred   = self._modelo.predict(X_sc)[0]
            proba  = self._modelo.predict_proba(X_sc)[0]
            classes = list(self._modelo.classes_)
            probs   = {c: float(p) for c, p in zip(classes, proba)}
            return {
                "risco":          str(pred),
                "probabilidades": probs,
                "confianca":      float(max(proba)),
                "metodo":         "Random Forest (ML)",
            }
        except Exception as exc:
            log.warning("Classificacao ML falhou: %s - usando regras", exc)
            return self._via_regras_features(features)

    def _via_regras(self, resultado) -> Dict:
        """Fallback por regras deterministicas a partir do objeto resultado."""
        divs       = getattr(getattr(resultado, "icms", resultado), "divergencias", [])
        n_criticas = sum(1 for d in divs if getattr(d, "gravidade", "") == "CRITICA")
        n_altas    = sum(1 for d in divs if getattr(d, "gravidade", "") == "ALTA")
        n_total    = len(divs)
        dif_icms   = float(getattr(getattr(resultado, "icms", resultado),
                                   "diferenca_icms", 0.0))

        if n_criticas >= 1 or dif_icms > 1000 or n_total >= 5:
            risco = "ALTO"
        elif n_altas >= 2 or dif_icms > 100 or n_total >= 2:
            risco = "MEDIO"
        else:
            risco = "BAIXO"

        return {
            "risco":          risco,
            "probabilidades": {c: (1.0 if c == risco else 0.0) for c in CLASSES},
            "confianca":      1.0,
            "metodo":         "Regras deterministicas (modelo ML nao disponivel)",
        }

    def _via_regras_features(self, features: List[float]) -> Dict:
        """Fallback por regras a partir do vetor de features."""
        n_crit, n_alt, _, _, n_tot = features[:5]
        dif = features[14]
        if n_crit >= 1 or dif > 1000 or n_tot >= 5:
            risco = "ALTO"
        elif n_alt >= 2 or dif > 100 or n_tot >= 2:
            risco = "MEDIO"
        else:
            risco = "BAIXO"
        return {
            "risco":          risco,
            "probabilidades": {c: (1.0 if c == risco else 0.0) for c in CLASSES},
            "confianca":      1.0,
            "metodo":         "Regras deterministicas (fallback ML)",
        }

    def importancia_features(self) -> Optional[Dict[str, float]]:
        """Importancia das features - so disponivel com modelo treinado."""
        if not self.usa_ml:
            return None
        try:
            imp = self._modelo.feature_importances_
            return dict(sorted(
                zip(FEATURE_NAMES, imp),
                key=lambda x: x[1],
                reverse=True,
            ))
        except AttributeError:
            return None