# ml/model_trainer.py
"""
Treinamento e atualização do modelo Random Forest.

O modelo é treinado com dados históricos de análises anteriores.
Quanto mais documentos analisados e rotulados, melhor o modelo fica.

FLUXO:
1. Sistema analisa documentos → gera ResultadoAnaliseICMS
2. Usuário confirma/corrige o risco classificado
3. Dados rotulados são salvos em data/historico_treino.csv
4. Este módulo retreina o modelo com os dados acumulados
"""

import csv
import numpy as np
import joblib
from pathlib import Path
from typing import List, Tuple, Optional
from config import MODEL_PATH, SCALER_PATH, DATA_DIR

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import classification_report
    SKLEARN_OK = True
except ImportError:
    SKLEARN_OK = False

from ml.risk_classifier import extrair_features_resultado, FEATURE_NAMES, CLASSES

HISTORICO_CSV = DATA_DIR / "historico_treino.csv"


# ─────────────────────────────────────────────
# SALVAR DADOS DE TREINO
# ─────────────────────────────────────────────

def salvar_amostra_treino(resultado, risco_confirmado: str):
    """
    Salva uma amostra de treino (features + rótulo confirmado).
    Chamado quando o usuário confirma/corrige a classificação.
    """
    if risco_confirmado.upper() not in CLASSES:
        print(f"[ERRO] Risco inválido: {risco_confirmado}. Use: {CLASSES}")
        return

    features = extrair_features_resultado(resultado)
    existe = HISTORICO_CSV.exists()

    with open(HISTORICO_CSV, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not existe:
            writer.writerow(FEATURE_NAMES + ["risco_label", "identificador"])
        writer.writerow(features + [risco_confirmado.upper(), resultado.identificador])

    print(f"[OK] Amostra salva: {resultado.identificador} → {risco_confirmado.upper()}")


# ─────────────────────────────────────────────
# CARREGAR DADOS DE TREINO
# ─────────────────────────────────────────────

def carregar_dados_treino() -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
    """Carrega histórico CSV e retorna X, y para treinamento."""
    if not HISTORICO_CSV.exists():
        return None, None

    X, y = [], []
    with open(HISTORICO_CSV, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                features = [float(row[fn]) for fn in FEATURE_NAMES]
                X.append(features)
                y.append(row["risco_label"])
            except (KeyError, ValueError):
                continue  # Pula linhas corrompidas

    if not X:
        return None, None

    return np.array(X), np.array(y)


# ─────────────────────────────────────────────
# TREINAR MODELO
# ─────────────────────────────────────────────

def treinar_modelo(verbose: bool = True) -> bool:
    """
    Treina ou retreina o modelo Random Forest com os dados históricos.
    Retorna True se treinamento bem-sucedido.
    """
    if not SKLEARN_OK:
        print("[ERRO] scikit-learn não instalado. Execute: pip install scikit-learn")
        return False

    X, y = carregar_dados_treino()

    if X is None or len(X) < 10:
        n_atual = len(X) if X is not None else 0
        print(f"[INFO] Dados insuficientes para treinar ({n_atual} amostras). "
              f"Mínimo: 10. Continue analisando documentos e confirmando os riscos.")
        _criar_modelo_sintetico(verbose)
        return True

    if verbose:
        print(f"[INFO] Treinando modelo com {len(X)} amostras...")

    # Distribuição de classes
    classes, contagens = np.unique(y, return_counts=True)
    if verbose:
        for cls, cnt in zip(classes, contagens):
            print(f"  {cls}: {cnt} amostras")

    # Normalização
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Random Forest - parâmetros otimizados para dados fiscais
    modelo = RandomForestClassifier(
        n_estimators=100,       # 100 árvores
        max_depth=10,           # Evita overfitting
        min_samples_split=3,
        min_samples_leaf=2,
        class_weight="balanced",  # Trata desbalanceamento de classes
        random_state=42,
        n_jobs=-1,              # Usa todos os CPUs disponíveis
    )

    # Validação cruzada (se tiver dados suficientes)
    if len(X) >= 30:
        scores = cross_val_score(modelo, X_scaled, y, cv=min(5, len(X)//6), scoring="f1_weighted")
        if verbose:
            print(f"[INFO] F1 Score (cross-val): {scores.mean():.3f} ± {scores.std():.3f}")

    # Treina no conjunto completo
    modelo.fit(X_scaled, y)

    # Salva modelo
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(modelo, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)

    if verbose:
        print(f"[OK] Modelo salvo em: {MODEL_PATH}")
        # Importância das features
        print("\n[INFO] Top 5 features mais importantes:")
        importancias = sorted(
            zip(FEATURE_NAMES, modelo.feature_importances_),
            key=lambda x: x[1], reverse=True
        )
        for feat, imp in importancias[:5]:
            print(f"  {feat}: {imp:.3f}")

    return True


def _criar_modelo_sintetico(verbose: bool = True):
    """
    Cria um modelo inicial com dados sintéticos baseados em regras.
    Isso permite o ML funcionar desde o primeiro uso,
    evoluindo conforme dados reais são acumulados.
    """
    if not SKLEARN_OK:
        return

    if verbose:
        print("[INFO] Criando modelo inicial com dados sintéticos...")

    # Gera amostras sintéticas representando padrões conhecidos
    np.random.seed(42)
    n = 300  # amostras sintéticas

    X_sint = []
    y_sint = []

    for _ in range(n // 3):
        # RISCO BAIXO: sem divergências
        X_sint.append([
            0, 0, np.random.randint(0, 2), 0, np.random.randint(0, 2),
            0, 0, 0, 0, 0, 0, 0, 0,
            np.random.uniform(0, 10000), 0.0, 0.0
        ])
        y_sint.append("BAIXO")

    for _ in range(n // 3):
        # RISCO MÉDIO: 1-2 altas
        n_altas = np.random.randint(1, 3)
        X_sint.append([
            0, n_altas, np.random.randint(0, 3), 0, n_altas + np.random.randint(0, 2),
            0, np.random.randint(0, 2), 0, np.random.randint(0, 2), np.random.randint(0, 2), 0, 0, 0,
            np.random.uniform(100, 50000), np.random.uniform(10, 500), np.random.uniform(0.01, 0.1)
        ])
        y_sint.append("MEDIO")

    for _ in range(n // 3):
        # RISCO ALTO: tem críticas ou grandes divergências
        n_crit = np.random.randint(1, 4)
        n_altas = np.random.randint(0, 3)
        X_sint.append([
            n_crit, n_altas, np.random.randint(0, 3), 0, n_crit + n_altas + np.random.randint(0, 3),
            np.random.randint(0, 2), np.random.randint(0, 2), np.random.randint(0, 2),
            np.random.randint(0, 2), np.random.randint(0, 2), np.random.randint(0, 2),
            np.random.randint(0, 2), np.random.randint(0, 2),
            np.random.uniform(1000, 100000), np.random.uniform(500, 10000), np.random.uniform(0.1, 1.0)
        ])
        y_sint.append("ALTO")

    X_np = np.array(X_sint)
    y_np = np.array(y_sint)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_np)

    modelo = RandomForestClassifier(
        n_estimators=100, max_depth=8, class_weight="balanced",
        random_state=42, n_jobs=-1
    )
    modelo.fit(X_scaled, y_np)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(modelo, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)

    if verbose:
        print(f"[OK] Modelo inicial criado com {n} amostras sintéticas.")
        print("[INFO] O modelo melhora automaticamente conforme você confirma riscos reais.")


def status_modelo() -> dict:
    """Retorna informações sobre o estado atual do modelo."""
    n_amostras = 0
    if HISTORICO_CSV.exists():
        with open(HISTORICO_CSV) as f:
            n_amostras = sum(1 for linha in f) - 1  # -1 header

    return {
        "modelo_existe": MODEL_PATH.exists(),
        "amostras_historico": n_amostras,
        "pronto_para_treino_real": n_amostras >= 10,
        "caminho_modelo": str(MODEL_PATH),
        "caminho_historico": str(HISTORICO_CSV),
    }
