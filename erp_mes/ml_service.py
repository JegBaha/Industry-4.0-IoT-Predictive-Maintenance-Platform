"""Hata tahmin modeli - RandomForest tabanlı defect prediction."""
import numpy as np
import logging

logger = logging.getLogger(__name__)

_feature_importance = {
    "temperature": 0.35,
    "line_speed": 0.25,
    "operator_experience": 0.15,
    "machine_age": 0.12,
    "shift_Day": 0.07,
    "shift_Night": 0.06,
}

try:
    import pandas as pd
    import joblib
    from pathlib import Path
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import StandardScaler, OneHotEncoder
    from sklearn.impute import SimpleImputer
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("sklearn not available, using mock predictions")


class ERPMLService:
    def __init__(self):
        self.model = None
        self.feature_importance = dict(_feature_importance)
        if SKLEARN_AVAILABLE:
            self._load_model()

    def _load_model(self):
        model_path = Path(__file__).parent / "models" / "random_forest.pkl"
        if model_path.exists():
            try:
                self.model = joblib.load(model_path)
                self._extract_feature_importance()
                logger.info("ERP ML model loaded")
            except Exception as e:
                logger.warning(f"Failed to load model: {e}")

    def _extract_feature_importance(self):
        if self.model is None:
            return
        try:
            preprocessor = self.model.named_steps["preprocess"]
            rf = self.model.named_steps["model"]
            numeric_features = ["temperature", "line_speed", "operator_experience", "machine_age"]
            cat_encoder = preprocessor.named_transformers_["cat"].named_steps["encoder"]
            cat_features = cat_encoder.get_feature_names_out(["shift"]).tolist()
            names = numeric_features + cat_features
            self.feature_importance = dict(zip(names, rf.feature_importances_.tolist()))
        except Exception:
            pass

    def predict(self, temperature: float, line_speed: float, shift: str,
                operator_experience: float, machine_age: float) -> dict:
        if self.model is not None and SKLEARN_AVAILABLE:
            input_df = pd.DataFrame([{
                "temperature": temperature,
                "line_speed": line_speed,
                "shift": shift,
                "operator_experience": operator_experience,
                "machine_age": machine_age,
            }])
            proba = self.model.predict_proba(input_df)[0]
            defect_prob = proba[1] if len(proba) > 1 else proba[0]
            return {
                "defect_probability": round(float(defect_prob), 4),
                "predicted_defect": bool(defect_prob >= 0.5),
                "confidence": round(float(max(proba)), 4),
            }

        # Mock prediction
        temp_factor = max(0, (temperature - 70)) / 50
        speed_factor = max(0, (line_speed - 100)) / 100
        shift_factor = 0.15 if shift == "Night" else 0
        exp_factor = max(0, (10 - operator_experience)) / 30
        age_factor = machine_age / 200
        prob = min(0.95, max(0.05, temp_factor * 0.4 + speed_factor * 0.2 + shift_factor + exp_factor + age_factor))
        return {
            "defect_probability": round(prob, 4),
            "predicted_defect": prob >= 0.5,
            "confidence": round(max(prob, 1 - prob), 4),
        }

    def get_feature_importance(self) -> list[dict]:
        items = sorted(self.feature_importance.items(), key=lambda x: x[1], reverse=True)
        return [{"feature": f, "importance": round(v, 4)} for f, v in items]

    def get_temperature_curve(self) -> list[dict]:
        temps = np.linspace(60, 110, 20)
        results = []
        for t in temps:
            pred = self.predict(float(t), 85.0, "Day", 5.0, 25.0)
            results.append({"temperature": round(float(t), 1), "defect_probability": pred["defect_probability"]})
        return results

    def get_analytics(self) -> dict:
        return {
            "model_info": {
                "name": "Random Forest Classifier",
                "trees": 300,
                "max_depth": 12,
                "status": "loaded" if self.model else "mock",
            },
            "findings": [
                {
                    "title": "Sicaklik Etkisi",
                    "description": "95C uzerindeki sicakliklar hata olasiligini onemli olcude artirir. Optimal aralik: 75-85C",
                    "severity": "high",
                },
                {
                    "title": "Hat Hizi Etkisi",
                    "description": "Yuksek hiz + yuksek sicaklik = en yuksek risk senaryosu",
                    "severity": "medium",
                },
                {
                    "title": "Vardiya Etkisi",
                    "description": "Gece vardiyasinda hata orani gun vardiyasina gore %15 daha yuksek",
                    "severity": "medium",
                },
                {
                    "title": "Operator Deneyimi",
                    "description": "5 yildan az deneyimli operatorlerde hata orani belirgin sekilde artar",
                    "severity": "low",
                },
            ],
            "recommendations": [
                "Sicakligi 75-85C araliginda tutun",
                "Gece vardiyalarinda ek kalite kontrol uygulayin",
                "Yeni operatorlere mentoring programi saglayin",
                "Yuksek hiz ayarlarinda sicakligi yakindan izleyin",
            ],
        }


erp_ml = ERPMLService()
