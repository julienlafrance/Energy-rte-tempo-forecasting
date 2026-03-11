from pydantic import BaseModel, Field
from datetime import date as _date, datetime
from typing import List

# Déclaration des outputs à renvoyer
# === Consumption ===
class HourlyPrediction(BaseModel):
    hour: datetime = Field(..., description="Heure de la prévision")
    predicted: float = Field(..., description="Consommation prédite en kWh", example=1200.5)
    lower: float = Field(..., description="Borne inférieure de l'intervalle de confiance", example=1100.0)
    upper: float = Field(..., description="Borne supérieure de l'intervalle de confiance", example=1300.0)

class ConsumptionForecast(BaseModel):
    date: _date = Field(..., description="Date de la prévision")
    predictions: List[HourlyPrediction] = Field(..., description="Liste des prévisions horaires")