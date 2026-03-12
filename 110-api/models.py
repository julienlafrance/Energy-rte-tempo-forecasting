from pydantic import BaseModel, Field
from datetime import date as _date, datetime
from typing import List, Optional

# Déclaration des outputs à renvoyer
# === Consumption ===
class HourlyPrediction(BaseModel):
    hour: datetime = Field(..., description="Heure de la prévision")
    predicted: Optional[float] = Field(None, description="Consommation prédite en kWh", example=1200.5)
    lower: Optional[float] = Field(None, description="Borne inférieure de l'intervalle de confiance", example=1100.0)
    upper: Optional[float] = Field(None, description="Borne supérieure de l'intervalle de confiance", example=1300.0)

class ConsumptionForecast(BaseModel):
    date: _date = Field(..., description="Date de la prévision")
    predictions: List[HourlyPrediction] = Field(..., description="Liste des prévisions horaires")
