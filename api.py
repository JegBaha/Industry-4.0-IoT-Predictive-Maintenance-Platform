"""FastAPI endpoints for telemetry and predictions."""
from fastapi import FastAPI
from pydantic import BaseModel

from config import api_cfg
from model_infer import load_model, predict

app = FastAPI(title="Smart Factory Simulator")


class WindowRequest(BaseModel):
    window: list[list[float]]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict_failure(req: WindowRequest):
    model = load_model(input_size=len(req.window[0]), checkpoint_path=api_cfg.__dict__.get("checkpoint_path"))
    score = predict(model, req.window)
    return {"failure_probability": score}
