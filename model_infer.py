"""Model inference utilities."""
import torch
import numpy as np
from typing import Optional

from config import model_cfg
from model_train import LSTMModel


def load_model(input_size: int, checkpoint_path: Optional[str] = None) -> LSTMModel:
    model = LSTMModel(input_size=input_size, hidden_size=model_cfg.hidden_size)
    if checkpoint_path:
        state = torch.load(checkpoint_path, map_location="cpu")
        model.load_state_dict(state)
    model.eval()
    return model


def predict(model: LSTMModel, window: np.ndarray) -> float:
    with torch.no_grad():
        tensor = torch.tensor(window, dtype=torch.float32).unsqueeze(0)
        logits = model(tensor).item()
        return float(torch.sigmoid(torch.tensor(logits)))
