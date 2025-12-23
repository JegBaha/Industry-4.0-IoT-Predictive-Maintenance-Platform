import numpy as np
from model_train import LSTMModel


def test_lstm_output_shape():
    model = LSTMModel(input_size=3, hidden_size=4)
    x = np.random.rand(2, 5, 3).astype(np.float32)
    import torch

    with torch.no_grad():
        out = model(torch.tensor(x))
    assert out.shape == (2,)
