"""PyTorch LSTM training."""
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from config import model_cfg
from etl_features import window_sequences


class LSTMModel(nn.Module):
    def __init__(self, input_size: int, hidden_size: int = 64):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        out = out[:, -1, :]
        return self.fc(out).squeeze(-1)


def train_model(X, y, checkpoint_path: str | None = None):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    dataset = TensorDataset(torch.tensor(X), torch.tensor(y))
    loader = DataLoader(dataset, batch_size=model_cfg.batch_size, shuffle=True)
    model = LSTMModel(input_size=X.shape[-1], hidden_size=model_cfg.hidden_size).to(device)
    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=model_cfg.learning_rate)

    model.train()
    for epoch in range(model_cfg.num_epochs):
        for batch_x, batch_y in loader:
            batch_x, batch_y = batch_x.to(device), batch_y.to(device)
            optimizer.zero_grad()
            preds = model(batch_x)
            loss = criterion(preds, batch_y)
            loss.backward()
            optimizer.step()

    if checkpoint_path:
        torch.save(model.state_dict(), checkpoint_path)
    return model
