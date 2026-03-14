# Dev Setup

## Prerequisites
- Python 3.11+
- 1Password CLI (`op`) configured
- Docker (for containerized deployment)
- Helm + kubectl (for k8s deployment)

## Local Development
```bash
# Clone the repo
git clone https://github.com/arkarctech/prediction-markets-trading-bot.git
cd prediction-markets-trading-bot

# Install dependencies
pip install -r apps/bot/requirements.txt

# Set up environment
cp apps/bot/.env.example apps/bot/.env
# Edit .env with your credentials

# Run in paper trading mode
cd apps/bot && python main.py
```

## Using 1Password
```bash
# Run with secrets from 1Password
op run --env-file=apps/bot/.env.op -- python apps/bot/main.py
```

## Helm Deployment
```bash
# Install on local k8s
helm install pmbot ./helm/prediction-bot

# Production values
helm install pmbot ./helm/prediction-bot -f helm/prediction-bot/values-prod.yaml
```
