# Bitfinex Lending Bot

An automated lending bot for Bitfinex that optimizes lending rates and manages fund allocation through a dynamic grid strategy.

## Features

- 🔄 Automatic market rate detection
- 📊 Dynamic grid-based offer placement
- ⚡ Real-time rate adjustment (every minute)
- 📈 Split orders for risk diversification
- 🤖 Fully automated operation

## How It Works

The bot continuously monitors the Bitfinex funding market and:
1. Fetches current market rates to determine optimal lending opportunities
2. Splits your available funds into multiple offers using a grid strategy
3. Places offers at competitive rates across different price points
4. Automatically adjusts offers every minute based on market conditions
5. Maximizes returns while maintaining competitive rates

## Example Usage

![Bot in Action](screenshot/bf_lending_log.png)


## Setup

deploy on railway or any other app platform supports docker

## Configuration

Accounts are configured per tenant. List them in `TENANTS`, then add one block of
variables per tenant (the tenant name is uppercased inside the variable names and
must match `[A-Za-z0-9_]+`):

```
TENANTS=aron,bob

TENANT_ARON_BF_API_KEY=...
TENANT_ARON_BF_API_SECRET=...
TENANT_ARON_PASSWORD=...          # dashboard login, must be unique per tenant
TENANT_ARON_FUND_CURRENCY=fUSD    # optional, defaults to fUSD
TENANT_ARON_DRY_RUN=0             # optional, 1 logs orders without placing them
```

The bot runs every tenant once per minute, staggered and isolated so one failure
does not affect the others. The dashboard resolves the signed-in tenant from the
password alone, which is why passwords must be unique.

Strategy parameters (`BASE_SPLIT_*`, `PREPOSITION_*`, `SPIKE_*`, …) stay global and
are shared by all tenants.

## Requirements

python 3.10
run the pip install -r requirements.txt 

## Tests

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt -r requirements-dev.txt
pytest -q tests/
```

`tests/` covers pure helpers (`_active_credits_rollups`, preposition touch math, spike rules) **without calling Bitfinex**.

## Disclaimer

This bot is for educational purposes only. Use at your own risk. Cryptocurrency lending carries inherent risks, and past performance does not guarantee future results.

## License

Free to use and modify
