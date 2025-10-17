# app/ingest/tickers.py
from typing import Dict

# Minimal starter directory; expand as you go
TICKER_DIR: Dict[str, dict] = {
    "TCS": {
        "nse_symbol": "TCS",
        "bse_code": "532540",
        "company_name": "Tata Consultancy Services Limited",
        "aliases": ["Tata Consultancy Services", "TCS"],
    },
    "TATAMOTORS": {
        "nse_symbol": "TATAMOTORS",
        "bse_code": "500570",
        "company_name": "Tata Motors Limited",
        "aliases": ["Tata Motors", "Tata Motors Ltd"],
    },
    "HEROMOTOCO": {
        "nse_symbol": "HEROMOTOCO",
        "bse_code": "500182",
        "company_name": "Hero MotoCorp Limited",
        "aliases": ["Hero MotoCorp", "Hero Motocorp", "Hero Motocorp Ltd"],
    },
    # Add more tickers as needed...
}

def resolve(symbol: str) -> dict:
    key = symbol.upper()
    if key in TICKER_DIR:
        return TICKER_DIR[key]
    return {
        "nse_symbol": key,
        "bse_code": "",
        "company_name": key,
        "aliases": [symbol],
    }

def get_bse_code(symbol: str) -> str:
    return resolve(symbol).get("bse_code", "")