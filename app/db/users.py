from __future__ import annotations
import sqlite3
from typing import List, Dict


def get_unique_active_tickers(db_path: str) -> List[str]:
    """
    Return distinct tickers users selected that are active in ticker_catalog.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ut.ticker
            FROM user_tickers ut
            JOIN ticker_catalog tc ON tc.ticker = ut.ticker
            WHERE COALESCE(tc.active,1)=1
        """)
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_user_tickers_map(db_path: str) -> Dict[str, List[str]]:
    """
    Return {email: [ticker1..]} ordered by rank for each user.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT email, ticker
            FROM user_tickers
            ORDER BY email, rank
        """)
        out: Dict[str, List[str]] = {}
        for email, t in cur.fetchall():
            out.setdefault(email, []).append(t)
        return out
    finally:
        conn.close()