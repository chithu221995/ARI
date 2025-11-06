# ...existing imports and code...

# Add this at the end of the file:

# ============================================================================
# VENDOR COST CONFIGURATION
# ============================================================================
from decimal import Decimal


def _parse_cost_env(prefix: str, default_cost: float = 0, default_credits: float = 0) -> float:
    """
    Parse vendor cost configuration from environment variables.
    
    Two modes:
    1. Cost per credit bundle: {PREFIX}_COST_PER_PURCHASE / {PREFIX}_CREDITS_PER_PURCHASE
    2. Direct cost per operation: {PREFIX}_COST_PER_SUMMARY
    
    Args:
        prefix: Environment variable prefix (e.g., "SCRAPINGDOG")
        default_cost: Default cost if not configured
        default_credits: Default credits if not configured
        
    Returns:
        Cost per single operation as float
    """
    import os
    
    # Try bundle pricing first
    cost_per_purchase = os.getenv(f"{prefix}_COST_PER_PURCHASE")
    credits_per_purchase = os.getenv(f"{prefix}_CREDITS_PER_PURCHASE")
    
    if cost_per_purchase and credits_per_purchase:
        try:
            cost = Decimal(cost_per_purchase)
            credits = Decimal(credits_per_purchase)
            if credits > 0:
                return float(cost / credits)
        except (ValueError, TypeError):
            pass
    
    # Fallback to direct cost per operation (for LLMs)
    cost_per_op = os.getenv(f"{prefix}_COST_PER_SUMMARY") or os.getenv(f"{prefix}_COST_PER_CALL")
    if cost_per_op:
        try:
            return float(cost_per_op)
        except (ValueError, TypeError):
            pass
    
    # Use defaults
    if default_credits > 0:
        return float(Decimal(default_cost) / Decimal(default_credits))
    return float(default_cost)


# Vendor cost per operation (in USD)
VENDOR_COSTS = {
    "scrapingdog": _parse_cost_env("SCRAPINGDOG", default_cost=0, default_credits=1),
    "diffbot": _parse_cost_env("DIFFBOT", default_cost=0, default_credits=1),
    "sendgrid": _parse_cost_env("SENDGRID", default_cost=0, default_credits=1),
    "gemini": _parse_cost_env("GEMINI", default_cost=0.000085, default_credits=1),  # ~$0.85 per 10k
    "openai": _parse_cost_env("OPENAI", default_cost=0.0067, default_credits=1),    # ~$67 per 10k
}