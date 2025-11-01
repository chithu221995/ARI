
"""Metrics recording utilities."""

import logging



log = logging.getLogger("ari.metrics")





def record_metric(category: str, provider: str, latency_ms: int, success: bool) -> None:

    """

    Record a metric for monitoring purposes.

    

    Args:

        category: The category of the metric (e.g., 'email')

        provider: The provider name (e.g., 'sendgrid')

        latency_ms: Latency in milliseconds

        success: Whether the operation was successful

    """

    log.info(

        "metric: category=%s provider=%s latency_ms=%d success=%s",

        category, provider, latency_ms, success

    )

