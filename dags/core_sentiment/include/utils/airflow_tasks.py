from datetime import timedelta

from airflow.sdk import task


def common_task(**extra_kwargs):
    """
    Custom wrapper for Airflow's @task decorator that applies default configuaration options to all tasks.

    Args:
        - **extra_kwargs: dict [Optional extra keyword arguments to override or extend the default task configurations.]

    Returns:
        - function [A decorated Airflow task function with default (and optionally overridden) settings].
    """

    # Apply default retry configurations for all tasks
    default_params = {"retries": 3, "retry_delay": timedelta(minutes=5)}

    # Merge default parameters with any user-provided overrides
    # If the same key exists in both, extra_kwargs takes precedence.
    combined_params = {**default_params, **extra_kwargs}

    # Return a decorated tasks using the merged configuration
    return task(**combined_params)
