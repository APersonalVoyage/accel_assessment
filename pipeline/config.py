import os
from dataclasses import dataclass, field


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    database: str = "IOWA_LIQUOR"
    schema: str = "RAW"
    warehouse: str = "COMPUTE_WH"
    role: str = "SYSADMIN"


@dataclass
class PipelineConfig:
    snowflake: SnowflakeConfig
    socrata_app_token: str = ""
    batch_size: int = 10_000
    max_retries: int = 3
    retry_delay: float = 5.0
    alert_webhook_url: str = ""  # Slack incoming webhook URL


def load_config() -> PipelineConfig:
    """Load config from environment variables. Raises if required vars are missing."""
    missing = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD") if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    return PipelineConfig(
        snowflake=SnowflakeConfig(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.environ.get("SNOWFLAKE_DATABASE", "IOWA_LIQUOR"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        ),
        socrata_app_token=os.environ.get("SOCRATA_APP_TOKEN", ""),
        batch_size=int(os.environ.get("BATCH_SIZE", "10000")),
        max_retries=int(os.environ.get("MAX_RETRIES", "3")),
        retry_delay=float(os.environ.get("RETRY_DELAY", "5.0")),
        alert_webhook_url=os.environ.get("ALERT_WEBHOOK_URL", ""),
    )
