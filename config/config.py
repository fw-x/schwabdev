from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

base_dir = Path(__file__).resolve().parent

class Settings(BaseSettings):
    redis_host: str = "redis"
    db_path: str = "/data"
    model_config = SettingsConfigDict(
        env_file=base_dir/".env", 
        extra="ignore")
    schwab_key: str = Field(alias="SCHWAB_KEY")
    schwab_secrete: str = Field(alias="SCHWAB_SECRETE")
    token_db: str = Field(alias="TOKEN_DB")

