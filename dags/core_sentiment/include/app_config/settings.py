from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load environment variables from the .env file into the runtime environment
load_dotenv()


# Define a class that reads environment variables and validates them
class Settings(BaseSettings):
    RAW_PAGEVIEWS_DIR: str
    PROCESSED_PAGEVIEWS_DIR: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    # Ollama configuration with defaults
    OLLAMA_HOST: str
    OLLAMA_MODEL: str
    OLLAMA_TIMEOUT: int

    # Configuration for how environment variables are loaded and handled
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )


# Creates a single instance to access configuration values throughout
config = Settings()  # type: ignore[call-arg]
