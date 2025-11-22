from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    bronze_db_type: str = Field(alias="bronze_db_type", default=None)
    bronze_db_host: str = Field(alias="bronze_db_host", default=None)
    bronze_db_port: str = Field(alias="bronze_db_port", default=None)
    bronze_db_user: str = Field(alias="bronze_db_user", default=None)
    bronze_db_pass: str = Field(alias="bronze_db_pass", default=None)
    bronze_db_name: str = Field(alias="bronze_db_name", default=None)

    silver_db_type: str = Field(alias="silver_db_type", default=None)
    silver_db_host: str = Field(alias="silver_db_host", default=None)
    silver_db_port: str = Field(alias="silver_db_port", default=None)
    silver_db_user: str = Field(alias="silver_db_user", default=None)
    silver_db_pass: str = Field(alias="silver_db_pass", default=None)
    silver_db_name: str = Field(alias="silver_db_name", default=None)

    gold_db_type: str = Field(alias="gold_db_type", default=None)
    gold_db_host: str = Field(alias="gold_db_host", default=None)
    gold_db_port: str = Field(alias="gold_db_port", default=None)
    gold_db_user: str = Field(alias="gold_db_user", default=None)
    gold_db_pass: str = Field(alias="gold_db_pass", default=None)
    gold_db_name: str = Field(alias="gold_db_name", default=None)

    def get_url(self, db_layer: str) -> str:
        if db_layer == "bronze":
            return f"{self.bronze_db_type}://{self.bronze_db_user}:{self.bronze_db_pass}@{self.bronze_db_host}:{self.bronze_db_port}/{self.bronze_db_name}"

        if db_layer == "silver":
            return f"{self.silver_db_type}://{self.silver_db_user}:{self.silver_db_pass}@{self.silver_db_host}:{self.silver_db_port}/{self.silver_db_name}"

        if db_layer == "gold":
            return f"{self.gold_db_type}://{self.gold_db_user}:{self.gold_db_pass}@{self.gold_db_host}:{self.gold_db_port}/{self.gold_db_name}"
        
        raise ValueError("db_layer tem de ser bronze ou silver ou gold.")


config = Config()
