from pydantic import BaseSettings


class Settings(BaseSettings):
    port: int
    snapshot_path: str
    snapshot_time: int
    ttl_time: int

    class Config:
        env_file = ".env"


settings = Settings()
