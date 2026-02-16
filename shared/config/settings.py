"""
Shared configuration settings for all microservices
"""
from typing import List
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Base settings for all services"""

    # Environment
    environment: str = Field(default="development", alias="ENVIRONMENT")

    # Database
    database_url: str = Field(..., alias="DATABASE_URL")
    postgres_user: str = Field(..., alias="POSTGRES_USER")
    postgres_password: str = Field(..., alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(..., alias="POSTGRES_DB")

    # Redis
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_password: str = Field(default="", alias="REDIS_PASSWORD")
    redis_db: int = Field(default=0, alias="REDIS_DB")

    # RabbitMQ
    rabbitmq_host: str = Field(default="localhost", alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, alias="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="guest", alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(default="guest", alias="RABBITMQ_PASSWORD")
    rabbitmq_vhost: str = Field(default="/", alias="RABBITMQ_VHOST")

    # JWT
    jwt_secret_key: str = Field(..., alias="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(default=30, alias="ACCESS_TOKEN_EXPIRE_MINUTES")
    refresh_token_expire_days: int = Field(default=7, alias="REFRESH_TOKEN_EXPIRE_DAYS")

    # CORS
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:3001", "http://localhost:5173"],
        alias="CORS_ORIGINS"
    )

    # Service Ports (using APP_ prefix to avoid K8s auto-injected env vars)
    auth_service_port: int = Field(default=8001, alias="APP_AUTH_SERVICE_PORT")
    master_admin_service_port: int = Field(default=8002, alias="APP_MASTER_ADMIN_SERVICE_PORT")
    restaurant_service_port: int = Field(default=8003, alias="APP_RESTAURANT_SERVICE_PORT")
    order_service_port: int = Field(default=8004, alias="APP_ORDER_SERVICE_PORT")
    kitchen_service_port: int = Field(default=8005, alias="APP_KITCHEN_SERVICE_PORT")
    notification_service_port: int = Field(default=8006, alias="APP_NOTIFICATION_SERVICE_PORT")
    api_gateway_port: int = Field(default=8000, alias="APP_API_GATEWAY_PORT")

    # New Relic
    new_relic_license_key: str = Field(default="", alias="NEW_RELIC_LICENSE_KEY")
    new_relic_app_name: str = Field(default="Restaurant-Management", alias="NEW_RELIC_APP_NAME")

    # File Upload
    max_upload_size: int = Field(default=5242880, alias="MAX_UPLOAD_SIZE")  # 5MB
    upload_dir: str = Field(default="/tmp/uploads", alias="UPLOAD_DIR")

    # QR Code
    qr_code_base_url: str = Field(default="http://localhost:3000/table", alias="QR_CODE_BASE_URL")
    qr_code_size: int = Field(default=300, alias="QR_CODE_SIZE")
    qr_code_error_correction: str = Field(default="M", alias="QR_CODE_ERROR_CORRECTION")

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="json", alias="LOG_FORMAT")

    # Rate Limiting
    rate_limit_per_minute: int = Field(default=100, alias="RATE_LIMIT_PER_MINUTE")

    # Session
    session_secret: str = Field(..., alias="SESSION_SECRET")
    session_expire_hours: int = Field(default=24, alias="SESSION_EXPIRE_HOURS")

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"

    @property
    def redis_url(self) -> str:
        """Construct Redis URL"""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def rabbitmq_url(self) -> str:
        """Construct RabbitMQ URL"""
        return f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_host}:{self.rabbitmq_port}{self.rabbitmq_vhost}"


# Global settings instance
settings = Settings()
