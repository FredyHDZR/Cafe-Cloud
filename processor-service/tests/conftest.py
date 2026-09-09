TEST_ENVIRONMENT = {
    "DATABASE_URL": "postgresql+asyncpg://processor_rw:processor_dev_pw@localhost:5432/cafecloud",
    "REDIS_URL": "redis://localhost:6379/0",
    "SERVICE_NAME": "processor-service",
    "LOG_LEVEL": "INFO",
}
