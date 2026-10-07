import logging


class HealthAccessLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        arguments = record.args
        if isinstance(arguments, tuple) and len(arguments) >= 3:
            return str(arguments[2]).split("?", 1)[0] != "/health"
        return True


def install_health_access_log_filter() -> None:
    access_logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(item, HealthAccessLogFilter) for item in access_logger.filters):
        access_logger.addFilter(HealthAccessLogFilter())
