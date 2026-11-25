class APIError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class DatabaseUnavailable(RuntimeError):
    """Raised when PostgreSQL is unreachable or a query cannot be completed."""
