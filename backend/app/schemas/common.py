from typing import Literal

from pydantic import BaseModel

DatasetId = Literal["metr-la", "pems-bay"]


class HealthResponse(BaseModel):
    status: Literal["live", "ready"]


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail
    details: list[dict[str, str]] | None = None
