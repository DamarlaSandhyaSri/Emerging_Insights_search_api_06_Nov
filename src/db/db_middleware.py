import uuid

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from src.core.config_loader import settings
from src.logger.console_logs import Loggercheck

logger_instance = Loggercheck(__name__)

logger = logger_instance.get_logger()


class Opensearch_middleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.session = uuid.uuid4()
        # request.state.index1 = settings.opensearch.index1
        try:
            response = await call_next(request)
        except Exception as ex:
            logger_instance.logg_message(
                f"{request.state.session} - Here is the error {ex}",
                "info",
            )
            response = JSONResponse(status_code=500, content={"message": "error connecting to db"})

        return response
