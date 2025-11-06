from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator,Callable
from opensearchpy import AsyncOpenSearch
from .config import OpenSearchSettings
from .client import build_async_client,close_async_client
from fastapi import FastAPI, Request, Depends
import os
from datetime import datetime
from loguru import logger

# Ensure log directory exists
os.makedirs("app/logs", exist_ok=True)

# Create log file with current date
log_filename = f"app/logs/{datetime.now().strftime('%Y-%m-%d')}.log"

logger.add(
    log_filename,
    rotation="1 day",
    retention="7 days",
    compression="zip",
    level="DEBUG",
    format="{time} | {level} | {message}",
    enqueue=True,
)

def lifespan_factory(settings:OpenSearchSettings):
    @asynccontextmanager
    async def lifespan(app:FastAPI)->AsyncGenerator[None,None]:
        app.state.OSCLIENT=build_async_client(settings)
        try:
            yield
        finally:
            await close_async_client(app.state.OSCLIENT)
    return lifespan


def dependency(request_attr:str="OSCLIENT")->callable[[Request],AsyncOpenSearch]:
    def _dep(request:Request)->AsyncOpenSearch:
        state = getattr(request,"state",None)
        if state is not None and hasattr(state,request_attr):
            return getattr(state,request_attr)
        return getattr(request.app.state,request_attr)
    return _dep
