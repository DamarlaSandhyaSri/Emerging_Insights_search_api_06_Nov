from __future__ import annotations
import boto3
from typing import Dict,Any
from opensearchpy import AsyncOpenSearch,AsyncHttpConnection,AWSV4SignerAsyncAuth,AsyncHttpConnection
from .config import OpenSearchSettings

def build_async_client(settings:OpenSearchSettings):
    kwargs:Dict[str,Any]=dict(
        hosts=[{"host": settings.os_endpoint, "port": int(settings.os_port)}],
        use_ssl=True,
        verify_certs=settings.verify_certs,
        connection_class=AsyncHttpConnection,
        http_compress=settings.http_compress,
        retry_on_timeout=settings.retry_on_timeout,
        max_retries=settings.max_retries,
        timeout=settings.timeout
    )
    session = boto3.Session()
    # if settings.profile_name:
    #     session = boto3.Session(profile_name=settings.profile_name)
    # else:
    #     session = boto3.Session(profile_name=settings.profile_name)

    credentials=session.get_credentials()
    kwargs["http_auth"]=AWSV4SignerAsyncAuth(credentials,settings.os_region,settings.service)

    return AsyncOpenSearch(**kwargs)

async def close_async_client(client:AsyncOpenSearch)->None:
    await client.close()

