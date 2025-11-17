import os

import boto3
from dynaconf import Dynaconf
from fastapi import Request
from opensearchpy import AsyncHttpConnection, AsyncOpenSearch, AWSV4SignerAsyncAuth

from src.logger.console_logs import Loggercheck

logger_instance = Loggercheck(__name__)

logger = logger_instance.get_logger()


def load_settings():
    # env = os.getenv("DYNACONF_ENV", "From Octopus")
    env = ""
    settings_file = "local.toml" if env == "local" else "config/settings.toml"
    logger_instance.logg_message(
        f"The configuration '{env}' has been loaded for this enviroment",
        "info",
    )
    return Dynaconf(settings_files=[settings_file], enviroments=True)


settings = load_settings()


def get_aws_auth():
    session = boto3.Session()
    # if os.getenv("DYNACONF_ENV", "local") == "local":
    #     # session = boto3.Session(profile_name="Comm-Prop-Sandbox")
    #     session = boto3.Session()
    # else:
    #     session = boto3.Session()
    credentials = session.get_credentials()
    return AWSV4SignerAsyncAuth(credentials, settings.aws_config.region, settings.aws_config.service)


def opensearch_connection():
    return AsyncOpenSearch(
        hosts=[{"host": settings.opensearch.host, "port": int(settings.opensearch.port)}],
        http_auth=get_aws_auth(),
        use_ssl=True,
        verify_certs=True,
        connection_class=AsyncHttpConnection,
    )


async def get_opensearch_client(request: Request):
    opensearch = opensearch_connection()
    try:
        logger.info(f"{request.state.session} - Establishing connection to OpenSearch client.")
        yield opensearch
    except Exception as e:
        logger.error(f"{request.state.session} - Error occurred while establishing OpenSearch client: {e}")
        raise
    finally:
        try:
            logger.info(f" {request.state.session} - Closing OpenSearch client connection.")
            await opensearch.close()
        except Exception as e:
            logger.error(f"{request.state.session} - Error occurred while closing OpenSearch client: {e}")
