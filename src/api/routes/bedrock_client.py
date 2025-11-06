import boto3
import json
from typing import Optional
from pydantic import BaseModel
from src.api.routes.logger import get_logger

logger = get_logger(__name__)

class BedrockConfig(BaseModel):
    ENDPOINT_URL : str = "https://bedrock-runtime.us-east-1.amazonaws.com"
    AWS_REGION: str = "us-east-1"
    # PROFILE_NAME: Optional[str] = "Comm-Prop-Sandbox"

class BedrockClient:
    def __init__(self, config: BedrockConfig):
        self.config = config
        
        try:
            session = boto3.Session()
            # session = boto3.Session(profile_name=config.PROFILE_NAME)
            # credentials = session.get_credentials().get_frozen_credentials()
            # credentials = session.get_credentials()

            self.client = boto3.client(
                "bedrock-runtime",
                region_name=config.AWS_REGION,
                endpoint_url=config.ENDPOINT_URL
                # aws_access_key_id=credentials.access_key,
                # aws_secret_access_key=credentials.secret_key,
                # aws_session_token=credentials.token,
            )
            logger.info("Bedrock client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Bedrock client: {e}")
            raise

    def invoke_model(self, model_id: str, prompt: str, max_tokens: int = 5000, temperature: float = 0.0):
        """Invoke Bedrock model with proper message format for Claude"""
        try:
            # Format for Claude 3.5 Sonnet
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = self.client.invoke_model(
                modelId=model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body)
            )
            
            response_body = json.loads(response["body"].read().decode("utf-8"))
            return response_body.get("content", [{}])[0].get("text", "")
            
        except Exception as e:
            logger.error(f"Error invoking model {model_id}: {e}")
            raise



























# import boto3
# from typing import Optional

# from pydantic import BaseModel


# class BedrockConfig(BaseModel):
#     ENDPOINT_URL : str = "https://bedrock-runtime.us-east-1.amazonaws.com"
#     AWS_REGION: str = "us-east-1"
#     PROFILE_NAME: Optional[str] = "Comm-Prop-Sandbox"


# class BedrockClient:
#     def __init__(self, config: BedrockConfig):
#         self.config = config
        
#         session = boto3.Session(profile_name=config.PROFILE_NAME)
#         credentials = session.get_credentials().get_frozen_credentials()

#         self.client = boto3.client(
#             "bedrock-runtime",
#             region_name=config.AWS_REGION,
#             endpoint_url=config.ENDPOINT_URL,
#             aws_access_key_id=credentials.access_key,
#             aws_secret_access_key=credentials.secret_key,
#             aws_session_token=credentials.token,
#         )

