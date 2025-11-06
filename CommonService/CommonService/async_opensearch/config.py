from pydantic import BaseModel, Field
from typing import Literal,Optional
class OpenSearchSettings(BaseModel):
    os_endpoint:str = Field(..., description="provide the opensearch endpoint")
    os_port:int = Field(default=443)
    service: Literal["es","aoss"]=Field(default="aoss",description="es for aws opensearch, aoss for serverless")
    profile_name:Optional[str]
    os_region:str=Field(...,description="provide region")
    verify_certs:bool = Field(default=True)
    timeout:int=Field(default=30)
    max_retries:int = Field(default=True)
    retry_on_timeout:bool = Field(default=True)
    http_compress:bool = Field(default=True)


