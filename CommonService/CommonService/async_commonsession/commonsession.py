import aioboto3
from pydantic import BaseModel,field_validator
from typing import Optional
class CommonSessionConfig(BaseModel):
    client_name:str
    region: str
    profile_name:Optional[str]

    @field_validator('client_name','region')
    def client_region_must_be_str(cls,v,field):
        if not isinstance(v,str):
            raise TypeError(f"{field.name} should be string")
        return v

    @field_validator('profile_name')
    def client_region_must_be_str(cls, v):
        if not isinstance(v, str) and v is not None:
            raise TypeError(f"{v} should be string")
        return v

class CommonSession:
    def __init__(self, config:CommonSessionConfig):


        self.client_name = config.client_name
        self.region = config.region
        self.profile_name = config.profile_name
        self.client = None
        # self.session = (aioboto3.Session(profile_name=self.profile_name) if self.profile_name else aioboto3.Session())
        self.session = aioboto3.Session()


    async def __aenter__(self):
        self.client = await self.session.client(
            service_name=self.client_name,
            region_name=self.region
        ).__aenter__()
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)























# async def main():

#     async with CommonSession(client_name="s3", region="us-east-1", profile_name="Comm-Prop-Sandbox") as s3_client:
#         response = await s3_client.list_buckets()
#         print(response)
#     print("-"*50)

#     async with CommonSession(client_name="bedrock", region="us-east-1", profile_name="Comm-Prop-Sandbox") as bedrock_client:
#         models = await bedrock_client.list_foundation_models()
#         print(models)

# import asyncio

# asyncio.run(main())