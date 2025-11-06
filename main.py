import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from CommonService.async_opensearch.config import OpenSearchSettings
from CommonService.async_opensearch.service import dependency, lifespan_factory

from src.api.routes.sample_route import sample_router
from src.api.routes.search_docs_v1 import search_router
from src.db.db_middleware import Opensearch_middleware

app = FastAPI(title="Emerging Insights", lifespan=lifespan_factory(
    settings=OpenSearchSettings(
        os_endpoint="a3brd8mlqqwa2qa6ukm3.us-east-1.aoss.amazonaws.com",
        os_port=443,
        # profile_name="Comm-Prop-Sandbox",
        os_region="us-east-1",
    )
))
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(Opensearch_middleware)


app.include_router(sample_router)
app.include_router(search_router)

if __name__ == "__main__":
    uvicorn.run(app=app, host="0.0.0.0", port=8098)
