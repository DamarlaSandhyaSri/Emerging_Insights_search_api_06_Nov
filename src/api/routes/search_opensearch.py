import json
from opensearchpy import OpenSearch
from typing import Dict, List, Any, Optional
import boto3
from requests_aws4auth import AWS4Auth
from opensearchpy import RequestsHttpConnection
from pydantic import BaseModel, Field
from typing import Literal
# from src.api.routes.logger import get_logger

# logger = get_logger(__name__)

# CONFIGURATION
class OpenSearchSettings(BaseModel):
    """
    Configuration settings for OpenSearch connection.
    Compatible with both AWS OpenSearch and OpenSearch Serverless (AOSS).
    """
    
    # Basic connection
    os_endpoint: str = Field(
        # default="mab9oebjshix9m9njfg8.us-east-1.aoss.amazonaws.com",
        default="a3brd8mlqqwa2qa6ukm3.us-east-1.aoss.amazonaws.com",  
        description="OpenSearch endpoint or domain"
    )
    os_port: int = Field(default=443, description="Port for OpenSearch endpoint")
    
    # AWS service type: es (OpenSearch Service) or aoss (Serverless)
    service: Literal["es", "aoss"] = Field(default="aoss")
    
    # AWS region and local credentials
    os_region: str = Field(default="us-east-1", description="AWS region, e.g., us-east-1")
    # profile_name: Optional[str] = Field(
    #     # default="Comm-Prop-Sandbox", 
    #     description="AWS CLI profile name"
    # )
    
    # Connection behavior
    verify_certs: bool = Field(default=True)
    timeout: int = Field(default=30)
    max_retries: int = Field(default=3)
    retry_on_timeout: bool = Field(default=True)
    http_compress: bool = Field(default=True)


# CLIENT BUILDER
def build_client(settings: OpenSearchSettings) -> OpenSearch:
    """
    Create a synchronous OpenSearch client.
    Works for both:
    - AWS OpenSearch Service (managed)
    - AWS OpenSearch Serverless (AOSS)
    """
    
    # Create AWS credentials if profile is provided
    session = boto3.Session()
    # if settings.profile_name:
    #     session = boto3.Session(profile_name=settings.profile_name)
    # else:
    #     session = boto3.Session()
    
    credentials = session.get_credentials().get_frozen_credentials()
    # credentials = session
    
    # AWS SigV4 authentication
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        settings.os_region,
        settings.service,
        session_token=credentials.token,
    )
    
    client = OpenSearch(
        hosts=[{"host": settings.os_endpoint, "port": settings.os_port}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=settings.verify_certs,
        http_compress=settings.http_compress,
        timeout=settings.timeout,
        connection_class=RequestsHttpConnection,
        max_retries=settings.max_retries,
        retry_on_timeout=settings.retry_on_timeout,
    )
    
    return client


settings = OpenSearchSettings()
client = build_client(settings)



def search_documents( index_name: str, query: Dict[str, Any], 
                        size: Optional[int]) -> Dict[str, Any]:
        """
        Search documents in the index.
        
        Args:
            index_name: Name of the index
            query: OpenSearch query DSL
            size: Number of results to return
        
        Returns:
            Search results
        """
        try:
            response = client.search(
                index=index_name,
                body=query,
                size=size
            )
            # total_hits = response.get('hits', {}).get('total', {}).get('value', 0)
            return response
        
        except Exception as e:
            print(f"✗ Error searching documents: {str(e)}")
            return {"error": str(e)}


# def execute_opensearch_query(self, query_body: dict):
#         """Execute an OpenSearch query using the existing client wrapper."""
#         try:
#             response = self.os_client.search_documents(
#                 index_name=self.index_name,
#                 query=query_body,
#                 size=50  # you can adjust as needed
#             )
#             hits = response.get("hits", {}).get("hits", [])
#             return [hit["_source"] for hit in hits]
#         except Exception as e:
#             print(f"Error executing OpenSearch query: {e}")
#             # logger.error(f"Error executing OpenSearch query: {e}")
#             # st.error(f"Error executing OpenSearch query: {str(e)}")
#             return None



# def get_unique_docs(results):
#     unique_docs = {}
#     for hit in results["hits"]["hits"]:
#         doc_id = hit["_source"]["doc_id"]
#         if doc_id not in unique_docs:
#             unique_docs[doc_id] = hit["_source"]

#     unique_list = list(unique_docs.values())
#     print(f"Unique docs count: {len(unique_list)}")
#     results = unique_list
#     results["hits"]["total"]["value"] = len(unique_list)
#     return results



# def get_unique_docs(search_results):
#     """Remove duplicate docs based on 'doc_id' but retain full hit objects."""
#     if not search_results or "hits" not in search_results:
#         return []

#     seen_doc_ids = set()
#     unique_hits = []

#     for hit in search_results["hits"]["hits"]:
#         doc_id = hit.get("_source", {}).get("doc_id")
#         if doc_id and doc_id not in seen_doc_ids:
#             seen_doc_ids.add(doc_id)
#             unique_hits.append(hit)

#     search_results["hits"]["hits"] = unique_hits
#     print(f"Unique docs count: {len(unique_hits)}")
#     return unique_hits


def get_unique_docs(search_results):
    """Remove duplicate docs based on 'doc_id' but retain full search result structure."""
    if not search_results or "hits" not in search_results:
        return search_results  # return as-is if invalid

    hits_data = search_results.get("hits", {})
    all_hits = hits_data.get("hits", [])

    seen_doc_ids = set()
    unique_hits = []

    for hit in all_hits:
        doc_id = hit.get("_source", {}).get("doc_id")
        if doc_id and doc_id not in seen_doc_ids:
            seen_doc_ids.add(doc_id)
            unique_hits.append(hit)

    print(f"Unique docs count: {len(unique_hits)}")

    # ✅ Rebuild the full structure
    return {
        "took": search_results.get("took", 0),
        "timed_out": search_results.get("timed_out", False),
        "_shards": search_results.get("_shards", {}),
        "hits": {
            "total": {
                "value": len(unique_hits),
                "relation": "eq"
            },
            "max_score": hits_data.get("max_score"),
            "hits": unique_hits
        }
    }




# url = "https://techxplore.com/news/2025-10-robots-automatic-fabric-muscle-commercialization.html"

url = "https://techxplore.com/news/2025-10-concentrationcontrolled-doping-ptype-polymer-semiconductor.html"
search_query = {
        "query": {
            "term": {
                "URL":  { "value":f"{url}"
            }
            }
        }
    }

search_query = {
  "query": {
    "match_all": {}
  }
}

index_name = "ei_articles_index-05-nov-test"
search_results = search_documents(index_name, search_query, size=10)
print(search_results)


