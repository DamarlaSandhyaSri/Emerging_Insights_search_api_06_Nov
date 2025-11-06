import json
import re
from src.api.routes.bedrock_client import BedrockClient, BedrockConfig
from src.api.routes.logger import get_logger
from src.api.routes.concern_risk_misc_naics import concerns_events, emerging_risks, misc_topics, naics_data
from src.api.routes.settings import BEDROCK_MODEL

logger = get_logger(__name__)

# ------------------ OPENSEARCH SCHEMA ------------------

OPENSEARCH_SCHEMA = """ 
Index Name: ei_articles_index-05-nov-test

Field Mappings (exact names from OpenSearch):
- title (text)
- data (text)
- description (text)
- reason_identified (text)
- published_time (text)
- last_update_time (text)
- injection_time (text)
- is_latest (boolean)
- url (keyword)
- concerns (keyword)
- emerging_risk_name (keyword)
- region (keyword)
- miscTopics (keyword)
- naicscode (keyword)
- naics_description (keyword)
- source (keyword)
- tag (keyword)
- doc_id (long)
- source_meta (object: {{rss_entry (text), title (text)}})
- chunk_id (integer)
- field (text) — field name from which chunk is derived
- chunk_text (text) — text chunk content
- chunk_vector (knn_vector, 1024-dim) — embedding vector for chunk_text

Vector Fields (for semantic search):
- chunk_vector (used for semantic similarity search across title, data, and reason_identified)

Available Values:
- Tags: Current, Potential New Trend, Untagged, Processing Error
"""

# ------------------ PROMPT TEMPLATE ------------------

QUERY_GENERATION_PROMPT = """
You are an expert at converting natural language queries into valid OpenSearch DSL queries.

INDEX SCHEMA:
{schema}

USER QUERY: {query}

CRITICAL INSTRUCTIONS:
1. Generate ONLY a valid JSON object for the OpenSearch query body.
2. Use EXACT field names from the schema (case-sensitive).
3. Query rules:
   - If the query involves *content*, *title*, *reason*, or any semantic topic (e.g. mentions data, reason_identified, title, or general article topic):
       → Use a similarity (vector) search on `chunk_vector` with a KNN query.
       Example:
       {{
         "knn": {{
           "field": "chunk_vector",
           "query_vector_builder": {{
             "text_embedding": {{
               "model_id": "embedding_model_id",
               "model_text": "<USER_QUERY_TEXT>"
             }}
           }},
           "k": 10,
           "num_candidates": 100
         }}
       }}
   - For metadata searches (e.g. tag, source, region, concerns, emerging_risk_name, miscTopics, naicscode):
       → Use `term`, `terms`, `match`, or `range` queries on those fields.
   - To combine similarity and filters (hybrid search):
       → Use a `bool` query where `must` includes both the `knn` and any filters.
       ✅ Do NOT place `knn` inside `filter`; it must be directly under `must`.

4. Field types:
   - Keyword: tag, source, concerns, emerging_risk_name, miscTopics, naicscode, naics_description, region, url
   - Text: title, description, data, reason_identified, chunk_text
   - Boolean: is_latest
   - Numeric: doc_id, chunk_id
   - Date/time: published_time, last_update_time, injection_time

5. Common query patterns:
   - "Show all articles" → match_all
   - "Tagged as Current" → term query on tag
   - "From Reuters" → term query on source
   - "With concern PFAS" → term query on concerns
   - "Show recent articles" → range query on published_time or last_update_time

6. Combination examples:
   - "Articles about climate change tagged as Current":
     → Use `bool.must` with knn query for "climate change" and a term query for tag="Current".
   - "Emerging risks in Europe":
     → term on region="Europe" + knn if context indicates semantic topic.
   - "Show PFAS untagged articles":
     → bool.must with term(tag="Untagged") and term(concerns="PFAS").
   - "Articles about wildfires in last 3 days":
     → bool.must with range on published_time and knn for "wildfires".

7. Always structure hybrid queries like this:
   {{
     "query": {{
       "bool": {{
         "must": [
           <other field filters>,
           {{
             "knn": {{
               "field": "chunk_vector",
               "query_vector_builder": {{
                 "text_embedding": {{
                   "model_id": "embedding_model_id",
                   "model_text": "<USER_QUERY_TEXT>"
                 }}
               }},
               "k": 10,
               "num_candidates": 100
             }}
           }}
         ]
       }}
     }}
   }}

8. Return ONLY the JSON object — no text, markdown, or explanations.

---

EXAMPLES:

Query: "Show me all articles tagged as Current"
{{
  "query": {{
    "term": {{
      "tag": "Current"
    }}
  }}
}}

Query: "Find articles about climate change"
{{
  "knn": {{
    "field": "chunk_vector",
    "query_vector_builder": {{
      "text_embedding": {{
        "model_id": "embedding_model_id",
        "model_text": "climate change"
      }}
    }},
    "k": 10,
    "num_candidates": 100
  }}
}}

Query: "Find articles about lawsuits tagged as Current"
{{
  "query": {{
    "bool": {{
      "must": [
        {{
          "term": {{
            "tag": "Current"
          }}
        }},
        {{
          "knn": {{
            "field": "chunk_vector",
            "query_vector_builder": {{
              "text_embedding": {{
                "model_id": "embedding_model_id",
                "model_text": "lawsuits"
              }}
            }},
            "k": 10,
            "num_candidates": 100
          }}
        }}
      ]
    }}
  }}
}}

Query: "Show untagged articles"
{{
  "query": {{
    "term": {{
      "tag": "Untagged"
    }}
  }}
}}

Query: "Show articles with wildfire for last 3 days"
{{
  "query": {{
    "bool": {{
      "must": [
        {{
          "range": {{
            "published_time": {{
              "gte": "now-3d/d",
              "lte": "now"
            }}
          }}
        }},
        {{
          "knn": {{
            "field": "chunk_vector",
            "query_vector_builder": {{
              "text_embedding": {{
                "model_id": "embedding_model_id",
                "model_text": "wildfire"
              }}
            }},
            "k": 10,
            "num_candidates": 100
          }}
        }}
      ]
    }}
  }}
}}
"""




# OPENSEARCH_SCHEMA = """
# Index Name: ei_articles_index-05-nov-test

# Field Mappings (exact names from OpenSearch):
# - title (text)
# - data (text)
# - description (text)
# - reason_identified (text)
# - published_time (text)
# - last_update_time (text)
# - injection_time (text)
# - is_latest (boolean)
# - url (keyword)
# - concerns (keyword)
# - emerging_risk_name (keyword)
# - region (keyword)
# - miscTopics (keyword)
# - naicscode (keyword)
# - naics_description (keyword)
# - source (keyword)
# - tag (keyword)
# - doc_id (long)
# - source_meta (object: {{rss_entry (text), title (text)}})
# - chunk_id (integer)
# - field (text) — field name from which chunk is derived
# - chunk_text (text) — text chunk content
# - chunk_vector (knn_vector, 1024-dim) — embedding vector for chunk_text

# Vector Fields (for semantic search):
# - chunk_vector (used for semantic similarity search across title, data, and reason_identified)

# Available Values:
# - Tags: Current, Potential New Trend, Untagged, Processing Error
# """

# # ------------------ PROMPT TEMPLATE ------------------

# QUERY_GENERATION_PROMPT = """
# You are an expert at converting natural language queries into valid OpenSearch DSL queries.

# INDEX SCHEMA:
# {schema}

# USER QUERY: {query}

# CRITICAL INSTRUCTIONS:
# 1. Generate ONLY a valid JSON object for the OpenSearch query body.
# 2. Use EXACT field names from the schema (case-sensitive).
# 3. Query rules:
#    - If the query is about *content*, *reason*, or *title* (e.g. mentions data, reason_identified, title, or general article topic):
#        → Use a similarity search (knn query) on `chunk_vector` using the query text as the embedding input.
#        Example:
#        {{
#          "knn": {{
#            "field": "chunk_vector",
#            "query_vector_builder": {{
#              "text_embedding": {{
#                "model_id": "embedding_model_id",
#                "model_text": "<USER_QUERY_TEXT>"
#              }}
#            }},
#            "k": 10,
#            "num_candidates": 100
#          }}
#        }}
#    - For other metadata-based searches (e.g. tag, source, concerns, region, naicscode, emerging_risk_name, miscTopics):
#        → Use `term`, `terms`, `match`, or `range` queries on those fields directly.
#    - Use `bool` queries to combine vector and field-based filters.
#      Example: filter by tag or region while doing semantic similarity search.

# 4. Field types:
#    - Keyword fields: tag, source, concerns, emerging_risk_name, miscTopics, naicscode, naics_description, region, url
#    - Text fields: title, description, data, reason_identified, chunk_text
#    - Boolean field: is_latest
#    - Numeric fields: doc_id, chunk_id
#    - Date/time fields: published_time, last_update_time, injection_time

# 5. Common query examples:
#    - "Show all articles" → match_all
#    - "Tagged as Current" → term query on tag field
#    - "From Reuters" → term query on source field
#    - "With concern PFAS" → term query on concerns field
#    - "Show recent articles" → range query on published_time or last_update_time

# 6. Combination examples:
#    - "Articles about climate change tagged as Current":
#      → Use knn search on chunk_vector for "climate change" + filter term on tag="Current".
#    - "Emerging risks in Europe":
#      → term filter on region="Europe" + optional knn or match depending on context.
#    - "Show untagged PFAS articles":
#      → bool query combining term filters on tag="Untagged" and concerns="PFAS".

# 7. Return ONLY the JSON object — no text, markdown, or explanations.

# EXAMPLES:

# Query: "Show me all articles tagged as Current"
# {{
#   "query": {{
#     "term": {{
#       "tag": "Current"
#     }}
#   }}
# }}

# Query: "Find articles about climate change"
# {{
#   "knn": {{
#     "field": "chunk_vector",
#     "query_vector_builder": {{
#       "text_embedding": {{
#         "model_id": "embedding_model_id",
#         "model_text": "climate change"
#       }}
#     }},
#     "k": 10,
#     "num_candidates": 100
#   }}
# }}

# Query: "Find articles about lawsuits tagged as Current"
# {{
#   "query": {{
#     "bool": {{
#       "must": [
#         {{
#           "term": {{
#             "tag": "Current"
#           }}
#         }}
#       ],
#       "filter": [
#         {{
#           "knn": {{
#             "field": "chunk_vector",
#             "query_vector_builder": {{
#               "text_embedding": {{
#                 "model_id": "embedding_model_id",
#                 "model_text": "lawsuits"
#               }}
#             }},
#             "k": 10,
#             "num_candidates": 100
#           }}
#         }}
#       ]
#     }}
#   }}
# }}

# Query: "Show untagged articles"
# {{
#   "query": {{
#     "term": {{
#       "tag": "Untagged"
#     }}
#   }}
# }}

# Now generate the OpenSearch query for the user's query. Return ONLY the JSON object.
# """



# OPENSEARCH_SCHEMA = """
# Index Name: ei_articles_index-31-oct

# Field Mappings (exact names from OpenSearch):
# - title (text) - Article title
# - source (keyword) - Source name
# - url (keyword) - Article URL
# - data (text) - Full article content
# - description (text) - Article description
# - reason_identified (text) - Reason for identification
# - concerns (keyword) - List of concerns
# - emerging_risk_name (keyword) - Emerging risk names
# - miscTopics (keyword) - Miscellaneous topics
# - naicscode (keyword) - NAICS code
# - naics_description (keyword) - NAICS description
# - tag (keyword) - Classification tag (Current, Potential New Trend, Untagged, etc.)
# - region (keyword) - Geographic region
# - date_time (text) - Date and time
# - chunk_index (long) - Chunk index for split documents
# - doc_id (long) - Document ID
# - total_chunks (long) - Total number of chunks
# - source_meta (object) - Source metadata with rss_entry and title

# Vector Fields (for semantic search):
# - data_vector (knn_vector, 1536-dim)
# - title_vector (knn_vector, 1536-dim)
# - description_vector (knn_vector, 1536-dim)
# - reason_identified_vector (knn_vector, 1536-dim)

# Available Values:
# - Concerns: {concerns}
# - Emerging Risks: {emerging_risks}
# - Misc Topics: {misc_topics}
# - NAICS Codes: {naics_data}
# - Tags: Current, Potential New Trend, Untagged, Processing Error
# """

# ------------------ PROMPT TEMPLATE ------------------

# QUERY_GENERATION_PROMPT = """You are an expert at converting natural language queries into valid OpenSearch DSL queries.

# INDEX SCHEMA:
# {schema}

# USER QUERY: {query}

# CRITICAL INSTRUCTIONS:
# 1. Generate ONLY a valid JSON object for the OpenSearch query body
# 2. Use EXACT field names from the schema (case-sensitive):
#    - Keyword fields: tag, source, concerns, emerging_risk_name, miscTopics, naicscode, naics_description, region, url
#    - Text fields: title, description, data, reason_identified, date_time
# 3. Query types:
#    - Use "term" for single exact keyword match
#    - Use "terms" for multiple keyword values (must be array)
#    - Use "match" for full-text search on text fields
#    - Use "multi_match" for searching across multiple text fields
#    - Use "bool" with "must", "should", "filter", or "must_not" for combinations
#    - Use "range" for date/numeric filters on date_time, chunk_index, doc_id
#    - Use "exists" to check if field has a value
# 4. Common patterns:
#    - "Show all" → match_all
#    - "tagged as X" → term query on tag field
#    - "about X" → match query on title or multi_match on title/description/data
#    - "from source X" → term query on source field
#    - "with concern X" → term query on concerns field
# 5. Return ONLY the JSON object - no explanations, markdown, or code blocks

# EXAMPLES:

# Query: "Show me all articles tagged as Current"
# {{
#   "query": {{
#     "term": {{
#       "tag": "Current"
#     }}
#   }}
# }}

# Query: "Find articles about climate change"
# {{
#   "query": {{
#     "multi_match": {{
#       "query": "climate change",
#       "fields": ["title", "description", "data"]
#     }}
#   }}
# }}

# Query: "Show articles with PFAS concerns and tagged as Current"
# {{
#   "query": {{
#     "bool": {{
#       "must": [
#         {{"term": {{"tag": "Current"}}}},
#         {{"term": {{"concerns": "PFAS"}}}}
#       ]
#     }}
#   }}
# }}

# Query: "Find articles from Reuters or Bloomberg"
# {{
#   "query": {{
#     "terms": {{
#       "source": ["Reuters", "Bloomberg"]
#     }}
#   }}
# }}

# Query: "Show articles about lawsuits or property damage"
# {{
#   "query": {{
#     "bool": {{
#       "should": [
#         {{"match": {{"data": "lawsuits"}}}},
#         {{"match": {{"data": "property damage"}}}}
#       ],
#       "minimum_should_match": 1
#     }}
#   }}
# }}

# Query: "Show all articles"
# {{
#   "query": {{
#     "match_all": {{}}
#   }}
# }}

# Query: "Show untagged articles"
# {{
#   "query": {{
#     "term": {{
#       "tag": "Untagged"
#     }}
#   }}
# }}

# Query: "Find articles with NAICS code 524126"
# {{
#   "query": {{
#     "term": {{
#       "naicscode": "524126"
#     }}
#   }}
# }}

# Now generate the OpenSearch query for the user's query. Return ONLY the JSON object.
# """

# ------------------ QUERY GENERATOR CLASS ------------------

class OpenSearchQueryGenerator:
    def __init__(self):
        self.bedrock = BedrockClient(BedrockConfig())
        self.model_id = BEDROCK_MODEL

    def _prepare_schema(self) -> str:
        """Prepare schema for the prompt."""
        return OPENSEARCH_SCHEMA.format(
            concerns=", ".join(concerns_events) + "...",
            emerging_risks=", ".join(emerging_risks) + "...",
            misc_topics=", ".join(misc_topics) + "...",
            naics_data=", ".join([f"{item['code']}" for item in naics_data]) + "..."
        )

    # def generate_query(self, user_query: str) -> dict:
    #     """Generate an OpenSearch query DSL body from a natural language query."""
    #     try:
    #         schema = self._prepare_schema()
    #         prompt = QUERY_GENERATION_PROMPT.format(
    #             schema=schema,
    #             query=user_query,
    #         ).replace("embedding_model_id", self.model_id)

    #         response_text = self.bedrock.invoke_model(
    #             model_id=self.model_id,
    #             prompt=prompt,
    #             max_tokens=2000,
    #             temperature=0.0
    #         )

    #         logger.info(f"LLM Response (first 500 chars): {response_text[:500]}...")
            
    #         query_body = self._extract_json(response_text)

    #         if not query_body or "query" not in query_body:
    #             logger.warning(f"Invalid query generated, using match_all. Response: {response_text[:200]}")
    #             return self._default_query()
            
    #         if "knn" in query_body:
    #             logger.info("Generated vector similarity (semantic) search query.")
    #         else:
    #             logger.info("Generated field-based query.")

    #         logger.info(f"Successfully generated OpenSearch query for: {user_query}")
    #         logger.info(f"Query body: {json.dumps(query_body, indent=2)}")
    #         return query_body

    #     except Exception as e:
    #         logger.error(f"Error generating OpenSearch query: {e}", exc_info=True)
    #         return self._default_query()


    def generate_query(self, user_query: str) -> dict:
      """Generate an OpenSearch query DSL body from a natural language query."""
      try:
          embedding_model_id = "amazon.titan-embed-text-v2:0"
          schema = self._prepare_schema()
          prompt = QUERY_GENERATION_PROMPT.format(schema=schema, query=user_query)
          prompt = prompt.replace("embedding_model_id", embedding_model_id)

          response_text = self.bedrock.invoke_model(
              model_id=self.model_id,
              prompt=prompt,
              max_tokens=2000,
              temperature=0.0
          )

          logger.info(f"LLM Response (first 500 chars): {response_text[:500]}...")
          query_body = self._extract_json(response_text)

          if not query_body or ("query" not in query_body and "knn" not in query_body):
              logger.warning(f"Invalid query generated, using match_all. Response: {response_text[:200]}")
              return self._default_query()

          if "knn" in query_body:
              logger.info("Generated vector similarity (semantic) search query.")
          else:
              logger.info("Generated field-based query.")

          logger.info(f"Query body: {json.dumps(query_body, indent=2)}")
          return query_body

      except Exception as e:
          logger.error(f"Error generating OpenSearch query: {e}", exc_info=True)
          return self._default_query()


    def _extract_json(self, text: str) -> dict:
        """Extract JSON from the LLM's response text with robust error handling."""
        # Remove common markdown artifacts
        text = text.strip()
        
        # Remove markdown code blocks if present
        if '```' in text:
            # Extract content between code blocks
            match = re.search(r'```(?:json)?\s*(.*?)\s*```', text, re.DOTALL)
            if match:
                text = match.group(1).strip()
        
        try:
            # Try direct parsing
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning(f"Direct JSON parse failed: {e}")
            
            try:
                # Find the first complete JSON object
                start = text.find('{')
                if start == -1:
                    logger.error(f"No opening brace found in: {text[:100]}")
                    return {}
                
                # Count braces to find matching closing brace
                brace_count = 0
                in_string = False
                escape_next = False
                end = start
                
                for i in range(start, len(text)):
                    char = text[i]
                    
                    if escape_next:
                        escape_next = False
                        continue
                    
                    if char == '\\':
                        escape_next = True
                        continue
                    
                    if char == '"':
                        in_string = not in_string
                        continue
                    
                    if not in_string:
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end = i + 1
                                break
                
                if end > start:
                    json_str = text[start:end]
                    parsed = json.loads(json_str)
                    logger.info("Successfully extracted JSON from text")
                    return parsed
                else:
                    logger.error("Could not find matching closing brace")
                    
            except json.JSONDecodeError as e2:
                logger.error(f"Failed to extract valid JSON: {e2}")
                logger.error(f"Attempted to parse: {text[start:min(start+200, len(text))]}...")
            except Exception as e3:
                logger.error(f"Unexpected error during JSON extraction: {e3}")
        
        logger.error("All JSON extraction attempts failed, returning empty dict")
        return {}

    def _default_query(self) -> dict:
        """Return a fallback match_all query."""
        return {"query": {"match_all": {}}}

































# import json
# import re
# from bedrock_client import BedrockClient, BedrockConfig
# from logger import get_logger
# from concern_risk_misc_naics import concerns_events, emerging_risks, misc_topics, naics_data
# from settings import BEDROCK_MODEL

# logger = get_logger(__name__)

# # ------------------ OPENSEARCH SCHEMA ------------------

# OPENSEARCH_SCHEMA = """
# Index Name: ei_articles_index-29-oct-sample

# Fields and Types:
# - title (text)
# - source (keyword)
# - URL (keyword)
# - Data (text)
# - Description (text)
# - ReasonIdentified (text)
# - Concerns (keyword)
# - EmergingRiskName (keyword)
# - MiscTopics (keyword)
# - NAICSCODE (keyword)
# - NAICSDescription (keyword)
# - Tag (keyword)
# - Region (keyword)
# - DateTime (text)
# - Data_vector (knn_vector, dimension=1536)
# - Title_vector (knn_vector, dimension=1536)
# - Description_vector (knn_vector, dimension=1536)
# - ReasonIdentified_vector (knn_vector, dimension=1536)

# Available Values for Classification Fields:
# - Concerns: {concerns}
# - Emerging Risks: {emerging_risks}
# - Misc Topics: {misc_topics}
# - NAICS Codes: {naics_data}
# """

# # ------------------ PROMPT TEMPLATE ------------------

# QUERY_GENERATION_PROMPT = """
# <role>
# You are an expert at converting natural language queries into valid OpenSearch DSL queries.
# </role>

# <index_schema>
# {schema}
# </index_schema>

# <task>
# Convert the following user query into a valid OpenSearch query body that can be passed to:
# `client.search(index=index_name, body=query, size=size)`
# </task>

# <user_query>
# {query}
# </user_query>

# <instructions>
# 1. Always generate the query under a `"query"` key.
# 2. Use appropriate OpenSearch DSL components:
#    - Use "term" or "terms" for exact keyword fields (Tag, Source, Concerns, EmergingRiskName, MiscTopics, NAICSCODE).
#    - Use "match" or "multi_match" for full-text fields (Title, Description, Data, ReasonIdentified).
#    - Combine filters using "bool" with "must", "should", or "filter".
#    - Use "range" for date-based filters on DateTime.
#    - For vector similarity, use "knn" query (e.g., Title_vector, Data_vector).
#    - If no condition is specified, return { "query": { "match_all": {} } }.
# 3. Do NOT include the "size" key; it’s handled externally.
# 4. Only return the JSON query body (no explanations or Markdown).
# </instructions>

# <output_format>
# Return ONLY a valid JSON object that can be directly used as the `body` parameter of `client.search()`.

# Example:

# {
#   "query": {
#     "bool": {
#       "must": [
#         { "term": { "Tag": "Current" }},
#         { "match": { "Title": "climate change" }}
#       ]
#     }
#   }
# }
# </output_format>

# <examples>

# User Query: "Show me all articles tagged as Current"
# Response:
# {
#   "query": { "term": { "Tag": "Current" } }
# }

# User Query: "Find articles about climate change with PFAS concerns"
# Response:
# {
#   "query": {
#     "bool": {
#       "must": [
#         { "term": { "EmergingRiskName": "Climate Change" }},
#         { "term": { "Concerns": "PFAS" }}
#       ]
#     }
#   }
# }

# User Query: "Show articles about lawsuits or property damage"
# Response:
# {
#   "query": {
#     "bool": {
#       "should": [
#         { "term": { "Concerns": "lawsuits" }},
#         { "term": { "Concerns": "property damage" }}
#       ]
#     }
#   }
# }

# User Query: "Show all articles"
# Response:
# {
#   "query": { "match_all": {} }
# }

# User Query: "Find similar articles to this title: Cyber attacks on insurers"
# Response:
# {
#   "knn": {
#     "field": "Title_vector",
#     "query_vector": [/* 1536-dim embedding vector */],
#     "k": 10,
#     "num_candidates": 50
#   }
# }
# </examples>

# <critical_rules>
# - Keyword fields → term/terms
# - Text fields → match/multi_match
# - Vector fields → knn
# - Date/time → range
# - Logical combinations → bool (must/should/must_not/filter)
# - Default → match_all
# - Return only JSON object
# </critical_rules>
# """

# # ------------------ QUERY GENERATOR CLASS ------------------

# class OpenSearchQueryGenerator:
#     def __init__(self):
#         self.bedrock = BedrockClient(BedrockConfig())
#         self.model_id = BEDROCK_MODEL

#     def _prepare_schema(self) -> str:
#         """Prepare schema for the prompt."""
#         return OPENSEARCH_SCHEMA.format(
#             concerns=", ".join(concerns_events[:20]) + "...",
#             emerging_risks=", ".join(emerging_risks[:20]) + "...",
#             misc_topics=", ".join(misc_topics),
#             naics_data=", ".join([f"{item['code']} - {item['description']}" for item in naics_data])
#         )

#     def generate_query(self, user_query: str) -> dict:
#         """Generate an OpenSearch query DSL body from a natural language query."""
#         try:
#             schema = self._prepare_schema()
#             prompt = QUERY_GENERATION_PROMPT.format(schema=schema, query=user_query)

#             response_text = self.bedrock.invoke_model(
#                 model_id=self.model_id,
#                 prompt=prompt,
#                 max_tokens=2000,
#                 temperature=0.0
#             )

#             print("LLM Response ----------------\n", response_text)
#             query_body = self._extract_json(response_text)

#             if not query_body or "query" not in query_body:
#                 logger.error(f"Failed to generate valid OpenSearch query for: {user_query}")
#                 return self._default_query()

#             logger.info(f"Generated OpenSearch query for: {user_query}")
#             return query_body

#         except Exception as e:
#             logger.error(f"Error generating OpenSearch query: {e}")
#             return self._default_query()

#     # def _extract_json(self, text: str) -> dict:
#     #     """Extract JSON from the LLM's response text."""
#     #     try:
#     #         return json.loads(text)
#     #     except json.JSONDecodeError:
#     #         try:
#     #             match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
#     #             if match:
#     #                 return json.loads(match.group(1))
#     #             match = re.search(r'\{.*\}', text, re.DOTALL)
#     #             if match:
#     #                 return json.loads(match.group(0))
#     #         except Exception as e:
#     #             logger.error(f"Failed to extract JSON: {e}")
#     #     return {}

#     def _extract_json(self, text: str) -> dict:
#         """Extract JSON from the LLM's response text."""
#         try:
#             # First try direct parsing
#             return json.loads(text)
#         except json.JSONDecodeError:
#             try:
#                 # Try to extract from code blocks
#                 match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
#                 if match:
#                     return json.loads(match.group(1))
                
#                 # Try to find any JSON object
#                 match = re.search(r'\{.*\}', text, re.DOTALL)
#                 if match:
#                     json_str = match.group(0)
#                     # Clean up common issues
#                     json_str = json_str.strip()
#                     return json.loads(json_str)
#             except Exception as e:
#                 logger.error(f"Failed to extract JSON from response: {text[:200]}...")
#                 logger.error(f"Error: {e}")
        
#         # If all else fails, return match_all query
#         logger.warning("Returning default match_all query due to parsing failure")
#         return {}

#     def _default_query(self) -> dict:
#         """Return a fallback match_all query."""
#         return { "query": { "match_all": {} } }
















# import json
# import re
# from bedrock_client import BedrockClient, BedrockConfig
# from logger import get_logger
# from concern_risk_misc_naics import concerns_events, emerging_risks, misc_topics, naics_data
# from settings import BEDROCK_MODEL

# logger = get_logger(__name__)

# # DynamoDB table schema
# TABLE_SCHEMA = """
# Table Name: CrawledData

# Primary Key: 
# - Partition Key: URL (String) - The unique URL of the crawled article
# - Sort Key: DateTime (String) - ISO 8601 timestamp when the article was processed

# Attributes:
# - Title (String): Title of the article
# - Source (String): Source website/publication of the article
# - URL (String): The web address of the article
# - Data (String): Full text content of the article
# - Description (String): Brief description or excerpt from the article
# - ReasonIdentified (String): AI-generated summary focusing on insurance-relevant risks and exposures
# - Concerns (String): Semicolon-separated list of identified concern events (e.g., "injuries;property damage;lawsuits")
# - EmergingRiskName (String): Semicolon-separated list of emerging risk categories (e.g., "Climate Change;PFAS;Ransomware")
# - MiscTopics (String): Semicolon-separated list of miscellaneous insurance topics (e.g., "home ownership;personal auto")
# - NAICSCODE (String): Industry classification code (e.g., "327910")
# - NAICSDescription (String): Description of the NAICS code industry (e.g., "Abrasive Product Manufacturing")
# - Tag (String): Classification tag - one of: "Current", "Potential New Trend", "Untagged", "Processing Error"

# Available Values for Classification Fields:
# - Concerns: {concerns}
# - Emerging Risks: {emerging_risks}
# - Misc Topics: {misc_topics}
# - NAICS Codes: {naics_data}
# """

# QUERY_GENERATION_PROMPT = """
# <role>You are an expert at converting natural language queries into DynamoDB filter expressions and query parameters.</role>

# <table_schema>
# {schema}
# </table_schema>

# <task>
# Convert the following user query into a structured JSON response that can be used to query DynamoDB.
# </task>

# <user_query>
# {query}
# </user_query>

# <instructions>
# 1. Analyze the user's intent and identify which fields they're querying
# 2. Determine if this is a simple scan with filters or if specific keys are mentioned
# 3. For concerns, emerging risks, or misc topics - match against the available values provided in the schema
# 4. Generate appropriate filter expressions using DynamoDB syntax:
#    - Use "attribute_exists(field)" to check if field exists
#    - Use "contains(field, value)" for substring matching
#    - DynamoDB does NOT support functions like lower() or upper()
#    - For case-insensitive intent, assume data is pre-normalized (e.g., stored lowercase) or leave filtering to application layer
#    - Use "field = value" for exact matching
#    - Use "begins_with(field, value)" for prefix matching
#    - Use "field IN (value1, value2)" for multiple value matching
#    - Use "AND", "OR" for combining conditions
# 5. For date ranges, convert to ISO format and use comparison operators
# 6. ALWAYS set projection_attributes to null - we ALWAYS want ALL columns returned

# </instructions>

# <output_format>
# Return ONLY valid JSON in this exact structure:

#     "query_type": "scan" or "query",
#     "partition_key": ("name": "URL", "value": "specific_url") or null,
#     "filter_expression": "DynamoDB filter expression string" or null,
#     "expression_attribute_names": ("#tag": "Tag", "#concerns": "Concerns") or null,
#     "expression_attribute_values": (":tag_val": "Current", ":concern_val": "injuries") or null,
#     "projection_attributes": null,
#     "limit": 200,
#     "explanation": "Brief explanation of what the query does"

# IMPORTANT: projection_attributes MUST ALWAYS be null - we always return ALL columns from the database.
# </output_format>

# <examples>
# User Query: "Show me all articles tagged as Current"
# Response:

#     "query_type": "scan",
#     "partition_key": null,
#     "filter_expression": "#tag = :tag_val",
#     "expression_attribute_names": "#tag": "Tag",
#     "expression_attribute_values": ":tag_val": "Current",
#     "projection_attributes": null,
#     "limit": 200,
#     "explanation": "Scanning for all records where Tag equals 'Current'"

# User Query: "Find articles about climate change with PFAS concerns"
# Response:

#     "query_type": "scan",
#     "partition_key": null,
#     "filter_expression": "contains(#emerg, :emerg_val1) AND contains(#emerg, :emerg_val2)",
#     "expression_attribute_names": "#emerg": "EmergingRiskName",
#     "expression_attribute_values": ":emerg_val1": "Climate Change", ":emerg_val2": "PFAS",
#     "projection_attributes": null,
#     "limit": 200,
#     "explanation": "Finding articles with both Climate Change and PFAS in emerging risks"

# User Query: "Show articles about lawsuits or property damage"
# Response:

#     "query_type": "scan",
#     "partition_key": null,
#     "filter_expression": "contains(#concerns, :concern1) OR contains(#concerns, :concern2)",
#     "expression_attribute_names": "#concerns": "Concerns",
#     "expression_attribute_values": ":concern1": "lawsuits", ":concern2": "property damage",
#     "projection_attributes": null,
#     "limit": 200,
#     "explanation": "Finding articles containing lawsuits or property damage concerns"


# User Query: "Show all articles"
# Response:

#     "query_type": "scan",
#     "partition_key": null,
#     "filter_expression": null,
#     "expression_attribute_names": null,
#     "expression_attribute_values": null,
#     "projection_attributes": null,
#     "limit": 200,
#     "explanation": "Retrieving all articles from the database"

# </examples>

# <critical_rules>
# - For Concerns, Emerging Risks, and Misc Topics: ALWAYS use values from the available lists in the schema
# - Use "contains()" for fields that store semicolon-separated values
# - Field names starting with uppercase letters need attribute name placeholders (#fieldname)
# - Always include "limit" to prevent overwhelming results
# - Set query_type to "query" ONLY if partition key (URL) is specifically mentioned
# - For untagged records: filter_expression should check for Tag being "Untagged" OR attribute_not_exists(Tag). 
#   (Empty string values must be handled in the application layer, not in DynamoDB.)
# - CRITICAL: projection_attributes MUST ALWAYS be null - never restrict columns, always return ALL attributes
# - expression_attribute_names should be null if no filter_expression uses them
# - expression_attribute_values should be null if no filter_expression uses them
# </critical_rules>

# """


# class QueryGenerator:
#     def __init__(self):
#         self.bedrock = BedrockClient(BedrockConfig())
#         self.model_id = BEDROCK_MODEL
        
#     def _prepare_schema(self) -> str:
#         """Prepare schema with available values for classification fields."""
#         return TABLE_SCHEMA.format(
#             concerns=", ".join(concerns_events[:20]) + "...",  # Show sample
#             emerging_risks=", ".join(emerging_risks[:20]) + "...",
#             misc_topics=", ".join(misc_topics),
#             naics_data=", ".join([f"{item['code']} - {item['description']}" for item in naics_data])
#         )
    
#     def generate_query(self, user_query: str) -> dict:
#         """Generate DynamoDB query parameters from natural language query."""
#         try:
#             schema = self._prepare_schema()
#             prompt = QUERY_GENERATION_PROMPT.format(
#                 schema=schema,
#                 query=user_query
#             )
            
#             response_text = self.bedrock.invoke_model(
#                 model_id=self.model_id,
#                 prompt=prompt,
#                 max_tokens=2000,
#                 temperature=0.0
#             )

#             print("response----------------",response_text)
            
#             # Extract JSON from response
#             query_params = self._extract_json(response_text)

#             print("Query parameters:-----------", query_params)
            
#             if not query_params:
#                 logger.error(f"Failed to generate query for: {user_query}")
#                 return self._get_default_query()
            
#             logger.info(f"Generated query: {query_params.get('explanation', 'No explanation')}")
#             return query_params
            
#         except Exception as e:
#             logger.error(f"Error generating query: {e}")
#             return self._get_default_query()
    
#     def _extract_json(self, response_text: str) -> dict:
#         """Extract JSON from LLM response."""
#         try:
#             return json.loads(response_text)
#         except json.JSONDecodeError:
#             try:
#                 # Try to extract JSON from markdown
#                 json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response_text, re.DOTALL)
#                 if json_match:
#                     return json.loads(json_match.group(1))
                
#                 # Try to find any JSON-like structure
#                 json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
#                 if json_match:
#                     return json.loads(json_match.group(0))
                
#                 return {}
#             except Exception as e:
#                 logger.error(f"Failed to parse JSON: {e}")
#                 return {}
    
#     def _get_default_query(self) -> dict:
#         """Return a default query that shows all records."""
#         return {
#             "query_type": "scan",
#             "partition_key": None,
#             "filter_expression": None,
#             "expression_attribute_names": None,
#             "expression_attribute_values": None,
#             "projection_attributes": None,
#             "limit": 50,
#             "explanation": "Showing all records (default query)"
#         }
    
#     def validate_query(self, query_params: dict) -> tuple[bool, str]:
#         """Validate the generated query parameters."""
#         required_fields = ["query_type", "limit"]
        
#         for field in required_fields:
#             if field not in query_params:
#                 return False, f"Missing required field: {field}"
        
#         if query_params["query_type"] not in ["scan", "query"]:
#             return False, "query_type must be 'scan' or 'query'"
        
#         if query_params["query_type"] == "query" and not query_params.get("partition_key"):
#             return False, "query type requires partition_key"
        
#         return True, "Valid query"