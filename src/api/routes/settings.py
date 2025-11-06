"""
Configuration settings for the Insurance Tagging System.
"""

# AWS Configuration
AWS_REGION = "us-east-1"                
# DYNAMO_TABLE = "CrawledData"   

# Bedrock Configuration
# BEDROCK_MODEL = "anthropic.claude-3-5-sonnet-20240620-v1:0"
BEDROCK_MODEL = "arn:aws:bedrock:us-east-1:982135724133:inference-profile/global.anthropic.claude-sonnet-4-5-20250929-v1:0"


# Processing Configuration
DEFAULT_BATCH_SIZE = 5
MAX_RETRY_ATTEMPTS = 3
LLM_MAX_TOKENS = 1000
LLM_TEMPERATURE = 0.0

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# DynamoDB Configuration
DYNAMO_READ_TIMEOUT = 1000
DYNAMO_AWS_PROFILE = "Comm-Prop-Sandbox"

# Bedrock Configuration
BEDROCK_AWS_PROFILE = "Comm-Prop-Sandbo"
BEDROCK_ENDPOINT_URL = "https://bedrock-runtime.us-east-1.amazonaws.com"

# Processing Limits
MAX_TEXT_LENGTH = 50000  # Maximum characters to process from Data field
MIN_TEXT_LENGTH = 50     # Minimum characters required for processing

# Classification Confidence Thresholds
MIN_CONFIDENCE_THRESHOLD = 0.8
HIGH_CONFIDENCE_THRESHOLD = 0.9

# Tagging Rules
VALID_TAGS = [
    "Current",
    "Potential New Trend", 
    "Untagged",
    "Processing Error"
]

# Field Mappings
REQUIRED_FIELDS = ["URL", "Data"]
OPTIONAL_FIELDS = [
    "Title", "Source", "ReasonIdentified", "Concerns", 
    "Description", "EmergingRiskName", "MiscTopics", 
    "NAICSCODE", "NAICSDescription", "DateTime", "Tag"
]





















# AWS_REGION = "us-east-1"                
# DYNAMO_TABLE = "CrawledData"   
# BEDROCK_MODEL = "anthropic.claude-3-5-sonnet-20240620-v1:0"