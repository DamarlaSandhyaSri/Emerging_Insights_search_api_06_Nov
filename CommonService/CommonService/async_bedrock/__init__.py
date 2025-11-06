"""
This module initializes the async_bedrock package and exposes TitanV1 and TitanV2 as its public API.
"""

# import TitanV1 and TitanV2 from CommonService.async_bedrock.base
from CommonService.async_bedrock.base import TitanV1, TitanV2


# import __all__ to specify the public API of the module
__all__ = ["TitanV1", "TitanV2"]