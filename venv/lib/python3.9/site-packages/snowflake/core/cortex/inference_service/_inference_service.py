from typing import TYPE_CHECKING

from snowflake.core._internal.telemetry import api_telemetry
from snowflake.core.cortex.inference_service._generated.api import CortexInferenceApi
from snowflake.core.cortex.inference_service._generated.api_client import (
    StoredProcApiClient,
)
from snowflake.core.cortex.inference_service._generated.models import (
    CompleteRequest,
)
from snowflake.core.rest import SSEClient


if TYPE_CHECKING:
    from snowflake.core import Root


class CortexInferenceService:
    """Represents the Snowflake Cortex Inference Service resource."""

    def __init__(self, root: "Root") -> None:
        self._api = CortexInferenceApi(
            root=root,
            resource_class=None,
            sproc_client=StoredProcApiClient(root=root),
        )

    @api_telemetry
    def complete(self, complete_request: CompleteRequest) -> SSEClient:
        """Perform LLM text completion inference, similar to snowflake.cortex.Complete.

        Parameters
        __________
        complete_request: CompleteRequest
            LLM text completion request.
        """
        return SSEClient(self._api.cortex_llm_inference_complete(
            complete_request,
            _preload_content=False
        ))
