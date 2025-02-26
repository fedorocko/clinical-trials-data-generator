from typing import Any


BRIDGE_OVERRIDE_PARAMETER_PREFIX = 'ENABLE_SNOW_API_FOR_'


class SnowApiParameters:
    """Wrapper that abstracts away the behavior from the parsing/reading of parameters.

    Args:
        params_map: A ``dict[str,str]`` of parameter names to their values
    """

    def __init__(self, params_map: dict[str, Any]) -> None:
        self.params_map = params_map

    def is_parameter_true(self, param_name: str, default: str) -> bool:
        return (self.params_map.get(param_name, default) or default
                .lower()
                .strip()
                in (
                    "true",
                    "t",
                    "yes",
                    "y",
                    "on",
                )
            )

    @property
    def should_retry_request(self) -> bool:
        return self.is_parameter_true('PARAM_USE_CLIENT_RETRY', 'true')

    @property
    def should_print_verbose_stack_trace(self) -> bool:
        return self.is_parameter_true('PARAM_PRINT_VERBOSE_STACK_TRACE', 'false')

    @property
    def fix_hostname(self) -> bool:
        return self.is_parameter_true('PARAM_FIX_HOSTNAME', 'true')
