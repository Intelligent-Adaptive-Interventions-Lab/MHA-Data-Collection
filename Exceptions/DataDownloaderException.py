class ConfigsNotSetException(Exception):
    """Exception raised when the configs are not loaded properly
    """
    pass


class RequestFailureException(Exception):
    """Exception raised when the API call fail
    """
    pass
