import os
import logging
from time import time
from typing import List, Union, Callable, Dict, Any, Optional
from functools import wraps

from azure.core.exceptions import AzureError, ResourceNotFoundError, HttpResponseError
from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import statsd

log_format = "%(asctime)s.%(msecs)03d [%(levelname)s] %(filename)s:%(lineno)d (%(funcName)s) - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.WARNING)


class AzureComputeManager:
    """
    Class to manage Azure Compute resources.

    Attributes:
        subscription_id (str): The Azure subscription ID.
        client_id (str): The Azure client ID.
        client_secret (str): The Azure client secret.
        tenant_id (str): The Azure tenant ID.
        credentials (ClientSecretCredential): The Azure service principal credentials.
        client (ComputeManagementClient): The Azure ComputeManagement client.
    """

    retry_decorator = retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(HttpResponseError),
    )

    def __init__(self) -> None:
        """
        Initializes Azure ComputeManagement client using environment variables.
        """
        load_dotenv()

        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")

        self._initialize_azure_compute_client()

    @staticmethod
    def measure_execution_time(func: Callable) -> Callable:
        """
        A decorator to measure the execution time of a function.

        Args:
            func (Callable): The function to measure.

        Returns:
            Callable: The decorated function.
        """

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            start_time = time()
            result = func(*args, **kwargs)
            end_time = time()
            return result

        return wrapped_func

    @staticmethod
    def exception_handler(function: Callable):
        """
        A decorator to handle exceptions for Azure operations.

        Args:
            function (Callable): The function where exceptions need to be handled.

        Returns:
            Callable: The decorated function.
        """

        @wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except HttpResponseError as e:
                if e.status >= 500:
                    logger.warning(f"Recoverable API error: {e}")
                    raise
                else:
                    logger.error(f"API error: {e}")
            except ResourceNotFoundError as e:
                logger.error(f"Resource not found: {e}")
            except PermissionError:
                logger.error("Insufficient permissions to perform operation.")
            except AzureError as e:
                logger.error(f"Azure operation failed: {e}")
            except Exception as e:
                logger.error(f"An unknown error occurred: {e}")

        return wrapper

    @measure_execution_time
    @retry_decorator
    @exception_handler
    def _initialize_azure_compute_client(self) -> None:
        """
        Create Azure ComputeManagement client using service principal credentials.

        Raises:
            ValueError: If any of the Azure credentials (client_id, client_secret, tenant_id) are missing.
        """
        if not all([self.client_id, self.client_secret, self.tenant_id]):
            raise ValueError("Missing Azure credentials")

        self.credentials = ClientSecretCredential(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
        )
        self.client = ComputeManagementClient(self.credentials, self.subscription_id)
        logger.info("Azure compute client successfully created.")
