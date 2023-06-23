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

    @measure_execution_time
    @retry_decorator
    @exception_handler
    def get_available_vm_sizes(self, location: str = "eastus") -> List[str]:
        """
        Retrieve a list of available VM sizes in the specified location.

        Args:
            location (str): The location to retrieve VM sizes from.

        Returns:
            List[str]: A list of available VM sizes.
        """
        vm_sizes = [
            vm.serialize()
            for vm in self.client.virtual_machine_sizes.list(location=location)
        ]
        logger.info("Retrieved a list of available VM sizes.")
        return vm_sizes

    @measure_execution_time
    @retry_decorator
    @exception_handler
    def get_vm_resources(
        self, node_sku: str, location: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve the CPU and memory configuration for a specific VM size (node SKU).

        Args:
            node_sku (str): The SKU of the node.
            location (str): The location to retrieve VM resources from.

        Returns:
            Dict[str, Any]: A dictionary containing the CPU and memory configuration of the VM if found, otherwise None.
        """
        vm_sizes = self.get_available_vm_sizes(location=location)

        for vm in vm_sizes:
            if vm["name"] == node_sku:
                return {"cpu": vm["numberOfCores"], "memory": vm["memoryInMB"]}

        return None

    @measure_execution_time
    @retry_decorator
    @exception_handler
    def suggest_node_pool(
        self,
        cpu_required: int,
        memory_required: int,
        location: str,
        vm_sizes: List[str],
    ) -> Optional[Dict[str, Union[str, int]]]:
        """
        Suggest a VM size and count based on the CPU and memory requirements, plus a buffer.

        Args:
            cpu_required (int): The number of CPU cores required.
            memory_required (int): The amount of memory required in MB.
            location (str): The location to retrieve VM resources from.
            vm_sizes (List[str]): A list of VM sizes.

        Returns:
            Optional[Dict[str, Union[str, int]]]: The suggested VM size and count if found, otherwise None.
        """
        # Add a 10% buffer to the requirements
        cpu_required = int(cpu_required * 1.1)
        memory_required = int(memory_required * 1.1)

        vm_details = [
            self.get_vm_resources(node_sku, location) for node_sku in vm_sizes
        ]

        # Prepare a list of dictionaries with VM size names and their details
        node_pools = [
            {"name": size, "cpu": detail["cpu"], "memory": detail["memory"]}
            for size, detail in zip(vm_sizes, vm_details)
            if detail
        ]

        # Check if any node pool can accommodate the requirements
        suitable_pools = [
            pool
            for pool in node_pools
            if pool["cpu"] >= cpu_required and pool["memory"] >= memory_required
        ]

        # If there are suitable pools, find the one with minimum total resources exceeding the requirement
        if suitable_pools:
            best_pool = min(
                suitable_pools,
                key=lambda pool: (pool["cpu"] - cpu_required)
                + (pool["memory"] - memory_required),
            )
            return {"name": best_pool["name"], "count": 1}

        # If no suitable pool found, calculate the minimum node count required for each pool
        pool_counts = [
            {
                "name": pool["name"],
                "count": max(
                    cpu_required // pool["cpu"], memory_required // pool["memory"]
                )
                + 1,
            }
            for pool in node_pools
        ]

        # Prefer fewer larger machines: if two pools require the same total resources, choose the one with the smaller count
        # If counts are also the same, choose the one with the larger CPU and memory
        best_pool = min(
            pool_counts,
            key=lambda pool: (
                pool["count"],
                -node_pools[
                    next(
                        index
                        for (index, d) in enumerate(node_pools)
                        if d["name"] == pool["name"]
                    )
                ]["cpu"],
                -node_pools[
                    next(
                        index
                        for (index, d) in enumerate(node_pools)
                        if d["name"] == pool["name"]
                    )
                ]["memory"],
            ),
        )

        return best_pool
