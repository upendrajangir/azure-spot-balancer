# Standard library imports
import os
import logging
from functools import wraps
from time import time
from typing import List, Union, Callable

# Third party imports
from azure.core.exceptions import AzureError, ResourceNotFoundError, HttpResponseError
from azure.identity import ClientSecretCredential
from azure.mgmt.containerservice import ContainerServiceClient
from azure.mgmt.containerservice.models import AgentPool
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import statsd


# Configure logging
log_format = "%(asctime)s.%(msecs)03d [%(levelname)s] %(filename)s:%(lineno)d (%(funcName)s) - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# Set the logging level for Azure SDK to WARNING
azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.WARNING)


class NodePoolManager:
    """
    Manages Azure AKS node pools.

    Attributes:
        subscription_id (str): The Azure subscription ID.
        client_id (str): The Azure client ID.
        client_secret (str): The Azure client secret.
        tenant_id (str): The Azure tenant ID.
        resource_group (str): The Azure resource group.
        cluster_name (str): The AKS cluster name.
        credentials (ServicePrincipalCredentials): The Azure service principal credentials.
        client (ContainerServiceClient): The Azure ContainerService client.
    """

    # Configure a retry decorator for recoverable errors
    retry_decorator = retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(HttpResponseError),
    )

    def __init__(self) -> None:
        """
        Initializes Azure and Kubernetes clients using environment variables.
        """
        load_dotenv()
        # Azure credentials
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.resource_group = os.getenv("AKS_RESOURCE_GROUP")
        self.cluster_name = os.getenv("AKS_CLUSTER_NAME")

        # Create Azure ContainerService client
        self._create_azure_client()

    @staticmethod
    def measure_execution_time(func: Callable) -> Callable:
        """
        A decorator for measuring the execution time of a function.

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
            # logger.info(
            #     f"Execution time for {func.__name__}: {end_time - start_time} seconds"
            # )
            return result

        return wrapped_func

    @staticmethod
    def handle_exceptions(function):
        @wraps(function)
        def exception_wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except HttpResponseError as e:
                if e.status >= 500:
                    logger.warning(
                        f"Recoverable API error when attempting to {function.__name__}: {e}"
                    )
                    raise
                else:
                    logger.error(
                        f"API error occurred when attempting to {function.__name__}: {e}"
                    )
            except ResourceNotFoundError as e:
                logger.error(f"Resource not found: {e}")
            except PermissionError:
                logger.error(f"Insufficient permissions to {function.__name__}")
            except AzureError as e:
                logger.error(f"Failed to {function.__name__}: {e}")
            except Exception as e:
                logger.error(
                    f"An unknown error occurred while {function.__name__}: {e}"
                )

        return exception_wrapper

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def _create_azure_client(self) -> None:
        """
        Create Azure ContainerService client using service principal credentials.

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
        self.client = ContainerServiceClient(self.credentials, self.subscription_id)
        logger.info("Azure client successfully created.")

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def check_cluster_status(self, cluster_name: str) -> bool:
        """
        Check the status of a specific Kubernetes cluster.

        Args:
            cluster_name (str): The name of the Kubernetes cluster.

        Returns:
            bool: True if the Kubernetes cluster is running, False otherwise.
        """
        try:
            cluster_info = self.client.managed_clusters.get(
                self.resource_group, cluster_name
            ).serialize()
        except Exception as e:
            logger.error(
                f"Failed to get information about the Kubernetes cluster: {cluster_name}. Error: {str(e)}"
            )
            return False

        power_state = (
            cluster_info.get("properties", {})
            .get("agentPoolProfiles", [{}])[0]
            .get("powerState", {})
            .get("code")
        )

        if power_state == "Running":
            logger.info(f"Kubernetes cluster {cluster_name} is running.")
            return True

        logger.info(f"Kubernetes cluster {cluster_name} is not running.")
        return False

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def list_node_pools(self) -> List[AgentPool]:
        """
        List all node pools in the Kubernetes cluster.

        Returns:
            List[AgentPool]: A list of AgentPool objects.
        """
        node_pools = self.client.agent_pools.list(
            self.resource_group, self.cluster_name
        )

        list_node_pools = list(node_pools)

        logger.info(f"Successfully retrieved {len(list_node_pools)} node pools.")

        return list_node_pools

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def get_node_pool(self, node_pool_name: str) -> AgentPool:
        """
        Get a specific node pool in the Kubernetes cluster.

        Args:
            node_pool_name (str): The name of the node pool.

        Returns:
            AgentPool: An AgentPool object.
        """
        node_pool = self.client.agent_pools.get(
            self.resource_group, self.cluster_name, node_pool_name
        )

        logger.info(f"Successfully retrieved node pool: {node_pool_name}")

        return node_pool

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def update_node_pool_autoscaling(
        self,
        node_pool_name: str,
        min_count: int,
        max_count: int,
        enable_auto_scaling: bool = True,
    ) -> bool:
        """
        Updates an existing AKS node pool to enable or disable autoscaling.

        Args:
            node_pool_name (str): The name of the node pool.
            min_count (int): The minimum number of nodes for autoscaling.
            max_count (int): The maximum number of nodes for autoscaling.
            enable_auto_scaling (bool): True to enable autoscaling, False to disable. Default is True.

        Returns:
            bool: True if the node pool is successfully updated, False otherwise.
        """
        self.client.agent_pools.begin_create_or_update(
            self.resource_group,
            self.cluster_name,
            node_pool_name,
            {
                "properties": {
                    "minCount": min_count,
                    "maxCount": max_count,
                    "enableAutoScaling": enable_auto_scaling,
                    "type": "VirtualMachineScaleSets",
                }
            },
        ).result()
        logger.info(
            f"Successfully configured autoscaling for node pool = {node_pool_name} with min_count = {min_count} and max_count = {max_count}."
        )
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def update_node_pool_manual_scaling(
        self, node_pool_name: str, count: int, enable_auto_scaling: bool = False
    ) -> bool:
        """
        Updates an existing AKS node pool to set a specific node count (manual scaling).

        Args:
            node_pool_name (str): The name of the node pool.
            count (int): The specific number of nodes for the manual scaling.
            enable_auto_scaling (bool): Should be False since autoscaling is not used. Default is False.

        Returns:
            bool: True if the node pool is successfully updated, False otherwise.
        """
        self.client.agent_pools.begin_create_or_update(
            self.resource_group,
            self.cluster_name,
            node_pool_name,
            {
                "properties": {
                    "count": count,
                    "enableAutoScaling": enable_auto_scaling,
                    "type": "VirtualMachineScaleSets",
                }
            },
        ).result()
        logger.info(
            f"Successfully scaled node pool: {node_pool_name} to {count} nodes."
        )
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def scale_node_pool(self, node_pool_name: str, desired_node_count: int) -> bool:
        """
        Scale the node pool to a specific node count.

        Args:
            node_pool_name (str): The name of the node pool.
            desired_node_count (int): The desired node count.

        Returns:
            bool: True if successful, False otherwise.
        """
        success = self._set_manual_scaling_for_node_pool(
            node_pool_name, desired_node_count, enable_auto_scaling=False
        )

        if success:
            logger.info(
                f"Successfully scaled node pool: {node_pool_name} to {desired_node_count} nodes."
            )
        return success

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def enable_node_pool_autoscaling(
        self, node_pool_name: str, min_nodes: int, max_nodes: int
    ) -> bool:
        """
        Enable autoscaling for the node pool with the specified minimum and maximum counts.

        Args:
            node_pool_name (str): The name of the node pool.
            min_nodes (int): The minimum node count for autoscaling.
            max_nodes (int): The maximum node count for autoscaling.

        Returns:
            bool: True if successful, False otherwise.
        """
        autoscaling_enabled = self._set_autoscaling_for_node_pool(
            node_pool_name, min_nodes, max_nodes, enable_auto_scaling=True
        )

        if autoscaling_enabled:
            logger.info(
                f"Successfully enabled autoscaling for node pool: {node_pool_name} with min_count = {min_nodes} and max_count = {max_nodes}."
            )
        return autoscaling_enabled

    # TODO: Star refactoring from here
    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def disable_node_pool_autoscaling(self, node_pool_name: str) -> bool:
        """
        Disable autoscaling for the specified node pool.

        Args:
            node_pool_name (str): The name of the node pool.

        Returns:
            bool: True if successful, False otherwise.
        """
        node_pool = self.get_node_pool(node_pool_name)

        if node_pool is None:
            logger.error(f"Node pool '{node_pool_name}' not found.")
            return False

        if not node_pool.enable_auto_scaling:
            logger.info(
                f"Autoscaling is already disabled for node pool '{node_pool_name}'."
            )
            return True

        autoscaling_disabled = self._set_autoscaling_for_node_pool(
            node_pool_name, node_pool.count, enable_auto_scaling=False
        )

        if autoscaling_disabled:
            logger.info(
                f"Successfully disabled autoscaling for node pool: {node_pool_name}."
            )
        return autoscaling_disabled

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def update_node_pool_autoscaling(
        self, node_pool_name: str, min_count: int, max_count: int
    ) -> bool:
        """
        Update the autoscaling configuration for the specified node pool.

        Args:
            node_pool_name (str): The name of the node pool.
            min_count (int): The updated minimum node count for autoscaling.
            max_count (int): The updated maximum node count for autoscaling.

        Returns:
            bool: True if successful, False otherwise.
        """
        node_pool = self.get_node_pool(node_pool_name)

        if node_pool is None:
            logger.error(f"Node pool '{node_pool_name}' not found.")
            return False

        if not node_pool.enable_auto_scaling:
            logger.warning(
                f"Autoscaling is currently disabled for node pool '{node_pool_name}', updating will enable it."
            )

        autoscaling_updated = self._set_autoscaling_for_node_pool(
            node_pool_name, min_count, max_count, enable_auto_scaling=True
        )

        if autoscaling_updated:
            logger.info(
                f"Successfully updated autoscaling configuration for node pool: {node_pool_name}."
            )
        return autoscaling_updated

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def increase_node_pool_count(self, node_pool_name: str) -> bool:
        """
        Increment the node count for the specified node pool.

        Args:
            node_pool_name (str): The name of the node pool.

        Returns:
            bool: True if successful, False otherwise.
        """
        node_pool = self.get_node_pool(node_pool_name)

        if node_pool is None:
            logger.error(f"Node pool '{node_pool_name}' not found.")
            return False

        if node_pool.enable_auto_scaling:
            logger.warning(
                f"Autoscaling is currently enabled for node pool '{node_pool_name}', updating will disable it."
            )

        updated_node_count = node_pool.count + 1
        manual_scaling_set = self._set_manual_scaling_for_node_pool(
            node_pool_name, updated_node_count, enable_auto_scaling=False
        )

        if manual_scaling_set:
            logger.info(
                f"Successfully incremented node count for node pool: {node_pool_name}. New count: {updated_node_count}."
            )
        return manual_scaling_set

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def decrease_node_pool_count(self, node_pool_name: str) -> bool:
        """
        Decrease the node count for the specified node pool.

        Args:
            node_pool_name (str): The name of the node pool.

        Returns:
            bool: True if successful, False otherwise.
        """
        node_pool = self.get_node_pool(node_pool_name)

        if node_pool is None:
            logger.error(f"Node pool '{node_pool_name}' not found.")
            return False

        if node_pool.count <= 0:
            logger.warning(f"Node count is already 0 for node pool '{node_pool_name}'.")
            return False

        if node_pool.enable_auto_scaling:
            logger.warning(
                f"Autoscaling is currently enabled for node pool '{node_pool_name}', updating will disable it."
            )

        updated_node_count = max(0, node_pool.count - 1)
        manual_scaling_set = self._set_manual_scaling_for_node_pool(
            node_pool_name, updated_node_count, enable_auto_scaling=False
        )

        if manual_scaling_set:
            logger.info(
                f"Successfully decreased node count for node pool: {node_pool_name}. New count: {updated_node_count}."
            )
        return manual_scaling_set
