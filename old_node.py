# Standard library imports
import os
import logging
import yaml
from functools import wraps
from time import time
from typing import List, Union, Callable, Optional, Any

# Third party imports
from dotenv import load_dotenv
import statsd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from kubernetes import client, config

# Configure logging
log_format = "%(asctime)s.%(msecs)03d [%(levelname)s] %(filename)s:%(lineno)d (%(funcName)s) - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


class NodeManager:
    _instance: Optional["NodeManager"] = None
    # Configure a retry decorator for recoverable errors
    retry_decorator = retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(client.ApiException),
    )

    def __new__(cls) -> "NodeManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self._create_kubernetes_client()
        self.api_instance = client.CoreV1Api()
        self.labels = [
            {"type": "spot"},
            {"beta.kubernetes.io/instance-type": "Standard_F2s_v2"},
        ]

    @staticmethod
    def measure_execution_time(function: Callable) -> Callable:
        """
        A decorator for measuring the execution time of a function.

        Args:
            func (Callable): The function to measure.

        Returns:
            Callable: The decorated function.
        """

        @wraps(function)
        def time_wrapper(*args, **kwargs):
            start_time = time()
            result = function(*args, **kwargs)
            end_time = time()
            logger.info(
                f"Execution time for {function.__name__}: {end_time - start_time} seconds"
            )
            return result

        return time_wrapper

    @staticmethod
    def handle_exceptions(function):
        @wraps(function)
        def exception_wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except client.ApiException as e:
                if e.status >= 500:  # Retry on server errors
                    logger.warning(
                        f"Recoverable API error when attempting to {function.__name__}: {e}"
                    )
                    raise
                else:  # Don't retry on client errors (4xx)
                    logger.error(
                        f"API error occurred when attempting to {function.__name__}: {e}"
                    )
            except PermissionError:
                logger.error(f"Insufficient permissions to {function.__name__}")
            except Exception as e:
                logger.error(
                    f"An unknown error occurred while {function.__name__}: {e}"
                )

        return exception_wrapper

    @staticmethod
    def _create_kubernetes_client() -> client.CoreV1Api:
        """Create a Kubernetes API client instance."""

        try:
            # Load Kubernetes configuration from default location
            config.load_kube_config()
        except FileNotFoundError:
            logger.error("Kubernetes config file not found.")
            raise
        except Exception as e:
            logger.error(f"Error loading Kubernetes config: {e}")
            raise

        try:
            # Create a Kubernetes API client
            api_instance = client.CoreV1Api()
        except Exception as e:
            logger.error(f"Error creating Kubernetes API client: {e}")
            raise

        return api_instance

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_node_status(self, node_name: str) -> Optional[str]:
        """
        Retrieve the status of a node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[str]: The status of the node if found, otherwise None.
        """
        return self.api_instance.read_node_status(node_name).status.conditions[-1].type

    @measure_execution_time
    def is_node_schedulable(self, node_name: str) -> Optional[bool]:
        """
        Check if a node is schedulable.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[bool]: True if the node is schedulable, False if not, and None if an error occurs.
        """
        return self.api_instance.read_node(node_name).spec.unschedulable

    @measure_execution_time
    def retrieve_all_nodes(self) -> Optional[List[Any]]:
        """
        Retrieve all nodes in the Kubernetes cluster.

        Returns:
            Optional[List[Any]]: A list of all nodes if found, otherwise None.
        """

        try:
            return self.api_instance.list_node(watch=False).items
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to retrieve all nodes list: {e}"
            )
        except PermissionError:
            logger.error("Insufficient permissions to retrieve all nodes list.")
        except Exception as e:
            logger.error(
                f"An unknown error occurred while retrieving all nodes list: {e}"
            )

        return None

    @measure_execution_time
    def retrieve_nodes_in_pool(self, nodepool_name: str) -> Optional[List[Any]]:
        """
        Retrieve all nodes in a specific node pool.

        Args:
            nodepool_name (str): The name of the node pool.

        Returns:
            Optional[List[Any]]: A list of nodes in the node pool if found, otherwise None.
        """

        try:
            all_nodes = self.retrieve_all_nodes()
            return [
                node
                for node in all_nodes
                if node.metadata.labels.get("agentpool") == nodepool_name
            ]
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to retrieve all nodes in nodepool: '{nodepool_name}': {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to retrieve all nodes in nodepool: '{nodepool_name}'."
            )
        except Exception as e:
            logger.error(
                f"An unknown error occurred while retrieving the list of nodes in nodepool: '{nodepool_name}': {e}"
            )

        return None

    @measure_execution_time
    def get_node(self, node_name: str) -> Optional[Any]:
        """
        Retrieve information about a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[Any]: Information about the node if found, otherwise None.
        """

        try:
            return self.api_instance.read_node(name=node_name)
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to retrieve node information: {e}"
            )
        except PermissionError:
            logger.error(f"Insufficient permissions to retrieve node: {node_name}")
        except Exception as e:
            logger.error(f"An unknown error occurred while retrieving node: {e}")

        return None

    @measure_execution_time
    def remove_node(self, node_name: str) -> bool:
        """
        Remove a node from the Kubernetes cluster.

        Args:
            node_name (str): The name of the node.

        Returns:
            bool: True if the node was removed successfully, otherwise False.
        """

        try:
            self.api_instance.delete_node(name=node_name)
            return True
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to remove node: '{node_name}': {e}"
            )
        except PermissionError:
            logger.error(f"Insufficient permissions to remove node: {node_name}")
        except Exception as e:
            logger.error(f"An unknown error occurred while removing node: {e}")

        return False

    @measure_execution_time
    def disable_node_scheduling(self, node_name: str) -> Optional[bool]:
        """
        Disable scheduling for a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[bool]: True if the node was cordoned successfully, otherwise False.
        """

        try:
            body = {"spec": {"unschedulable": True}}
            self.api_instance.patch_node(node_name, body)
            return True
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to cordon the node: {e}"
            )
        except PermissionError:
            logger.error(f"Insufficient permissions to cordon the node: {node_name}")
        except Exception as e:
            logger.error(f"An unknown error occurred while cordoning the node: {e}")

        return None

    @measure_execution_time
    def enable_node_scheduling(self, node_name: str) -> Optional[bool]:
        """
        Enable scheduling for a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[bool]: True if the node was uncordoned successfully, otherwise False.
        """

        try:
            body = {"spec": {"unschedulable": False}}
            self.api_instance.patch_node(node_name, body)
            return True
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to uncordon the node: {e}"
            )
        except PermissionError:
            logger.error(f"Insufficient permissions to uncordon the node: {node_name}")
        except Exception as e:
            logger.error(f"An unknown error occurred while uncordoning the node: {e}")

        return None

    @measure_execution_time
    def apply_taint_to_node(self, node_name: str, taints: dict) -> Optional[bool]:
        """
        Apply a taint to a node.

        Args:
            node_name (str): The name of the node.
            taints (list): The taints to apply.

        Returns:
            Optional[bool]: True if the taint was applied successfully, otherwise False.
        """

        try:
            body = {"spec": {"taints": [taints]}}
            self.api_instance.patch_node(node_name, body)
            logger.info(f"Successfully applied taints to node: {node_name}")
            return True
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to apply taints on node: {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to apply taints on node: {node_name}"
            )
        except Exception as e:
            logger.error(f"An unknown error occurred while tainting the node: {e}")

        return None

    @measure_execution_time
    def apply_label_to_node(self, node_name: str, labels: dict) -> Optional[bool]:
        """
        Apply a label to a node.

        Args:
            node_name (str): The name of the node.
            labels (list): The labels to apply.

        Returns:
            Optional[bool]: True if the label was applied successfully, otherwise False.
        """

        try:
            body = {"metadata": {"labels": labels}}
            self.api_instance.patch_node(node_name, body)
            logger.info(f"Successfully applied labels to node: {node_name}")
            return True
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to apply labels on node: {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to apply labels on node: {node_name}"
            )
        except Exception as e:
            logger.error(f"An unknown error occurred while labeling the node: {e}")

        return None

    @measure_execution_time
    def remove_label_from_node(self, node_name: str, label_key: str) -> bool:
        """
        Remove a label from the specified node.

        Args:
            node_name (str): The name of the node.
            label_key (str): The key of the label to remove.

        Returns:
            bool: True if successful, False otherwise.

        Raises:
            ResourceNotFoundError: If the node is not found.
            HttpResponseError: If there is an HTTP response error while removing the label.
            AzureError: If there is an error removing the label with Azure.
        """
        try:
            current_labels = self.get_node(node_name).metadata.labels
            if label_key not in current_labels:
                logger.warning(f"Label '{label_key}' not found in node '{node_name}'.")
                return True
            del current_labels[label_key]
            print(current_labels)
            body = {"metadata": {"labels": current_labels}}
            self.api_instance.patch_node(node_name, body)
            logger.info(f"Label '{label_key}' removed from node '{node_name}'.")
            return True
        except Exception as e:
            logger.error(f"An error occurred while removing label from node: {str(e)}")
            return False

    @measure_execution_time
    def list_pods(self, pod_namespace: str = "default") -> List[client.V1Pod]:
        try:
            return self.api_instance.list_namespaced_pod(pod_namespace)
        except client.ApiException as e:
            logger.error(f"An API error occurred when attempting to list pods: {e}")
        except PermissionError:
            logger.error(
                f"Insufficient permissions to list pods in namespace: {pod_namespace}"
            )
        except Exception as e:
            logger.error(f"An unknown error occurred while listing pods: {e}")

    @measure_execution_time
    def list_pods_in_node(self, node_name: str) -> List[client.V1Pod]:
        try:
            return self.api_instance.list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={node_name}"
            )
        except client.ApiException as e:
            logger.error(f"An API error occurred when attempting to list pods: {e}")
        except PermissionError:
            logger.error(f"Insufficient permissions to list pods in node: {node_name}")
        except Exception as e:
            logger.error(f"An unknown error occurred while listing pods: {e}")

    @measure_execution_time
    def list_pods_in_nodepool(self, nodepool_name: str) -> List[client.V1Pod]:
        try:
            return self.api_instance.list_pod_for_all_namespaces(
                label_selector=f"agentpool={nodepool_name}"
            )
        except client.ApiException as e:
            logger.error(f"An API error occurred when attempting to list pods: {e}")
        except PermissionError:
            logger.error(
                f"Insufficient permissions to list pods in nodepool: {nodepool_name}"
            )
        except Exception as e:
            logger.error(f"An unknown error occurred while listing pods: {e}")

    @measure_execution_time
    def list_pods_in_cluster(self) -> List[client.V1Pod]:
        try:
            return self.api_instance.list_pod_for_all_namespaces()
        except client.ApiException as e:
            logger.error(f"An API error occurred when attempting to list pods: {e}")
        except PermissionError:
            logger.error(f"Insufficient permissions to list pods in cluster")
        except Exception as e:
            logger.error(f"An unknown error occurred while listing pods: {e}")

    @measure_execution_time
    def get_pod(self, pod_name: str, pod_namespace: str = "default") -> client.V1Pod:
        try:
            return self.api_instance.read_namespaced_pod(pod_name, pod_namespace)
        except client.ApiException as e:
            logger.error(f"An API error occurred when attempting to get pod: {e}")
        except PermissionError:
            logger.error(f"Insufficient permissions to get pod: {pod_name}")
        except Exception as e:
            logger.error(f"An unknown error occurred while getting pod: {e}")

    @measure_execution_time
    def get_deployment(
        self, deployment_name: str, deployment_namespace: str = "default"
    ) -> client.V1Deployment:
        try:
            return self.api_instance.read_namespaced_deployment(
                deployment_name, deployment_namespace
            )
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to get deployment: {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to get deployment: {deployment_name}"
            )
        except Exception as e:
            logger.error(f"An unknown error occurred while getting deployment: {e}")

    @measure_execution_time
    def get_pod_resource_quota(
        self, pod_name: str, pod_namespace: str = "default"
    ) -> List[dict]:
        try:
            return self.api_instance.list_namespaced_resource_quota(
                pod_namespace, field_selector=f"metadata.name={pod_name}"
            )
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to get pod resource quota: {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to get pod resource quota: {pod_name}"
            )
        except Exception as e:
            logger.error(
                f"An unknown error occurred while getting pod resource quota: {e}"
            )

    @measure_execution_time
    def get_resource_quota_for_node(self, node_name: str) -> List[dict]:
        try:
            return self.api_instance.list_resource_quota_for_all_namespaces(
                field_selector=f"metadata.name={node_name}"
            )
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to get resource quota for node: {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to get resource quota for node: {node_name}"
            )
        except Exception as e:
            logger.error(
                f"An unknown error occurred while getting resource quota for node: {e}"
            )

    @measure_execution_time
    def get_deployment_pdb(
        self, deployment_name: str, deployment_namespace: str = "default"
    ) -> client.V1PodDisruptionBudget:
        try:
            return self.api_instance.read_namespaced_pod_disruption_budget(
                deployment_name, deployment_namespace
            )
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to get pod disruption budget: {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to get pod disruption budget: {deployment_name}"
            )
        except Exception as e:
            logger.error(
                f"An unknown error occurred while getting pod disruption budget: {e}"
            )

    @measure_execution_time
    def update_deployment_pdb(
        self,
        deployment_name: str,
        deployment_namespace: str = "default",
        min_available: int = 1,
    ) -> bool:
        try:
            pdb = self.api_instance.read_namespaced_pod_disruption_budget(
                deployment_name, deployment_namespace
            )
            pdb.spec.min_available = min_available
            self.api_instance.replace_namespaced_pod_disruption_budget(
                deployment_name, deployment_namespace, pdb
            )
            logger.info(
                f"Pod disruption budget for deployment '{deployment_name}' updated to '{min_available}'"
            )
            return True
        except client.ApiException as e:
            logger.error(
                f"An API error occurred when attempting to update pod disruption budget for deployment: {e}"
            )
        except PermissionError:
            logger.error(
                f"Insufficient permissions to update pod disruption budget for deployment: {deployment_name}"
            )
        except Exception as e:
            logger.error(
                f"An unknown error occurred while updating pod disruption budget for deployment: {e}"
            )
        return False

    @measure_execution_time
    def evict_workload_from_node(
        self, node_name: str, workload_name: str, workload_namespace: str = "default"
    ) -> bool:
        try:
            self.api_instance.delete_collection_namespaced_pod(
                workload_namespace,
                field_selector=f"spec.nodeName={node_name},metadata.name={workload_name}",
                propagation_policy="Foreground",
            )
            logger.info(f"Workload '{workload_name}' evicted from node '{node_name}'.")
            return True
        except Exception as e:
            logger.error(f"An error occurred while evicting workload: {str(e)}")
            return False

    @measure_execution_time
    def evict_workload_from_namespace(
        self, workload_name: str, workload_namespace: str = "default"
    ) -> bool:
        try:
            self.api_instance.delete_collection_namespaced_pod(
                workload_namespace,
                field_selector=f"metadata.name={workload_name}",
                propagation_policy="Foreground",
            )
            logger.info(
                f"Workload '{workload_name}' evicted from namespace '{workload_namespace}'."
            )
            return True
        except Exception as e:
            logger.error(f"An error occurred while evicting workload: {str(e)}")
            return False

    def evict_pod(
        self,
        pod_name: str,
        pod_namespace: str = "default",
        termination_period: int = 30,
    ) -> bool:
        try:
            self.api_instance.delete_namespaced_pod(
                pod_name,
                pod_namespace,
                body=client.V1DeleteOptions(
                    propagation_policy="Foreground",
                    grace_period_seconds=termination_period,
                ),
            )
            logger.info(f"Pod '{pod_name}' evicted from namespace '{pod_namespace}'.")
            return True
        except Exception as e:
            logger.error(f"An error occurred while evicting pod: {str(e)}")
            return False


if __name__ == "__main__":
    manager = NodeManager()
    # manager.apply_label_to_node("aks-systempool01-42694388-vmss00000c", {"foo": "bar"})
    # manager.apply_taint_to_node(
    #     "aks-systempool01-42694388-vmss00000c",
    #     {
    #         "effect": "PreferNoSchedule",
    #         "key": "test",
    #         "value": "code",
    #     },
    # )
    # manager.remove_label_from_node("aks-systempool01-42694388-vmss00000c", "foo")
    # manager.evict_pod("backend-5cd8bcd564-9t6rl", "default", 5)
    manager.retrieve_node_status("aks-systempool01-42694388-vmss00000c")
