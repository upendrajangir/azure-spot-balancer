# Standard library imports
import logging
from functools import wraps
from time import time
from typing import List, Callable, Optional, Any

# Third party imports
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
            # logger.info(
            #     f"Execution time for {function.__name__}: {end_time - start_time} seconds"
            # )
            return result

        return time_wrapper

    @staticmethod
    def handle_exceptions(function):
        @wraps(function)
        def exception_wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except client.ApiException as e:
                if e.status >= 500:
                    logger.warning(
                        f"Recoverable API error when attempting to {function.__name__}: {e}"
                    )
                    raise
                else:
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
        node_status = (
            self.api_instance.read_node_status(node_name).status.conditions[-1].type
        )
        if node_status is not None:
            logger.info(
                f"Successfully retrieved status of node '{node_name}': {node_status}"
            )
        else:
            logger.warning(f"No status found for node '{node_name}'")
        return node_status

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def is_node_schedulable(self, node_name: str) -> Optional[bool]:
        """
        Check if a node is schedulable.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[bool]: True if the node is schedulable, False if not, and None if an error occurs.
        """
        schedulability = self.api_instance.read_node(node_name).spec.unschedulable
        if schedulability is None:
            logger.info(f"Node '{node_name}' is schedulable.")
        else:
            logger.info(f"Node '{node_name}' is not schedulable.")
            return False
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_all_nodes(self) -> Optional[List[Any]]:
        """
        Retrieve all nodes in the Kubernetes cluster.

        Returns:
            Optional[List[Any]]: A list of all nodes if found, otherwise None.
        """
        nodes = self.api_instance.list_node(watch=False).items
        if nodes:
            logger.info(f"Successfully retrieved {len(nodes)} node(s) in the cluster.")
        else:
            logger.warning("No nodes found in the cluster.")
        return nodes

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_nodes_in_pool(self, node_pool_name: str) -> Optional[List[Any]]:
        """
        Retrieve all nodes in a specific node pool.

        Args:
            node_pool_name (str): The name of the node pool.

        Returns:
            Optional[List[Any]]: A list of nodes in the node pool if found, otherwise None.
        """
        all_nodes = self.retrieve_all_nodes()

        # filter nodes by node_pool name
        nodes_in_pool = [
            node
            for node in all_nodes
            if node.metadata.labels.get("agentpool") == node_pool_name
        ]

        if nodes_in_pool:
            logger.info(
                f"Successfully retrieved {len(nodes_in_pool)} node(s) in the '{node_pool_name}' node pool."
            )
        else:
            logger.warning(f"No nodes found in the '{node_pool_name}' node pool.")

        return nodes_in_pool

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def get_node(self, node_name: str) -> Optional[Any]:
        """
        Retrieve information about a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[Any]: Information about the node if found, otherwise None.
        """
        node_info = self.api_instance.read_node(node_name)

        if node_info:
            logger.info(f"Successfully retrieved information for node '{node_name}'.")
        else:
            logger.info(f"No information found for node '{node_name}'.")

        return node_info

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def remove_node(self, node_name: str) -> bool:
        """
        Remove a node from the Kubernetes cluster.

        Args:
            node_name (str): The name of the node.

        Returns:
            bool: True if the node was removed successfully, otherwise False.
        """
        self.api_instance.delete_node(name=node_name)
        logger.info(
            f"Node '{node_name}' was successfully removed from the Kubernetes cluster."
        )
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def disable_node_scheduling(self, node_name: str) -> Optional[bool]:
        """
        Disable scheduling for a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[bool]: True if the node was cordoned successfully, otherwise False.
        """
        self.api_instance.patch_node(node_name, {"spec": {"unschedulable": True}})
        logger.info(f"Scheduling was successfully disabled for node '{node_name}'.")
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def enable_node_scheduling(self, node_name: str) -> Optional[bool]:
        """
        Enable scheduling for a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            Optional[bool]: True if the node was uncordoned successfully, otherwise False.
        """
        self.api_instance.patch_node(node_name, {"spec": {"unschedulable": False}})
        logger.info(f"Scheduling was successfully enabled for node '{node_name}'.")
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def apply_taint_to_node(
        self, node_name: str, taints: dict, taint_effect: str
    ) -> Optional[bool]:
        """
        Apply a taint to a node.

        Args:
            node_name (str): The name of the node.
            taints (dict): The taints to apply.

        Returns:
            Optional[bool]: True if the taint was applied successfully, otherwise False.
        """
        response = self.api_instance.patch_node(
            node_name,
            {
                "spec": {
                    "taints": [
                        {
                            "effect": f"{taint_effect}",
                            "key": f"{list(taints.keys())[0]}",
                            "value": f"{list(taints.values())[0]}",
                        }
                    ]
                }
            },
        )
        logger.info(
            f"Taints '{taints}' were successfully applied to node '{node_name}'."
        )
        return response is not None

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def remove_taint_from_node(self, node_name: str, taint_key: str) -> bool:
        """
        Remove a specific taint from a node.

        Args:
            node_name (str): The name of the node.
            taint_key (str): The key of the taint to remove.

        Returns:
            bool: True if the taint was removed successfully, otherwise False.
        """
        current_taints = self.api_instance.read_node(node_name).spec.taints
        filtered_taints = list(filter(lambda x: x.key != taint_key, current_taints))
        self.api_instance.patch_node(
            node_name,
            {"spec": {"taints": filtered_taints}},
        )
        logger.info(
            f"Taint '{taint_key}' was successfully removed from node '{node_name}'."
        )
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def apply_node_label(self, node_name: str, labels: dict) -> Optional[bool]:
        """
        Apply a label to a node.

        Args:
            node_name (str): The name of the node.
            labels (dict): The labels to apply.

        Returns:
            Optional[bool]: True if the label was applied successfully, otherwise False.
        """
        response = self.api_instance.patch_node(
            node_name, {"metadata": {"labels": labels}}
        )
        logger.info(
            f"Labels '{labels}' were successfully applied to node '{node_name}'."
        )
        return response is not None

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def remove_node_label(self, node_name: str, label_key: str) -> bool:
        """
        Remove a specific label from the specified node.

        Args:
            node_name (str): The name of the node.
            label_key (str): The key of the label to remove.

        Returns:
            bool: True if the removal operation is successful, False otherwise.

        Raises:
            ResourceNotFoundError: If the node is not found.
            HttpResponseError: If there is an HTTP response error while removing the label.
            AzureError: If there is an error removing the label with Azure.
        """
        current_node = self.get_node(node_name)
        current_labels = current_node.metadata.labels

        if label_key not in current_labels:
            logger.warning(f"Label '{label_key}' not found on node '{node_name}'.")
            return True

        current_labels[label_key] = None
        operation_successful = self.apply_node_label(node_name, current_labels)

        if operation_successful:
            logger.info(
                f"Label '{label_key}' was successfully removed from node '{node_name}'."
            )

        return operation_successful is not None

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def list_all_pods_in_namespace(
        self, namespace: str = "default"
    ) -> List[client.V1Pod]:
        """
        Retrieve a list of all pods within a specified namespace in the Kubernetes cluster.

        Args:
            namespace (str, optional): The namespace within which to list the pods. Defaults to 'default'.

        Returns:
            List[client.V1Pod]: A list of V1Pod objects representing the pods within the specified namespace.
        """
        pod_list = self.api_instance.list_namespaced_pod(namespace)
        logger.info(f"Successfully listed all pods in namespace '{namespace}'")
        return pod_list

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def list_pods_in_specific_node(self, node_name: str) -> List[client.V1Pod]:
        """
        Retrieve all pods running on a specific node.

        Args:
            node_name (str): The name of the node.

        Returns:
            List[client.V1Pod]: A list of V1Pod objects representing the pods running on the specified node.
        """
        pod_list = self.api_instance.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name}"
        )
        logger.info(f"Successfully listed all pods in node '{node_name}'")
        return pod_list

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def list_pods_in_specific_node_pool(
        self, node_pool_name: str
    ) -> List[client.V1Pod]:
        """
        Retrieve all pods running in a specific node pool.

        Args:
            node_pool_name (str): The name of the node pool.

        Returns:
            List[client.V1Pod]: A list of V1Pod objects representing the pods running in the specified node pool.
        """
        pod_list = self.api_instance.list_pod_for_all_namespaces(
            label_selector=f"agentpool={node_pool_name}"
        )
        logger.info(f"Successfully listed all pods in node pool '{node_pool_name}'")
        return pod_list

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def list_all_pods_in_cluster(self) -> List[client.V1Pod]:
        """
        Retrieve all pods in the Kubernetes cluster.

        Returns:
            List[client.V1Pod]: A list of V1Pod objects representing all the pods in the cluster.
        """
        pod_list = self.api_instance.list_pod_for_all_namespaces()
        logger.info("Successfully retrieved all pods in the cluster.")
        return pod_list

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_pod(
        self, pod_name: str, pod_namespace: str = "default"
    ) -> client.V1Pod:
        """
        Retrieve a specific pod in the Kubernetes cluster.

        Args:
            pod_name (str): The name of the pod to retrieve.
            pod_namespace (str, optional): The namespace of the pod. Defaults to "default".

        Returns:
            client.V1Pod: A V1Pod object representing the specified pod.
        """
        pod_info = self.api_instance.read_namespaced_pod(pod_name, pod_namespace)
        logger.info(
            f"Successfully retrieved pod '{pod_name}' in namespace '{pod_namespace}'."
        )
        return pod_info

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_deployment(
        self, deployment_name: str, deployment_namespace: str = "default"
    ) -> client.V1Deployment:
        """
        Retrieve a specific deployment in the Kubernetes cluster.

        Args:
            deployment_name (str): The name of the deployment to retrieve.
            deployment_namespace (str, optional): The namespace of the deployment. Defaults to "default".

        Returns:
            client.V1Deployment: A V1Deployment object representing the specified deployment.
        """
        deployment_info = self.api_instance.read_namespaced_deployment(
            deployment_name, deployment_namespace
        )
        logger.info(
            f"Successfully retrieved deployment '{deployment_name}' in namespace '{deployment_namespace}'."
        )
        return deployment_info

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_pod_resource_quota(
        self, pod_name: str, pod_namespace: str = "default"
    ) -> List[dict]:
        """
        Retrieve the resource quota for a specific pod in the Kubernetes cluster.

        Args:
            pod_name (str): The name of the pod.
            pod_namespace (str, optional): The namespace of the pod. Defaults to "default".

        Returns:
            List[dict]: The resource quota for the specified pod.
        """
        pod_resource_quota = self.api_instance.list_namespaced_resource_quota(
            pod_namespace, field_selector=f"metadata.name={pod_name}"
        )
        logger.info(
            f"Successfully retrieved resource quota for pod '{pod_name}' in namespace '{pod_namespace}'."
        )
        return pod_resource_quota

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_node_resource_quota(self, node_name: str) -> List[dict]:
        """
        Retrieve the resource quota for a specific node in the Kubernetes cluster.

        Args:
            node_name (str): The name of the node.

        Returns:
            List[dict]: The resource quota for the specified node.
        """
        node_resource_quota = self.api_instance.list_node_field_selector(
            field_selector=f"metadata.name={node_name}"
        )
        logger.info(f"Successfully retrieved resource quota for node '{node_name}'.")
        return node_resource_quota

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def retrieve_deployment_pdb(
        self, deployment_name: str, deployment_namespace: str = "default"
    ) -> client.V1PodDisruptionBudget:
        """
        Retrieve the Pod Disruption Budget (PDB) for a specific deployment in the Kubernetes cluster.

        Args:
            deployment_name (str): The name of the deployment.
            deployment_namespace (str, optional): The namespace of the deployment. Defaults to "default".

        Returns:
            client.V1PodDisruptionBudget: The PDB for the specified deployment.
        """
        pdb = self.api_instance.read_namespaced_pod_disruption_budget(
            deployment_name, deployment_namespace
        )
        logger.info(
            f"Successfully retrieved Pod Disruption Budget for deployment '{deployment_name}' in namespace '{deployment_namespace}'."
        )
        return pdb

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def update_deployment_pdb(
        self,
        deployment_name: str,
        deployment_namespace: str = "default",
        min_available: int = 1,
    ) -> bool:
        """
        Update the Pod Disruption Budget (PDB) for a specific deployment in the Kubernetes cluster.

        Args:
            deployment_name (str): The name of the deployment.
            deployment_namespace (str, optional): The namespace of the deployment. Defaults to "default".
            min_available (int, optional): The minimum number of pods that should be available during a voluntary disruption. Defaults to 1.

        Returns:
            bool: True if the PDB was updated successfully, False otherwise.
        """
        current_pdb = self.retrieve_deployment_pdb(
            deployment_name, deployment_namespace
        )
        current_pdb.spec.min_available = min_available
        updated_pdb = self.api_instance.replace_namespaced_pod_disruption_budget(
            deployment_name, deployment_namespace, current_pdb
        )
        if updated_pdb:
            logger.info(
                f"Successfully updated Pod Disruption Budget for deployment '{deployment_name}' in namespace '{deployment_namespace}' with min_available '{min_available}'."
            )
            return True
        else:
            return False

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def evict_all_pods_from_node(
        self, node_name: str, termination_period: int = 30
    ) -> bool:
        """
        Evict all pods from a specific node in the Kubernetes cluster.

        Args:
            node_name (str): The name of the node from which to evict pods.
            termination_period (int, optional): The period (in seconds) to wait before forcefully terminating the pod.
                                                Defaults to 30.

        Returns:
            bool: True if all pods are successfully evicted, False otherwise.
        """
        pods_list = self.list_pods_in_node(node_name)
        for pod in pods_list.items:
            eviction_result = self.evict_pod(
                pod.metadata.name,
                pod.metadata.namespace,
                termination_period=termination_period,
            )
            if not eviction_result:
                logger.error(
                    f"Failed to evict pod '{pod.metadata.name}' in namespace '{pod.metadata.namespace}' from node '{node_name}'."
                )
                return False
        logger.info(f"All pods have been successfully evicted from node '{node_name}'.")
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def evict_all_pods_from_namespace(
        self, namespace: str = "default", termination_period: int = 30
    ) -> bool:
        """
        Evict all pods from a specific namespace in the Kubernetes cluster.

        Args:
            namespace (str): The name of the namespace from which to evict pods. Default is "default".
            termination_period (int): The period (in seconds) to wait before forcefully terminating the pod.
                                    Default is 30 seconds.

        Returns:
            bool: True if all pods are successfully evicted, False otherwise.
        """
        pod_list = self.list_all_pods_in_namespace(namespace)

        for pod in pod_list.items:
            if not self.evict_pod(
                pod.metadata.name,
                namespace,
                termination_period=termination_period,
            ):
                logger.error(
                    f"Failed to evict pod '{pod.metadata.name}' in namespace '{namespace}'."
                )
                return False

        logger.info(
            f"All pods from namespace '{namespace}' have been successfully evicted."
        )
        return True

    @measure_execution_time
    @retry_decorator
    @handle_exceptions
    def evict_pod(
        self,
        pod_name: str,
        namespace: str = "default",
        termination_period: int = 30,
    ) -> bool:
        """
        Evict a specific pod from a given namespace in the Kubernetes cluster.

        Args:
            pod_name (str): The name of the pod to evict.
            namespace (str): The namespace from which to evict the pod. Default is "default".
            termination_period (int): The period (in seconds) to wait before forcefully terminating the pod.
                                    Default is 30 seconds.

        Returns:
            bool: True if the pod is successfully evicted, False otherwise.
        """
        self.api_instance.delete_namespaced_pod(
            pod_name,
            namespace,
            body=client.V1DeleteOptions(
                propagation_policy="Foreground",
                grace_period_seconds=termination_period,
            ),
        )
        logger.info(
            f"Pod '{pod_name}' in namespace '{namespace}' has been successfully evicted."
        )
        return True
