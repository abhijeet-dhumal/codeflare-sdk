import os
import random
import string, yaml
import subprocess
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from minio import Minio


dir_path = os.path.dirname(os.path.realpath(__file__))


def get_ray_image():
    default_ray_image = "quay.io/rhoai/ray:2.23.0-py39-cu121"
    return os.getenv("RAY_IMAGE", default_ray_image)


def get_disconnected_setup_env_variables():
    env_vars = dict()
    # Use specified pip index url instead of default(https://pypi.org/simple) if related environment variables exists
    if "PIP_INDEX_URL" in os.environ:
        env_vars["PIP_INDEX_URL"] = os.environ.get("PIP_INDEX_URL")
        env_vars["PIP_TRUSTED_HOST"] = os.environ.get("PIP_TRUSTED_HOST")

    # Use specified storage bucket to download datasets from
    if "AWS_DEFAULT_ENDPOINT" in os.environ:
        env_vars["AWS_DEFAULT_ENDPOINT"] = os.environ.get("AWS_DEFAULT_ENDPOINT")
        env_vars["AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID")
        env_vars["AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY")
        env_vars["AWS_STORAGE_BUCKET"] = os.environ.get("AWS_STORAGE_BUCKET")
    return env_vars


def random_choice():
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choices(alphabet, k=5))


def create_namespace(self):
    self.namespace = f"test-ns-{random_choice()}"
    namespace_body = client.V1Namespace(
        metadata=client.V1ObjectMeta(name=self.namespace)
    )
    self.api_instance.create_namespace(namespace_body)


def create_new_resource_flavor(self):
    self.resource_flavor = f"test-resource-flavor-{random_choice()}"
    create_resource_flavor(self, self.resource_flavor)


def create_new_cluster_queue(self):
    self.cluster_queue = f"test-cluster-queue-{random_choice()}"
    create_cluster_queue(self, self.cluster_queue, self.resource_flavor)


def create_new_local_queue(self):
    self.local_queue = f"test-local-queue-{random_choice()}"
    create_local_queue(self, self.cluster_queue, self.local_queue)


def create_namespace_with_name(self, namespace_name):
    self.namespace = namespace_name
    try:
        namespace_body = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=self.namespace)
        )
        self.api_instance.create_namespace(namespace_body)
    except Exception as e:
        raise RuntimeError(e)


def delete_namespace(self):
    if hasattr(self, "namespace"):
        self.api_instance.delete_namespace(self.namespace)


def initialize_kubernetes_client(self):
    config.load_kube_config()
    # Initialize Kubernetes client
    self.api_instance = client.CoreV1Api()
    self.custom_api = client.CustomObjectsApi(self.api_instance.api_client)


def run_oc_command(args):
    try:
        result = subprocess.run(
            ["oc"] + args, capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing 'oc {' '.join(args)}': {e}")
        return None


def create_cluster_queue(self, cluster_queue, flavor):
    cluster_queue_json = {
        "apiVersion": "kueue.x-k8s.io/v1beta1",
        "kind": "ClusterQueue",
        "metadata": {"name": cluster_queue},
        "spec": {
            "namespaceSelector": {},
            "resourceGroups": [
                {
                    "coveredResources": ["cpu", "memory", "nvidia.com/gpu"],
                    "flavors": [
                        {
                            "name": flavor,
                            "resources": [
                                {"name": "cpu", "nominalQuota": 9},
                                {"name": "memory", "nominalQuota": "36Gi"},
                                {"name": "nvidia.com/gpu", "nominalQuota": 0},
                            ],
                        }
                    ],
                }
            ],
        },
    }

    try:
        # Check if cluster-queue exists
        self.custom_api.get_cluster_custom_object(
            group="kueue.x-k8s.io",
            plural="clusterqueues",
            version="v1beta1",
            name=cluster_queue,
        )
        print(f"'{cluster_queue}' already exists")
    except:
        # create cluster-queue
        self.custom_api.create_cluster_custom_object(
            group="kueue.x-k8s.io",
            plural="clusterqueues",
            version="v1beta1",
            body=cluster_queue_json,
        )
        print(f"'{cluster_queue}' created")

    self.cluster_queue = cluster_queue


def create_resource_flavor(self, flavor):
    resource_flavor_json = {
        "apiVersion": "kueue.x-k8s.io/v1beta1",
        "kind": "ResourceFlavor",
        "metadata": {"name": flavor},
    }

    try:
        # Check if resource flavor exists
        self.custom_api.get_cluster_custom_object(
            group="kueue.x-k8s.io",
            plural="resourceflavors",
            version="v1beta1",
            name=flavor,
        )
        print(f"'{flavor}' already exists")
    except:
        # create kueue resource flavor
        self.custom_api.create_cluster_custom_object(
            group="kueue.x-k8s.io",
            plural="resourceflavors",
            version="v1beta1",
            body=resource_flavor_json,
        )
        print(f"'{flavor}' created!")

    self.resource_flavor = flavor


def create_local_queue(self, cluster_queue, local_queue):
    local_queue_json = {
        "apiVersion": "kueue.x-k8s.io/v1beta1",
        "kind": "LocalQueue",
        "metadata": {
            "namespace": self.namespace,
            "name": local_queue,
            "annotations": {"kueue.x-k8s.io/default-queue": "true"},
        },
        "spec": {"clusterQueue": cluster_queue},
    }

    try:
        # Check if local-queue exists in given namespace
        self.custom_api.get_namespaced_custom_object(
            group="kueue.x-k8s.io",
            namespace=self.namespace,
            plural="localqueues",
            version="v1beta1",
            name=local_queue,
        )
        print(f"'{local_queue}' already exists in namespace '{self.namespace}'")
    except:
        # create local-queue
        self.custom_api.create_namespaced_custom_object(
            group="kueue.x-k8s.io",
            namespace=self.namespace,
            plural="localqueues",
            version="v1beta1",
            body=local_queue_json,
        )
        print(f"'{local_queue}' created in namespace '{self.namespace}'")

    self.local_queue = local_queue


def create_kueue_resources(self):
    print("creating Kueue resources ...")
    create_new_resource_flavor(self)
    create_new_cluster_queue(self)
    create_new_local_queue(self)


def delete_kueue_resources(self):
    # Delete if given cluster-queue exists
    try:
        self.custom_api.delete_cluster_custom_object(
            group="kueue.x-k8s.io",
            plural="clusterqueues",
            version="v1beta1",
            name=self.cluster_queue,
        )
        print(f"\n'{self.cluster_queue}' cluster-queue deleted")
    except Exception as e:
        print(f"\nError deleting cluster-queue '{self.cluster_queue}' : {e}")

    # Delete if given resource-flavor exists
    try:
        self.custom_api.delete_cluster_custom_object(
            group="kueue.x-k8s.io",
            plural="resourceflavors",
            version="v1beta1",
            name=self.resource_flavor,
        )
        print(f"'{self.resource_flavor}' resource-flavor deleted")
    except Exception as e:
        print(f"\nError deleting resource-flavor '{self.resource_flavor}' : {e}")


def download_all_from_storage_bucket(
    endpoint, access_key, secret_key, bucket_name, dir_path=dir_path
):
    client = Minio(
        endpoint, access_key=access_key, secret_key=secret_key, cert_check=False
    )

    # for bucket in client.list_buckets():
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    else:
        print(f"Directory '{dir_path}' already exists")
    # download all files from storage bucket recursively
    for item in client.list_objects(bucket_name, recursive=True):
        dataset_file_path = os.path.join(dir_path, item.object_name)
        if not os.path.exists(dataset_file_path):
            client.fget_object(bucket_name, item.object_name, dataset_file_path)
        else:
            print(f"File-path '{dataset_file_path}' already exists")


# For example : Can be used for minio storage instance deployment using minio-deployment.yaml file
def apply_yaml_deployment(yaml_file_path):
    # Load the OpenShift/Kubernetes configuration
    config.load_kube_config()

    with open(yaml_file_path, "r") as file:
        yaml_data = yaml.safe_load_all(file)

        k8s_client = client.ApiClient()
        for data in yaml_data:
            try:
                kind = data["kind"]
                namespace = data["metadata"].get("namespace", "default")

                # Apply the resource based on its kind
                if kind == "Deployment":
                    apps_v1_api = client.AppsV1Api(k8s_client)
                    apps_v1_api.create_namespaced_deployment(
                        namespace=namespace, body=data
                    )
                elif kind == "Service":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.create_namespaced_service(
                        namespace=namespace, body=data
                    )
                elif kind == "PersistentVolumeClaim":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.create_namespaced_persistent_volume_claim(
                        namespace=namespace, body=data
                    )
                elif kind == "Secret":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.create_namespaced_secret(namespace=namespace, body=data)
                elif kind == "ConfigMap":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.create_namespaced_config_map(
                        namespace=namespace, body=data
                    )
                elif kind == "Route":
                    # OpenShift-specific Route creation
                    route_v1_api = client.CustomObjectsApi(k8s_client)
                    group = "route.openshift.io"
                    version = "v1"
                    plural = "routes"
                    route_v1_api.create_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        body=data,
                    )
                else:
                    print(f"Unsupported resource kind: {kind}")
            except ApiException as e:
                print(f"Exception when creating resource {kind}: {e}\n")


def delete_yaml_deployment(yaml_file_path):
    # Load the OpenShift/Kubernetes configuration
    config.load_kube_config()

    with open(yaml_file_path, "r") as file:
        yaml_data = yaml.safe_load_all(file)

        k8s_client = client.ApiClient()
        for data in yaml_data:
            try:
                kind = data["kind"]
                name = data["metadata"]["name"]
                namespace = data["metadata"].get("namespace", "default")

                # Delete the resource based on its kind
                if kind == "Deployment":
                    apps_v1_api = client.AppsV1Api(k8s_client)
                    apps_v1_api.delete_namespaced_deployment(
                        name=name, namespace=namespace
                    )
                elif kind == "Service":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.delete_namespaced_service(
                        name=name, namespace=namespace
                    )
                elif kind == "PersistentVolumeClaim":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.delete_namespaced_persistent_volume_claim(
                        name=name, namespace=namespace
                    )
                elif kind == "Secret":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.delete_namespaced_secret(name=name, namespace=namespace)
                elif kind == "ConfigMap":
                    core_v1_api = client.CoreV1Api(k8s_client)
                    core_v1_api.delete_namespaced_config_map(
                        name=name, namespace=namespace
                    )
                elif kind == "Route":
                    # OpenShift-specific Route deletion
                    route_v1_api = client.CustomObjectsApi(k8s_client)
                    group = "route.openshift.io"
                    version = "v1"
                    plural = "routes"
                    route_v1_api.delete_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        name=name,
                    )
                else:
                    print(f"Unsupported resource kind: {kind}")
            except ApiException as e:
                print(f"Exception when deleting resource {kind}: {e}\n")
