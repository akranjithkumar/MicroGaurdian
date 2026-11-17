from kubernetes import client, config


NAMESPACE = "microguardian"
DEPLOYMENT_NAME = "microguardian-app"


def get_apps_api():
    config.load_kube_config()
    return client.AppsV1Api()


def get_current_replicas():
    apps_api = get_apps_api()

    deployment = apps_api.read_namespaced_deployment(
        name=DEPLOYMENT_NAME,
        namespace=NAMESPACE
    )

    return deployment.spec.replicas


def scale_deployment(replicas):
    apps_api = get_apps_api()

    body = {"spec": {"replicas": replicas}}

    apps_api.patch_namespaced_deployment_scale(
        name=DEPLOYMENT_NAME,
        namespace=NAMESPACE,
        body=body
    )

    print(f"Scaling {DEPLOYMENT_NAME} to {replicas} replicas")


if __name__ == "__main__":
    print("Current replicas:", get_current_replicas())

    # Testing only: increase replicas from 2 to 3
    scale_deployment(3)