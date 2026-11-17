import time

from prometheus_client import get_request_rate
from k8s_client import get_current_replicas, scale_deployment


MIN_REPLICAS = 2
MAX_REPLICAS = 5

SCALE_UP_THRESHOLD = 5.0
SCALE_DOWN_THRESHOLD = 1.0

CHECK_INTERVAL = 10


def make_scaling_decision():

    request_rate = get_request_rate()
    current_replicas = get_current_replicas()

    print("\n----- MICROGUARDIAN MONITOR -----")
    print("Request rate:", round(request_rate, 2))
    print("Current replicas:", current_replicas)

    if request_rate > SCALE_UP_THRESHOLD:

        if current_replicas < MAX_REPLICAS:
            new_replicas = current_replicas + 1

            print("Decision: SCALE UP")
            scale_deployment(new_replicas)

        else:
            print("Maximum replicas reached")

    elif request_rate < SCALE_DOWN_THRESHOLD:

        if current_replicas > MIN_REPLICAS:
            new_replicas = current_replicas - 1

            print("Decision: SCALE DOWN")
            scale_deployment(new_replicas)

        else:
            print("Minimum replicas maintained")

    else:
        print("Decision: MAINTAIN")


if __name__ == "__main__":

    print("MicroGuardian continuous monitoring started")

    while True:
        try:
            make_scaling_decision()
            time.sleep(CHECK_INTERVAL)

        except Exception as error:
            print("Controller error:", error)
            time.sleep(CHECK_INTERVAL)