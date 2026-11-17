import requests


PROMETHEUS_URL = "http://localhost:9090"


def get_metric(query):
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=5
        )

        response.raise_for_status()
        data = response.json()

        if data["status"] != "success":
            print("Prometheus query failed")
            return []

        return data["data"]["result"]

    except requests.RequestException as error:
        print("Prometheus connection error:", error)
        return []


def check_application():
    results = get_metric('up{job="microguardian-app"}')

    if not results:
        print("No application metrics found")
        return

    for result in results:
        print("Target:", result["metric"]["instance"])
        print("Status:", result["value"][1])


def get_request_rate():
    query = 'sum(rate(app_requests_total[1m]))'
    results = get_metric(query)

    if results:
        rate = float(results[0]["value"][1])
        print("Application request rate:", round(rate, 2), "requests/sec")
        return rate

    print("No request-rate data available")
    return 0.0


if __name__ == "__main__":
    print("Checking Prometheus connection...")
    check_application()

    print("\nChecking application traffic...")
    get_request_rate()