
import time
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

app = FastAPI(title="MicroGuardian Sample Application")

REQUEST_COUNT = Counter(
    "app_requests_total",
    "Total number of application requests",
    ["method", "endpoint"]
)

REQUEST_LATENCY = Histogram(
    "app_request_duration_seconds",
    "Application request processing duration"
)


@app.middleware("http")
async def monitor_requests(request, call_next):
    start = time.time()

    response = await call_next(request)

    duration = time.time() - start

    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path
    ).inc()

    REQUEST_LATENCY.observe(duration)

    return response


@app.get("/")
def home():
    return {"message": "MicroGuardian application is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/work")
def work():
    time.sleep(0.2)
    return {"message": "Work completed"}


@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
