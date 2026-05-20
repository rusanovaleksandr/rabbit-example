from fastapi import FastAPI
import router
import asyncio

app = FastAPI()


@app.get("/")
async def home():
    return {
        "status": "Race Events Producer API (RabbitMQ) is running",
        "available_endpoints": ["/start_race_events", "/stop_race_events", "/race_status", "/docs"],
    }


app.include_router(router.route)


@app.on_event("startup")
async def startup_event():
    print("Auto-starting race events generator...")
    try:
        await router.start_race_events()
    except Exception as e:
        print("Failed to auto-start race events:", e)


@app.on_event("shutdown")
async def shutdown_event():
    print("Stopping race events generator...")
    try:
        await router.stop_race_events()
    except Exception as e:
        print("Failed to stop race events cleanly:", e)
