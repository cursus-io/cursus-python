"""Minimal FastAPI app with Cursus producer."""

from contextlib import asynccontextmanager

from cursus import AsyncProducer, ProducerConfig, Acks

try:
    from fastapi import FastAPI
    from pydantic import BaseModel
except ImportError:
    raise SystemExit("pip install fastapi uvicorn pydantic")

producer: AsyncProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    config = ProducerConfig(
        brokers=["localhost:9000"],
        topic="api-events",
        partitions=4,
        acks=Acks.ONE,
    )
    producer = AsyncProducer(config)
    await producer.start()
    yield
    await producer.close()


app = FastAPI(lifespan=lifespan)


class PublishRequest(BaseModel):
    message: str
    key: str = ""


@app.post("/publish")
async def publish(req: PublishRequest):
    assert producer is not None
    seq = await producer.send(req.message, key=req.key)
    return {"seq": seq}


@app.get("/health")
async def health():
    return {"status": "ok"}
