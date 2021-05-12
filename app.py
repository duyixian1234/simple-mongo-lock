import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

db = AsyncIOMotorClient("mongodb://127.0.0.1:27017/test").get_default_database()

app = FastAPI()


@asynccontextmanager
async def useLock():
    doc = await db.test.find_one_and_update(
        {"_id": "lock", "status": 0}, {"$set": {"status": 1}}, return_document=True
    )
    if not doc:
        raise ValueError("Can't Access Lock")
    try:
        yield doc
    except Exception as e:
        await db.test.find_one_and_update(
            {"_id": "lock", "status": 1}, {"$set": {"status": 0}}
        )
        raise e
    await db.test.find_one_and_update(
        {"_id": "lock", "status": 1}, {"$set": {"status": 0}}
    )


@app.on_event("startup")
async def prepare_data():
    await db.test.delete_one(dict(_id="lock"))
    await db.test.delete_one(dict(_id="data"))
    await db.test.insert_one(dict(_id="lock", status=0))
    await db.test.insert_one(dict(_id="data", value=0))


@app.get("/data")
async def get_data():
    return await db.test.find_one(dict(_id="data"))


@app.get("/unsafe-add")
async def add():
    data = await db.test.find_one(dict(_id="data"))
    await db.test.find_one_and_update(
        dict(_id="data"), {"$set": {"value": data["value"] + 1}}
    )
    return dict(success=True)


@app.get("/safe-add")
async def add():
    async with useLock():
        data = await db.test.find_one(dict(_id="data"))
        await db.test.find_one_and_update(
            dict(_id="data"), {"$set": {"value": data["value"] + 1}}
        )
        return dict(success=True)


@app.get("/atomic-add")
async def add():
    await db.test.find_one_and_update(dict(_id="data"), {"$inc": {"value": 1}})
    return dict(success=True)
