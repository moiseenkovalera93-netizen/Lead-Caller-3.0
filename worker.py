import redis
import json
import asyncio
import time
import os
import logging
from twilio.rest import Client

# =====================
# LOGGING
# =====================
logging.basicConfig(level=logging.INFO)

# =====================
# REDIS
# =====================
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

QUEUE_KEY = "queue:calls"
LEAD_PREFIX = "lead:"

# =====================
# TWILIO
# =====================
twilio = Client(
    os.getenv("TWILIO_ACCOUNT_SID"),
    os.getenv("TWILIO_AUTH_TOKEN")
)

TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER")
NEXFIELD_NUMBER = os.getenv("NEXFIELD_NUMBER")

# =====================
# SETTINGS
# =====================
CALL_DELAY = 120
MAX_ATTEMPTS = 3


# =====================
# LEAD UPDATE
# =====================
def update_lead(phone: str, data: dict):
    key = f"{LEAD_PREFIX}{phone}"
    lead = json.loads(r.get(key))

    lead.update(data)
    r.set(key, json.dumps(lead))

    return lead


# =====================
# CALL LOGIC
# =====================
async def process_call(task):
    phone = task["phone"]
    lead_key = task["lead_key"]

    await asyncio.sleep(CALL_DELAY)

    lead = json.loads(r.get(lead_key))
    attempts = lead["attempts"]

    try:
        update_lead(phone, {"status": "CALLING"})

        twilio.calls.create(
            to=phone,
            from_=TWILIO_FROM_NUMBER,
            twiml=f"<Response><Say>Hello, please hold.</Say><Dial>{NEXFIELD_NUMBER}</Dial></Response>"
        )

        update_lead(phone, {
            "status": "CALLED",
            "attempts": attempts + 1,
            "last_error": None
        })

        logging.info(f"CALLED: {phone}")

    except Exception as e:
        attempts += 1

        if attempts >= MAX_ATTEMPTS:
            update_lead(phone, {
                "status": "FAILED",
                "attempts": attempts,
                "last_error": str(e)
            })
        else:
            update_lead(phone, {
                "status": "RETRYING",
                "attempts": attempts,
                "last_error": str(e)
            })

            # requeue
            r.rpush(QUEUE_KEY, json.dumps(task))

        logging.error(f"CALL FAILED {phone}: {e}")


# =====================
# WORKER LOOP
# =====================
async def worker():
    while True:
        _, data = r.blpop(QUEUE_KEY)
        task = json.loads(data)

        asyncio.create_task(process_call(task))


if __name__ == "__main__":
    asyncio.run(worker())
