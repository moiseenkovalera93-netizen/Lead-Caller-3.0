import re
import json
import time
import redis
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# =====================
# REDIS
# =====================
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

QUEUE_KEY = "queue:calls"
DEDUP_PREFIX = "dedup:"
LEAD_PREFIX = "lead:"

# =====================
# PHONE PARSER
# =====================
def extract_phone(text: str):
    match = re.search(r'(\+1[\s\-]?\d{10}|\b\d{10}\b)', text)
    if not match:
        return None

    digits = re.sub(r'[^\d]', '', match.group())

    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"

    return None


# =====================
# CRM
# =====================
def create_or_update_lead(phone: str):
    key = f"{LEAD_PREFIX}{phone}"

    if not r.exists(key):
        lead = {
            "phone": phone,
            "status": "NEW",
            "attempts": 0,
            "created_at": time.time(),
            "last_error": None
        }
        r.set(key, json.dumps(lead))

    return key


# =====================
# HANDLER
# =====================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    phone = extract_phone(text)

    if not phone:
        return

    # anti-duplicate (5 min)
    dedup_key = f"{DEDUP_PREFIX}{phone}"
    if r.get(dedup_key):
        return

    r.setex(dedup_key, 300, "1")

    lead_key = create_or_update_lead(phone)

    lead = json.loads(r.get(lead_key))
    lead["status"] = "QUEUED"
    r.set(lead_key, json.dumps(lead))

    task = {
        "phone": phone,
        "lead_key": lead_key,
        "created_at": time.time()
    }

    r.rpush(QUEUE_KEY, json.dumps(task))

    await update.message.reply_text(f"Lead queued: {phone}")


# =====================
# APP
# =====================
app = Application.builder().token("YOUR_TELEGRAM_BOT_TOKEN").build()
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

app.run_polling()
