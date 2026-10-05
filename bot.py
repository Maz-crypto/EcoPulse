#!/usr/bin/env python3
"""
EcoPulse Bot — النسخة النهائية المستقرة مع دعم موجز الساعة وGemini الاحتياطي
✅ قناة تحكم ثابتة (من .env)
✅ جميع الأوامر تعمل فورًا
✅ استجابة تلقائية لأي رسالة غير معروفة
✅ كشف دقيق للبيانات الاقتصادية
✅ نشر فوري مشروط (600 مشاهدة أو 8 دقائق)
✅ موجز ساعة اقتصادي تلقائي
✅ دعم OpenAI + Gemini (احتياطي تلقائي)
✅ نظام حماية ضد انقطاع الاتصال (إعادة المحاولة التلقائية)
"""

import asyncio
import os
import logging
import re
import time
import json
from datetime import datetime, timedelta
from collections import deque
from collections import defaultdict

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from dotenv import load_dotenv
from openai import OpenAI
from google import genai

# ---------------- تحميل الإعدادات ----------------
load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

if not SESSION_STRING or SESSION_STRING == "":
    logging.critical("❌ SESSION_STRING مفقود في .env — لا يمكن تشغيل البوت على الخادم!")
    exit(1)

# --- القنوات من .env (ثابتة) ---
SOURCE_CHANNEL = os.getenv("SOURCE_CHANNEL", "me")
SOURCE_CHANNEL_2 = os.getenv("SOURCE_CHANNEL_2", "me")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "me")
ANALYST_TARGET = os.getenv("ANALYST_TARGET", "")
CONTROL_CHANNEL = os.getenv("CONTROL_CHANNEL", "me")
ANALYST_SOURCE = os.getenv("ANALYST_SOURCE", "")
HOURLY_SOURCE = os.getenv("HOURLY_SOURCE", "")
HOURLY_TARGET = os.getenv("HOURLY_TARGET", "")

ANALYST_SOURCE_ID = None
ANALYST_TARGET_ID = None
HOURLY_SOURCE_ID = None
HOURLY_TARGET_ID = None

# ---------------- إعدادات النشر ----------------
# يمكن تغيير هذه القيم مباشرة من قناة التحكم دون إعادة تشغيل البوت.
IMMEDIATE_MIN_VIEWS = int(os.getenv("IMMEDIATE_MIN_VIEWS", "500"))
IMMEDIATE_TIMEOUT = int(os.getenv("IMMEDIATE_TIMEOUT", str(8 * 60)))
MIN_VIEWS_FOR_NEXT = int(os.getenv("MIN_VIEWS_FOR_NEXT", "500"))
PUBLISHER_DELAY = int(os.getenv("PUBLISHER_DELAY", "10"))
VIEW_CHECK_INTERVAL = int(os.getenv("VIEW_CHECK_INTERVAL", "60"))
ANALYST_POST_INTERVAL = int(os.getenv("ANALYST_POST_INTERVAL", "900"))
HOURLY_MAX_WORDS = int(os.getenv("HOURLY_MAX_WORDS", "120"))
SETTINGS_FILE = os.getenv("SETTINGS_FILE", "ecopulse_runtime_settings.json")

def load_runtime_settings():
    """تحميل إعدادات التحكم المحفوظة إن وجدت."""
    global IMMEDIATE_MIN_VIEWS, IMMEDIATE_TIMEOUT, MIN_VIEWS_FOR_NEXT
    global PUBLISHER_DELAY, VIEW_CHECK_INTERVAL, ANALYST_POST_INTERVAL, HOURLY_MAX_WORDS
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for name in (
                "IMMEDIATE_MIN_VIEWS", "IMMEDIATE_TIMEOUT", "MIN_VIEWS_FOR_NEXT",
                "PUBLISHER_DELAY", "VIEW_CHECK_INTERVAL", "ANALYST_POST_INTERVAL",
                "HOURLY_MAX_WORDS"
            ):
                if name in data:
                    globals()[name] = int(data[name])
            logging.info("✅ تم تحميل إعدادات التحكم المحفوظة.")
    except Exception as e:
        logging.warning(f"⚠️ تعذر تحميل إعدادات التحكم: {e}")

def save_runtime_settings():
    """حفظ إعدادات التحكم الحالية لتبقى بعد إعادة التشغيل."""
    data = {
        "IMMEDIATE_MIN_VIEWS": IMMEDIATE_MIN_VIEWS,
        "IMMEDIATE_TIMEOUT": IMMEDIATE_TIMEOUT,
        "MIN_VIEWS_FOR_NEXT": MIN_VIEWS_FOR_NEXT,
        "PUBLISHER_DELAY": PUBLISHER_DELAY,
        "VIEW_CHECK_INTERVAL": VIEW_CHECK_INTERVAL,
        "ANALYST_POST_INTERVAL": ANALYST_POST_INTERVAL,
        "HOURLY_MAX_WORDS": HOURLY_MAX_WORDS,
    }
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logging.error(f"❌ تعذر حفظ إعدادات التحكم: {e}")
        return False

# ---------------- مفاتيح الذكاء الاصطناعي ----------------
OPENAI_KEYS = os.getenv("OPENAI_API_KEYS", "").split(",")
GEMINI_KEYS = os.getenv("GEMINI_API_KEYS", "").split(",")

if not OPENAI_KEYS or OPENAI_KEYS == [""]:
    logging.warning("⚠️ لم يتم العثور على مفاتيح OpenAI في ملف .env")
if not GEMINI_KEYS or GEMINI_KEYS == [""]:
    logging.warning("⚠️ لم يتم العثور على مفاتيح Gemini في ملف .env")

if not OPENAI_KEYS and not GEMINI_KEYS:
    raise ValueError("❌ لا توجد مفاتيح OpenAI أو Gemini صالحة في ملف .env")

# ---------------- إعدادات عامة ----------------
KEYWORDS_LIST = ["JUST IN", "MACRO", "$MACRO", "marco", "FEDERAL", "warsh", "WARSH", "TRUMP", "FED'S", "FED", "🔴"]
EMOJI_IMMEDIATE = "🚨"
EMOJI_SCHEDULED = "📝"
EMOJI_ALERT = "⚠️🚨"
EMOJI_HOURLY = "⏰"
CHANNEL_WATERMARK = " "
HOURLY_SIGNATURE = os.getenv("HOURLY_SIGNATURE", "— موجز الساعة")

# ---------------- التهيئة ----------------
# تم إضافة إعدادات التايم أوت لاصطياد الانقطاع سريعاً بدلاً من التجمد
client = TelegramClient(
    StringSession(SESSION_STRING), 
    API_ID, 
    API_HASH,
    connection_retries=3,
    timeout=15,
    auto_reconnect=True
)

translation_queue = deque()
hourly_queue = deque()
posted_texts = set()
MAX_POSTED_HISTORY = 100

# === متغيرات التحكم ===
bot_active = False
publish_immediate = True
publish_economic = True
publish_analysis = True
publish_scheduled = True
publish_hourly = True
dry_run_mode = os.getenv("DRY_RUN", "0").lower() in ("1", "true", "yes")

# متغيرات التحكم في النشر الفوري
last_immediate_post_id = None
last_immediate_post_time = datetime.now()

# إحصاءات
stats = {
    "posts": 0,
    "economic": 0,
    "immediate": 0,
    
    "scheduled": 0,
    "analysis": 0,
    "hourly": 0,
    "flood_waits": 0,
    "openai_usage": 0,
    "gemini_usage": 0
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot_activity.log", "a", encoding="utf-8")]
)

# كتم رسائل إعادة الاتصال الداخلية لمكتبة Telethon لمنع الامتلاء في اللوج
logging.getLogger('telethon').setLevel(logging.CRITICAL)

load_runtime_settings()

# ---------------- إدارة مفاتيح الذكاء الاصطناعي ----------------
class AIManager:
    def __init__(self, openai_keys, gemini_keys):
        self.openai_keys = [k.strip() for k in openai_keys if k.strip()]
        self.gemini_keys = [k.strip() for k in gemini_keys if k.strip()]
        self.openai_index = 0
        self.gemini_index = 0
        self.failed_openai = {}
        self.failed_gemini = {}
        self.failure_cooldown = 3600
        self.usage_stats = {"openai": defaultdict(int), "gemini": defaultdict(int)}
        logging.info(f"Intialized AIManager with {len(self.openai_keys)} OpenAI keys and {len(self.gemini_keys)} Gemini keys")

    def _get_usable_keys(self, key_type):
        now = time.time()
        if key_type == "openai":
            keys = self.openai_keys
            failed = self.failed_openai
        else:
            keys = self.gemini_keys
            failed = self.failed_gemini
        usable = []
        for key in keys:
            fail_time = failed.get(key)
            if not fail_time or (now - fail_time) > self.failure_cooldown:
                usable.append(key)
        return usable

    def get_openai_client(self):
        usable_keys = self._get_usable_keys("openai")
        if not usable_keys:
            logging.warning("⚠️ جميع مفاتيح OpenAI معطّلة")
            return None
        key = usable_keys[self.openai_index % len(usable_keys)]
        self.openai_index += 1
        self.usage_stats["openai"][key] += 1
        logging.debug(f"🔑 استخدام مفتاح OpenAI: {key[:5]}... (الاستخدام: {self.usage_stats['openai'][key]})")
        return OpenAI(api_key=key)

    def get_gemini_client(self):
        usable_keys = self._get_usable_keys("gemini")

        if not usable_keys:
            logging.warning("⚠️ جميع مفاتيح Gemini معطّلة")
            return None

        key = usable_keys[self.gemini_index % len(usable_keys)]
        self.gemini_index += 1
        self.usage_stats["gemini"][key] += 1

        logging.debug(
            f"🔑 استخدام مفتاح Gemini: {key[:5]}... "
            f"(الاستخدام: {self.usage_stats['gemini'][key]})"
        )

        return genai.Client(api_key=key)

    def mark_openai_failed(self, key: str, error: str = ""):
        self.failed_openai[key] = time.time()
        logging.warning(f"🚫 مفتاح OpenAI معطّل: {key[:5]}... — {error}")
        usable = self._get_usable_keys("openai")
        logging.info(f"📊 حالة مفاتيح OpenAI: {len(usable)}/{len(self.openai_keys)} نشطة")

    def mark_gemini_failed(self, key: str, error: str = ""):
        self.failed_gemini[key] = time.time()
        logging.warning(f"🚫 مفتاح Gemini معطّل: {key[:5]}... — {error}")
        usable = self._get_usable_keys("gemini")
        logging.info(f"📊 حالة مفاتيح Gemini: {len(usable)}/{len(self.gemini_keys)} نشطة")

    def get_status(self) -> str:
        openai_usable = self._get_usable_keys("openai")
        gemini_usable = self._get_usable_keys("gemini")
        openai_failed = [k for k in self.openai_keys if k not in openai_usable]
        gemini_failed = [k for k in self.gemini_keys if k not in gemini_usable]
        status = (
            f"🔑 OpenAI: {len(self.openai_keys)} | نشطة: {len(openai_usable)} | معطّلة: {len(openai_failed)}\n"
            f"🔑 Gemini: {len(self.gemini_keys)} | نشطة: {len(gemini_usable)} | معطّلة: {len(gemini_failed)}\n"
            f"📈 OpenAI الاستخدام: {dict(self.usage_stats['openai'])}\n"
            f"📈 Gemini الاستخدام: {dict(self.usage_stats['gemini'])}\n"
            f"❌ OpenAI المعطّلة: {[k[:5]+'...' for k in openai_failed]}\n"
            f"❌ Gemini المعطّلة: {[k[:5]+'...' for k in gemini_failed]}"
        )
        return status

ai_manager = AIManager(OPENAI_KEYS, GEMINI_KEYS)

# ---------------- أدوات مساعدة ----------------
def log_activity(task: str, message_id: int):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"[{now}] ({task}) -> نشر رسالة ID={message_id}")
    if "اقتصادي" in task:
        stats["economic"] += 1
    elif "فوري" in task and "اقتصادي" not in task:
        stats["immediate"] += 1
    elif "مجدول" in task:
        stats["scheduled"] += 1
    elif "تحليل" in task:
        stats["analysis"] += 1
    elif "موجز" in task:
        stats["hourly"] += 1
    stats["posts"] += 1

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"http\S+|www\.\S+", "", text)
    text = re.sub(r"\$", "", text)
    text = re.sub(r"(\.{3,}|…+)$", "", text)
    return text.strip()

def is_meaningful_text(text: str) -> bool:
    if not text:
        return False
    cleaned = re.sub(r"http\S+|www\.\S+", "", text)
    cleaned = re.sub(r"[^\w\s\u0600-\u06FF]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return len(cleaned) >= 10 and len(cleaned.split()) >= 2

# ---------------- حل معرفات القنوات ----------------
async def resolve_channel(channel_input: str):
    try:
        channel_input = channel_input.strip()
        if channel_input == "me":
            me = await client.get_me()
            return me.id
        elif channel_input.startswith('@'):
            entity = await client.get_entity(channel_input)
            return entity.id
        elif channel_input.lstrip('-').isdigit():
            return int(channel_input)
        else:
            entity = await client.get_entity(channel_input)
            return entity.id
    except Exception as e:
        raise ValueError(f"قناة غير صالحة '{channel_input}': {str(e)[:100]}")

# ---------------- كشف البيانات الاقتصادية ----------------
def is_economic_data(text: str) -> bool:
    pattern = r"""
        (?:
            \b(?:ACT(?:UAL)?|FORECAST|EST(?:IMATED)?|PREV(?:IOUS)?|REVISED?)\b
            [:=;]?\s*[-+]?\d+(?:\.\d+)?%?(?:[MBK]|MILLION|BILLION|THOUSAND)?|
            [-+]?\d+(?:\.\d+)?%?\s+(?:VS|VERSUS|VS\.)\s+[-+]?\d+(?:\.\d+)?%?|
            \([^)]*(?:ACT(?:UAL)?|FORECAST|EST|PREV|REVISED?)[^)]*\d[^)]*\)|
            \b(?:PMI|ISM|JOLTS|CPI|GDP|NFP|NONFARM|JOBS?|ORDERS?|DURABLE|FACTORY|IVES?|PRICES?|EMPLOYMENT|NEW\s+ORDERS?)\b
            .{0,50}?(?:\d+(?:\.\d+)?%?|[-+]\d+(?:\.\d+)?%?)|
            \b\d+(?:\.\d+)?[MBK](?:ILLION|ILLION)?\b
        )
        .*?
        (?:
            (?:ACT(?:UAL)?|FORECAST|EST|PREV|REVISED?)|
            \d+(?:\.\d+)?%?|
            [MBK]
        )
    """
    return bool(re.search(pattern, text, re.IGNORECASE | re.VERBOSE))

# ---------------- التحقق من شروط النشر الفوري ----------------
async def can_publish_immediate() -> bool:
    global last_immediate_post_id, last_immediate_post_time
    
    if last_immediate_post_id is None:
        return True
    
    views = 0
    try:
        post = await client.get_messages(TARGET_CHANNEL_ID, ids=last_immediate_post_id)
        views = post.views or 0
        if views >= IMMEDIATE_MIN_VIEWS:
            logging.info(f"✅ مشاهدات كافية ({views} ≥ {IMMEDIATE_MIN_VIEWS})")
            return True
    except Exception as e:
        logging.warning(f"فشل جلب المشاهدات: {e}")
    
    elapsed = (datetime.now() - last_immediate_post_time).total_seconds()
    if elapsed >= IMMEDIATE_TIMEOUT:
        logging.info(f"✅ مرور الوقت الكافي ({elapsed:.0f} ثانية ≥ {IMMEDIATE_TIMEOUT})")
        return True
    
    logging.info(f"⏳ لا توجد شروط نشر فوري بعد: {views} مشاهدة، {elapsed:.0f} ثانية")
    return False

# ---------------- متغيرات معرفات القنوات (بعد التحويل) ----------------
SOURCE_CHANNEL_ID = None
SOURCE_CHANNEL_2_ID = None
TARGET_CHANNEL_ID = None
ANALYST_TARGET_ID = None
CONTROL_CHANNEL_ID = None

# ---------------- وظائف معالجة الذكاء الاصطناعي ----------------
async def call_openai(prompt: str, user_content: str, max_retries=3):
    """استدعاء OpenAI باستخدام gpt-4o-mini"""
    for attempt in range(max_retries):
        client_ai = ai_manager.get_openai_client()
        if not client_ai:
            return None
            
        try:
            response = client_ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_content},
                ],
                timeout=15.0
            )
            stats["openai_usage"] += 1
            return response.choices[0].message.content.strip()
        except Exception as e:
            error_str = str(e)
            logging.warning(f"❌ فشل OpenAI (محاولة {attempt + 1}): {error_str[:100]}...")
            ai_manager.mark_openai_failed(client_ai.api_key, error_str)
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
    return None

async def call_gemini(prompt: str, user_content: str, max_retries=3):
    """استدعاء Gemini باستخدام google-genai SDK"""

    for attempt in range(max_retries):
        client_gemini = ai_manager.get_gemini_client()

        if not client_gemini:
            return None

        try:
            full_prompt = f"{prompt}\n\nالمحتوى:\n{user_content}"

            response = await client_gemini.aio.models.generate_content(
                model="gemini-3.6-flash",
                contents=full_prompt
            )

            stats["gemini_usage"] += 1

            if response.text:
                return response.text.strip()

            logging.warning("⚠️ Gemini أعاد استجابة فارغة")

        except Exception as e:
            error_str = str(e)

            logging.warning(
                f"❌ فشل Gemini (محاولة {attempt + 1}): "
                f"{error_str[:200]}"
            )

            if ai_manager.gemini_keys:
                current_key = ai_manager.gemini_keys[
                    (ai_manager.gemini_index - 1)
                    % len(ai_manager.gemini_keys)
                ]

                ai_manager.mark_gemini_failed(
                    current_key,
                    error_str
                )

            if attempt < max_retries - 1:
                await asyncio.sleep(2)

    return None

async def call_ai_with_fallback(prompt: str, user_content: str) -> str:
    
    logging.info("Call Gemini ")
    result = await call_openai(prompt, user_content)
    if result is not None:
        return result
        
    """استدعاء Gemini أولاً، ثم OpenAI كخيار احتياطي"""
    result = await call_gemini(prompt, user_content)
    if result is not None:
        return result
        
    logging.error("💥 فشل كل من OpenAI وGemini في معالجة الطلب")
    return user_content

# ---------------- تحليل وترجمة ----------------
async def analyze_and_translate(text: str, target_lang: str, max_retries: int = 6, retry_delay: int = 5) -> dict:
    if not text:
        return {"impact": "⚪ تأثير محايد", "translation": ""}

    system_prompt = (
        "أنت محلل اقتصادي ومتداول ومترجم محترف في عام 2026 حيث ترامب هو رئيس امريكا. "
        "حلّل الخبر، ثم أعد صياغته بالعربية بأسلوب اقتصادي مختصر مع اضافة اموجن مناسب. "
        "أولاً، قدم تقييمًا للتأثير من كلمتين إلى أربع. "
        "ثم ضع ### ثم أعد الصياغة بالعربية."
    )
    
    content = await call_ai_with_fallback(system_prompt, text)
    
    if content:
        parts = content.split("###", 1)
        impact = parts[0].strip() if parts else "⚪ تأثير محايد"
        translation = parts[1].strip() if len(parts) > 1 else text
        return {"impact": impact, "translation": translation}
    else:
        return {"impact": "⚪ تأثير محايد", "translation": text}

# ---------------- تنسيق المنشور ----------------
async def format_final_text(text: str, emoji: str, signature: str = None, attention=False) -> str:
    if signature is None:
        signature = os.getenv("SIGNATURE", "— EcoPulse")

    cleaned = clean_text(text)
    if not is_meaningful_text(cleaned):
        logging.debug("🗑️ تم تجاهل نص غير ذي معنى في التنسيق")
        return ""

    if is_economic_data(text):
        logging.info("📡 كشف بيانات اقتصادية")
        system_prompt = (
            "أنت محرر أخبار اقتصادية محترف. "
            "استخرج البيانات واعرضها بالقالب:\n"
            "🔴 صدر الآن :\n\n"
            "💠 {الدولة}\n"
            "🔵 {المؤشر}\n\n"
            "🕒 السابق :\n"
            "🕒 التقدير :\n"
            "🕓 الحالي :\n\n"
            "👈 النتيجة : تحليل ≤ 9 كلمات."
        )
        translation = await call_ai_with_fallback(system_prompt, text)
        if not translation:
            fallback = f"🔴 **بيانات اقتصادية**\n\n```{clean_text(text)[:200]}...```\n\n{signature}"
            return fallback
        final_text = f"{translation}\n\n{signature}\n\n{CHANNEL_WATERMARK}"
        return final_text[:4000]

    elif "MACRO" in text.upper():
        system_prompt = "أنت محلل اقتصادي بعقلية متداول حيث ترامب هو الرئيس الحالي لامريكا. قم بتحليل الخبر بالعربية ≤ 10 كلمات."
        translation = await call_ai_with_fallback(system_prompt, text)
        if not translation:
            fallback = f"💡 **تحليل اقتصادي**\n\n```{clean_text(text)[:150]}...```\n\n{signature}"
            return fallback
        final_text = f"{translation}\n\n{signature}\n\n{CHANNEL_WATERMARK}"
        return final_text[:4000]

    else:
        result = await analyze_and_translate(text, "ar")
        header_attention = f"{EMOJI_ALERT} **إنتباه:**\n\n" if attention else ""
        final_text = f"{header_attention}{result['impact']}\n\n{emoji} {result['translation']}\n\n{signature}\n\n{CHANNEL_WATERMARK}"
        return final_text[:4000]

# ---------------- إرسال الرسائل ----------------
async def forward_or_send(message, caption: str, task_name="", target_channel=None):
    if not caption or not caption.strip():
        logging.debug(f"❌ تجاهل نشر رسالة فارغة ID={message.id}")
        return None

    if not target_channel:
        target_channel = TARGET_CHANNEL_ID

    if dry_run_mode:
        logging.info(f"[🧪 DRY-RUN] {task_name}: {caption[:100]}...")
        return type('obj', (), {'id': 999})()

    text_signature = caption.strip()
    if text_signature in posted_texts:
        logging.info(f"❌ تم تجاهل الرسالة ID={message.id} لأنها مكررة")
        return
    posted_texts.add(text_signature)
    if len(posted_texts) > MAX_POSTED_HISTORY:
        posted_texts.pop()
    try:
        sent = await client.send_message(target_channel, caption, link_preview=False)
        log_activity(task_name, message.id)
        return sent
    except FloodWaitError as fe:
        stats["flood_waits"] += 1
        logging.warning(f"⏳ Flood wait: الانتظار {fe.seconds} ثانية...")
        await asyncio.sleep(fe.seconds + 1)
        return await client.send_message(target_channel, caption, link_preview=False)
    except Exception:
        logging.exception("Error while sending message")

# ---------------- أدوات إعدادات التحكم ----------------
def format_duration(seconds: int) -> str:
    seconds = int(seconds)
    if seconds % 3600 == 0:
        return f"{seconds // 3600} ساعة"
    if seconds % 60 == 0:
        return f"{seconds // 60} دقيقة"
    return f"{seconds} ثانية"

def settings_status() -> str:
    return (
        "⚙️ **إعدادات التحكم الحالية | Runtime Settings**\n\n"
        f"⚡ حد المشاهدات للفوري: `{IMMEDIATE_MIN_VIEWS}`\n"
        f"⏱️ مهلة الفوري: `{format_duration(IMMEDIATE_TIMEOUT)}`\n"
        f"📈 حد مشاهدات المنشور التالي: `{MIN_VIEWS_FOR_NEXT}`\n"
        f"⏳ الفاصل بين المنشورات: `{PUBLISHER_DELAY}` ثانية\n"
        f"🔄 فحص المشاهدات كل: `{VIEW_CHECK_INTERVAL}` ثانية\n"
        f"🧠 فاصل التحليل: `{format_duration(ANALYST_POST_INTERVAL)}`\n"
        f"⏰ حد كلمات موجز الساعة: `{HOURLY_MAX_WORDS}`\n"
        f"💾 الحفظ التلقائي: `{SETTINGS_FILE}`"
    )

def parse_duration(value: str):
    """يدعم: 30s / 5m / 2h أو رقمًا بالثواني."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smh]?)\s*", value.lower())
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2)
    multiplier = {"": 1, "s": 1, "m": 60, "h": 3600}[unit]
    result = int(n * multiplier)
    return result if result > 0 else None

def setting_command(text: str):
    """يعيد (اسم الإعداد، القيمة) أو None."""
    m = re.match(r"^(?:إعداد|اعداد|set)\s+(.+?)\s+(.+)$", text, re.IGNORECASE)
    if not m:
        return None
    key = m.group(1).strip().lower()
    value = m.group(2).strip()
    aliases = {
        "الفوري": "immediate_views",
        "مشاهدات الفوري": "immediate_views",
        "حد الفوري": "immediate_views",
        "immediate views": "immediate_views",
        "immediate_views": "immediate_views",
        "مهلة الفوري": "immediate_timeout",
        "مدة الفوري": "immediate_timeout",
        "immediate timeout": "immediate_timeout",
        "immediate_timeout": "immediate_timeout",
        "مشاهدات التالي": "next_views",
        "حد التالي": "next_views",
        "حد المجدول": "next_views",
        "next views": "next_views",
        "next_views": "next_views",
        "فاصل النشر": "publish_delay",
        "تأخير النشر": "publish_delay",
        "publish delay": "publish_delay",
        "publish_delay": "publish_delay",
        "فحص المشاهدات": "view_check",
        "view check": "view_check",
        "view_check": "view_check",
        "فاصل التحليل": "analyst_interval",
        "التحليل": "analyst_interval",
        "analyst interval": "analyst_interval",
        "analyst_interval": "analyst_interval",
        "كلمات الموجز": "hourly_words",
        "حد الموجز": "hourly_words",
        "hourly words": "hourly_words",
        "hourly_words": "hourly_words",
    }
    return aliases.get(key), value

# ---------------- معالجة التحكم بالعربية والإنجليزية ----------------
@client.on(events.NewMessage(chats=[]))
async def control_handler(event):
    global bot_active, publish_immediate, publish_economic
    global publish_analysis, publish_scheduled, publish_hourly, dry_run_mode
    global IMMEDIATE_MIN_VIEWS, IMMEDIATE_TIMEOUT, MIN_VIEWS_FOR_NEXT
    global PUBLISHER_DELAY, VIEW_CHECK_INTERVAL, ANALYST_POST_INTERVAL, HOURLY_MAX_WORDS

    raw_text = (event.raw_text or "").strip()
    text = raw_text.lower().strip() if raw_text else "مساعدة"

    # مجموعات أوامر ثنائية اللغة
    commands = {
        "start": {"تفعيل", "تشغيل", "ابدأ", "start", "enable", "activate", "/start"},
        "stop": {"ايقاف", "إيقاف", "تعطيل", "توقف", "stop", "disable", "deactivate", "/stop"},
        "immediate_on": {"نشر فوري on", "نشر فوري تشغيل", "فوري on", "فوري تشغيل", "immediate on", "immediate enable", "immediate start", "instant on", "instant enable"},
        "immediate_off": {"نشر فوري off", "نشر فوري إيقاف", "نشر فوري تعطيل", "فوري off", "فوري إيقاف", "immediate off", "immediate disable", "instant off", "instant disable"},
        "economic_on": {"اقتصادي on", "اقتصادي تشغيل", "اقتصاد on", "economic on", "economic enable", "economic start", "economy on", "macro on"},
        "economic_off": {"اقتصادي off", "اقتصادي إيقاف", "اقتصادي تعطيل", "اقتصاد off", "economic off", "economic disable", "economy off", "macro off"},
        "analysis_on": {"تحليل on", "تحليل تشغيل", "تحليل تفعيل", "analysis on", "analysis enable", "analysis start", "analyst on"},
        "analysis_off": {"تحليل off", "تحليل إيقاف", "تحليل تعطيل", "analysis off", "analysis disable", "analyst off"},
        "scheduled_on": {"مجدول on", "مجدول تشغيل", "مجدول تفعيل", "scheduled on", "scheduled enable", "scheduled start", "scheduler on"},
        "scheduled_off": {"مجدول off", "مجدول إيقاف", "مجدول تعطيل", "scheduled off", "scheduled disable", "scheduler off"},
        "hourly_on": {"موجز on", "موجز تشغيل", "موجز تفعيل", "hourly on", "hourly enable", "hourly start", "summary on"},
        "hourly_off": {"موجز off", "موجز إيقاف", "موجز تعطيل", "hourly off", "hourly disable", "summary off"},
        "hourly_now": {"موجز الآن", "موجز الان", "أنشئ موجز", "انشئ موجز", "hourly now", "summary now", "generate summary", "generate hourly", "generate hourly summary"},
        "status": {"حالة", "الحالة", "status", "state", "/status"},
        "keys": {"مفاتيح", "مفاتيح ai", "مفاتيح الذكاء", "keys", "ai keys", "api keys", "key status"},
        "queue": {"مكدس", "المكدس", "queue", "queues", "queue status"},
        "stats": {"إحصاء", "احصاء", "إحصائيات", "احصائيات", "stats", "statistics", "stat"},
        "channels": {"قنوات", "القنوات", "channels", "channel status", "channel"},
        "settings": {"إعدادات", "اعدادات", "settings", "config", "configuration"},
        "save_settings": {"حفظ الإعدادات", "حفظ الاعدادات", "save settings", "save config"},
        "publish_next": {"نشر التالي", "نشر الآن من المكدس", "publish next", "publish queued"},
        "clear_queue": {"مسح المخزن", "مسح المكدس", "مسح الطابور", "clear queue", "queue clear", "clear queues", "clear storage"},
        "reset": {"إعادة تعيين", "اعادة تعيين", "مسح التكرار", "reset", "reset history", "clear history"},
        "dry_on": {"وضع تجربة on", "تجربة on", "dry run on", "dryrun on", "test mode on", "test on"},
        "dry_off": {"وضع تجربة off", "تجربة off", "dry run off", "dryrun off", "test mode off", "test off"},
        "help": {"مساعدة", "ساعدني", "الأوامر", "الاوامر", "help", "commands", "/help", "/commands"},
    }

    # أوامر الإعدادات المرنة: إعداد <اسم> <قيمة>
    parsed_setting = setting_command(raw_text)
    if parsed_setting:
        setting_name, raw_value = parsed_setting
        await event.reply( setting_name )
        if setting_name is None:
            await event.reply(
                "❌ **إعداد غير معروف**\n\n"
                "استخدم `إعدادات` لرؤية الخيارات، أو مثال:\n"
                "`إعداد مشاهدات الفوري 50`\n"
                "`إعداد مهلة الفوري 10m`"
            )
            return

        duration_fields = {"immediate_timeout", "publish_delay", "view_check", "analyst_interval"}
        try:
            if setting_name in duration_fields:
                value = parse_duration(raw_value)
                if value is None:
                    raise ValueError("صيغة مدة غير صحيحة")
            else:
                value = int(raw_value)
                if value <= 0:
                    raise ValueError("يجب أن تكون القيمة أكبر من صفر")

            if setting_name == "immediate_views":
                IMMEDIATE_MIN_VIEWS = value
                label = "حد مشاهدات النشر الفوري"
            elif setting_name == "immediate_timeout":
                IMMEDIATE_TIMEOUT = value
                label = "مهلة النشر الفوري"
            elif setting_name == "next_views":
                MIN_VIEWS_FOR_NEXT = value
                label = "حد مشاهدات المنشور التالي"
            elif setting_name == "publish_delay":
                PUBLISHER_DELAY = value
                label = "الفاصل بين المنشورات"
            elif setting_name == "view_check":
                VIEW_CHECK_INTERVAL = value
                label = "فاصل فحص المشاهدات"
            elif setting_name == "analyst_interval":
                ANALYST_POST_INTERVAL = value
                label = "فاصل التحليل"
            else:
                HOURLY_MAX_WORDS = value
                label = "حد كلمات موجز الساعة"

            save_runtime_settings()
            await event.reply(
                f"✅ **تم تحديث الإعداد**\n\n"
                f"⚙️ {label}: `{format_duration(value) if setting_name in duration_fields else value}`\n\n"
                "💾 تم حفظه وسيبقى بعد إعادة التشغيل."
            )
        except ValueError:
            await event.reply(
                "❌ **قيمة غير صالحة.**\n\n"
                "للأرقام: `50`\n"
                "للمدد: `30s` أو `5m` أو `2h`."
            )
        return

    # تحديد الأمر بالضبط لتجنب تداخل كلمات مثل "on" داخل أمر آخر
    action = next((name for name, variants in commands.items() if text in variants), None)

    if action == "start":
        bot_active = True
        logging.info("✅ تم تفعيل البوت بالكامل.")
        await event.reply("✅ **تم تفعيل البوت بالكامل**\n\n🤖 Bot is now fully active.")
        return

    if action == "stop":
        bot_active = False
        logging.info("⛔ تم إيقاف البوت بالكامل.")
        await event.reply("⛔ **تم إيقاف البوت بالكامل**\n\n🤖 Bot has been completely stopped.")
        return

    if action == "immediate_on":
        publish_immediate = True
        await event.reply("✅ **تم تفعيل النشر الفوري**\n\n⚡ Immediate publishing: ON")
        return

    if action == "immediate_off":
        publish_immediate = False
        await event.reply("⛔ **تم إيقاف النشر الفوري**\n\n⚡ Immediate publishing: OFF")
        return

    if action == "economic_on":
        publish_economic = True
        await event.reply("✅ **تم تفعيل البيانات الاقتصادية**\n\n📊 Economic data: ON")
        return

    if action == "economic_off":
        publish_economic = False
        await event.reply("⛔ **تم إيقاف البيانات الاقتصادية**\n\n📊 Economic data: OFF")
        return

    if action == "analysis_on":
        publish_analysis = True
        await event.reply("✅ **تم تفعيل قناة التحليل**\n\n🧠 Analysis: ON")
        return

    if action == "analysis_off":
        publish_analysis = False
        await event.reply("⛔ **تم إيقاف قناة التحليل**\n\n🧠 Analysis: OFF")
        return

    if action == "scheduled_on":
        publish_scheduled = True
        await event.reply("✅ **تم تفعيل النشر المجدول**\n\n📝 Scheduled publishing: ON")
        return

    if action == "scheduled_off":
        publish_scheduled = False
        await event.reply("⛔ **تم إيقاف النشر المجدول**\n\n📝 Scheduled publishing: OFF")
        return

    if action == "hourly_on":
        publish_hourly = True
        await event.reply("✅ **تم تفعيل موجز الساعة**\n\n⏰ Hourly summary: ON")
        return

    if action == "hourly_off":
        publish_hourly = False
        await event.reply("⛔ **تم إيقاف موجز الساعة**\n\n⏰ Hourly summary: OFF")
        return

    if action == "hourly_now":
        if not publish_hourly:
            await event.reply("⚠️ **موجز الساعة معطّل حاليًا.**\n\nأرسل `موجز on` أو `hourly on` أولًا.")
        else:
            await generate_hourly_summary(manual=True)
            await event.reply("✅ **تم طلب إنشاء موجز الساعة يدويًا.**\n\n⏰ Hourly summary generation requested.")
        return

    if action == "status":
        status = (
            "📊 **حالة البوت | Bot Status**\n\n"
            f"🤖 البوت | Bot: {'✅ نشط | ACTIVE' if bot_active else '⛔ متوقف | STOPPED'}\n"
            f"⚡ النشر الفوري | Immediate: {'✅ ON' if publish_immediate else '⛔ OFF'}\n"
            f"📊 الاقتصادي | Economic: {'✅ ON' if publish_economic else '⛔ OFF'}\n"
            f"🧠 التحليل | Analysis: {'✅ ON' if publish_analysis else '⛔ OFF'}\n"
            f"📝 المجدول | Scheduled: {'✅ ON' if publish_scheduled else '⛔ OFF'}\n"
            f"⏰ موجز الساعة | Hourly: {'✅ ON' if publish_hourly else '⛔ OFF'}\n"
            f"📥 المكدس العادي | Normal Queue: {len(translation_queue)}\n"
            f"🕗 مكدس الساعة | Hourly Queue: {len(hourly_queue)}\n"
            f"🧪 وضع التجربة | Dry Run: {'✅ ON' if dry_run_mode else '🚀 OFF'}"
        )
        await event.reply(status)
        return

    if action == "keys":
        status = ai_manager.get_status()
        await event.reply(f"🔧 **حالة مفاتيح الذكاء الاصطناعي | AI Keys**\n\n{status}")
        return

    if action == "queue":
        count1 = len(translation_queue)
        count2 = len(hourly_queue)
        msg = f"📥 **المكدسات | Queues**\n\n📨 العادي | Normal: {count1}\n⏰ الساعة | Hourly: {count2}\n"

        if count1 > 0:
            preview1 = "\n".join(
                f"{i + 1}. {(item[0].message.message or '')[:50]}..."
                for i, item in enumerate(list(translation_queue)[:5])
            )
            msg += f"\n**العادي | Normal:**\n{preview1}\n"

        if count2 > 0:
            preview2 = "\n".join(
                f"{i + 1}. {item[:50]}..."
                for i, item in enumerate(list(hourly_queue)[-5:])
            )
            msg += f"\n**الساعة | Hourly:**\n{preview2}"

        await event.reply(msg)
        return

    if action == "stats":
        await event.reply(
            "📈 **إحصاءات النشر | Publishing Statistics**\n\n"
            f"📊 المجموع | Total: {stats['posts']}\n"
            f"📊 اقتصادي | Economic: {stats['economic']}\n"
            f"⚡ فوري | Immediate: {stats['immediate']}\n"
            f"📝 مجدول | Scheduled: {stats['scheduled']}\n"
            f"🧠 تحليل | Analysis: {stats['analysis']}\n"
            f"⏰ موجز | Hourly: {stats['hourly']}\n"
            f"🤖 OpenAI: {stats['openai_usage']}\n"
            f"✨ Gemini: {stats['gemini_usage']}\n"
            f"⏳ FloodWait: {stats['flood_waits']}"
        )
        return

    if action == "channels":
        await event.reply(
            "📡 **القنوات | Channels**\n\n"
            f"📥 المصدر 1 | Source 1: `{SOURCE_CHANNEL_ID}`\n"
            f"📥 المصدر 2 | Source 2: `{SOURCE_CHANNEL_2_ID}`\n"
            f"📤 الهدف | Target: `{TARGET_CHANNEL_ID}`\n"
            f"🧠 التحليل | Analysis: `{ANALYST_TARGET_ID or 'غير مفعل | OFF'}`\n"
            f"⏰ موجز المصدر | Hourly Source: `{HOURLY_SOURCE_ID or 'غير مفعل | OFF'}`\n"
            f"⏰ موجز الهدف | Hourly Target: `{HOURLY_TARGET_ID or 'غير مفعل | OFF'}`\n"
            f"🎛️ التحكم | Control: `{CONTROL_CHANNEL_ID}`"
        )
        return

    if action == "settings":
        await event.reply(settings_status())
        return

    if action == "save_settings":
        ok = save_runtime_settings()
        await event.reply(
            "💾 **تم حفظ إعدادات التحكم.**" if ok
            else "❌ **فشل حفظ إعدادات التحكم.**"
        )
        return

    if action == "publish_next":
        if not bot_active:
            await event.reply("⚠️ البوت متوقف. أرسل `تفعيل` أولًا.")
            return
        if not translation_queue:
            await event.reply("📭 **المكدس العادي فارغ.**")
            return
        try:
            event_item, emoji, _, _ = translation_queue.popleft()
            cleaned = clean_text(event_item.message.message or "")
            final_text = await format_final_text(cleaned, emoji)
            sent = await forward_or_send(event_item.message, final_text, "نشر يدوي من المكدس")
            if sent:
                await event.reply("✅ **تم نشر العنصر التالي يدويًا من المكدس.**")
            else:
                await event.reply("⚠️ **تعذر نشر العنصر التالي.**")
        except Exception as e:
            logging.exception("Manual queued publish failed")
            await event.reply(f"❌ **فشل النشر اليدوي:** `{str(e)[:150]}`")
        return

    if action == "clear_queue":
        count1 = len(translation_queue)
        count2 = len(hourly_queue)
        translation_queue.clear()
        hourly_queue.clear()
        await event.reply(
            f"🧹 **تم مسح المكدسات | Queues Cleared**\n\n"
            f"📨 Normal: {count1}\n⏰ Hourly: {count2}\n📊 Total: {count1 + count2}"
        )
        return

    if action == "reset":
        before = len(posted_texts)
        posted_texts.clear()
        await event.reply(f"♻️ **تمت إعادة التعيين | Reset Complete**\n\n🗑️ Cleared {before} temporary records.")
        return

    if action == "dry_on":
        dry_run_mode = True
        logging.info("🧪 تم تفعيل وضع التجربة.")
        await event.reply("🧪 **تم تفعيل وضع التجربة**\n\nNo messages will be actually published.")
        return

    if action == "dry_off":
        dry_run_mode = False
        logging.info("🚀 تم إيقاف وضع التجربة.")
        await event.reply("🚀 **تم إيقاف وضع التجربة**\n\nReal publishing is now active.")
        return

    if action == "help":
        help_msg = (
            "🛠️ **دليل التحكم المتقدم | Advanced Control**\n\n"
            "🤖 **التشغيل العام**\n"
            "`تفعيل` / `start` — تشغيل جميع المعالجات\n"
            "`ايقاف` / `stop` — إيقاف المعالجة والنشر\n\n"

            "⚡ **أنماط النشر**\n"
            "`نشر فوري on/off` — تشغيل/إيقاف الأخبار الفورية\n"
            "`اقتصادي on/off` — تشغيل/إيقاف البيانات الاقتصادية\n"
            "`تحليل on/off` — تشغيل/إيقاف قناة التحليل\n"
            "`مجدول on/off` — تشغيل/إيقاف طابور النشر المجدول\n"
            "`موجز on/off` — تشغيل/إيقاف موجز الساعة\n"
            "`موجز الآن` — إنشاء موجز يدويًا\n"
            "`نشر التالي` — نشر أول عنصر في المكدس فورًا\n\n"

            "⚙️ **التحكم الدقيق بالإعدادات**\n"
            "`إعدادات` — عرض كل القيم الحالية\n"
            "`إعداد مشاهدات الفوري 50` — لا يعتبر الفوري جاهزًا قبل 50 مشاهدة\n"
            "`إعداد مهلة الفوري 10m` — نشر الفوري بعد 10 دقائق كحد أقصى\n"
            "`إعداد مشاهدات التالي 800` — انتظار 800 مشاهدة قبل المنشور التالي\n"
            "`إعداد فاصل النشر 10s` — تأخير بين المنشورات\n"
            "`إعداد فحص المشاهدات 60s` — تكرار فحص المشاهدات\n"
            "`إعداد فاصل التحليل 15m` — أقل مدة بين منشورات التحليل\n"
            "`إعداد حد الموجز 120` — الحد الأقصى لكلمات موجز الساعة\n"
            "💡 المدد تقبل `s` ثوانٍ، `m` دقائق، `h` ساعات.\n"
            "💾 الإعدادات تُحفظ تلقائيًا في ملف runtime وتبقى بعد إعادة التشغيل.\n\n"

            "📊 **المراقبة**\n"
            "`حالة` — حالة التشغيل والطوابير\n"
            "`إعدادات` — تفاصيل إعدادات النشر\n"
            "`مفاتيح` — حالة مفاتيح AI\n"
            "`مكدس` — محتوى الطوابير\n"
            "`إحصاء` — إحصاءات النشر واستخدام AI\n"
            "`قنوات` — القنوات المرتبطة\n\n"

            "🧹 **الصيانة والاختبار**\n"
            "`مسح المخزن` — تفريغ الطوابير\n"
            "`إعادة تعيين` — مسح سجل منع التكرار المؤقت\n"
            "`وضع تجربة on/off` — اختبار دون نشر فعلي\n"
            "`حفظ الإعدادات` — حفظ الإعدادات يدويًا\n\n"

            "🧪 **أمثلة عملية**\n"
            "• رفع سرعة الفوري: `إعداد مشاهدات الفوري 10` + `إعداد مهلة الفوري 3m`\n"
            "• جعل النشر أكثر تحفظًا: `إعداد مشاهدات التالي 1500`\n"
            "• تقليل ضغط Telegram: `إعداد فاصل النشر 30s`\n"
            "• تحليل كل 30 دقيقة: `إعداد فاصل التحليل 30m`\n\n"

            "🌐 جميع الأوامر الأساسية تعمل بالعربية والإنجليزية."
        )
        await event.reply(help_msg)
        return

    # أمر غير معروف
    await event.reply(
        "❓ **أمر غير معروف | Unknown Command**\n\n"
        "🇸🇦 أرسل `مساعدة` لعرض الأوامر.\n"
        "🇬🇧 Send `help` to show all commands."
    )

# ---------------- معالجة المصادر ----------------
async def handle_source(event, emoji):
    global bot_active, last_immediate_post_id, last_immediate_post_time, publish_immediate, publish_economic
    
    if not bot_active:
        return
    message = event.message
    if message.action:
        return
    text = message.message or ""
    cleaned = clean_text(text)
    
    if publish_economic and is_economic_data(cleaned):
        final_text = await format_final_text(cleaned, emoji)
        sent = await forward_or_send(message, final_text, "نشر فوري (اقتصادي)")
        if sent:
            last_immediate_post_id = sent.id
            last_immediate_post_time = datetime.now()
        return
    
    if not publish_economic and is_economic_data(cleaned):
        logging.info(f"🚫 تم تجاهل بيانات اقتصادية ID={message.id}")
        return

    text_lower = cleaned.lower()
    if publish_immediate and any(keyword.lower() in text_lower for keyword in KEYWORDS_LIST):
        can_publish = await can_publish_immediate()
        if can_publish:
            final_text = await format_final_text(cleaned, emoji)
            sent = await forward_or_send(message, final_text, "نشر فوري")
            if sent:
                last_immediate_post_id = sent.id
                last_immediate_post_time = datetime.now()
        else:
            translation_queue.append((event, emoji, None, None))
            logging.info(f"⏳ تأجيل (لا تحقق شروط الفوري) ID={message.id}")
        return

    translation_queue.append((event, emoji, None, None))
    logging.info(f"📥 أُضيفت الرسالة ID={message.id} للمكدس")

# ---------------- معالجة مصدر موجز الساعة ----------------
async def handle_hourly_source(event):
    global bot_active, publish_hourly
    if not bot_active or not publish_hourly:
        return
    message = event.message
    if message.action:
        return
    text = message.message or ""
    cleaned = clean_text(text)
    if is_meaningful_text(cleaned):
        hourly_queue.append(cleaned)
        logging.info(f"🕗 أُضيفت رسالة إلى مكدس موجز الساعة ID={message.id}")

# ---------------- القناة التحليلية ----------------
analyst_last_post_time = 0

async def analyst_handler(event):
    global bot_active, analyst_last_post_time, publish_analysis
    
    if not bot_active or not publish_analysis or not ANALYST_TARGET_ID:
        return

    message = event.message
    if message.action:
        return

    current_time = datetime.now().timestamp()
    if current_time - analyst_last_post_time < ANALYST_POST_INTERVAL:
        return

    text = message.message or ""
    cleaned = clean_text(text)
    result = await analyze_and_translate(cleaned, "ar")
    signature = os.getenv("ANALYST_SIGNATURE", "— تحليل")
    final_text = f"{EMOJI_ALERT} {result['translation']}\n\n{signature}\n\n{CHANNEL_WATERMARK}"
    sent = await forward_or_send(message, final_text, "نشر تحليل", target_channel=ANALYST_TARGET_ID)
    
    if sent:
        analyst_last_post_time = current_time

# ---------------- إنشاء موجز الساعة ----------------
async def generate_hourly_summary(manual=False):
    global publish_hourly
    if not publish_hourly or not HOURLY_TARGET_ID:
        return

    if not hourly_queue:
        logging.info("📭 مكدس موجز الساعة فارغ — لن يتم النشر.")
        return

    combined_text = "\n".join(hourly_queue)
    hourly_queue.clear()

    system_prompt = (
        "أنت محرر اقتصادي محترف في عام 2026. حيث ترمب هو رئيس اميركا"
        "لخص الأخبار التالية في موجز ساعة اقتصادي شامل بالعربية. "
        "ركز على التأثيرات الرئيسية، المؤشرات، وتصريحات المسؤولين. "
        f"اجعله جذابًا ومختصرًا (لا يتجاوز {HOURLY_MAX_WORDS} كلمة). "
        "ابدأ بعنوان جذاب مثل: '📊 موجز الساعة الاقتصادية'."
    )
    
    summary = await call_ai_with_fallback(system_prompt, combined_text)
    if not summary:
        summary = f"📊 **موجز الساعة الاقتصادية**\n\nفشل في التوليد. الأصل:\n```{combined_text[:300]}...```"

    signature = HOURLY_SIGNATURE
    final_text = f"{summary}\n\n{signature}\n\n{CHANNEL_WATERMARK}"[:4000]

    class FakeMessage:
        id = int(time.time())
    fake_msg = FakeMessage()

    sent = await forward_or_send(fake_msg, final_text, "نشر موجز ساعة", target_channel=HOURLY_TARGET_ID)
    if sent:
        logging.info("✅ تم نشر موجز الساعة بنجاح.")

# ---------------- جدولة موجز الساعة ----------------
async def hourly_scheduler():
    while True:
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        sleep_seconds = (next_hour - now).total_seconds()
        logging.info(f"😴 سينتظر {sleep_seconds:.0f} ثانية حتى موجز الساعة التالي ({next_hour.strftime('%H:%M')}).")
        await asyncio.sleep(sleep_seconds)
        if bot_active and publish_hourly:
            await generate_hourly_summary()

# ---------------- النشر المجدول ----------------
async def publisher():
    global bot_active, publish_scheduled
    last_post_id = None
    while True:
        if not bot_active or not publish_scheduled:
            await asyncio.sleep(5)
            continue
        try:
            event, emoji, _, _ = translation_queue.popleft()
        except IndexError:
            await asyncio.sleep(1)
            continue
        if last_post_id:
            try:
                last_post = await client.get_messages(TARGET_CHANNEL_ID, ids=last_post_id)
                views = last_post.views or 0
                while views < MIN_VIEWS_FOR_NEXT:
                    await asyncio.sleep(VIEW_CHECK_INTERVAL)
                    last_post = await client.get_messages(TARGET_CHANNEL_ID, ids=last_post_id)
                    views = last_post.views or 0
            except Exception:
                pass
        cleaned = clean_text(event.message.message or "")
        final_text = await format_final_text(cleaned, emoji)
        sent = await forward_or_send(event.message, final_text, "نشر مجدول")
        if sent:
            last_post_id = sent.id
        await asyncio.sleep(PUBLISHER_DELAY)

# ---------------- حلقة التشغيل الرئيسية المستمرة ----------------
async def main():
    global SOURCE_CHANNEL_ID, SOURCE_CHANNEL_2_ID, TARGET_CHANNEL_ID, ANALYST_TARGET_ID, ANALYST_SOURCE_ID, CONTROL_CHANNEL_ID, HOURLY_SOURCE_ID, HOURLY_TARGET_ID
    
    handlers_registered = False
    background_tasks_started = False

    while True:
        try:
            if not client.is_connected():
                await client.start()
            
            me = await client.get_me()
            logging.info(f"✅ تسجيل الدخول باسم: {me.first_name}")
            
            # تسجيل المستمعات (Handlers) مرة واحدة فقط
            if not handlers_registered:
                CONTROL_CHANNEL_ID = await resolve_channel(CONTROL_CHANNEL)
                SOURCE_CHANNEL_ID = await resolve_channel(SOURCE_CHANNEL)
                SOURCE_CHANNEL_2_ID = await resolve_channel(SOURCE_CHANNEL_2)
                TARGET_CHANNEL_ID = await resolve_channel(TARGET_CHANNEL)
                if ANALYST_SOURCE:
                    ANALYST_SOURCE_ID = await resolve_channel(ANALYST_SOURCE)
                if ANALYST_TARGET:
                    ANALYST_TARGET_ID = await resolve_channel(ANALYST_TARGET)
                if HOURLY_SOURCE:
                    HOURLY_SOURCE_ID = await resolve_channel(HOURLY_SOURCE)
                if HOURLY_TARGET:
                    HOURLY_TARGET_ID = await resolve_channel(HOURLY_TARGET)
                
                logging.info(f"✅ القنوات جاهزة: تحكم={CONTROL_CHANNEL_ID}")

                client.add_event_handler(control_handler, events.NewMessage(chats=[CONTROL_CHANNEL_ID]))
                client.add_event_handler(lambda e: handle_source(e, EMOJI_IMMEDIATE), events.NewMessage(chats=[SOURCE_CHANNEL_ID]))
                client.add_event_handler(lambda e: handle_source(e, EMOJI_SCHEDULED), events.NewMessage(chats=[SOURCE_CHANNEL_2_ID]))
                
                if ANALYST_SOURCE_ID and ANALYST_TARGET_ID:
                    client.add_event_handler(analyst_handler, events.NewMessage(chats=[ANALYST_SOURCE_ID]))
                
                if HOURLY_SOURCE_ID:
                    client.add_event_handler(handle_hourly_source, events.NewMessage(chats=[HOURLY_SOURCE_ID]))

                handlers_registered = True

            # تشغيل المهام الخلفية (المجدول) مرة واحدة فقط في الخلفية بشكل آمن
            if not background_tasks_started:
                asyncio.create_task(publisher())
                asyncio.create_task(hourly_scheduler())
                background_tasks_started = True

            logging.info("🤖 EcoPulse Bot جاهز ومستقر — في انتظار الأوامر والبيانات...")
            
            # سيبقى البوت يعمل هنا طالما الاتصال مستقر
            await client.run_until_disconnected()
            
            # في حال توقف الاتصال وخروج run_until_disconnected
            logging.warning("⚠️ تم فصل الاتصال بالشبكة. جاري محاولة إعادة الاتصال بعد 5 دقائق (300 ثانية)...")
            await asyncio.sleep(30)

        except Exception as e:
            # التقاط أي أخطاء حرجة أو Timeouts لضمان بقاء السكربت حياً
            logging.error(f"❌ حدث خطأ غير متوقع أدى لفصل البوت: {e}")
            logging.info("⏳ جاري الانتظار لمدة 5 دقائق قبل إعادة التشغيل التلقائي...")
            await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 تم إيقاف البوت يدوياً عن طريق المستخدم.")
