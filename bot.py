#!/usr/bin/env python3
"""
EcoPulse Bot — النسخة النهائية المستقرة مع دعم موجز الساعة
✅ قناة تحكم ثابتة (من .env)
✅ جميع الأوامر تعمل فورًا
✅ استجابة تلقائية لأي رسالة غير معروفة
✅ كشف دقيق للبيانات الاقتصادية
✅ نشر فوري مشروط (600 مشاهدة أو 8 دقائق)
✅ موجز ساعة اقتصادي تلقائي
"""

import asyncio
import os
import logging
import re
import time
from datetime import datetime, timedelta
from collections import deque
from collections import defaultdict

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from dotenv import load_dotenv
from openai import OpenAI

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
HOURLY_SOURCE = os.getenv("HOURLY_SOURCE", "")  # ← جديد
HOURLY_TARGET = os.getenv("HOURLY_TARGET", "")  # ← جديد

ANALYST_SOURCE_ID = None
ANALYST_TARGET_ID = None
HOURLY_SOURCE_ID = None
HOURLY_TARGET_ID = None

# ---------------- إعدادات النشر ----------------
IMMEDIATE_MIN_VIEWS = 600
IMMEDIATE_TIMEOUT = 8 * 60
MIN_VIEWS_FOR_NEXT = int(os.getenv("MIN_VIEWS_FOR_NEXT", "800"))

# ---------------- مفاتيح OpenAI ----------------
API_KEYS = os.getenv("OPENAI_API_KEYS", "").split(",")
if not API_KEYS or API_KEYS == [""]:
    raise ValueError("❌ لم يتم العثور على مفاتيح OpenAI في ملف .env")

# ---------------- إعدادات عامة ----------------
KEYWORDS_LIST = ["JUST IN", "MACRO", "$MACRO", "marco", "FEDERAL", "POWELL", "powell", "TRUMP", "FED'S", "FED", "🔴"]
EMOJI_IMMEDIATE = "🚨"
EMOJI_SCHEDULED = "📝"
EMOJI_ALERT = "⚠️🚨"
EMOJI_HOURLY = "⏰"
CHANNEL_WATERMARK = " "
HOURLY_SIGNATURE = os.getenv("HOURLY_SIGNATURE", "— موجز الساعة")  # ← جديد

# ---------------- التهيئة ----------------
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
translation_queue = deque()
hourly_queue = deque()  # ← جديد: مكدس أخبار موجز الساعة
posted_texts = set()
MAX_POSTED_HISTORY = 100

# === متغيرات التحكم ===
bot_active = False
publish_immediate = True      # النشر الفوري (غير الاقتصادي)
publish_economic = True       # البيانات الاقتصادية
publish_analysis = True       # قناة التحليل
publish_scheduled = True      # الناشر المجدول
publish_hourly = True         # ← جديد: موجز الساعة
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
    "hourly": 0,  # ← جديد
    "flood_waits": 0
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot_activity.log", "a", encoding="utf-8")]
)

# ---------------- إدارة مفاتيح OpenAI ----------------
class OpenAIManager:
    def __init__(self, keys):
        self.keys = [k.strip() for k in keys if k.strip()]
        if not self.keys:
            raise ValueError("❌ لا توجد مفاتيح OpenAI صالحة")
        self.index = 0
        self.failed_keys = {}
        self.failure_cooldown = 3600
        self.usage_stats = defaultdict(int)
        logging.info(f"Intialized OpenAIManager with {len(self.keys)} keys")

    def _get_usable_keys(self):
        now = time.time()
        usable = []
        for key in self.keys:
            fail_time = self.failed_keys.get(key)
            if not fail_time or (now - fail_time) > self.failure_cooldown:
                usable.append(key)
        return usable

    def get_client(self):
        usable_keys = self._get_usable_keys()
        if not usable_keys:
            logging.warning("⚠️ جميع المفاتيح معطّلة — إعادة تفعيل الجميع")
            self.failed_keys.clear()
            usable_keys = self.keys

        key = usable_keys[self.index % len(usable_keys)]
        self.index += 1
        self.usage_stats[key] += 1
        logging.debug(f"🔑 استخدام مفتاح: {key[:5]}... (الاستخدام: {self.usage_stats[key]})")
        return OpenAI(api_key=key)

    def mark_failed(self, key: str, error: str = ""):
        self.failed_keys[key] = time.time()
        logging.warning(f"🚫 مفتاح معطّل: {key[:5]}... — {error}")
        usable = self._get_usable_keys()
        logging.info(f"📊 حالة المفاتيح: {len(usable)}/{len(self.keys)} نشطة")

    def get_status(self) -> str:
        usable = self._get_usable_keys()
        failed = [k for k in self.keys if k not in usable]
        return (
            f"🔑 المفاتيح: {len(self.keys)} | نشطة: {len(usable)} | معطّلة: {len(failed)}\n"
            f"📈 الاستخدام: {dict(self.usage_stats)}\n"
            f"❌ المعطّلة: {[k[:5]+'...' for k in failed]}"
        )

openai_manager = OpenAIManager(API_KEYS)

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
    """
    يتحقق مما إذا كان النص يحتوي على محتوى ذي معنى (ليس فقط روابط، رموز، أو فراغات).
    """
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

# ---------------- تحليل وترجمة ----------------
async def analyze_and_translate(text: str, target_lang: str, max_retries: int = 6, retry_delay: int = 5) -> dict:
    if not text:
        return {"impact": "⚪ تأثير محايد", "translation": ""}

    attempt = 0
    while attempt < max_retries:
        client_ai = openai_manager.get_client()
        try:
            response = client_ai.chat.completions.create(
               # model="gpt-4o-mini",
                model="gpt-5.2",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "أنت محلل اقتصادي ومترجم محترف في عام 2026 حيث ترامب هو رئيس امريكا. "
                            "حلّل الخبر، ثم أعد صياغته بالعربية بأسلوب اقتصادي مختصر. "
                            "أولاً، قدم تقييمًا للتأثير من كلمتين إلى أربع. "
                            "ثم ضع ### ثم أعد الصياغة بالعربية."
                        ),
                    },
                    {"role": "user", "content": text},
                ],
                temperature=0.3,
            )

            content = response.choices[0].message.content.strip()
            parts = content.split("###", 1)
            impact = parts[0].strip() if parts else "⚪ تأثير محايد"
            translation = parts[1].strip() if len(parts) > 1 else text
            return {"impact": impact, "translation": translation}
        except Exception as e:
            error_str = str(e)
            logging.warning(f"❌ محاولة {attempt + 1} فشلت: {error_str[:100]}...")
            if 'client_ai' in locals() and hasattr(client_ai, 'api_key'):
                openai_manager.mark_failed(client_ai.api_key, error_str)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                logging.error("⚠️ فشل التحليل بعد جميع المحاولات.")
                return {"impact": "⚪ تأثير محايد", "translation": text}
            attempt += 1

# ---------------- تنسيق المنشور ----------------
async def format_final_text(text: str, emoji: str, signature: str = None, attention=False) -> str:
    if signature is None:
        signature = os.getenv("SIGNATURE", "— EcoPulse")

    cleaned = clean_text(text)
    if not is_meaningful_text(cleaned):
        logging.debug("🗑️ تم تجاهل نص غير ذي معنى في التنسيق")
        return ""

    client_ai = openai_manager.get_client()

    if is_economic_data(text):
        logging.info("📡 كشف بيانات اقتصادية")
        try:
            response = client_ai.chat.completions.create(
                model="gpt-5.2",
                messages=[
                    {
                        "role": "system",
                        "content":(
                            "أنت محرر أخبار اقتصادية محترف. "
                            "استخرج البيانات واعرضها بالقالب:\n"
                            "🔴 صدر الآن :\n\n"
                            "💠 {الدولة}\n"
                            "🔵 {المؤشر}\n\n"
                            "🕒 السابق :\n"
                            "🕒 التقدير :\n"
                            "🕓 الحالي :\n\n"
                            "👈 النتيجة : تحليل ≤ 9 كلمات."
                        ),
                    },
                    {"role": "user", "content": text},
                ],
                temperature=0.5,
            )
            translation = response.choices[0].message.content.strip()
        except Exception as e:
            error_str = str(e)
            logging.warning(f"⚠️ فشل في معالجة ACTUAL: {error_str[:100]}...")
            if 'client_ai' in locals() and hasattr(client_ai, 'api_key'):
                openai_manager.mark_failed(client_ai.api_key, error_str)
            fallback = f"🔴 **بيانات اقتصادية**\n\n```{clean_text(text)[:200]}...```\n\n{signature}"
            return fallback

        final_text = f"{translation}\n\n{signature}\n\n{CHANNEL_WATERMARK}"
        return final_text[:4000]

    elif "MACRO" in text.upper():
        try:
            response = client_ai.chat.completions.create(
                model="gpt-5.2",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "أنت محلل اقتصادي حيث ترامب هو الرئيس الحالي لامريكا. قم بتحليل الخبر بالعربية ≤ 10 كلمات."
                        ),
                    },
                    {"role": "user", "content": text},
                ],
                temperature=0.7,
            )
            translation = response.choices[0].message.content.strip()
        except Exception as e:
            error_str = str(e)
            logging.warning(f"⚠️ فشل في التحليل (MACRO): {error_str[:100]}...")
            if 'client_ai' in locals() and hasattr(client_ai, 'api_key'):
                openai_manager.mark_failed(client_ai.api_key, error_str)
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

# ---------------- معالجة التحكم (مرتبطة بقناة التحكم الثابتة) ----------------
@client.on(events.NewMessage(chats=[]))  # سيتم ربطها في main()
async def control_handler(event):
    global bot_active, publish_immediate, publish_economic, publish_analysis, publish_scheduled, publish_hourly, dry_run_mode
    
    raw_text = event.raw_text.strip()
    if not raw_text:
        raw_text = "مساعدة"
    
    text = raw_text
    
    # === التحكم العام ===
    if "تفعيل" in text:
        bot_active = True
        logging.info("✅ تم تفعيل البوت كاملاً.")
        await event.reply("✅ تم تفعيل البوت كاملاً.")
    
    elif "ايقاف" in text:
        bot_active = False
        logging.info("⛔ تم إيقاف البوت كاملاً.")
        await event.reply("⛔ تم إيقاف البوت كاملاً.")
    
    # === التحكم الجزئي ===
    elif "نشر فوري on" in text:
        publish_immediate = True
        logging.info("✅ تم تفعيل النشر الفوري (غير الاقتصادي).")
        await event.reply("✅ تم تفعيل النشر الفوري (غير الاقتصادي).")
    
    elif "نشر فوري off" in text:
        publish_immediate = False
        logging.info("⛔ تم إيقاف النشر الفوري (غير الاقتصادي).")
        await event.reply("⛔ تم إيقاف النشر الفوري (غير الاقتصادي).")
    
    elif "اقتصادي on" in text:
        publish_economic = True
        logging.info("✅ تم تفعيل معالجة البيانات الاقتصادية.")
        await event.reply("✅ تم تفعيل معالجة البيانات الاقتصادية.")
    
    elif "اقتصادي off" in text:
        publish_economic = False
        logging.info("⛔ تم إيقاف معالجة البيانات الاقتصادية.")
        await event.reply("⛔ تم إيقاف معالجة البيانات الاقتصادية.")
    
    elif "تحليل on" in text:
        publish_analysis = True
        logging.info("✅ تم تفعيل قناة التحليل.")
        await event.reply("✅ تم تفعيل قناة التحليل.")
    
    elif "تحليل off" in text:
        publish_analysis = False
        logging.info("⛔ تم إيقاف قناة التحليل.")
        await event.reply("⛔ تم إيقاف قناة التحليل.")
    
    elif "مجدول on" in text:
        publish_scheduled = True
        logging.info("✅ تم تفعيل الناشر المجدول.")
        await event.reply("✅ تم تفعيل الناشر المجدول.")
    
    elif "مجدول off" in text:
        publish_scheduled = False
        logging.info("⛔ تم إيقاف الناشر المجدول.")
        await event.reply("⛔ تم إيقاف الناشر المجدول.")
    
    # === التحكم بموجز الساعة ===
    elif "موجز on" in text:
        publish_hourly = True
        logging.info("✅ تم تفعيل موجز الساعة.")
        await event.reply("✅ تم تفعيل موجز الساعة.")
    
    elif "موجز off" in text:
        publish_hourly = False
        logging.info("⛔ تم إيقاف موجز الساعة.")
        await event.reply("⛔ تم إيقاف موجز الساعة.")
    
    elif "موجز الآن" in text:
        if not publish_hourly:
            await event.reply("⚠️ موجز الساعة معطّل حاليًا. أرسل `موجز on` أولًا.")
        else:
            await generate_hourly_summary(manual=True)
            await event.reply("✅ تم طلب إنشاء موجز الساعة يدويًا.")

    # === المراقبة ===
    elif "حالة" in text:
        status = (
            f"📊 **حالة البوت**\n"
            f"- نشط: {'✅' if bot_active else '⛔'}\n"
            f"- نشر فوري: {'✅' if publish_immediate else '⛔'}\n"
            f"- اقتصادي: {'✅' if publish_economic else '⛔'}\n"
            f"- تحليل: {'✅' if publish_analysis else '⛔'}\n"
            f"- مجدول: {'✅' if publish_scheduled else '⛔'}\n"
            f"- موجز ساعة: {'✅' if publish_hourly else '⛔'}\n"  # ← جديد
            f"- مكدس عادي: {len(translation_queue)}\n"
            f"- مكدس ساعة: {len(hourly_queue)}\n"
            f"- وضع تجربة: {'🧪' if dry_run_mode else '🚀'}"
        )
        await event.reply(status)
    
    elif "مفاتيح" in text:
        status = openai_manager.get_status()
        await event.reply(f"🔧 **حالة مفاتيح OpenAI**\n\n{status}")
    
    elif "مكدس" in text:
        count1 = len(translation_queue)
        count2 = len(hourly_queue)
        msg = f"📥 **المكدس العادي**: {count1} رسالة\n"
        msg += f"🕗 **مكدس موجز الساعة**: {count2} رسالة\n\n"
        if count1 > 0:
            preview1 = "\n".join([f"{i+1}. {item[0].message.message[:30]}..." for i, item in enumerate(list(translation_queue)[:3])])
            msg += f"**العادي**:\n{preview1}\n\n"
        if count2 > 0:
            preview2 = "\n".join([f"{i+1}. {msg[:30]}..." for i, msg in enumerate(list(hourly_queue)[-3:])])
            msg += f"**موجز الساعة**:\n{preview2}"
        await event.reply(msg)
    
    elif "إحصاء" in text:
        await event.reply(
            f"📈 **إحصاءات النشر**\n"
            f"- المجموع: {stats['posts']}\n"
            f"- اقتصادي: {stats['economic']}\n"
            f"- فوري: {stats['immediate']}\n"
            f"- مجدول: {stats['scheduled']}\n"
            f"- تحليل: {stats['analysis']}\n"
            f"- موجز ساعة: {stats['hourly']}\n"  # ← جديد
            f"- تجميد: {stats['flood_waits']}"
        )
    
    elif "قنوات" in text:
        await event.reply(
            f"📡 **القنوات الحالية**\n"
            f"- المصدر 1: `{SOURCE_CHANNEL_ID}`\n"
            f"- المصدر 2: `{SOURCE_CHANNEL_2_ID}`\n"
            f"- الهدف: `{TARGET_CHANNEL_ID}`\n"
            f"- تحليل: `{ANALYST_TARGET_ID or 'غير مفعل'}`\n"
            f"- موجز مصدر: `{HOURLY_SOURCE_ID or 'غير مفعل'}`\n"
            f"- موجز هدف: `{HOURLY_TARGET_ID or 'غير مفعل'}`\n"
            f"- التحكم: `{CONTROL_CHANNEL_ID}`"
        )
    
    # === الصيانة ===
    elif "مسح المخزن" in text:
        count1 = len(translation_queue)
        count2 = len(hourly_queue)
        translation_queue.clear()
        hourly_queue.clear()
        await event.reply(f"🧹 تم مسح {count1 + count2} رسالة من المكدسين.")
    
    elif "إعادة تعيين" in text:
        before = len(posted_texts)
        posted_texts.clear()
        await event.reply(f"♻️ تم مسح {before} سجل مؤقت.")
    
    elif "وضع تجربة on" in text:
        dry_run_mode = True
        logging.info("🧪 تم تفعيل وضع التجربة.")
        await event.reply("🧪 تم تفعيل وضع التجربة (لن يُنشر فعليًا).")
    elif "وضع تجربة off" in text:
        dry_run_mode = False
        logging.info("🚀 تم إيقاف وضع التجربة.")
        await event.reply("🚀 تم إيقاف وضع التجربة (النشر الفعلي نشط).")
    
    # === عرض المساعدة الكاملة ===
    elif "مساعدة" in text:
        help_msg = (
            "🛠️ **أوامر التحكم الكاملة**\n"
            "```\n"
            "# التحكم العام\n"
            "تفعيل / ايقاف\n\n"
            "# التحكم الجزئي\n"
            "اقتصادي on/off\n"
            "نشر فوري on/off\n"
            "تحليل on/off\n"
            "مجدول on/off\n"
            "موجز on/off\n"
            "موجز الآن\n\n"
            "# المراقبة\n"
            "حالة\n"
            "مفاتيح\n"
            "مكدس\n"
            "إحصاء\n"
            "قنوات\n\n"
            "# الصيانة\n"
            "مسح المخزن\n"
            "إعادة تعيين\n"
            "وضع تجربة on/off\n"
            "```\n"
            "💡 جميع الأوامر تعمل في قناة التحكم فقط."
        )
        await event.reply(help_msg)

    # === المساعدة التلقائية ===
    else:
        quick_help = (
            "🔍 **أمر غير معروف**\n\n"
            "🛠️ الأوامر الأساسية:\n"
            "• `تفعيل` / `ايقاف`\n"
            "• `اقتصادي on` / `off`\n"
            "• `نشر فوري on` / `off`\n"
            "• `تحليل on` / `off`\n"
            "• `مجدول on` / `off`\n"
            "• `موجز on` / `off`\n"
            "• `موجز الآن`\n\n"
            "📌 أرسل **مساعدة** لعرض جميع الأوامر بالتفصيل."
        )
        await event.reply(quick_help)

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
    
    # ✅ 1. البيانات الاقتصادية
    if publish_economic and is_economic_data(cleaned):
        final_text = await format_final_text(cleaned, emoji)
        sent = await forward_or_send(message, final_text, "نشر فوري (اقتصادي)")
        if sent:
            last_immediate_post_id = sent.id
            last_immediate_post_time = datetime.now()
        return
    
    # 🚫 بيانات اقتصادية لكن النشر الاقتصادي متوقف
    if not publish_economic and is_economic_data(cleaned):
        logging.info(f"🚫 تم تجاهل بيانات اقتصادية ID={message.id}")
        return

    # ✅ 2. النشر الفوري العادي
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

    # ✅ 3. الباقي
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
ANALYST_POST_INTERVAL = 900
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

    # جمع جميع الأخبار في نص واحد
    combined_text = "\n".join(hourly_queue)
    hourly_queue.clear()  # تفريغ المكدس

    client_ai = openai_manager.get_client()
    try:
        response = client_ai.chat.completions.create(
            model="gpt-5.2",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "أنت محرر اقتصادي محترف في عام 2026. حيث ترمب هو رئيس اميركا"
                        "لخص الأخبار التالية في موجز ساعة اقتصادي شامل بالعربية. "
                        "ركز على التأثيرات الرئيسية، المؤشرات، وتصريحات المسؤولين. "
                        "اجعله جذابًا ومختصرًا (لا يتجاوز 120 كلمة). "
                        "ابدأ بعنوان جذاب مثل: '📊 موجز الساعة الاقتصادية'."
                    ),
                },
                {"role": "user", "content": combined_text},
            ],
            temperature=0.6,
        )
        summary = response.choices[0].message.content.strip()
    except Exception as e:
        error_str = str(e)
        logging.warning(f"⚠️ فشل في إنشاء موجز الساعة: {error_str[:100]}...")
        if hasattr(client_ai, 'api_key'):
            openai_manager.mark_failed(client_ai.api_key, error_str)
        summary = f"📊 **موجز الساعة الاقتصادية**\n\nفشل في التوليد. الأصل:\n```{combined_text[:300]}...```"

    signature = HOURLY_SIGNATURE
    final_text = f"{summary}\n\n{signature}\n\n{CHANNEL_WATERMARK}"[:4000]

    # إنشاء رسالة وهمية لاستخدامها في forward_or_send
    class FakeMessage:
        id = int(time.time())
    fake_msg = FakeMessage()

    sent = await forward_or_send(fake_msg, final_text, "نشر موجز ساعة", target_channel=HOURLY_TARGET_ID)
    if sent:
        logging.info("✅ تم نشر موجز الساعة بنجاح.")

# ---------------- جدولة موجز الساعة ----------------
async def hourly_scheduler():
    """ينشر موجز الساعة كل ساعة عند الدقيقة 00."""
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
                    await asyncio.sleep(60)
                    last_post = await client.get_messages(TARGET_CHANNEL_ID, ids=last_post_id)
                    views = last_post.views or 0
            except Exception:
                pass
        cleaned = clean_text(event.message.message or "")
        final_text = await format_final_text(cleaned, emoji)
        sent = await forward_or_send(event.message, final_text, "نشر مجدول")
        if sent:
            last_post_id = sent.id
        await asyncio.sleep(10)

# ---------------- التشغيل ----------------
async def main():
    global SOURCE_CHANNEL_ID, SOURCE_CHANNEL_2_ID, TARGET_CHANNEL_ID, ANALYST_TARGET_ID, ANALYST_SOURCE_ID, CONTROL_CHANNEL_ID, HOURLY_SOURCE_ID, HOURLY_TARGET_ID
    
    await client.start()
    me = await client.get_me()
    logging.info(f"✅ تسجيل الدخول باسم: {me.first_name}")
    
    # ✅ تهيئة جميع القنوات من .env (ثابتة)
    try:
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
    except Exception as e:
        logging.critical(f"❌ فشل تهيئة القنوات: {e}")
        return
    
    # ✅ ربط ثابت بقناة التحكم (من .env فقط)
    client.add_event_handler(control_handler, events.NewMessage(chats=[CONTROL_CHANNEL_ID]))
    
    # ✅ ربط المصادر
    client.add_event_handler(lambda e: handle_source(e, EMOJI_IMMEDIATE), events.NewMessage(chats=[SOURCE_CHANNEL_ID]))
    client.add_event_handler(lambda e: handle_source(e, EMOJI_SCHEDULED), events.NewMessage(chats=[SOURCE_CHANNEL_2_ID]))
    
    # ✅ ربط قناة التحليل
    if ANALYST_SOURCE_ID and ANALYST_TARGET_ID:
        client.add_event_handler(
            analyst_handler,
            events.NewMessage(chats=[ANALYST_SOURCE_ID])
        )
    
    # ✅ ربط مصدر موجز الساعة
    if HOURLY_SOURCE_ID:
        client.add_event_handler(handle_hourly_source, events.NewMessage(chats=[HOURLY_SOURCE_ID]))

    logging.info("🤖 EcoPulse Bot جاهز — في انتظار الأوامر في قناة التحكم.")
    # تشغيل الجدولة والمراقبة بالتوازي
    await asyncio.gather(
        publisher(),
        hourly_scheduler(),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 تم إيقاف البوت يدوياً.")
    except Exception as e:
        logging.critical(f"💥 خطأ فادح: {e}", exc_info=True)


