"""Аварийный двусторонний мост MAX Messenger <-> Telegram

Обеспечивает сквозную маршрутизацию обращений клиентов из MAX в Telegram-группу/чат
операторов дежурной смены ОППО на время аварий и техработ в ELMA365.
"""

import os
import sys
import asyncio
import logging
import sqlite3
import io
import csv
import html
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

# Принудительная установка UTF-8 для консоли Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    import httpx
except ImportError:
    print("[ERROR] Не установлена библиотека httpx. Запустите: pip install httpx")
    sys.exit(1)

try:
    from aiohttp import web
except ImportError:
    web = None

# Настройка красивого консольного логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bridge")


def load_env(env_path: str) -> Dict[str, str]:
    """Простая загрузка .env без сторонних библиотек."""
    config = {}
    if not os.path.exists(env_path):
        return config
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            config[k.strip()] = v.strip().strip("'\"")
    return config


# Поиск файла конфигурации: сначала .env, затем .env.bridge
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_file = os.path.join(BASE_DIR, ".env")
if not os.path.exists(env_file):
    env_file = os.path.join(BASE_DIR, ".env.bridge")

cfg = load_env(env_file)

MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN") or cfg.get("MAX_BOT_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or cfg.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_OPERATOR_CHAT_ID = os.getenv("TELEGRAM_OPERATOR_CHAT_ID") or cfg.get("TELEGRAM_OPERATOR_CHAT_ID", "")
MAX_API_BASE_URL = (os.getenv("MAX_API_BASE_URL") or cfg.get("MAX_API_BASE_URL", "https://botapi.max.ru")).rstrip("/")

# Валидация настроек
if not MAX_BOT_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_OPERATOR_CHAT_ID:
    print("\n" + "=" * 65)
    print("[!] ОШИБКА КОНФИГУРАЦИИ: Заполните переменные в файле .env:")
    print("   1. MAX_BOT_TOKEN")
    print("   2. TELEGRAM_BOT_TOKEN")
    print("   3. TELEGRAM_OPERATOR_CHAT_ID")
    print(f"   Путь к файлу: {env_file}")
    print("=" * 65 + "\n")
    sys.exit(1)

try:
    TG_CHAT_ID = int(TELEGRAM_OPERATOR_CHAT_ID)
except ValueError:
    logger.error("TELEGRAM_OPERATOR_CHAT_ID должен быть целым числом (например, 123456789 или -1001234567890)!")
    sys.exit(1)

USE_FORUM_TOPICS = (os.getenv("USE_FORUM_TOPICS") or cfg.get("USE_FORUM_TOPICS", "true")).strip().lower() in ("true", "1", "yes")

# Режим транспорта: polling (по умолчанию) или webhook
TRANSPORT_MODE = (os.getenv("TRANSPORT_MODE") or cfg.get("TRANSPORT_MODE", "polling")).strip().lower()
WEBHOOK_BASE_URL = (os.getenv("WEBHOOK_BASE_URL") or cfg.get("WEBHOOK_BASE_URL", "")).rstrip("/")
try:
    WEBHOOK_PORT = int(os.getenv("PORT") or os.getenv("WEBHOOK_PORT") or cfg.get("WEBHOOK_PORT", "8080"))
except ValueError:
    WEBHOOK_PORT = 8080
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") or cfg.get("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET") or cfg.get("WEBHOOK_SECRET", "")

# База данных истории и маппинга
DB_PATH = os.path.join(BASE_DIR, "bridge_history.db")


class Storage:
    """Хранилище истории, топиков и связок сообщений в SQLite."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            # Таблица персональных топиков в супергруппе: MAX user_id -> message_thread_id
            conn.execute("""
                CREATE TABLE IF NOT EXISTS client_topics (
                    max_user_id INTEGER PRIMARY KEY,
                    message_thread_id INTEGER NOT NULL,
                    topic_name TEXT,
                    created_at TEXT NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_client_topics_thread ON client_topics (message_thread_id);")
            # Таблица маппинга TG message_id -> MAX user_id
            conn.execute("""
                CREATE TABLE IF NOT EXISTS message_links (
                    tg_message_id INTEGER PRIMARY KEY,
                    max_user_id INTEGER NOT NULL,
                    max_user_name TEXT,
                    created_at TEXT NOT NULL
                );
            """)
            # Таблица полной истории диалогов
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dialogue_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    max_user_id INTEGER NOT NULL,
                    max_user_name TEXT,
                    operator_name TEXT,
                    text TEXT,
                    has_attachments INTEGER DEFAULT 0,
                    attachment_info TEXT,
                    tg_message_id INTEGER,
                    ticket_id INTEGER
                );
            """)
            # Таблица заявок (ServiceDesk)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_number INTEGER UNIQUE,
                    max_user_id INTEGER NOT NULL,
                    max_user_name TEXT,
                    message_thread_id INTEGER,
                    status TEXT DEFAULT 'OPEN',
                    assigned_operator TEXT,
                    initial_text TEXT,
                    resolution TEXT,
                    created_at TEXT NOT NULL,
                    first_response_at TEXT,
                    closed_at TEXT,
                    rt_seconds INTEGER,
                    mttr_seconds INTEGER
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tickets_user_status ON tickets (max_user_id, status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tickets_thread_status ON tickets (message_thread_id, status);")
            try:
                conn.execute("ALTER TABLE dialogue_history ADD COLUMN ticket_id INTEGER;")
            except sqlite3.OperationalError:
                pass
            conn.commit()

    def save_topic(self, max_user_id: int, message_thread_id: int, topic_name: str):
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO client_topics (max_user_id, message_thread_id, topic_name, created_at) VALUES (?, ?, ?, ?)",
                (max_user_id, message_thread_id, topic_name, now)
            )
            conn.commit()

    def get_topic_by_max_user(self, max_user_id: int) -> Optional[int]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT message_thread_id FROM client_topics WHERE max_user_id = ?",
                (max_user_id,)
            )
            row = cursor.fetchone()
            if row:
                return row[0]
        return None

    def get_topic_name_by_thread(self, message_thread_id: int) -> str:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT topic_name FROM client_topics WHERE message_thread_id = ?",
                (message_thread_id,)
            )
            row = cursor.fetchone()
            if row and row[0]:
                return row[0]
        return f"Топик {message_thread_id}"

    def get_max_user_by_topic(self, message_thread_id: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT max_user_id, topic_name FROM client_topics WHERE message_thread_id = ?",
                (message_thread_id,)
            )
            row = cursor.fetchone()
            if row:
                # Извлекаем имя из topic_name (например "Иван Иванов [MAX: 12345]")
                name = row[1] or f"Клиент {row[0]}"
                if " [MAX:" in name:
                    name = name.split(" [MAX:")[0].strip()
                return {"max_user_id": row[0], "max_user_name": name}
        return None

    def save_link(self, tg_msg_id: int, max_user_id: int, max_user_name: str):
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO message_links (tg_message_id, max_user_id, max_user_name, created_at) VALUES (?, ?, ?, ?)",
                (tg_msg_id, max_user_id, max_user_name, now)
            )
            conn.commit()

    def get_max_user_by_tg_msg(self, tg_msg_id: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT max_user_id, max_user_name FROM message_links WHERE tg_message_id = ?",
                (tg_msg_id,)
            )
            row = cursor.fetchone()
            if row:
                return {"max_user_id": row[0], "max_user_name": row[1]}
        return None

    def delete_topic(self, max_user_id: int):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM client_topics WHERE max_user_id = ?", (max_user_id,))
            conn.commit()

    def update_ticket_thread(self, ticket_id: int, message_thread_id: int):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE tickets SET message_thread_id = ? WHERE id = ?",
                (message_thread_id, ticket_id)
            )
            conn.commit()

    def get_active_ticket(self, max_user_id: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT id, ticket_number, max_user_id, max_user_name, message_thread_id,
                       status, assigned_operator, initial_text, resolution,
                       created_at, first_response_at, closed_at, rt_seconds, mttr_seconds
                FROM tickets
                WHERE max_user_id = ? AND status != 'CLOSED'
                ORDER BY id DESC LIMIT 1
                """,
                (max_user_id,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def get_active_ticket_by_thread(self, message_thread_id: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT id, ticket_number, max_user_id, max_user_name, message_thread_id,
                       status, assigned_operator, initial_text, resolution,
                       created_at, first_response_at, closed_at, rt_seconds, mttr_seconds
                FROM tickets
                WHERE message_thread_id = ? AND status != 'CLOSED'
                ORDER BY id DESC LIMIT 1
                """,
                (message_thread_id,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def get_last_ticket_by_thread(self, message_thread_id: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT id, ticket_number, max_user_id, max_user_name, message_thread_id,
                       status, assigned_operator, initial_text, resolution,
                       created_at, first_response_at, closed_at, rt_seconds, mttr_seconds
                FROM tickets
                WHERE message_thread_id = ?
                ORDER BY id DESC LIMIT 1
                """,
                (message_thread_id,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def create_ticket(
        self,
        max_user_id: int,
        max_user_name: str,
        message_thread_id: Optional[int],
        initial_text: str = ""
    ) -> Dict[str, Any]:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT COALESCE(MAX(ticket_number), 0) + 1 FROM tickets;")
            next_num = cursor.fetchone()[0]
            cursor = conn.execute(
                """
                INSERT INTO tickets
                (ticket_number, max_user_id, max_user_name, message_thread_id, status, initial_text, created_at)
                VALUES (?, ?, ?, ?, 'OPEN', ?, ?)
                """,
                (next_num, max_user_id, max_user_name, message_thread_id, initial_text, now_str)
            )
            ticket_id = cursor.lastrowid
            conn.commit()
            return {
                "id": ticket_id,
                "ticket_number": next_num,
                "max_user_id": max_user_id,
                "max_user_name": max_user_name,
                "message_thread_id": message_thread_id,
                "status": "OPEN",
                "initial_text": initial_text,
                "created_at": now_str,
                "assigned_operator": None,
                "first_response_at": None,
                "closed_at": None,
                "rt_seconds": None,
                "mttr_seconds": None,
                "resolution": None,
            }

    def record_first_response(self, ticket_id_or_user_id: int, operator_name: str) -> Optional[Dict[str, Any]]:
        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id_or_user_id,))
            row = cursor.fetchone()
            if not row:
                cursor = conn.execute(
                    "SELECT * FROM tickets WHERE max_user_id = ? AND status != 'CLOSED' ORDER BY id DESC LIMIT 1",
                    (ticket_id_or_user_id,)
                )
                row = cursor.fetchone()
            if not row or row["first_response_at"]:
                return None

            ticket_id = row["id"]
            created_str = row["created_at"]
            try:
                created_dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                rt_seconds = max(0, int((now_dt - created_dt).total_seconds()))
            except Exception:
                rt_seconds = 0

            conn.execute(
                """
                UPDATE tickets
                SET status = 'IN_PROGRESS',
                    first_response_at = ?,
                    rt_seconds = ?,
                    assigned_operator = ?
                WHERE id = ? AND first_response_at IS NULL
                """,
                (now_str, rt_seconds, operator_name, ticket_id)
            )
            conn.commit()

            cursor = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,))
            updated_row = cursor.fetchone()
            return dict(updated_row) if updated_row else None

    def close_ticket(self, ticket_id: int, operator_name: str, resolution: str) -> Optional[Dict[str, Any]]:
        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,))
            row = cursor.fetchone()
            if not row:
                return None

            created_str = row["created_at"]
            try:
                created_dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                mttr_seconds = max(0, int((now_dt - created_dt).total_seconds()))
            except Exception:
                mttr_seconds = 0

            assigned = row["assigned_operator"] or operator_name
            conn.execute(
                """
                UPDATE tickets
                SET status = 'CLOSED',
                    closed_at = ?,
                    mttr_seconds = ?,
                    resolution = ?,
                    assigned_operator = ?
                WHERE id = ?
                """,
                (now_str, mttr_seconds, resolution, assigned, ticket_id)
            )
            conn.commit()

            cursor = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,))
            updated_row = cursor.fetchone()
            return dict(updated_row) if updated_row else None

    def log_message(
        self,
        direction: str,
        max_user_id: int,
        max_user_name: str,
        text: str,
        operator_name: str = "",
        has_attachments: bool = False,
        attachment_info: str = "",
        tg_message_id: Optional[int] = None,
        ticket_id: Optional[int] = None,
    ):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO dialogue_history 
                (timestamp, direction, max_user_id, max_user_name, operator_name, text, has_attachments, attachment_info, tg_message_id, ticket_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    direction,
                    max_user_id,
                    max_user_name,
                    operator_name,
                    text,
                    1 if has_attachments else 0,
                    attachment_info,
                    tg_message_id,
                    ticket_id,
                )
            )
            conn.commit()

    def export_tickets_csv(self) -> bytes:
        """Экспорт реестра заявок в формат CSV (UTF-8-SIG) для Excel."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id, ticket_number, max_user_id, max_user_name, message_thread_id,
                       status, assigned_operator, initial_text, resolution,
                       created_at, first_response_at, closed_at, rt_seconds, mttr_seconds
                FROM tickets
                ORDER BY ticket_number ASC;
            """)
            rows = cursor.fetchall()

            output = io.StringIO()
            writer = csv.writer(output, delimiter=";")
            headers = [
                "№ заявки", "MAX ID клиента", "Клиент", "Инженер", "Статус",
                "Суть обращения", "Решение / Заметка", "Время открытия (UTC)",
                "Первый ответ (UTC)", "Время закрытия (UTC)",
                "RT (сек)", "RT (читаемый)", "MTTR (сек)", "MTTR (читаемый)"
            ]
            writer.writerow(headers)
            for r in rows:
                writer.writerow([
                    f"#{r['ticket_number']}",
                    r["max_user_id"],
                    r["max_user_name"] or "",
                    r["assigned_operator"] or "Не назначен",
                    r["status"],
                    r["initial_text"] or "",
                    r["resolution"] or "",
                    r["created_at"] or "",
                    r["first_response_at"] or "",
                    r["closed_at"] or "",
                    r["rt_seconds"] if r["rt_seconds"] is not None else "",
                    format_duration(r["rt_seconds"]),
                    r["mttr_seconds"] if r["mttr_seconds"] is not None else "",
                    format_duration(r["mttr_seconds"]),
                ])
            return output.getvalue().encode("utf-8-sig")

    def export_history_csv(self) -> bytes:
        """Экспорт стенограммы диалогов в формат CSV (UTF-8-SIG) для Excel."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id, timestamp, direction, max_user_id, max_user_name, operator_name,
                       text, has_attachments, attachment_info, tg_message_id, ticket_id
                FROM dialogue_history
                ORDER BY id ASC;
            """)
            rows = cursor.fetchall()

            output = io.StringIO()
            writer = csv.writer(output, delimiter=";")
            headers = [
                "ID", "Время (UTC)", "Направление", "№ Заявки", "MAX ID",
                "Клиент", "Оператор", "Текст сообщения", "Вложения", "Инфо о вложениях"
            ]
            writer.writerow(headers)
            for r in rows:
                dir_label = "Клиент -> Оператор" if r["direction"] == "CLIENT_TO_OPERATOR" else "Оператор -> Клиент"
                t_label = f"#{r['ticket_id']}" if r["ticket_id"] else "—"
                writer.writerow([
                    r["id"],
                    r["timestamp"],
                    dir_label,
                    t_label,
                    r["max_user_id"],
                    r["max_user_name"] or "",
                    r["operator_name"] or "",
                    r["text"] or "",
                    "Да" if r["has_attachments"] else "Нет",
                    r["attachment_info"] or "",
                ])
            return output.getvalue().encode("utf-8-sig")


storage = Storage(DB_PATH)


def format_duration(seconds: Optional[int]) -> str:
    """Форматирование секунд в человекочитаемую строку."""
    if seconds is None:
        return "—"
    m = seconds // 60
    s = seconds % 60
    if m > 0:
        return f"{m} мин {s} сек"
    return f"{s} сек"


def get_topic_link(chat_id: int, thread_id: Optional[int], topic_name: str) -> str:
    """Формирование кликабельной HTML-ссылки на топик форума Telegram."""
    if not thread_id:
        return f"<b>{html.escape(topic_name)}</b>"
    clean_chat = str(chat_id)
    if clean_chat.startswith("-100"):
        clean_chat = clean_chat[4:]
    elif clean_chat.startswith("-"):
        clean_chat = clean_chat[1:]
    return f'<a href="https://t.me/c/{clean_chat}/{thread_id}">{html.escape(topic_name)}</a>'



class TelegramClient:
    """Клиент Telegram Bot API."""

    def __init__(self, token: str):
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.file_base_url = f"https://api.telegram.org/file/bot{token}"
        self.last_error = ""

    async def get_me(self) -> Optional[Dict[str, Any]]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{self.base_url}/getMe")
            if resp.status_code == 200 and resp.json().get("ok"):
                return resp.json()["result"]
        return None

    async def create_forum_topic(self, chat_id: int, name: str) -> Optional[int]:
        """Создать персональный топик (ветку) для клиента в супергруппе."""
        url = f"{self.base_url}/createForumTopic"
        payload = {"chat_id": chat_id, "name": name[:128]}
        try:
            async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200 and resp.json().get("ok"):
                    self.last_error = ""
                    return resp.json()["result"]["message_thread_id"]
                else:
                    self.last_error = resp.text
                    logger.warning("Не удалось создать топик '%s': %s -> %s", name, resp.status_code, resp.text)
        except Exception as e:
            self.last_error = str(e)
            logger.error("Ошибка вызова createForumTopic: %s", e)
        return None

    async def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: Optional[int] = None,
        message_thread_id: Optional[int] = None,
    ) -> Optional[int]:
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
        if message_thread_id:
            payload["message_thread_id"] = message_thread_id

        async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200 and resp.json().get("ok"):
                self.last_error = ""
                return resp.json()["result"]["message_id"]
            else:
                self.last_error = resp.text
                logger.error("Ошибка отправки в Telegram: %s -> %s", resp.status_code, resp.text)
        return None

    async def send_photo(
        self,
        chat_id: int,
        photo_bytes: bytes,
        caption: str = "",
        filename: str = "photo.jpg",
        message_thread_id: Optional[int] = None,
    ) -> Optional[int]:
        url = f"{self.base_url}/sendPhoto"
        data = {"chat_id": str(chat_id), "caption": caption, "parse_mode": "HTML"}
        if message_thread_id:
            data["message_thread_id"] = str(message_thread_id)
        files = {"photo": (filename, photo_bytes, "image/jpeg")}
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.post(url, data=data, files=files)
            if resp.status_code == 200 and resp.json().get("ok"):
                self.last_error = ""
                return resp.json()["result"]["message_id"]
            else:
                self.last_error = resp.text
                logger.error("Ошибка отправки фото в Telegram: %s -> %s", resp.status_code, resp.text)
        return None

    async def send_document(
        self,
        chat_id: int,
        doc_bytes: bytes,
        filename: str,
        caption: str = "",
        message_thread_id: Optional[int] = None,
    ) -> Optional[int]:
        url = f"{self.base_url}/sendDocument"
        data = {"chat_id": str(chat_id), "caption": caption, "parse_mode": "HTML"}
        if message_thread_id:
            data["message_thread_id"] = str(message_thread_id)
        files = {"document": (filename, doc_bytes, "application/octet-stream")}
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.post(url, data=data, files=files)
            if resp.status_code == 200 and resp.json().get("ok"):
                self.last_error = ""
                return resp.json()["result"]["message_id"]
            else:
                self.last_error = resp.text
                logger.error("Ошибка отправки документа в Telegram: %s -> %s", resp.status_code, resp.text)
        return None

    async def send_video(
        self,
        chat_id: int,
        video_bytes: bytes,
        caption: str = "",
        filename: str = "video.mp4",
        message_thread_id: Optional[int] = None,
    ) -> Optional[int]:
        """Отправить видеозапись в Telegram с нативным видеоплеером."""
        url = f"{self.base_url}/sendVideo"
        data = {
            "chat_id": str(chat_id),
            "caption": caption,
            "parse_mode": "HTML",
            "supports_streaming": "true",
        }
        if message_thread_id:
            data["message_thread_id"] = str(message_thread_id)
        files = {"video": (filename, video_bytes, "video/mp4")}
        try:
            async with httpx.AsyncClient(timeout=60.0, verify=False) as client:
                resp = await client.post(url, data=data, files=files)
                if resp.status_code == 200 and resp.json().get("ok"):
                    self.last_error = ""
                    return resp.json()["result"]["message_id"]
                else:
                    self.last_error = resp.text
                    logger.error("Ошибка отправки видео в Telegram: %s -> %s", resp.status_code, resp.text)
        except Exception as e:
            self.last_error = str(e)
            logger.error("Исключение при send_video в Telegram: %s", e)
        return None

    async def send_voice(
        self,
        chat_id: int,
        voice_bytes: bytes,
        caption: str = "",
        message_thread_id: Optional[int] = None,
    ) -> Optional[int]:
        """Отправить голосовое сообщение в Telegram с нативным плеером."""
        url = f"{self.base_url}/sendVoice"
        data = {"chat_id": str(chat_id), "caption": caption, "parse_mode": "HTML"}
        if message_thread_id:
            data["message_thread_id"] = str(message_thread_id)
        files = {"voice": ("voice.ogg", voice_bytes, "audio/ogg")}
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                resp = await client.post(url, data=data, files=files)
                if resp.status_code == 200 and resp.json().get("ok"):
                    self.last_error = ""
                    return resp.json()["result"]["message_id"]
                else:
                    self.last_error = resp.text
                    logger.error("Ошибка отправки голосового в Telegram: %s -> %s", resp.status_code, resp.text)
        except Exception as e:
            self.last_error = str(e)
            logger.error("Исключение при send_voice в Telegram: %s", e)
        return None

    async def send_audio(
        self,
        chat_id: int,
        audio_bytes: bytes,
        filename: str = "audio.mp3",
        caption: str = "",
        message_thread_id: Optional[int] = None,
    ) -> Optional[int]:
        """Отправить аудиозапись в Telegram."""
        url = f"{self.base_url}/sendAudio"
        data = {"chat_id": str(chat_id), "caption": caption, "parse_mode": "HTML"}
        if message_thread_id:
            data["message_thread_id"] = str(message_thread_id)
        files = {"audio": (filename, audio_bytes, "audio/mpeg")}
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                resp = await client.post(url, data=data, files=files)
                if resp.status_code == 200 and resp.json().get("ok"):
                    self.last_error = ""
                    return resp.json()["result"]["message_id"]
                else:
                    self.last_error = resp.text
                    logger.error("Ошибка отправки аудио в Telegram: %s -> %s", resp.status_code, resp.text)
        except Exception as e:
            self.last_error = str(e)
            logger.error("Исключение при send_audio в Telegram: %s", e)
        return None

    async def get_file_bytes(self, file_id: str) -> Optional[tuple[bytes, str]]:
        """Скачать файл из Telegram."""
        try:
            async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
                resp = await client.get(f"{self.base_url}/getFile?file_id={file_id}")
                if resp.status_code != 200 or not resp.json().get("ok"):
                    logger.error("Telegram getFile error for %s: %s -> %s", file_id, resp.status_code, resp.text)
                    return None
                file_path = resp.json()["result"]["file_path"]
                filename = os.path.basename(file_path)
                
                file_resp = await client.get(f"{self.file_base_url}/{file_path}")
                if file_resp.status_code == 200:
                    logger.info("Успешно скачан файл из Telegram: %s (%d байт)", filename, len(file_resp.content))
                    return file_resp.content, filename
                else:
                    logger.error("Ошибка скачивания файла из Telegram (%s): %s", file_path, file_resp.status_code)
        except Exception as e:
            logger.error("Исключение при get_file_bytes из Telegram: %s", e)
        return None

    async def delete_message(self, chat_id: int, message_id: int) -> bool:
        """Удалить сообщение в Telegram (для автоочистки системных уведомлений)."""
        url = f"{self.base_url}/deleteMessage"
        data = {"chat_id": str(chat_id), "message_id": str(message_id)}
        try:
            async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
                resp = await client.post(url, data=data)
                return resp.status_code == 200 and resp.json().get("ok", False)
        except Exception:
            return False

    async def set_webhook(self, url: str, secret_token: Optional[str] = None) -> bool:
        """Установка вебхука в Telegram."""
        api_url = f"{self.base_url}/setWebhook"
        payload: Dict[str, Any] = {"url": url, "drop_pending_updates": False}
        if secret_token:
            payload["secret_token"] = secret_token
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(api_url, json=payload)
                res = resp.json()
                if res.get("ok"):
                    logger.info("🟢 Telegram вебхук успешно установлен: %s", url)
                    return True
                logger.error("Ошибка установки вебхука Telegram: %s", res)
        except Exception as e:
            logger.error("Сбой установки вебхука Telegram: %s", e)
        return False

    async def delete_webhook(self, drop_pending_updates: bool = False) -> bool:
        """Снятие вебхука в Telegram для безопасного возврата на Long Polling."""
        api_url = f"{self.base_url}/deleteWebhook"
        payload = {"drop_pending_updates": drop_pending_updates}
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(api_url, json=payload)
                res = resp.json()
                if res.get("ok"):
                    logger.info("🟢 Telegram вебхук снят, активен режим Long Polling.")
                    return True
        except Exception as e:
            logger.error("Ошибка снятия вебхука Telegram: %s", e)
        return False


class MaxClient:
    """Клиент MAX Bot API с поддержкой двухэтапной загрузки вложений."""

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": self.token,
            "Content-Type": "application/json",
        }

    async def get_me(self) -> Optional[Dict[str, Any]]:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            resp = await client.get(f"{self.base_url}/me", headers=self._headers())
            if resp.status_code == 200:
                return resp.json()
        return None

    async def upload_image(self, filename: str, image_bytes: bytes, mime_type: str = "image/jpeg") -> Optional[str]:
        """Двухэтапная загрузка изображения в MAX API (uploads -> iu.oneme.ru)."""
        try:
            async with httpx.AsyncClient(verify=False, timeout=20.0) as client:
                init_res = await client.post(f"{self.base_url}/uploads?type=image", headers=self._headers())
                if init_res.status_code != 200:
                    logger.error("MAX init image upload failed: %s -> %s", init_res.status_code, init_res.text)
                    return None
                upload_url = init_res.json().get("url")
                if not upload_url:
                    return None

                files = {"file": (filename, image_bytes, mime_type)}
                up_res = await client.post(upload_url, files=files)
                if up_res.status_code == 200:
                    data = up_res.json()
                    photos = data.get("photos", {})
                    for p_info in photos.values():
                        if isinstance(p_info, dict) and p_info.get("token"):
                            return p_info["token"]
                else:
                    logger.error("MAX upload image bytes failed: %s -> %s", up_res.status_code, up_res.text)
        except Exception as e:
            logger.error("Исключение при upload_image в MAX: %s", e)
        return None

    async def upload_file(self, filename: str, file_bytes: bytes, mime_type: str = "application/octet-stream") -> Optional[str]:
        """Двухэтапная загрузка документа/файла в MAX API (uploads -> fu.oneme.ru)."""
        try:
            async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                init_res = await client.post(f"{self.base_url}/uploads?type=file", headers=self._headers())
                if init_res.status_code != 200:
                    logger.error("MAX init file upload failed: %s -> %s", init_res.status_code, init_res.text)
                    return None
                upload_url = init_res.json().get("url")
                if not upload_url:
                    return None

                files = {"file": (filename, file_bytes, mime_type)}
                up_res = await client.post(upload_url, files=files)
                if up_res.status_code == 200:
                    data = up_res.json()
                    return data.get("token")
                else:
                    logger.error("MAX upload file bytes failed: %s -> %s", up_res.status_code, up_res.text)
        except Exception as e:
            logger.error("Исключение при upload_file в MAX: %s", e)
        return None

    async def upload_audio(self, filename: str, audio_bytes: bytes, mime_type: str = "audio/ogg") -> Optional[str]:
        """Двухэтапная загрузка аудио/голосового в MAX API (uploads?type=audio -> omu.okcdn.ru)."""
        try:
            async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                init_res = await client.post(f"{self.base_url}/uploads?type=audio", headers=self._headers())
                if init_res.status_code != 200:
                    logger.error("MAX init audio upload failed: %s -> %s", init_res.status_code, init_res.text)
                    return None
                upload_url = init_res.json().get("url")
                audio_token = init_res.json().get("token")
                if not upload_url or not audio_token:
                    return None

                files = {"data": (filename, audio_bytes, mime_type)}
                up_res = await client.post(upload_url, files=files)
                if up_res.status_code == 200:
                    return audio_token
                else:
                    logger.error("MAX upload audio bytes failed: %s -> %s", up_res.status_code, up_res.text)
        except Exception as e:
            logger.error("Исключение при upload_audio в MAX: %s", e)
        return None

    async def send_message(
        self,
        user_id: int,
        text: str,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> tuple[bool, str]:
        """Отправка сообщения в MAX с поддержкой вложений и retry при attachment.not.ready."""
        url = f"{self.base_url}/messages?user_id={user_id}"
        payload: Dict[str, Any] = {
            "text": text or "",
            "format": "markdown",
        }
        if attachments:
            payload["attachments"] = attachments

        max_attempts = 6 if attachments else 1
        last_error = ""

        for attempt in range(1, max_attempts + 1):
            try:
                async with httpx.AsyncClient(verify=False, timeout=20.0) as client:
                    resp = await client.post(url, headers=self._headers(), json=payload)
                    if resp.status_code in (200, 201):
                        return True, ""

                    err_body = resp.text
                    last_error = f"HTTP {resp.status_code}: {err_body}"

                    # Если вложение еще обрабатывается хранилищем MAX
                    if "attachment.not.ready" in err_body or "not.processed" in err_body:
                        logger.warning(
                            "MAX API: вложение обрабатывается сервером (попытка %d/%d). Ожидание %d сек...",
                            attempt, max_attempts, attempt
                        )
                        await asyncio.sleep(attempt * 1.0)
                        continue
                    else:
                        logger.error("Ошибка отправки сообщения в MAX (user_id=%s): %s", user_id, last_error)
                        return False, last_error
            except Exception as e:
                last_error = str(e)
                logger.error("Исключение при отправке в MAX (попытка %d/%d): %s", attempt, max_attempts, e)
                await asyncio.sleep(1.0)

        logger.error("Не удалось доставить сообщение в MAX после %d попыток: %s", max_attempts, last_error)
        return False, last_error

    async def get_video_url(self, video_token: str) -> Optional[str]:
        """Получить прямой URL видеозаписи из MAX API по video_token."""
        url = f"{self.base_url}/videos/{video_token}"
        try:
            async with httpx.AsyncClient(verify=False, timeout=15.0) as client:
                resp = await client.get(url, headers=self._headers())
                if resp.status_code == 200:
                    data = resp.json()
                    urls = data.get("urls") or {}
                    for q in ("mp4_720", "mp4_1080", "mp4_480", "mp4_360", "mp4_240", "mp4"):
                        if urls.get(q):
                            return urls[q]
                    if data.get("url"):
                        return data["url"]
                else:
                    logger.warning("MAX videos API: %s -> %s", resp.status_code, resp.text)
        except Exception as e:
            logger.error("Ошибка при получении видео из MAX API (%s): %s", video_token, e)
        return None

    async def download_file(self, url: str) -> Optional[bytes]:
        """Скачать вложение из MAX."""
        try:
            async with httpx.AsyncClient(verify=False, timeout=25.0) as client:
                headers = self._headers() if "botapi.max.ru" in url else {}
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    return resp.content
        except Exception as e:
            logger.error("Ошибка скачивания файла из MAX (%s): %s", url, e)
        return None

    async def set_webhook(self, url: str) -> bool:
        """Установка вебхука в MAX API."""
        api_url = f"{self.base_url}/subscriptions"
        try:
            async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
                resp = await client.post(api_url, headers=self._headers(), json={"url": url})
                if resp.status_code == 200 and resp.json().get("success"):
                    logger.info("🟢 MAX API вебхук успешно установлен: %s", url)
                    return True
                logger.error("Ошибка установки вебхука MAX API (%s): %s", resp.status_code, resp.text)
        except Exception as e:
            logger.error("Сбой установки вебхука MAX API: %s", e)
        return False

    async def delete_bridge_subscriptions(self, target_url: Optional[str] = None) -> bool:
        """Удаление ТОЛЬКО собственных вебхуков моста в MAX API (не трогая боевой вебхук ЭЛМЫ!)."""
        api_url = f"{self.base_url}/subscriptions"
        try:
            async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
                resp = await client.get(api_url, headers=self._headers())
                if resp.status_code == 200:
                    subs = resp.json().get("subscriptions", [])
                    for sub in subs:
                        sub_url = sub.get("url", "")
                        # Удаляем ТОЛЬКО если это вебхук нашего моста
                        is_our_webhook = False
                        if target_url and target_url in sub_url:
                            is_our_webhook = True
                        elif any(marker in sub_url.lower() for marker in ("/webhook/max", "bothost", "emergency", "bridge")):
                            is_our_webhook = True

                        if is_our_webhook:
                            del_resp = await client.delete(f"{api_url}?url={sub_url}", headers=self._headers())
                            logger.info("Удален временный вебхук аварийного моста: %s -> %s", sub_url, del_resp.status_code)
                        else:
                            logger.info("🛡️ Сохранен существующий вебхук сторонней системы (ELMA365 / Прод): %s", sub_url)
                    return True
        except Exception as e:
            logger.error("Ошибка при проверке вебхук-подписок MAX API: %s", e)
        return False


tg_client = TelegramClient(TELEGRAM_BOT_TOKEN)
max_client = MaxClient(MAX_API_BASE_URL, MAX_BOT_TOKEN)


# ==============================================================================
# ЦИКЛ 1: MAX -> TELEGRAM
# ==============================================================================

def extract_sender_name(sender: dict) -> str:
    parts = []
    if sender.get("first_name"):
        parts.append(sender["first_name"])
    if sender.get("last_name"):
        parts.append(sender["last_name"])
    if not parts and sender.get("name"):
        parts.append(sender["name"])
    if not parts and sender.get("username"):
        parts.append(f"@{sender['username']}")
    return " ".join(parts).strip() or f"Пользователь {sender.get('user_id', 'Unknown')}"


async def process_max_update(up: dict):
    """Обработка одного входящего события из MAX (сообщение, старт бота, вложения)."""
    up_type = up.get("update_type")
    if up_type not in ("message_created", "bot_started"):
        return

    msg = up.get("message", {})
    sender = msg.get("sender", {}) or up.get("user", {})
    body = msg.get("body", {})

    user_id = sender.get("user_id")
    if not user_id:
        return
    user_name = extract_sender_name(sender)
    text = (body.get("text") or "").strip()
    attachments = body.get("attachments") or []

    if up_type == "bot_started" and not text:
        text = "/start"

    logger.info("📩 Входящее из MAX от %s (ID: %s): %s (вложений: %s)", user_name, user_id, text[:60], len(attachments))

    # Определение топика (ветки форума) для клиента
    thread_id = None
    topic_name = f"{user_name} [MAX: {user_id}]"[:128]
    if USE_FORUM_TOPICS:
        thread_id = storage.get_topic_by_max_user(user_id)
        if not thread_id:
            thread_id = await tg_client.create_forum_topic(TG_CHAT_ID, topic_name)
            if thread_id:
                storage.save_topic(user_id, thread_id, topic_name)
                logger.info("[FORUM] Создан топик '%s' (thread_id: %s)", topic_name, thread_id)
        else:
            topic_name = storage.get_topic_name_by_thread(thread_id)

    # Проверка/создание активной заявки ServiceDesk
    active_ticket = storage.get_active_ticket(user_id)
    if not active_ticket:
        initial_desc = text if text else ("[Вложение]" if attachments else "")
        active_ticket = storage.create_ticket(
            max_user_id=user_id,
            max_user_name=user_name,
            message_thread_id=thread_id,
            initial_text=initial_desc,
        )
        t_num = active_ticket["ticket_number"]
        logger.info("🎫 Создана заявка #%s для %s", t_num, user_name)

        # Отправка карточки открытия заявки в GENERAL чат супергруппы
        topic_link = get_topic_link(TG_CHAT_ID, thread_id, topic_name)
        safe_desc = html.escape(initial_desc[:250]) if initial_desc else "<i>[Вложение/Медиа]</i>"
        general_card = (
            f"🎫 <b>НОВАЯ ЗАЯВКА #{t_num}</b>\n"
            f"👤 <b>Клиент:</b> {html.escape(user_name)} <code>[MAX: {user_id}]</code>\n"
            f"💬 <b>Проблема:</b> {safe_desc}\n"
            f"📍 <b>Ветка:</b> {topic_link}"
        )
        await tg_client.send_message(
            chat_id=TG_CHAT_ID,
            text=general_card,
            message_thread_id=None,
        )

    t_num = active_ticket["ticket_number"] if active_ticket else "?"
    header = f"📩 <b>[MAX] {html.escape(user_name)}</b> <code>(ID: {user_id})</code> | <b>Заявка #{t_num}</b>:\n\n"
    safe_body = html.escape(text) if text else "<i>[Без текста]</i>"
    full_text = header + safe_body

    tg_msg_id = None
    has_media = False
    attachment_info_list = []

    # Подготовка списка вложений
    for att in attachments:
        att_type = att.get("type")
        p = att.get("payload") or {}
        file_url = p.get("url") or p.get("download_url") or p.get("file_url") or ""
        if att_type == "video" and not file_url and p.get("token"):
            file_url = await max_client.get_video_url(p["token"])
        if not file_url and p.get("token"):
            file_url = f"{MAX_API_BASE_URL}/files/{p.get('token')}"
        if file_url:
            has_media = True
            attachment_info_list.append(file_url)

    async def _send_incoming_to_tg(target_tid: Optional[int]) -> Optional[int]:
        m_id = None
        for att in attachments:
            att_type = att.get("type")
            p = att.get("payload") or {}
            file_url = p.get("url") or p.get("download_url") or p.get("file_url") or ""
            if att_type == "video" and not file_url and p.get("token"):
                file_url = await max_client.get_video_url(p["token"])
            if not file_url and p.get("token"):
                file_url = f"{MAX_API_BASE_URL}/files/{p.get('token')}"

            if file_url:
                file_bytes = await max_client.download_file(file_url)
                if file_bytes:
                    if att_type == "video" or "video" in str(p.get("type", "")):
                        token_part = str(p.get("token") or "")[:8]
                        filename = p.get("name") or (f"video_{token_part}.mp4" if token_part else "video.mp4")
                        if not any(filename.lower().endswith(ext) for ext in (".mp4", ".mov", ".avi", ".mkv", ".webm")):
                            filename += ".mp4"
                    else:
                        filename = p.get("name") or "file.bin"
                    fn_lower = filename.lower()

                    if att_type == "image":
                        m_id = await tg_client.send_photo(
                            TG_CHAT_ID, file_bytes, caption=full_text, filename=filename, message_thread_id=target_tid
                        )
                    elif att_type == "video" or fn_lower.endswith((".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v")):
                        m_id = await tg_client.send_video(
                            TG_CHAT_ID, file_bytes, caption=full_text, filename=filename, message_thread_id=target_tid
                        )
                        if not m_id:
                            # Фолбэк на отправку документом с правильным именем и расширением .mp4
                            m_id = await tg_client.send_document(
                                TG_CHAT_ID, file_bytes, filename=filename, caption=full_text, message_thread_id=target_tid
                            )
                    elif att_type == "audio" or fn_lower.endswith((".ogg", ".oga", ".opus")):
                        m_id = await tg_client.send_voice(
                            TG_CHAT_ID, file_bytes, caption=full_text, message_thread_id=target_tid
                        )
                        if not m_id:
                            m_id = await tg_client.send_audio(
                                TG_CHAT_ID, file_bytes, filename=filename or "voice.mp3", caption=full_text, message_thread_id=target_tid
                            )
                    elif fn_lower.endswith((".mp3", ".wav", ".m4a", ".aac", ".flac")):
                        m_id = await tg_client.send_audio(
                            TG_CHAT_ID, file_bytes, filename=filename, caption=full_text, message_thread_id=target_tid
                        )
                    else:
                        m_id = await tg_client.send_document(
                            TG_CHAT_ID, file_bytes, filename=filename, caption=full_text, message_thread_id=target_tid
                        )
        if not m_id:
            m_id = await tg_client.send_message(TG_CHAT_ID, full_text, message_thread_id=target_tid)
        return m_id

    tg_msg_id = await _send_incoming_to_tg(thread_id)

    # Автоматическое восстановление ветки, если топик был удален в Telegram
    if not tg_msg_id and ("thread not found" in (tg_client.last_error or "").lower() or "topic not found" in (tg_client.last_error or "").lower()):
        logger.warning("⚠️ Ветка %s была удалена в Telegram! Автоматически восстанавливаем топик для %s...", thread_id, user_name)
        storage.delete_topic(user_id)
        new_thread_id = await tg_client.create_forum_topic(TG_CHAT_ID, topic_name)
        if new_thread_id:
            thread_id = new_thread_id
            storage.save_topic(user_id, thread_id, topic_name)
            if active_ticket:
                storage.update_ticket_thread(active_ticket["id"], thread_id)
            tg_msg_id = await _send_incoming_to_tg(thread_id)

    if tg_msg_id:
        storage.save_link(tg_msg_id, user_id, user_name)
        storage.log_message(
            direction="CLIENT_TO_OPERATOR",
            max_user_id=user_id,
            max_user_name=user_name,
            text=text,
            has_attachments=has_media,
            attachment_info=", ".join(attachment_info_list),
            tg_message_id=tg_msg_id,
            ticket_id=active_ticket["id"] if active_ticket else None,
        )


async def poll_max_updates():
    """Слушаем события из MAX через Long Polling и пересылаем в Telegram."""
    marker = None
    logger.info("🟢 Запущен опрос MAX Bot API (Long Polling)...")

    while True:
        try:
            url = f"{MAX_API_BASE_URL}/updates?timeout=20"
            if marker is not None:
                url += f"&marker={marker}"

            async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                resp = await client.get(url, headers=max_client._headers())
                if resp.status_code == 429:
                    logger.warning("MAX API 429 (Rate Limit), пауза 3 сек...")
                    await asyncio.sleep(3.0)
                    continue
                if resp.status_code != 200:
                    logger.error("MAX API статус %s: %s", resp.status_code, resp.text)
                    await asyncio.sleep(2.0)
                    continue
                data = resp.json()

            updates = data.get("updates", [])
            new_marker = data.get("marker")
            if new_marker is not None:
                marker = new_marker

            for up in updates:
                try:
                    await process_max_update(up)
                except Exception as ex:
                    logger.error("Ошибка обработки события MAX: %s", ex, exc_info=True)

            await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            break
        except httpx.ReadTimeout:
            continue
        except Exception as e:
            logger.error("Ошибка в цикле опроса MAX: %s", e, exc_info=True)
            await asyncio.sleep(3.0)


# ==============================================================================
# ЦИКЛ 2: TELEGRAM -> MAX (ОБРАБОТЧИК И LONG POLLING)
# ==============================================================================

async def process_telegram_update(up: dict):
    """Обработка одного входящего события из Telegram (сообщение оператора, команды)."""
    msg = up.get("message")
    if not msg:
        return

    chat = msg.get("chat", {})
    chat_id = chat.get("id")

    # Проверяем, что сообщение пришло именно из чата операторов
    if chat_id != TG_CHAT_ID:
        return

    sender = msg.get("from", {})
    if not sender or sender.get("is_bot"):
        return

    # Игнорируем сервисные сообщения супергруппы (создание топика, закрепление и т.д.)
    if any(k in msg for k in ("forum_topic_created", "forum_topic_edited", "forum_topic_closed", "forum_topic_reopened", "pinned_message")):
        return

    op_name = extract_sender_name(sender)
    reply_to = msg.get("reply_to_message")
    msg_thread_id = msg.get("message_thread_id")
    text = (msg.get("text") or msg.get("caption") or "").strip()

    # Внутренние заметки инженеров (# или //) оставляем только в Telegram
    if text.startswith(("#", "//")):
        return

    # Команды управления заявками ServiceDesk
    if text.startswith("/"):
        cmd_parts = text.split(maxsplit=1)
        cmd = cmd_parts[0].lower()
        cmd_arg = cmd_parts[1].strip() if len(cmd_parts) > 1 else ""

        if cmd in ("/close", "/done", "/resolve"):
            target_ticket = None
            if msg_thread_id:
                target_ticket = storage.get_active_ticket_by_thread(msg_thread_id)
            elif reply_to:
                link = storage.get_max_user_by_tg_msg(reply_to.get("message_id"))
                if link:
                    target_ticket = storage.get_active_ticket(link["max_user_id"])

            if not target_ticket:
                warn_msg = await tg_client.send_message(
                    chat_id=TG_CHAT_ID,
                    text="⚠️ <i>В этой ветке нет активной открытой заявки.</i>",
                    reply_to_message_id=msg.get("message_id"),
                    message_thread_id=msg_thread_id,
                )
                if warn_msg:
                    async def _del_warn(m_id: int):
                        await asyncio.sleep(4.0)
                        await tg_client.delete_message(TG_CHAT_ID, m_id)
                    asyncio.create_task(_del_warn(warn_msg))
                return

            closed_t = storage.close_ticket(target_ticket["id"], op_name, cmd_arg)
            if closed_t:
                t_num = closed_t["ticket_number"]
                rt_str = format_duration(closed_t.get("rt_seconds"))
                mttr_str = format_duration(closed_t.get("mttr_seconds"))
                res_disp = html.escape(cmd_arg) if cmd_arg else "<i>(без описания решения)</i>"
                t_name = storage.get_topic_name_by_thread(msg_thread_id) if msg_thread_id else (closed_t.get("max_user_name") or "")
                topic_link = get_topic_link(TG_CHAT_ID, msg_thread_id, t_name)

                # 1. Итоговый отчет в GENERAL чат супергруппы
                general_report = (
                    f"✅ <b>ЗАЯВКА #{t_num} ЗАКРЫТА</b>\n"
                    f"👤 <b>Клиент:</b> {html.escape(closed_t['max_user_name'])} <code>[MAX: {closed_t['max_user_id']}]</code>\n"
                    f"👨‍💻 <b>Инженер:</b> {html.escape(op_name)}\n"
                    f"⏱ <b>Время отклика (RT):</b> {rt_str}\n"
                    f"⏳ <b>Время решения (MTTR):</b> {mttr_str}\n"
                    f"📝 <b>Решение:</b> {res_disp}\n"
                    f"📍 <b>Ветка:</b> {topic_link}"
                )
                await tg_client.send_message(
                    chat_id=TG_CHAT_ID,
                    text=general_report,
                    message_thread_id=None,
                )

            return

        elif cmd in ("/status", "/ticket"):
            target_ticket = None
            if msg_thread_id:
                target_ticket = storage.get_active_ticket_by_thread(msg_thread_id) or storage.get_last_ticket_by_thread(msg_thread_id)
            elif reply_to:
                link = storage.get_max_user_by_tg_msg(reply_to.get("message_id"))
                if link:
                    target_ticket = storage.get_active_ticket(link["max_user_id"])

            if not target_ticket:
                await tg_client.send_message(
                    chat_id=TG_CHAT_ID,
                    text="ℹ️ <i>В этой ветке пока нет зарегистрированных заявок.</i>",
                    reply_to_message_id=msg.get("message_id"),
                    message_thread_id=msg_thread_id,
                )
            else:
                t_num = target_ticket["ticket_number"]
                st = target_ticket["status"]
                rt_val = format_duration(target_ticket.get("rt_seconds"))
                created_str = target_ticket.get("created_at")
                elapsed_str = "—"
                if created_str:
                    try:
                        c_dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        sec = max(0, int((datetime.now(timezone.utc) - c_dt).total_seconds()))
                        elapsed_str = format_duration(sec)
                    except Exception:
                        pass

                status_card = (
                    f"ℹ️ <b>Паспорт заявки #{t_num}</b>\n"
                    f"Статус: <b>{st}</b>\n"
                    f"Клиент: {html.escape(target_ticket.get('max_user_name') or '—')}\n"
                    f"Инженер: {html.escape(target_ticket.get('assigned_operator') or 'Ожидает ответа')}\n"
                    f"⏱ Время отклика (RT): {rt_val}\n"
                    f"⏳ В работе: {elapsed_str}"
                )
                await tg_client.send_message(
                    chat_id=TG_CHAT_ID,
                    text=status_card,
                    reply_to_message_id=msg.get("message_id"),
                    message_thread_id=msg_thread_id,
                )
            return

        elif cmd in ("/export", "/report", "/csv"):
            csv_bytes = storage.export_tickets_csv()
            ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tickets_registry_{ts_str}.csv"
            caption = f"📊 <b>Реестр заявок ServiceDesk на {datetime.now().strftime('%d.%m.%Y %H:%M')} (UTC)</b>"

            await tg_client.send_document(
                chat_id=TG_CHAT_ID,
                doc_bytes=csv_bytes,
                filename=filename,
                caption=caption,
                message_thread_id=msg_thread_id,
            )

            if cmd_arg in ("all", "full", "history"):
                hist_bytes = storage.export_history_csv()
                h_filename = f"dialogue_history_{ts_str}.csv"
                await tg_client.send_document(
                    chat_id=TG_CHAT_ID,
                    doc_bytes=hist_bytes,
                    filename=h_filename,
                    caption="📜 <b>Стенограмма диалогов (Excel UTF-8)</b>",
                    message_thread_id=msg_thread_id,
                )
            return

        # Прочие неизвестные команды боту игнорируем
        return

    link_info = None

    # 1. Приоритет: если сообщение написано внутри персонального топика клиента
    if msg_thread_id:
        link_info = storage.get_max_user_by_topic(msg_thread_id)

    # 2. Приоритет: если оператор ответил через Reply на конкретное сообщение
    if not link_info and reply_to:
        target_tg_msg_id = reply_to.get("message_id")
        link_info = storage.get_max_user_by_tg_msg(target_tg_msg_id)

    if not link_info:
        # Сообщение в общем чате вне топиков и без Reply - пропускаем
        return

    max_user_id = link_info["max_user_id"]
    max_user_name = link_info["max_user_name"]

    photos = msg.get("photo")
    doc = msg.get("document")
    video = msg.get("video")
    animation = msg.get("animation")
    voice = msg.get("voice")
    audio = msg.get("audio")

    attachments = []
    has_media = False
    media_info = []

    # 1. Если оператор отправил фото в Telegram
    if photos:
        has_media = True
        best_photo = photos[-1]  # максимальное качество
        res = await tg_client.get_file_bytes(best_photo["file_id"])
        if res:
            p_bytes, p_name = res
            img_token = await max_client.upload_image(p_name or "photo.jpg", p_bytes)
            if img_token:
                attachments.append({"type": "image", "payload": {"token": img_token}})
                media_info.append(f"Фото: {p_name or 'photo.jpg'}")
            else:
                logger.error("Не удалось загрузить фото оператора в MAX API")

    # 2. Если оператор отправил файл / документ в Telegram
    elif doc:
        has_media = True
        doc_id = doc.get("file_id")
        doc_name = doc.get("file_name") or "document.bin"
        mime_type = doc.get("mime_type") or "application/octet-stream"

        res = await tg_client.get_file_bytes(doc_id)
        if res:
            d_bytes, _ = res
            # Проверяем, не аудио ли это
            if mime_type.startswith("audio/") or doc_name.lower().endswith((".mp3", ".ogg", ".wav", ".m4a", ".aac")):
                audio_token = await max_client.upload_audio(doc_name, d_bytes)
                if audio_token:
                    attachments.append({"type": "audio", "payload": {"token": audio_token}})
                    media_info.append(f"Аудиофайл: {doc_name}")
                else:
                    # Фолбэк на обычный файл
                    f_token = await max_client.upload_file(doc_name, d_bytes)
                    if f_token:
                        attachments.append({"type": "file", "payload": {"token": f_token}})
                        media_info.append(f"Файл: {doc_name}")
            else:
                f_token = await max_client.upload_file(doc_name, d_bytes)
                if f_token:
                    attachments.append({"type": "file", "payload": {"token": f_token}})
                    media_info.append(f"Файл: {doc_name}")

    # 3. Если оператор записал голосовое сообщение (Voice)
    elif voice:
        has_media = True
        voice_id = voice.get("file_id")
        res = await tg_client.get_file_bytes(voice_id)
        if res:
            v_bytes, _ = res
            audio_token = await max_client.upload_audio("voice.ogg", v_bytes)
            if audio_token:
                attachments.append({"type": "audio", "payload": {"token": audio_token}})
                media_info.append("Голосовое сообщение (нативный плеер)")
            else:
                # Фолбэк как обычный файл
                f_token = await max_client.upload_file("voice.ogg", v_bytes)
                if f_token:
                    attachments.append({"type": "file", "payload": {"token": f_token}})
                    media_info.append("Голосовое (файл)")

    # 4. Если оператор отправил аудиозапись (музыка/подкаст)
    elif audio:
        has_media = True
        a_id = audio.get("file_id")
        a_name = audio.get("file_name") or "audio.mp3"
        res = await tg_client.get_file_bytes(a_id)
        if res:
            a_bytes, _ = res
            audio_token = await max_client.upload_audio(a_name, a_bytes)
            if audio_token:
                attachments.append({"type": "audio", "payload": {"token": audio_token}})
                media_info.append(f"Аудиозапись: {a_name}")

    # 5. Если видео или анимация (GIF)
    elif video or animation:
        has_media = True
        v_obj = video or animation
        v_id = v_obj.get("file_id")
        v_name = v_obj.get("file_name") or ("video.mp4" if video else "animation.mp4")
        res = await tg_client.get_file_bytes(v_id)
        if res:
            v_bytes, _ = res
            f_token = await max_client.upload_file(v_name, v_bytes)
            if f_token:
                attachments.append({"type": "file", "payload": {"token": f_token}})
                media_info.append(f"Видео/Анимация: {v_name}")

    if not text and not attachments:
        return

    logger.info(
        "📤 Оператор %s -> MAX [User %s]: %s (вложений: %s)",
        op_name, max_user_id, text[:60] if text else "[Медиа]", len(attachments)
    )

    # Отправка клиенту в MAX
    success, err_msg = await max_client.send_message(
        user_id=max_user_id,
        text=text,
        attachments=attachments,
    )

    if success:
        # Поиск активной заявки и фиксация первого ответа (RT)
        target_ticket = None
        if msg_thread_id:
            target_ticket = storage.get_active_ticket_by_thread(msg_thread_id)
        if not target_ticket:
            target_ticket = storage.get_active_ticket(max_user_id)

        first_resp_ticket = None
        if target_ticket and not target_ticket.get("first_response_at"):
            first_resp_ticket = storage.record_first_response(target_ticket["id"], op_name)

        if first_resp_ticket and first_resp_ticket.get("rt_seconds") is not None and first_resp_ticket.get("status") == "IN_PROGRESS":
            t_num = first_resp_ticket["ticket_number"]
            rt_disp = format_duration(first_resp_ticket.get("rt_seconds"))
            t_name = storage.get_topic_name_by_thread(msg_thread_id) if msg_thread_id else max_user_name
            topic_link = get_topic_link(TG_CHAT_ID, msg_thread_id, t_name)

            progress_card = (
                f"👨‍💻 <b>ЗАЯВКА #{t_num} ВЗЯТА В РАБОТУ</b>\n"
                f"👤 <b>Клиент:</b> {html.escape(max_user_name)} <code>[MAX: {max_user_id}]</code>\n"
                f"👨‍🔧 <b>Инженер:</b> {html.escape(op_name)}\n"
                f"⏱ <b>Время первого отклика (RT):</b> {rt_disp}\n"
                f"📍 <b>Ветка:</b> {topic_link}"
            )
            await tg_client.send_message(
                chat_id=TG_CHAT_ID,
                text=progress_card,
                message_thread_id=None,
            )

        # Служебное подтверждение доставки
        confirm_id = await tg_client.send_message(
            chat_id=TG_CHAT_ID,
            text=f"<i>[OK] Доставлено клиенту в MAX ({op_name})</i>",
            reply_to_message_id=msg.get("message_id"),
            message_thread_id=msg_thread_id,
        )
        if confirm_id:
            async def _auto_delete(msg_id: int):
                await asyncio.sleep(1.5)
                await tg_client.delete_message(TG_CHAT_ID, msg_id)
            asyncio.create_task(_auto_delete(confirm_id))

        # Сохраняем в историю
        logged_ticket_id = (
            first_resp_ticket["id"] if first_resp_ticket
            else (target_ticket["id"] if target_ticket else None)
        )
        storage.log_message(
            direction="OPERATOR_TO_CLIENT",
            max_user_id=max_user_id,
            max_user_name=max_user_name,
            operator_name=op_name,
            text=text,
            has_attachments=has_media,
            attachment_info=", ".join(media_info),
            tg_message_id=msg.get("message_id"),
            ticket_id=logged_ticket_id,
        )
    else:
        err_hint = f"\n<code>{err_msg[:200]}</code>" if err_msg else ""
        await tg_client.send_message(
            chat_id=TG_CHAT_ID,
            text=f"<b>[ERROR] Ошибка доставки:</b> Не удалось отправить сообщение в MAX API.{err_hint}",
            reply_to_message_id=msg.get("message_id"),
            message_thread_id=msg_thread_id,
        )


async def poll_telegram_updates():
    """Слушаем события из Telegram через Long Polling и пересылаем ответы операторов в MAX."""
    offset = 0
    logger.info("🟢 Запущен опрос Telegram Bot API (Long Polling)...")

    while True:
        try:
            url = f"{tg_client.base_url}/getUpdates?offset={offset}&timeout=20"
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.error("Telegram getUpdates статус %s: %s", resp.status_code, resp.text)
                    await asyncio.sleep(2.0)
                    continue
                data = resp.json()

            for up in data.get("result", []):
                offset = up["update_id"] + 1
                try:
                    await process_telegram_update(up)
                except Exception as ex:
                    logger.error("Ошибка обработки события Telegram: %s", ex, exc_info=True)

            await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            break
        except httpx.ReadTimeout:
            continue
        except Exception as e:
            logger.error("Ошибка в цикле опроса Telegram: %s", e, exc_info=True)
            await asyncio.sleep(3.0)


# ==============================================================================
# ОБРАБОТЧИКИ ВЕБХУКОВ (ДЛЯ РЕЖИМА WEBHOOK)
# ==============================================================================

async def max_webhook_handler(request: "web.Request") -> "web.Response":
    """Прием входящих вебхуков от MAX Messenger Bot API."""
    try:
        data = await request.json()
        if isinstance(data, list):
            for item in data:
                await process_max_update(item)
        elif isinstance(data, dict):
            if "updates" in data and isinstance(data["updates"], list):
                for item in data["updates"]:
                    await process_max_update(item)
            else:
                await process_max_update(data)
        return web.Response(text="OK", status=200)
    except Exception as e:
        logger.error("Ошибка в max_webhook_handler: %s", e, exc_info=True)
        return web.Response(text="Internal Error", status=500)


async def telegram_webhook_handler(request: "web.Request") -> "web.Response":
    """Прием входящих вебхуков от Telegram Bot API."""
    if WEBHOOK_SECRET:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret != WEBHOOK_SECRET:
            logger.warning("Отклонен неавторизованный вебхук Telegram: неверный secret_token")
            return web.Response(text="Unauthorized", status=403)
    try:
        data = await request.json()
        if isinstance(data, dict):
            await process_telegram_update(data)
        return web.Response(text="OK", status=200)
    except Exception as e:
        logger.error("Ошибка в telegram_webhook_handler: %s", e, exc_info=True)
        return web.Response(text="Internal Error", status=500)


async def health_handler(request: "web.Request") -> "web.Response":
    """Проверка жизнеспособности сервиса для Bothost и облачных балансировщиков."""
    return web.json_response({
        "status": "healthy",
        "service": "emergency_sd_bridge",
        "transport": TRANSPORT_MODE,
        "topics_enabled": USE_FORUM_TOPICS,
    })


# ==============================================================================
# ТОЧКА ВХОДА С ПОДДЕРЖКОЙ POLLING И WEBHOOK РЕЖИМОВ
# ==============================================================================

async def main():
    print("\n" + "=" * 65)
    print(f"[*] ЗАПУСК АВАРИЙНОГО МОСТА: MAX Messenger <-> Telegram [РЕЖИМ: {TRANSPORT_MODE.upper()}]")
    print("=" * 65)

    # Проверка подключения к Telegram
    tg_me = await tg_client.get_me()
    if tg_me:
        logger.info("[OK] Подключение к Telegram: @%s (%s)", tg_me.get("username"), tg_me.get("first_name"))
    else:
        logger.error("[ERROR] Не удалось подключиться к Telegram! Проверьте TELEGRAM_BOT_TOKEN в .env")
        return

    # Проверка подключения к MAX
    max_me = await max_client.get_me()
    if max_me:
        logger.info("[OK] Подключение к MAX: %s (@%s, ID: %s)", max_me.get("name"), max_me.get("username"), max_me.get("user_id"))
    else:
        logger.error("[ERROR] Не удалось подключиться к MAX! Проверьте MAX_BOT_TOKEN в .env")
        return

    logger.info("[INFO] Чат операторов Telegram: %s", TG_CHAT_ID)
    logger.info("[INFO] База данных истории: %s", DB_PATH)
    print("=" * 65)
    print("[READY] МОСТ АКТИВЕН: Чтобы ответить клиенту, напишите в его ветку (топик)!")
    print("=" * 65 + "\n")

    if TRANSPORT_MODE == "webhook":
        if not web:
            logger.error("[ERROR] Для режима webhook необходимо установить aiohttp: pip install aiohttp")
            return
        if not WEBHOOK_BASE_URL:
            logger.error("[ERROR] Режим webhook активен, но не указан WEBHOOK_BASE_URL в .env!")
            logger.error("Укажите WEBHOOK_BASE_URL=https://... (публичный адрес бота на хостинге).")
            return

        clean_base = WEBHOOK_BASE_URL.rstrip("/")
        max_hook = f"{clean_base}/webhook/max"
        tg_hook = f"{clean_base}/webhook/telegram"

        logger.info("[WEBHOOK] Регистрация вебхука MAX: %s", max_hook)
        await max_client.set_webhook(max_hook)

        logger.info("[WEBHOOK] Регистрация вебхука Telegram: %s", tg_hook)
        await tg_client.set_webhook(tg_hook, secret_token=WEBHOOK_SECRET or None)

        app = web.Application()
        app.router.add_post("/webhook/max", max_webhook_handler)
        app.router.add_post("/webhook/telegram", telegram_webhook_handler)
        app.router.add_get("/health", health_handler)
        app.router.add_get("/", health_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, WEBHOOK_HOST, WEBHOOK_PORT)
        logger.info("🚀 Вебхук-сервер запущен на http://%s:%s", WEBHOOK_HOST, WEBHOOK_PORT)
        await site.start()

        # Держим приложение активным
        while True:
            await asyncio.sleep(3600)

    else:
        # Режим Long Polling: безопасно очищаем только собственные вебхуки моста, сохраняя вебхук ЭЛМЫ
        logger.info("[POLLING] Проверка вебхуков моста для работы в режиме Long Polling...")
        await max_client.delete_bridge_subscriptions()
        await tg_client.delete_webhook()

        logger.info("🟢 Запущен параллельный Long Polling для MAX и Telegram...")
        await asyncio.gather(
            poll_max_updates(),
            poll_telegram_updates(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Мост остановлен пользователем.")
