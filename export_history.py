"""Скрипт экспорта истории обращений и реестра заявок из аварийного моста в CSV и JSON.

Запуск:
    python export_history.py

Создает файлы:
    - tickets_registry_YYYYMMDD_HHMMSS.csv (реестр заявок дежурной смены для руководства и ELMA365)
    - tickets_registry_YYYYMMDD_HHMMSS.json
    - history_export_YYYYMMDD_HHMMSS.csv (полная стенограмма всех сообщений с UTF-8-SIG для Excel)
    - history_export_YYYYMMDD_HHMMSS.json
"""

import os
import sys
import sqlite3
import csv
import json
from datetime import datetime
from typing import Optional

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bridge_history.db")

if not os.path.exists(DB_PATH):
    print(f"[!] База данных истории не найдена по пути: {DB_PATH}")
    print("Сначала запустите мост и проведите хотя бы одно сообщение.")
    sys.exit(1)


def format_duration(seconds: Optional[int]) -> str:
    """Форматирование секунд в читаемый вид (X мин Y сек)."""
    if seconds is None:
        return "—"
    m = seconds // 60
    s = seconds % 60
    if m > 0:
        return f"{m} мин {s} сек"
    return f"{s} сек"


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Проверяем наличие таблицы tickets
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tickets';")
has_tickets_table = cursor.fetchone() is not None

ticket_rows = []
if has_tickets_table:
    cursor.execute("""
        SELECT id, ticket_number, max_user_id, max_user_name, message_thread_id,
               status, assigned_operator, initial_text, resolution,
               created_at, first_response_at, closed_at, rt_seconds, mttr_seconds
        FROM tickets
        ORDER BY ticket_number ASC;
    """)
    ticket_rows = cursor.fetchall()

# Запрос сообщений
try:
    cursor.execute("""
        SELECT id, timestamp, direction, max_user_id, max_user_name, operator_name,
               text, has_attachments, attachment_info, tg_message_id, ticket_id
        FROM dialogue_history
        ORDER BY id ASC;
    """)
except sqlite3.OperationalError:
    cursor.execute("""
        SELECT id, timestamp, direction, max_user_id, max_user_name, operator_name,
               text, has_attachments, attachment_info, tg_message_id, NULL as ticket_id
        FROM dialogue_history
        ORDER BY id ASC;
    """)

history_rows = cursor.fetchall()
conn.close()

if not history_rows and not ticket_rows:
    print("ℹ️ В базе данных пока нет сохраненных заявок и сообщений.")
    sys.exit(0)

ts_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

# ==============================================================================
# 1. ЭКСПОРТ РЕЕСТРА ЗАЯВОК (ServiceDesk)
# ==============================================================================
ticket_records = []
if ticket_rows:
    for t in ticket_rows:
        t_dict = dict(t)
        ticket_records.append({
            "№ заявки": f"#{t_dict['ticket_number']}",
            "MAX ID клиента": t_dict["max_user_id"],
            "Клиент": t_dict["max_user_name"] or "",
            "Инженер": t_dict["assigned_operator"] or "Не назначен",
            "Статус": t_dict["status"],
            "Суть обращения": t_dict["initial_text"] or "",
            "Решение / Заметка": t_dict["resolution"] or "",
            "Время открытия (UTC)": t_dict["created_at"] or "",
            "Первый ответ (UTC)": t_dict["first_response_at"] or "",
            "Время закрытия (UTC)": t_dict["closed_at"] or "",
            "RT (сек)": t_dict["rt_seconds"] if t_dict["rt_seconds"] is not None else "",
            "RT (читаемый)": format_duration(t_dict["rt_seconds"]),
            "MTTR (сек)": t_dict["mttr_seconds"] if t_dict["mttr_seconds"] is not None else "",
            "MTTR (читаемый)": format_duration(t_dict["mttr_seconds"]),
        })

    tickets_csv_file = os.path.join(BASE_DIR, f"tickets_registry_{ts_suffix}.csv")
    with open(tickets_csv_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(ticket_records[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(ticket_records)

    tickets_json_file = os.path.join(BASE_DIR, f"tickets_registry_{ts_suffix}.json")
    with open(tickets_json_file, "w", encoding="utf-8") as f:
        json.dump(ticket_records, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 2. ЭКСПОРТ СТЕНОГРАММЫ СООБЩЕНИЙ
# ==============================================================================
history_records = []
total_in = 0
total_out = 0

for r in history_rows:
    r_dict = dict(r)
    direction_label = "Клиент -> Оператор" if r_dict["direction"] == "CLIENT_TO_OPERATOR" else "Оператор -> Клиент"
    if r_dict["direction"] == "CLIENT_TO_OPERATOR":
        total_in += 1
    else:
        total_out += 1

    history_records.append({
        "№": r_dict["id"],
        "№ заявки": f"#{r_dict['ticket_id']}" if r_dict.get("ticket_id") else "",
        "Время (UTC)": r_dict["timestamp"],
        "Направление": direction_label,
        "MAX ID клиента": r_dict["max_user_id"],
        "Имя клиента": r_dict["max_user_name"] or "",
        "Оператор (Telegram)": r_dict["operator_name"] or "",
        "Текст сообщения": r_dict["text"] or "",
        "Вложения": "Да" if r_dict["has_attachments"] else "Нет",
        "Ссылки на вложения": r_dict["attachment_info"] or "",
        "TG Message ID": r_dict["tg_message_id"] or "",
    })

history_csv_file = os.path.join(BASE_DIR, f"history_export_{ts_suffix}.csv")
if history_records:
    with open(history_csv_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(history_records[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(history_records)

    history_json_file = os.path.join(BASE_DIR, f"history_export_{ts_suffix}.json")
    with open(history_json_file, "w", encoding="utf-8") as f:
        json.dump(history_records, f, ensure_ascii=False, indent=2)

print("\n" + "=" * 65)
print("[*] ВЫГРУЗКА ДАННЫХ ИЗ АВАРИЙНОГО МОСТА ЗАВЕРШЕНА")
print("=" * 65)
if ticket_records:
    print(f"📊 Заявок в реестре:    {len(ticket_records)}")
    closed_cnt = sum(1 for t in ticket_records if t["Статус"] == "CLOSED")
    open_cnt = len(ticket_records) - closed_cnt
    print(f"   Закрыто:             {closed_cnt}")
    print(f"   В работе / Открыто:  {open_cnt}")
    print(f"📄 Реестр заявок (CSV): {tickets_csv_file}")
    print(f"📄 Реестр заявок (JSON):{tickets_json_file}")
    print("-" * 65)

print(f"💬 Всего сообщений:     {len(history_records)}")
print(f"   Входящих от клиентов:{total_in}")
print(f"   Ответов операторов:  {total_out}")
print(f"📄 История чата (CSV):  {history_csv_file}")
print("=" * 65 + "\n")
