"""Универсальная точка входа для облачного хостинга (Bothost, PaaS, Docker).

Запускает аварийный мост MAX <-> Telegram.
"""
import sys
import asyncio
import logging
from bridge import main

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Мост остановлен оператором.")
        sys.exit(0)
