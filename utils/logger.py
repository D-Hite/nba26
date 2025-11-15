import logging
import inspect
import os

# Ensure logs folder exists
os.makedirs("logs", exist_ok=True)

# ---- MAIN LOGGER ----
app_logger = logging.getLogger("StatLogger")
app_logger.setLevel(logging.INFO)

if not app_logger.handlers:
    handler = logging.FileHandler("logs/log.log")
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s | %(filename)s | %(funcName)s"
    )
    handler.setFormatter(formatter)
    app_logger.addHandler(handler)

# ---- SQL LOGGER ----
sql_logger = logging.getLogger("SQLLogger")
sql_logger.setLevel(logging.INFO)

if not sql_logger.handlers:
    sql_handler = logging.FileHandler("logs/sql.log")
    sql_formatter = logging.Formatter("%(asctime)s | SQL | %(message)s")
    sql_handler.setFormatter(sql_formatter)
    sql_logger.addHandler(sql_handler)

# ---- CONVENIENCE FUNCTIONS ----

def info(msg: str):
    caller = inspect.stack()[1].function
    app_logger.info(f"[{caller}] {msg}")

def warning(msg: str):
    caller = inspect.stack()[1].function
    app_logger.warning(f"[{caller}] {msg}")

def error(msg: str):
    caller = inspect.stack()[1].function
    app_logger.error(f"[{caller}] {msg}")

def sql(msg: str):
    sql_logger.info(msg)
