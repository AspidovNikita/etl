#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import csv
import sys
import os
import xml.etree.ElementTree as ET
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any

import hydra
from omegaconf import DictConfig
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ------------------------------------------------------------------------------
# HTTP Session Setup
# ------------------------------------------------------------------------------
def create_http_session(cfg: DictConfig) -> requests.Session:
    """Создаёт сессию requests с заданным User-Agent."""
    session = requests.Session()
    session.headers.update({"User-Agent": cfg.api.user_agent})
    return session


# ------------------------------------------------------------------------------
# Database Layer (with retry)
# ------------------------------------------------------------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.Error),
    reraise=True
)
def get_db_connection(cfg: DictConfig) -> psycopg2.extensions.connection:
    """Устанавливает соединение с PostgreSQL, используя параметры из конфигурации."""
    try:
        conn = psycopg2.connect(
            dbname=cfg.db.name,
            user=cfg.db.user,
            password=cfg.db.password,
            host=cfg.db.host,
            port=cfg.db.port
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        logging.getLogger(__name__).exception("Database connection failure")
        raise


def initialize_tables(cfg: DictConfig) -> None:
    """Создаёт таблицы для хранения курсов валют и логов ETL-запусков."""
    logger = logging.getLogger(__name__)
    with get_db_connection(cfg) as conn:
        with conn.cursor() as cur:
            # Таблица курсов валют с уникальным ограничением
            cur.execute("""
                CREATE TABLE IF NOT EXISTS currency_rates (
                    id SERIAL PRIMARY KEY,
                    currency_code VARCHAR(3) NOT NULL,
                    currency_name VARCHAR(100),
                    unit INTEGER,
                    rate DECIMAL(15,4),
                    rate_date DATE NOT NULL,
                    etl_id VARCHAR(32),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            # Добавляем уникальное ограничение, если его ещё нет
            cur.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'currency_rates_rate_date_currency_code_key'
                    ) THEN
                        ALTER TABLE currency_rates 
                        ADD CONSTRAINT currency_rates_rate_date_currency_code_key 
                        UNIQUE (rate_date, currency_code);
                    END IF;
                END $$;
            """)
            # Индекс для ускорения выборок (опционально)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_currency_rates_date_code 
                ON currency_rates(rate_date, currency_code);
            """)
            
            # Таблица логов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl_runs (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(32) UNIQUE NOT NULL,
                    started_at TIMESTAMP DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    source_url TEXT,
                    records_processed INTEGER,
                    status VARCHAR(20),
                    error_message TEXT
                );
            """)
            conn.commit()
    logger.info("Database tables checked/created successfully.")
def persist_currency_rates(
    cfg: DictConfig,
    rates: List[Dict[str, Any]],
    etl_id: str,
    rate_date: str
) -> int:
    """
    Сохраняет список курсов валют в таблицу currency_rates.
    Используется UPSERT на основе комбинации (rate_date, currency_code).
    """
    logger = logging.getLogger(__name__)
    if not rates:
        logger.info("No currency data to insert.")
        return 0

    query = """
        INSERT INTO currency_rates 
            (currency_code, currency_name, unit, rate, rate_date, etl_id)
        VALUES %s
        ON CONFLICT (rate_date, currency_code) DO UPDATE SET
            currency_name = EXCLUDED.currency_name,
            unit = EXCLUDED.unit,
            rate = EXCLUDED.rate,
            etl_id = EXCLUDED.etl_id,
            created_at = NOW();
    """

    values = [
        (
            r["code"],
            r["name"],
            r["unit"],
            r["rate"],
            rate_date,
            etl_id
        )
        for r in rates
    ]

    try:
        with get_db_connection(cfg) as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, values)
                conn.commit()
        logger.info(f"Persisted {len(rates)} currency rates (UPSERT).")
    except Exception as e:
        logger.exception("Error while saving currency rates to database")
        raise
    return len(rates)


def log_etl_run(
    cfg: DictConfig,
    run_id: str,
    source_url: str,
    records_processed: int,
    status: str,
    error_message: Optional[str] = None,
    finished_at: Optional[datetime] = None
) -> None:
    """Записывает информацию о запуске ETL в таблицу etl_runs."""
    logger = logging.getLogger(__name__)
    finished_at = finished_at or datetime.now()
    try:
        with get_db_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_runs (run_id, source_url, records_processed, status, error_message, finished_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO NOTHING;
                """, (run_id, source_url, records_processed, status, error_message, finished_at))
                conn.commit()
        logger.info("ETL run logged in database.")
    except Exception as e:
        logger.exception("Failed to write ETL log to database")


# ------------------------------------------------------------------------------
# CSV Logging (fallback)
# ------------------------------------------------------------------------------
def write_csv_log(
    cfg: DictConfig,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    records_processed: int,
    status: str,
    error_message: Optional[str] = None
) -> None:
    """Дописывает строку лога в CSV-файл."""
    logger = logging.getLogger(__name__)
    filename = cfg.csv_log.log_file
    try:
        file_exists = os.path.isfile(filename)
        with open(filename, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            if not file_exists:
                writer.writerow([
                    "run_id", "started_at", "finished_at", "records_processed",
                    "status", "error_message"
                ])
            writer.writerow([
                run_id,
                started_at.strftime("%Y-%m-%d %H:%M:%S"),
                finished_at.strftime("%Y-%m-%d %H:%M:%S"),
                records_processed,
                status,
                error_message or ""
            ])
        logger.info(f"Log entry appended to CSV: {filename}")
    except Exception as e:
        logger.exception("Failed to write log to CSV")


# ------------------------------------------------------------------------------
# Data Source: Central Bank of Russia (XML)
# ------------------------------------------------------------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def fetch_currency_xml(session: requests.Session, url: str) -> bytes:
    """Загружает XML-ленту курсов валют с сайта ЦБ РФ."""
    logger = logging.getLogger(__name__)
    logger.debug(f"Fetching XML from {url}")
    try:
        resp = session.get(url)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.exception("Error while downloading currency XML")
        raise
    return resp.content


def parse_currency_xml(xml_content: bytes) -> List[Dict[str, Any]]:
    """
    Парсит XML, возвращает список словарей с полями:
    code, name, unit, rate.
    """
    root = ET.fromstring(xml_content)
    rates = []
    for valute in root.findall("Valute"):
        char_code = valute.find("CharCode").text
        name = valute.find("Name").text
        nominal = int(valute.find("Nominal").text)
        value = valute.find("Value").text.replace(",", ".")
        rate = float(value)

        rates.append({
            "code": char_code,
            "name": name,
            "unit": nominal,
            "rate": rate / nominal  # приводим к курсу за единицу валюты
        })
    return rates


def save_rates_to_csv(rates: List[Dict[str, Any]], filename: str) -> None:
    """Сохраняет список курсов в CSV-файл (для отладки или резерва)."""
    logger = logging.getLogger(__name__)
    if not rates:
        logger.info("No rates to save to CSV.")
        return
    try:
        with open(filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=rates[0].keys(), delimiter=";")
            writer.writeheader()
            writer.writerows(rates)
        logger.info(f"Saved {len(rates)} currency rates to {filename}")
    except Exception as e:
        logger.exception("Error writing CSV with rates")
        raise


# ------------------------------------------------------------------------------
# Main Hydra Application
# ------------------------------------------------------------------------------
@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:
    # Настройка логирования (все сообщения в консоль и, возможно, в файл)
    log_handlers = [logging.StreamHandler()]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=log_handlers
    )
    logger = logging.getLogger(__name__)

    start_time = datetime.now()
    run_id = uuid.uuid4().hex
    records_processed = 0
    status = "error"
    error_message = None
    rate_date = datetime.now().strftime("%Y-%m-%d")  # дата, на которую получены курсы

    try:
        session = create_http_session(cfg)

        # Загрузка XML с курсами
        xml_url = cfg.api.cbr_url
        logger.info(f"Fetching currency rates from {xml_url}")
        xml_data = fetch_currency_xml(session, xml_url)

        # Парсинг
        rates = parse_currency_xml(xml_data)
        logger.info(f"Parsed {len(rates)} currency records.")

        # Сохранение
        if cfg.parser.db_enabled:
            initialize_tables(cfg)
            saved = persist_currency_rates(cfg, rates, run_id, rate_date)
            records_processed = saved
        else:
            if cfg.parser.no_csv:
                logger.info("First 5 rates:")
                for r in rates[:5]:
                    logger.info(f"{r['code']}: {r['name']} – {r['unit']} {r['rate']:.4f}")
            else:
                save_rates_to_csv(rates, cfg.parser.output_file)
                records_processed = len(rates)

        status = "success"

    except Exception as e:
        error_message = str(e)[:500]
        logger.exception("Critical error during script execution")
        status = "error"

    finally:
        finish_time = datetime.now()

        # Логирование в БД
        if cfg.parser.db_enabled:
            log_etl_run(
                cfg, run_id, cfg.api.cbr_url,
                records_processed, status, error_message, finish_time
            )

        # Логирование в CSV (если не отключено)
        if not cfg.csv_log.no_log:
            write_csv_log(
                cfg, run_id, start_time, finish_time,
                records_processed, status, error_message
            )


if __name__ == "__main__":
    main()