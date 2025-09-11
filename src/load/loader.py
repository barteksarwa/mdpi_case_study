# src/load/loader.py
import json
import psycopg2
import psycopg2.extras
from logging import Logger
from src.utils.config import Config

class Loader:
    def __init__(self, config: Config, logger: Logger):
        self.config = config
        self.logger = logger
        self.table_name = "crossref_data"
        self.conn = None
        self.cursor = None
        self.create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            doi TEXT UNIQUE,
            type TEXT,
            title TEXT,
            authors JSONB,
            published_date DATE,
            journal TEXT,
            publisher TEXT,
            volume TEXT,
            issue TEXT,
            page TEXT,
            print_issn TEXT,
            electronic_issn TEXT,
            abstract TEXT,
            license_url TEXT,
            reference_count INTEGER,
            is_referenced_by_count INTEGER
        );
        """
        self.insert_query = f"""
        INSERT INTO {self.table_name} (
            doi, type, title, authors, published_date, journal, publisher,
            volume, issue, page, print_issn, electronic_issn, abstract, license_url,
            reference_count, is_referenced_by_count
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (doi) DO NOTHING;
        """
        self.connect_to_db()
        self.create_table()

    def connect_to_db(self):
        try:
            self.conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                dbname=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Connected to Postgres via psycopg2.")
        except Exception as e:
            self.logger.error("DB connection error: %s", e)
            raise

    def create_table(self):
        try:
            self.cursor.execute(self.create_table_query)
            self.conn.commit()
            self.logger.info("Table ensured.")
        except Exception as e:
            self.conn.rollback()
            self.logger.error("Create table failed: %s", e)
            raise

    def load_data(self, data):
        try:
            for record in data:
                authors_json = psycopg2.extras.Json(record.get("authors", []))
                self.cursor.execute(self.insert_query, (
                    record.get("doi"),
                    record.get("type"),
                    record.get("title"),
                    authors_json,
                    record.get("published_date"),
                    record.get("journal"),
                    record.get("publisher"),
                    record.get("volume"),
                    record.get("issue"),
                    record.get("page"),
                    record.get("print_issn"),
                    record.get("electronic_issn"),
                    record.get("abstract"),
                    record.get("license_url"),
                    record.get("reference_count"),
                    record.get("is_referenced_by_count"),
                ))
            self.conn.commit()
            self.logger.info("Loaded %d records into %s", len(data), self.table_name)
        except Exception as e:
            self.conn.rollback()
            self.logger.error("Error loading data: %s", e)
            raise
        finally:
            self.cursor.close()
            self.conn.close()
            self.logger.info("DB connection closed.")
