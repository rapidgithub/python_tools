#!/usr/bin/env python3
"""
Robust reader for Hive Web UI pages.

Notes:
- Requires: requests, beautifulsoup4
- Put configuration in config.props with section [default] and keys:
    log_level (INFO, WARNING, ERROR) - optional, default WARNING
    hs2_host
    hs2_webport
"""
import configparser
import logging
import sys
from typing import List, Tuple, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout

DEFAULT_TIMEOUT = 5  # seconds
RETRY_ATTEMPTS = 2


def load_config(path: str = "config.props") -> configparser.ConfigParser:
    """Load configuration from file."""
    cfg = configparser.ConfigParser()
    read_files = cfg.read(path)
    if not read_files:
        logging.warning(
            "Config file %s not found; using environment/defaults where possible.",
            path
        )
    return cfg


def setup_logging(cfg: configparser.ConfigParser) -> None:
    """Configure logging based on config settings."""
    level_str = "WARNING"
    try:
        level_str = cfg.get("default", "log_level")
    except (configparser.NoSectionError, configparser.NoOptionError):
        pass

    level = getattr(logging, level_str.upper(), logging.WARNING)
    logging.basicConfig(
        format="%(asctime)-15s ::%(levelname)s:: %(message)s", level=level
    )


def get_base_url(cfg: configparser.ConfigParser) -> str:
    """Retrieve base URL from configuration."""
    try:
        host = cfg.get("default", "hs2_host")
        port = cfg.get("default", "hs2_webport")
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logging.error("Missing configuration values: %s", e)
        raise
    return f"http://{host}:{port}"


def fetch_soup(
    session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT
) -> Optional[BeautifulSoup]:
    """Fetch URL and return BeautifulSoup or None on failure (caller may decide)."""
    last_exc = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "lxml")
        except (RequestException, Timeout) as ex:
            last_exc = ex
            logging.debug("Attempt %d: failed to fetch %s: %s", attempt, url, ex)
    logging.error(
        "Unable to connect to %s after %d attempts: %s",
        url,
        RETRY_ATTEMPTS,
        last_exc
    )
    return None


def process_table(table_body) -> Tuple[List[str], List[List[List[str]]]]:
    """
    Process HTML table and extract hrefs and data.

    Returns a tuple (hrefs, data_lists)
      - hrefs: list of href strings found in the table (only href attribute values)
      - data_lists: list of table row lists for the table (each element is a list of
                    rows; each row is list of cell texts)
    """
    hrefs = [a.get("href") for a in table_body.find_all("a", href=True)]
    rows = table_body.find_all("tr")
    data = []
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if cols:
            data.append(cols)
    # data wrapped in list for consistency with possible multiple tables processed
    return hrefs, [data]


def extract_values_from_data_lists(data_lists: List[List[List[str]]]) -> List[str]:
    """
    Extract values from data table lists.

    Given a list of data tables (each a list of rows), extract second-column values
    where possible. Returns list of values found.
    """
    result = []
    for data in data_lists:
        for row in data:
            if len(row) >= 2:
                key = row[0]
                val = row[1]
                logging.info("Found %s=%s", key, val)
                result.append(val)
    return result


def print_details(
    session: requests.Session, base_url: str, table_soup, table_type: str
) -> List[str]:
    """
    Process table and extract details.

    Given a table soup (the table element from the base page), find query links
    and visit each.
    """
    hrefs, base_data_lists = process_table(table_soup)
    if not hrefs:
        logging.info("No %s query found.", table_type)
        return []

    logging.info("Printing %d %s queries.", len(hrefs), table_type)
    collected_data_lists: List[List[List[str]]] = list(base_data_lists)

    for href in hrefs:
        full_url = urljoin(base_url + "/", href)  # ensure proper joining
        logging.debug("Visiting query page: %s", full_url)
        page_soup = fetch_soup(session, full_url)
        if page_soup is None:
            logging.warning("Skipping %s due to fetch failure.", full_url)
            continue
        tables = page_soup.find_all("table")
        if not tables:
            logging.debug("No tables found on page %s", full_url)
            continue
        _, data_lists = process_table(tables[0])
        collected_data_lists.extend(data_lists)

    return extract_values_from_data_lists(collected_data_lists)


def main():
    """Main entry point for the Hive Web UI reader."""
    cfg = load_config()
    setup_logging(cfg)
    try:
        base_url = get_base_url(cfg)
    except (configparser.NoSectionError, configparser.NoOptionError):
        logging.error("Unable to determine base URL from config; exiting.")
        sys.exit(1)

    session = requests.Session()
    open_results: List[str] = []
    closed_results: List[str] = []

    root_soup = fetch_soup(session, base_url)
    if root_soup is None:
        logging.error("Failed to fetch root page %s; exiting.", base_url)
        sys.exit(1)

    tables = root_soup.find_all("table")
    # Expecting at least three tables; be defensive.
    if len(tables) < 3:
        logging.error(
            "Unexpected page layout: expected at least 3 tables on %s, found %d",
            base_url,
            len(tables)
        )
    else:
        open_table = tables[1]
        closed_table = tables[2]
        open_results = print_details(session, base_url, open_table, "running")
        closed_results = print_details(session, base_url, closed_table, "closed")

    print(
        f"List of open: {open_results}\nList of closed: {closed_results}"
    )


if __name__ == "__main__":
    main()
