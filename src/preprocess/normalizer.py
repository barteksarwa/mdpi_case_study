from datetime import datetime
from typing import List, Dict, Any
import logging

class Normalizer:
    """
    Normalizer class to flatten and standardize CrossRef items for staging.
    """

    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(__name__)

    def normalize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a single CrossRef item into a flat dictionary for SQL staging.

        :param data: Raw item dict from CrossRef API.
        :return: Normalized dict ready for staging.
        :raises ValueError: If DOI is missing.
        """
        doi = data.get("DOI", "").strip().lower()
        if not doi:
            raise ValueError("Missing DOI in item")

        authors = self.__normalize_authors(data.get("author", []))
        if not authors:
            self.logger.warning(f"No authors for DOI {doi}, defaulting to Unknown")
            authors = [{'given': '', 'family': 'Unknown'}]

        normalized = {
            "doi": doi,
            "type": data.get("type", "unknown"),
            "title": self.__safe_first(data.get("title"), "Unknown"),
            "authors": authors,
            "published_date": self.__normalize_date(
                data.get("issued", {}),
                data.get("created", {}),
                doi
            ),
            "journal": self.__safe_first(
                data.get("container-title"),
                self.__safe_first(data.get("short-container-title"), "")
            ),
            "publisher": data.get("publisher", "").strip(),
            "volume": data.get("volume"),
            "issue": data.get("issue"),
            "page": data.get("page"),
            "print_issn": self.__get_issn(data.get("issn-type", []), "print"),
            "electronic_issn": self.__get_issn(data.get("issn-type", []), "electronic"),
            "abstract": data.get("abstract", "").strip(),
            "license_url": self.__get_license_url(data.get("license", [])),
            "reference_count": data.get("reference-count", 0),
            "is_referenced_by_count": data.get("is-referenced-by-count", 0),
        }

        if normalized["title"] == "Unknown":
            self.logger.warning(f"Empty title for DOI {doi}")

        return normalized

    # --- Helpers ---
    def __normalize_authors(self, authors: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        return [
            {
                "given": a.get("given", "").strip(),
                "family": a.get("family", "").strip(),
            }
            for a in authors if a.get("given") or a.get("family")
        ]

    def __normalize_date(self, issued: Dict, created: Dict, doi: str) -> str:
        date_parts = issued.get("date-parts", [[None]])[0] if issued.get("date-parts") else []
        created_parts = created.get("date-parts", [[None]])[0] if created.get("date-parts") else []

        if not date_parts or date_parts[0] is None:
            self.logger.warning(f"Missing issued date for DOI {doi}")
            return None

        year = date_parts[0]
        month = date_parts[1] if len(date_parts) > 1 else 1
        day = date_parts[2] if len(date_parts) > 2 else 1

        created_year = created_parts[0] or datetime.now().year
        current_year = datetime.now().year

        # if year > current_year + 1:
        #     year = created_year

        # Sanitize
        month = month if 1 <= month <= 12 else 1
        day = day if 1 <= day <= 31 else 1

        return f"{year:04d}-{month:02d}-{day:02d}"

    def __get_issn(self, issn_list: List[Dict[str, Any]], issn_type: str) -> str:
        for issn in issn_list:
            if issn.get("type") == issn_type:
                return issn.get("value")
        return None

    def __get_license_url(self, license_list: List[Dict[str, Any]]) -> str:
        return license_list[0].get("URL", "") if license_list else ""

    def __safe_first(self, lst: List[str], default: str = "") -> str:
        return lst[0].strip() if lst else default
