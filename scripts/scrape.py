import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import urllib3
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).resolve().parent.parent
URLS_FILE = BASE_DIR / "urls.json"
RAW_HTML_DIR = BASE_DIR / "data" / "raw_html"
STRUCTURED_DIR = BASE_DIR / "data" / "structured"
REPORTS_DIR = BASE_DIR / "data" / "reports"
SCRAPE_REPORT_FILE = REPORTS_DIR / "scrape_report.json"

RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)
STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

BLOCKED_TEXT_PATTERNS = [
    "dns points to prohibited ip",
    "error 1000",
    "cloudflare",
    "access denied",
    "forbidden",
    "403 forbidden",
    "temporarily unavailable",
]

AUTH_TEXT_PATTERNS = [
    "se connecter",
    "connexion",
    "authentification",
    "login",
    "mot de passe",
    "sign in",
]

UI_NOISE_PHRASES = [
    "skip to content",
    "skip to main content",
    "aller au contenu principal",
    "visual accessibility options",
    "reset all accessibility settings",
    "content adjustements",
    "text spacing",
    "bigger text",
    "line height",
    "dyslexia friendly",
    "display adjustements",
    "contrast +",
    "screen reader",
    "highlight links",
    "cursor",
    "hide images",
    "saturation",
    "spark vibe",
    "fornet",
    "spark vibe fornet",
    "facebook",
    "youtube",
    "instagram",
    "go to the next page",
    "continuer la lecture",
    "plus d'actualites",
    "plus d'actualites",
    "toutes les videos",
    "s'abonner a",
    "francais",
    "العربية",
]

PLACEHOLDER_PATTERNS = [
    r"__[\w\-]+",
    r"\blorem ipsum\b",
]

KEYWORDS_BY_CATEGORY = {
    "cnss": [
        "cnss",
        "immatriculation",
        "allocation",
        "pension",
        "salarie",
        "taawidaty",
        "damancom",
    ],
    "anam": [
        "anam",
        "amo",
        "assurance maladie",
        "couverture medicale",
        "prestataire",
        "inpe",
    ],
    "service_public": [
        "maroc",
        "service numerique",
        "idarati",
        "watiqa",
        "passeport",
        "casier judiciaire",
        "consulat",
        "visa",
    ],
    "cnie": [
        "cnie",
        "cin",
        "carte nationale",
        "identite",
        "epolice",
        "anthropometrique",
    ],
}


def create_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


SESSION = create_session()


def sanitize_filename(text: str) -> str:
    text = re.sub(r"https?://", "", text)
    text = re.sub(r"[^\w\-\.]", "_", text)
    return text[:180]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def suspicious_char_count(text: str) -> int:
    return sum(text.count(marker) for marker in ("Ã", "Â", "â", "Ø", "Ù"))


def fix_mojibake(text: str) -> str:
    best = text
    best_score = suspicious_char_count(text)

    for _ in range(2):
        improved = False
        for encoding in ("latin-1", "cp1252"):
            try:
                candidate = best.encode(encoding).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue

            candidate_score = suspicious_char_count(candidate)
            if candidate_score < best_score:
                best = candidate
                best_score = candidate_score
                improved = True

        if not improved:
            break

    return best


def normalize_text(text: str) -> str:
    text = clean_text(fix_mojibake(text)).lower()
    replacements = {
        "Ã©": "e",
        "Ã¨": "e",
        "Ãª": "e",
        "Ã ": "a",
        "Ã¢": "a",
        "Ã®": "i",
        "Ã¯": "i",
        "Ã´": "o",
        "Ã¹": "u",
        "Ã»": "u",
        "Ã§": "c",
        "â€™": "'",
        "é": "e",
        "è": "e",
        "ê": "e",
        "à": "a",
        "â": "a",
        "î": "i",
        "ï": "i",
        "ô": "o",
        "ù": "u",
        "û": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


def normalize_spacing(text: str) -> str:
    text = clean_text(fix_mojibake(text))
    text = text.replace("…", "...")
    text = re.sub(r"\s+([,.;:!?%)\]])", r"\1", text)
    text = re.sub(r"([(\[])\s+", r"\1", text)
    text = re.sub(r"\b([A-Za-zÀ-ÿ]+)\s+\1\b", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(juille)\s+t\b", r"\1t", text, flags=re.IGNORECASE)
    text = re.sub(r"\bn\s+°\s*", "n° ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=\d)\s+(?=\d{3}\b)", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_noise_text(text: str) -> bool:
    normalized = normalize_text(text)
    normalized = normalized.lstrip("#").strip()
    normalized = normalized.lstrip("-").strip()
    if not normalized:
        return True
    if normalized in UI_NOISE_PHRASES:
        return True
    if normalized in {"x", "facebook x youtube instagram"}:
        return True
    if re.fullmatch(r"[0-9]+", normalized):
        return True
    if normalized in {".", "..", "..."}:
        return True
    if normalized.startswith("en savoir plus sur "):
        return True
    if normalized.startswith("voir plus"):
        return True
    if normalized.startswith("go to the next page"):
        return True
    if any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in PLACEHOLDER_PATTERNS):
        return True
    return False


def remove_noise_lines(text: str) -> str:
    lines = []
    for raw_line in text.splitlines():
        line = normalize_spacing(raw_line)
        if not line or is_noise_text(line):
            continue
        lines.append(line)
    return "\n".join(lines)


def fetch_html(url: str) -> tuple[str | None, dict]:
    meta = {
        "requested_url": url,
        "final_url": url,
        "http_status": None,
        "used_insecure_ssl": False,
        "error": None,
    }

    try:
        response = SESSION.get(url, timeout=25, allow_redirects=True)
        meta["http_status"] = response.status_code
        meta["final_url"] = response.url
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return response.text, meta
    except requests.exceptions.SSLError:
        try:
            response = SESSION.get(url, timeout=25, allow_redirects=True, verify=False)
            meta["http_status"] = response.status_code
            meta["final_url"] = response.url
            meta["used_insecure_ssl"] = True
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            return response.text, meta
        except requests.RequestException as exc:
            meta["error"] = str(exc)
            return None, meta
    except requests.RequestException as exc:
        if getattr(exc, "response", None) is not None:
            meta["http_status"] = exc.response.status_code
            meta["final_url"] = exc.response.url
        meta["error"] = str(exc)
        return None, meta


def get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def extract_title(soup: BeautifulSoup, fallback_title: str) -> str:
    if soup.title and soup.title.get_text(strip=True):
        return normalize_spacing(soup.title.get_text())

    h1 = soup.find("h1")
    if h1:
        return normalize_spacing(h1.get_text())

    return normalize_spacing(fallback_title)


def score_candidate_text(text: str) -> int:
    if not text:
        return 0

    score = len(text)
    score += text.count("\n") * 20

    if ":" in text:
        score += 40

    if any(token in normalize_text(text) for token in ["piece", "document", "demande", "procedure", "condition"]):
        score += 100

    return score


def has_ancestor(node: Tag, names: set[str]) -> bool:
    parent = node.parent
    while isinstance(parent, Tag):
        if parent.name in names:
            return True
        parent = parent.parent
    return False


def format_table_row(row: Tag) -> str:
    cells = []
    for cell in row.find_all(["th", "td"], recursive=False):
        text = normalize_spacing(cell.get_text(" ", strip=True))
        if text and not is_noise_text(text):
            cells.append(text)

    if not cells:
        return ""

    return " | ".join(cells)


def extract_semantic_blocks(container: Tag) -> list[str]:
    blocks = []

    for node in container.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "tr"], recursive=True):
        if node.name == "p" and has_ancestor(node, {"li"}):
            continue
        if node.name == "li" and has_ancestor(node, {"nav"}):
            continue

        if node.name.startswith("h"):
            level = int(node.name[1])
            block = f'{"#" * level} {normalize_spacing(node.get_text(" ", strip=True))}'
        elif node.name == "li":
            block = f"- {normalize_spacing(node.get_text(" ", strip=True))}"
        elif node.name == "tr":
            block = format_table_row(node)
        else:
            block = normalize_spacing(node.get_text(" ", strip=True))

        if not block or is_noise_text(block):
            continue
        blocks.append(block)

    deduped = []
    seen = set()
    for block in blocks:
        normalized = normalize_text(block)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(block)

    return deduped


def extract_list_items(container: Tag) -> list[str]:
    items = []
    for li in container.find_all("li"):
        text = normalize_spacing(li.get_text(" ", strip=True))
        if len(text) < 4 or is_noise_text(text):
            continue
        items.append(text)
    return dedupe_preserve_order(items)


def select_best_container(soup: BeautifulSoup) -> tuple[str, list[str], list[str]]:
    for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    selectors = [
        "main",
        "article",
        "[role='main']",
        ".content",
        ".article",
        ".page-content",
        ".field-item",
        ".container",
        "section",
        "body",
    ]

    best_text = ""
    best_blocks: list[str] = []
    best_items: list[str] = []
    best_score = 0

    for selector in selectors:
        for candidate in soup.select(selector):
            blocks = extract_semantic_blocks(candidate)
            text = remove_noise_lines("\n".join(blocks))
            score = score_candidate_text(text)
            if score > best_score and len(text) >= 120:
                best_text = text
                best_blocks = blocks
                best_items = extract_list_items(candidate)
                best_score = score

    return best_text, best_blocks, best_items


def classify_list_items(list_items: list[str]) -> tuple[list[str], list[str], list[str]]:
    steps = []
    required_documents = []
    eligibility_conditions = []

    for item in list_items:
        lower = normalize_text(item)

        if any(word in lower for word in [
            "etape",
            "demande",
            "deposer",
            "remplir",
            "presenter",
            "prendre rendez",
            "procedure",
            "renouvellement",
            "inscription",
            "suivi",
        ]):
            steps.append(item)

        if any(word in lower for word in [
            "copie",
            "document",
            "piece",
            "justificatif",
            "formulaire",
            "photo",
            "certificat",
            "cnie",
            "cin",
            "extrait",
            "rib",
        ]):
            required_documents.append(item)

        if any(word in lower for word in [
            "condition",
            "beneficiaire",
            "admissible",
            "eligib",
            "avoir",
            "etre",
            "doit",
            "resider",
            "age",
            "jours",
        ]):
            eligibility_conditions.append(item)

    return (
        dedupe_preserve_order(steps),
        dedupe_preserve_order(required_documents),
        dedupe_preserve_order(eligibility_conditions),
    )


def dedupe_preserve_order(items: list[str]) -> list[str]:
    deduped = []
    seen = set()

    for item in items:
        cleaned = normalize_spacing(item)
        if not cleaned or is_noise_text(cleaned):
            continue
        normalized = normalize_text(cleaned)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(cleaned)

    return deduped


def detect_keywords(text: str, category: str) -> list[str]:
    normalized = normalize_text(text)
    found = []
    for keyword in KEYWORDS_BY_CATEGORY.get(category, []):
        if keyword in normalized:
            found.append(keyword)
    return found


def extract_fees(text: str) -> str | None:
    match = re.search(r"(\d+(?:[.,]\d+)?\s*(?:dirham|dirhams|dh|mad))", text, re.I)
    return match.group(1) if match else None


def extract_processing_time(text: str) -> str | None:
    match = re.search(r"(\d+\s*(?:jour|jours|semaine|semaines|mois|an|ans|heure|heures|h))", text, re.I)
    return match.group(1) if match else None


def detect_page_status(title: str, content: str, final_url: str, page_type: str) -> tuple[str, str | None]:
    combined = normalize_text(f"{title} {content} {final_url}")

    if any(pattern in combined for pattern in BLOCKED_TEXT_PATTERNS):
        return "blocked", "blocked_or_error_page"

    if page_type == "authentification":
        return "skipped", "authentication_page"

    if any(pattern in combined for pattern in AUTH_TEXT_PATTERNS) and len(content) < 500:
        return "skipped", "login_page_without_public_content"

    if len(content) < 120:
        return "skipped", "content_too_short"

    return "success", None


def build_record(item: dict, category: str, html: str, fetch_meta: dict) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title = extract_title(soup, item.get("titre", "Sans titre"))
    content, semantic_blocks, list_items = select_best_container(soup)
    steps, required_documents, eligibility_conditions = classify_list_items(list_items)
    final_url = fetch_meta.get("final_url") or item["url"]
    domain = get_domain(final_url)
    status, skip_reason = detect_page_status(title, content, final_url, item.get("type", "procedure"))

    return {
        "id": sanitize_filename(item["url"]),
        "url": item["url"],
        "final_url": final_url,
        "domain": domain,
        "title": title,
        "category": category,
        "page_type": item.get("type", "procedure"),
        "language": item.get("langue", "fr"),
        "priority": item.get("priorite", "moyenne"),
        "responsible_entity": category.upper(),
        "submission_place": None,
        "fees": extract_fees(content),
        "processing_time": extract_processing_time(content),
        "steps": steps,
        "required_documents": required_documents,
        "eligibility_conditions": eligibility_conditions,
        "content_sections": semantic_blocks,
        "keywords": detect_keywords(f"{title} {content}", category),
        "content": content,
        "content_length": len(content),
        "list_items_count": len(list_items),
        "http_status": fetch_meta.get("http_status"),
        "status": status,
        "skip_reason": skip_reason,
        "fetched_at": now_iso(),
    }


def load_urls() -> dict:
    with open(URLS_FILE, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_html(item: dict, use_local_raw: bool) -> tuple[str | None, dict]:
    filename = sanitize_filename(item["url"])
    raw_html_path = RAW_HTML_DIR / f"{filename}.html"

    if use_local_raw and raw_html_path.exists():
        return (
            raw_html_path.read_text(encoding="utf-8"),
            {
                "requested_url": item["url"],
                "final_url": item["url"],
                "http_status": 200,
                "used_insecure_ssl": False,
                "error": None,
            },
        )

    html, fetch_meta = fetch_html(item["url"])
    if html:
        raw_html_path.write_text(html, encoding="utf-8")
    return html, fetch_meta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape ou reprocess les pages source.")
    parser.add_argument(
        "--from-raw",
        action="store_true",
        help="Reconstruit les fichiers structures a partir des HTML locaux sans acces reseau.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Temps d'attente entre les requetes HTTP.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    urls_data = load_urls()
    report_entries = []
    status_counter = Counter()
    total_processed = 0

    for category, items in urls_data["data"].items():
        for item in tqdm(items, desc=f"Scraping {category}"):
            html, fetch_meta = load_html(item, use_local_raw=args.from_raw)
            if not args.from_raw:
                time.sleep(args.delay)

            if not html:
                status_counter["fetch_error"] += 1
                report_entries.append(
                    {
                        "url": item["url"],
                        "category": category,
                        "page_type": item.get("type", "procedure"),
                        "status": "fetch_error",
                        "reason": fetch_meta.get("error"),
                        "http_status": fetch_meta.get("http_status"),
                        "final_url": fetch_meta.get("final_url"),
                    }
                )
                continue

            filename = sanitize_filename(item["url"])
            record = build_record(item, category, html, fetch_meta)
            structured_path = STRUCTURED_DIR / f"{filename}.json"
            structured_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

            total_processed += 1
            status_counter[record["status"]] += 1
            report_entries.append(
                {
                    "url": record["url"],
                    "final_url": record["final_url"],
                    "category": record["category"],
                    "page_type": record["page_type"],
                    "status": record["status"],
                    "reason": record["skip_reason"],
                    "http_status": record["http_status"],
                    "content_length": record["content_length"],
                    "structured_file": str(structured_path.relative_to(BASE_DIR)),
                }
            )

    report = {
        "generated_at": now_iso(),
        "declared_total_urls": urls_data.get("metadata", {}).get("total_urls"),
        "processed_pages": total_processed,
        "status_counts": dict(status_counter),
        "entries": report_entries,
    }
    SCRAPE_REPORT_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nScraping termine. {total_processed} pages traitees.")
    print(f"Rapport: {SCRAPE_REPORT_FILE}")


if __name__ == "__main__":
    main()
