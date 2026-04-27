import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
STRUCTURED_DIR = BASE_DIR / "data" / "structured"
REPORTS_DIR = BASE_DIR / "data" / "reports"
OUTPUT_FILE = BASE_DIR / "dataset.json"
DATASET_REPORT_FILE = REPORTS_DIR / "dataset_report.json"

REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_record(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_text(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def content_fingerprint(title: str, content: str) -> str:
    normalized = normalize_text(f"{title}\n{content}")
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def dedupe_list(items: list[str]) -> list[str]:
    deduped = []
    seen = set()

    for item in items:
        cleaned = item.strip()
        if not cleaned:
            continue
        normalized = normalize_text(cleaned)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(cleaned)

    return deduped


def build_entry(record: dict) -> dict | None:
    title = record.get("title", "").strip()
    content = record.get("content", "").strip()
    source = (record.get("final_url") or record.get("url") or "").strip()

    if not title or not content or not source:
        return None

    status = record.get("status")
    if status and status != "success":
        return None

    domain = record.get("domain", "").strip()
    if not domain and source:
        domain = urlparse(source).netloc.lower()

    return {
        "id": record.get("id", "").strip(),
        "title": title,
        "content": content,
        "content_sections": dedupe_list(record.get("content_sections", [])),
        "source": source,
        "requested_url": record.get("url", "").strip(),
        "domain": domain,
        "category": record.get("category", "").strip(),
        "page_type": record.get("page_type", "").strip(),
        "language": record.get("language", "fr").strip(),
        "priority": record.get("priority", "moyenne").strip(),
        "responsible_entity": record.get("responsible_entity", "").strip(),
        "fees": record.get("fees"),
        "processing_time": record.get("processing_time"),
        "keywords": dedupe_list(record.get("keywords", [])),
        "steps": dedupe_list(record.get("steps", [])),
        "required_documents": dedupe_list(record.get("required_documents", [])),
        "eligibility_conditions": dedupe_list(record.get("eligibility_conditions", [])),
        "content_length": record.get("content_length", len(content)),
        "fetched_at": record.get("fetched_at"),
    }


def main() -> None:
    dataset = []
    seen_sources = set()
    seen_fingerprints = set()
    category_counter = Counter()
    domain_counter = Counter()
    skipped_files = []

    for path in sorted(STRUCTURED_DIR.glob("*.json")):
        record = load_record(path)
        entry = build_entry(record)

        if not entry:
            skipped_files.append(
                {
                    "file": path.name,
                    "status": record.get("status"),
                    "reason": record.get("skip_reason"),
                }
            )
            continue

        if entry["source"] in seen_sources:
            skipped_files.append(
                {
                    "file": path.name,
                    "status": "duplicate_source",
                    "reason": entry["source"],
                }
            )
            continue

        fingerprint = content_fingerprint(entry["title"], entry["content"])
        if fingerprint in seen_fingerprints:
            skipped_files.append(
                {
                    "file": path.name,
                    "status": "duplicate_content",
                    "reason": fingerprint,
                }
            )
            continue

        seen_sources.add(entry["source"])
        seen_fingerprints.add(fingerprint)
        dataset.append(entry)
        category_counter[entry["category"]] += 1
        domain_counter[entry["domain"]] += 1

    dataset.sort(key=lambda item: (item["category"], item["title"], item["source"]))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as handle:
        json.dump(dataset, handle, ensure_ascii=False, indent=2)

    report = {
        "generated_at": now_iso(),
        "dataset_entries": len(dataset),
        "category_counts": dict(category_counter),
        "domain_counts": dict(domain_counter),
        "skipped_files": skipped_files,
    }
    with open(DATASET_REPORT_FILE, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)

    print(f"dataset.json cree: {len(dataset)} entrees")
    print(f"rapport: {DATASET_REPORT_FILE}")


if __name__ == "__main__":
    main()
