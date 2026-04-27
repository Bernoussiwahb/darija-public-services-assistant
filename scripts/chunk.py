import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "dataset.json"
OUTPUT_DIR = BASE_DIR / "data" / "chunks"
REPORTS_DIR = BASE_DIR / "data" / "reports"
OUTPUT_FILE = OUTPUT_DIR / "chunks.json"
CHUNK_REPORT_FILE = REPORTS_DIR / "chunk_report.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

MIN_WORDS = 200
TARGET_WORDS = 240
MAX_WORDS = 300


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def word_count(text: str) -> int:
    return len(text.split())


def split_blocks(entry: dict) -> list[str]:
    sections = [section.strip() for section in entry.get("content_sections", []) if section.strip()]
    if sections:
        return sections

    content = entry.get("content", "").strip()
    if not content:
        return []

    return [block.strip() for block in content.split("\n\n") if block.strip()]


def is_heading(block: str) -> bool:
    return block.startswith("#")


def is_list_item(block: str) -> bool:
    return block.startswith("- ")


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    return [part.strip() for part in parts if part.strip()]


def split_long_paragraph(text: str, max_words: int = MAX_WORDS) -> list[str]:
    sentences = split_sentences(text)
    if len(sentences) <= 1:
        words = text.split()
        return [" ".join(words[i:i + max_words]).strip() for i in range(0, len(words), max_words) if words[i:i + max_words]]

    segments = []
    current = []
    current_words = 0

    for sentence in sentences:
        sentence_words = word_count(sentence)
        if current and current_words + sentence_words > max_words:
            segments.append(" ".join(current).strip())
            current = [sentence]
            current_words = sentence_words
        else:
            current.append(sentence)
            current_words += sentence_words

    if current:
        segments.append(" ".join(current).strip())

    return segments


def build_units(blocks: list[str]) -> list[dict]:
    units = []
    current_headings: list[str] = []
    current_paragraphs: list[str] = []
    current_list_items: list[str] = []

    def flush_paragraphs() -> None:
        nonlocal current_paragraphs
        if not current_paragraphs:
            return
        text = "\n".join(current_paragraphs).strip()
        units.append({"headings": current_headings.copy(), "body": text, "kind": "paragraph"})
        current_paragraphs = []

    def flush_list() -> None:
        nonlocal current_list_items
        if not current_list_items:
            return
        units.append({"headings": current_headings.copy(), "body": "\n".join(current_list_items).strip(), "kind": "list"})
        current_list_items = []

    for block in blocks:
        if not block:
            continue

        if is_heading(block):
            flush_paragraphs()
            flush_list()
            level = len(block) - len(block.lstrip("#"))
            current_headings = [heading for heading in current_headings if (len(heading) - len(heading.lstrip("#"))) < level]
            current_headings.append(block)
            continue

        if is_list_item(block):
            flush_paragraphs()
            current_list_items.append(block)
            continue

        flush_list()
        current_paragraphs.append(block)

    flush_paragraphs()
    flush_list()
    return units


def split_unit(unit: dict, max_words: int = MAX_WORDS) -> list[dict]:
    body = unit["body"].strip()
    if not body:
        return []

    heading_budget = word_count("\n".join(unit["headings"]))
    body_max_words = max(80, max_words - heading_budget)

    if unit["kind"] == "list":
        items = [line.strip() for line in body.splitlines() if line.strip()]
        expanded_items = []
        for item in items:
            item_words = word_count(item)
            if item_words <= body_max_words:
                expanded_items.append(item)
                continue

            item_text = item[2:].strip() if item.startswith("- ") else item
            split_items = split_long_paragraph(item_text, max_words=body_max_words)
            expanded_items.extend([f"- {segment}" for segment in split_items if segment.strip()])

        split_units = []
        current_items = []
        current_words = 0

        for item in expanded_items:
            item_words = word_count(item)
            if current_items and current_words + item_words > body_max_words:
                split_units.append({"headings": unit["headings"], "body": "\n".join(current_items), "kind": unit["kind"]})
                current_items = [item]
                current_words = item_words
            else:
                current_items.append(item)
                current_words += item_words

        if current_items:
            split_units.append({"headings": unit["headings"], "body": "\n".join(current_items), "kind": unit["kind"]})
        return split_units

    split_units = []
    for segment in split_long_paragraph(body, max_words=body_max_words):
        split_units.append({"headings": unit["headings"], "body": segment, "kind": unit["kind"]})
    return split_units


def render_chunk(headings: list[str], bodies: list[str]) -> str:
    parts = []
    if headings:
        parts.extend(headings)
    parts.extend(body for body in bodies if body.strip())
    return "\n".join(parts).strip()


def chunk_units(units: list[dict], min_words: int = MIN_WORDS, target_words: int = TARGET_WORDS, max_words: int = MAX_WORDS) -> list[str]:
    normalized_units = []
    for unit in units:
        full_text = render_chunk(unit["headings"], [unit["body"]])
        if word_count(full_text) > max_words:
            normalized_units.extend(split_unit(unit, max_words=max_words))
        else:
            normalized_units.append(unit)

    chunks = []
    current_headings: list[str] = []
    current_bodies: list[str] = []
    current_words = 0

    def flush_chunk() -> None:
        nonlocal current_headings, current_bodies, current_words
        if not current_bodies:
            return
        chunks.append(render_chunk(current_headings, current_bodies))
        current_headings = []
        current_bodies = []
        current_words = 0

    for unit in normalized_units:
        unit_headings = unit["headings"]
        unit_body = unit["body"]
        body_words = word_count(unit_body)
        heading_words = word_count("\n".join(unit_headings)) if unit_headings != current_headings else 0
        projected = current_words + body_words + heading_words

        if current_bodies and projected > max_words:
            if current_words >= min_words:
                flush_chunk()
            else:
                candidate_chunk = render_chunk(unit_headings, [unit_body])
                candidate_words = word_count(candidate_chunk)
                if candidate_words >= min_words:
                    flush_chunk()
                elif current_bodies:
                    flush_chunk()

        if not current_bodies:
            current_headings = unit_headings.copy()
            current_bodies = [unit_body]
            current_words = word_count(render_chunk(current_headings, current_bodies))
            continue

        if unit_headings != current_headings and current_words >= min_words:
            flush_chunk()
            current_headings = unit_headings.copy()
            current_bodies = [unit_body]
            current_words = word_count(render_chunk(current_headings, current_bodies))
            continue

        if unit_headings != current_headings:
            current_headings = unit_headings.copy()

        current_bodies.append(unit_body)
        current_words = word_count(render_chunk(current_headings, current_bodies))

        if current_words >= target_words:
            flush_chunk()

    flush_chunk()
    return chunks


def chunk_blocks(blocks: list[str], min_words: int = MIN_WORDS, target_words: int = TARGET_WORDS, max_words: int = MAX_WORDS) -> list[str]:
    units = build_units(blocks)
    return chunk_units(units, min_words=min_words, target_words=target_words, max_words=max_words)


def main() -> None:
    if not INPUT_FILE.exists():
        print("Le fichier dataset.json est introuvable.")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as handle:
        dataset = json.load(handle)

    all_chunks = []
    total_chunks = 0
    category_counter = Counter()

    for doc_id, entry in enumerate(dataset, start=1):
        title = entry.get("title", "").strip()
        source = entry.get("source", "").strip()

        if not title or not source:
            continue

        blocks = split_blocks(entry)
        content_chunks = chunk_blocks(blocks)
        if not content_chunks:
            continue

        category_counter[entry.get("category", "unknown")] += len(content_chunks)

        for chunk_index, chunk in enumerate(content_chunks, start=1):
            chunk_record = {
                "chunk_id": f"doc_{doc_id}_chunk_{chunk_index}",
                "doc_id": entry.get("id") or f"doc_{doc_id}",
                "title": title,
                "source": source,
                "requested_url": entry.get("requested_url", ""),
                "domain": entry.get("domain", ""),
                "category": entry.get("category", ""),
                "page_type": entry.get("page_type", ""),
                "language": entry.get("language", "fr"),
                "priority": entry.get("priority", "moyenne"),
                "chunk_index": chunk_index,
                "total_chunks_in_doc": len(content_chunks),
                "text": chunk,
            }
            all_chunks.append(chunk_record)
            total_chunks += 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as handle:
        json.dump(all_chunks, handle, ensure_ascii=False, indent=2)

    report = {
        "generated_at": now_iso(),
        "documents": len(dataset),
        "chunks": total_chunks,
        "category_chunk_counts": dict(category_counter),
    }
    with open(CHUNK_REPORT_FILE, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)

    print(f"Chunking termine. {total_chunks} chunks enregistres dans {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
