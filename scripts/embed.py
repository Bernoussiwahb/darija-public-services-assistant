import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "chunks" / "chunks.json"
OUTPUT_DIR = BASE_DIR / "embeddings"
REPORTS_DIR = BASE_DIR / "data" / "reports"
FAISS_INDEX_FILE = OUTPUT_DIR / "index.faiss"
COMBINED_FILE = OUTPUT_DIR / "records.jsonl"
EMBED_REPORT_FILE = REPORTS_DIR / "embedding_report.json"

MODEL_NAME = "intfloat/multilingual-e5-large"
FALLBACK_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_BATCH_SIZE = 16

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genere les embeddings des chunks.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Taille de batch pour l'encodage.")
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Charge uniquement un modele deja present en cache local.",
    )
    return parser.parse_args()


def load_embedding_model(local_only: bool) -> tuple[SentenceTransformer | None, str]:
    model_candidates = [MODEL_NAME, FALLBACK_MODEL_NAME]

    for model_name in model_candidates:
        try:
            model = SentenceTransformer(model_name, local_files_only=local_only)
            return model, model_name
        except Exception as exc:
            print(f"Chargement du modele impossible: {model_name} -> {exc}")

    return None, ""


def prepare_texts(chunks: list[dict], model_name: str) -> list[str]:
    texts = [chunk["text"].strip() for chunk in chunks]

    if model_name.startswith("intfloat/multilingual-e5"):
        return [f"passage: {text}" for text in texts]

    return texts


def save_combined_records(chunks: list[dict], embeddings: np.ndarray) -> None:
    with open(COMBINED_FILE, "w", encoding="utf-8") as handle:
        for chunk, vector in zip(chunks, embeddings, strict=True):
            record = dict(chunk)
            record["embedding"] = vector.tolist()
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()

    if not INPUT_FILE.exists():
        print("Le fichier chunks.json est introuvable.")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as handle:
        chunks = json.load(handle)

    chunks = [chunk for chunk in chunks if chunk.get("text", "").strip()]
    if not chunks:
        print("Aucun chunk exploitable trouve.")
        return

    print(f"{len(chunks)} chunks charges.")

    print("Chargement du modele d'embeddings...")
    model, resolved_model_name = load_embedding_model(local_only=args.local_only)
    if model is None:
        print("Aucun modele exploitable n'a pu etre charge.")
        return

    texts = prepare_texts(chunks, resolved_model_name)

    print(f"Generation des embeddings avec {resolved_model_name}...")
    embeddings = model.encode(
        texts,
        batch_size=args.batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    embeddings = np.asarray(embeddings, dtype="float32")
    dimension = embeddings.shape[1]

    index = faiss.IndexFlatIP(dimension)
    index.add(embeddings)

    faiss.write_index(index, str(FAISS_INDEX_FILE))
    save_combined_records(chunks, embeddings)

    report = {
        "generated_at": now_iso(),
        "model_name": resolved_model_name,
        "preferred_model_name": MODEL_NAME,
        "fallback_model_name": FALLBACK_MODEL_NAME,
        "chunks_indexed": len(chunks),
        "embedding_dimension": int(dimension),
        "batch_size": int(args.batch_size),
        "faiss_metric": "inner_product_on_normalized_embeddings",
        "combined_output_file": str(COMBINED_FILE.relative_to(BASE_DIR)),
        "faiss_index_file": str(FAISS_INDEX_FILE.relative_to(BASE_DIR)),
    }
    with open(EMBED_REPORT_FILE, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)

    print(f"Index FAISS sauvegarde dans {FAISS_INDEX_FILE}")
    print(f"Embeddings + metadonnees sauvegardes dans {COMBINED_FILE}")


if __name__ == "__main__":
    main()
