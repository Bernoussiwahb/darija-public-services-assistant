import json
from pathlib import Path

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

BASE_DIR = Path(__file__).resolve().parent.parent
FAISS_INDEX_FILE = BASE_DIR / "embeddings" / "index.faiss"
METADATA_FILE = BASE_DIR / "embeddings" / "metadata.json"
MODEL_NAME = "all-MiniLM-L6-v2"


def load_embedding_model() -> SentenceTransformer | None:
    try:
        return SentenceTransformer(MODEL_NAME)
    except Exception as exc:
        print(f"Chargement standard du modele impossible: {exc}")
        print("Nouvelle tentative en mode cache local uniquement...")
        try:
            return SentenceTransformer(MODEL_NAME, local_files_only=True)
        except Exception as local_exc:
            print(f"Mode hors ligne impossible: {local_exc}")
            print("Embeddings indisponibles tant que le modele n'est pas present localement.")
            return None


def main() -> None:
    if not FAISS_INDEX_FILE.exists():
        print("Le fichier index.faiss est introuvable.")
        print("Lance d'abord: .\\venv\\Scripts\\python.exe scripts\\embed.py")
        return

    if not METADATA_FILE.exists():
        print("Le fichier metadata.json est introuvable.")
        print("Lance d'abord: .\\venv\\Scripts\\python.exe scripts\\embed.py")
        return

    print("Chargement de l'index FAISS...")
    index = faiss.read_index(str(FAISS_INDEX_FILE))

    print("Chargement des metadonnees...")
    with open(METADATA_FILE, "r", encoding="utf-8") as handle:
        metadata = json.load(handle)

    print("Chargement du modele d'embeddings...")
    model = load_embedding_model()
    if model is None:
        return

    query = input("\nEntre ta question: ").strip()
    if not query:
        print("Aucune question saisie.")
        return

    top_k_raw = input("Nombre de resultats [3]: ").strip()
    top_k = int(top_k_raw) if top_k_raw.isdigit() and int(top_k_raw) > 0 else 3

    print("\nTransformation de la question en vecteur...")
    query_vector = model.encode(
        [query],
        convert_to_numpy=True,
        normalize_embeddings=True,
    ).astype("float32")

    print("Recherche des segments les plus pertinents...")
    scores, indices = index.search(query_vector, top_k)

    print("\n" + "=" * 80)
    print("Resultats les plus pertinents")
    print("=" * 80)

    for rank, idx in enumerate(indices[0], start=1):
        if idx < 0 or idx >= len(metadata):
            continue

        chunk = metadata[idx]
        print(f"\n--- Resultat {rank} ---")
        print(f"Titre    : {chunk.get('title', 'Sans titre')}")
        print(f"Source   : {chunk.get('source', 'Sans source')}")
        print(f"Categorie: {chunk.get('category', 'n/a')}")
        print(f"Type     : {chunk.get('page_type', 'n/a')}")
        print(f"Chunk ID : {chunk.get('chunk_id', 'Sans ID')}")
        print(f"Score    : {scores[0][rank - 1]:.4f}")
        print("Texte    :")
        print(chunk.get("text", ""))


if __name__ == "__main__":
    main()
