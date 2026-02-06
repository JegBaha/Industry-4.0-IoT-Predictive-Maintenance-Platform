"""
Vector Store for RAG System
Embedding oluşturma ve vektör depolama/arama
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import numpy as np

from .document_processor import DocumentChunk, Document

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """Arama sonucu"""
    chunk: DocumentChunk
    score: float
    distance: float


class VectorStore:
    """
    Vektör deposu sınıfı
    - Embedding oluşturma (sentence-transformers)
    - Vektör depolama (ChromaDB veya numpy fallback)
    - Semantik arama
    """

    def __init__(
        self,
        persist_directory: str = "./rag_data",
        collection_name: str = "smart_factory_docs",
        embedding_model: str = "all-MiniLM-L6-v2",
        use_gpu: bool = False
    ):
        self.persist_directory = Path(persist_directory)
        self.persist_directory.mkdir(parents=True, exist_ok=True)

        self.collection_name = collection_name
        self.embedding_model_name = embedding_model
        self.use_gpu = use_gpu

        self.embedder = None
        self.chroma_client = None
        self.collection = None

        # Fallback: numpy tabanlı basit vektör deposu
        self.use_fallback = False
        self.fallback_embeddings: List[np.ndarray] = []
        self.fallback_chunks: List[DocumentChunk] = []
        self.fallback_ids: List[str] = []

        self._init_embedder()
        self._init_vector_store()

    def _init_embedder(self):
        """Embedding modelini yükle"""
        try:
            from sentence_transformers import SentenceTransformer

            device = "cuda" if self.use_gpu else "cpu"
            self.embedder = SentenceTransformer(self.embedding_model_name, device=device)
            logger.info(f"Embedding model yüklendi: {self.embedding_model_name} ({device})")

        except ImportError:
            logger.warning("sentence-transformers bulunamadı. Basit TF-IDF embedding kullanılacak.")
            self.embedder = None

    def _init_vector_store(self):
        """Vektör deposunu başlat"""
        try:
            import chromadb
            from chromadb.config import Settings

            self.chroma_client = chromadb.PersistentClient(
                path=str(self.persist_directory / "chroma"),
                settings=Settings(anonymized_telemetry=False)
            )

            self.collection = self.chroma_client.get_or_create_collection(
                name=self.collection_name,
                metadata={"description": "Smart Factory RAG Documents"}
            )
            logger.info(f"ChromaDB collection oluşturuldu: {self.collection_name}")

        except ImportError:
            logger.warning("ChromaDB bulunamadı. Numpy fallback kullanılacak.")
            self.use_fallback = True
            self._load_fallback_data()

    def _load_fallback_data(self):
        """Numpy fallback verilerini yükle"""
        embeddings_path = self.persist_directory / "embeddings.npy"
        chunks_path = self.persist_directory / "chunks.json"

        if embeddings_path.exists() and chunks_path.exists():
            self.fallback_embeddings = list(np.load(str(embeddings_path)))

            with open(chunks_path, 'r', encoding='utf-8') as f:
                chunks_data = json.load(f)
                self.fallback_chunks = [
                    DocumentChunk(**c) for c in chunks_data
                ]
                self.fallback_ids = [c.id for c in self.fallback_chunks]

            logger.info(f"Fallback veri yüklendi: {len(self.fallback_chunks)} chunk")

    def _save_fallback_data(self):
        """Numpy fallback verilerini kaydet"""
        if self.fallback_embeddings:
            embeddings_path = self.persist_directory / "embeddings.npy"
            np.save(str(embeddings_path), np.array(self.fallback_embeddings))

        if self.fallback_chunks:
            chunks_path = self.persist_directory / "chunks.json"
            with open(chunks_path, 'w', encoding='utf-8') as f:
                json.dump([c.to_dict() for c in self.fallback_chunks], f, ensure_ascii=False, indent=2)

    def embed_text(self, text: str) -> np.ndarray:
        """Metni embedding'e çevir"""
        if self.embedder:
            return self.embedder.encode(text, convert_to_numpy=True)
        else:
            # Basit TF-IDF benzeri fallback
            return self._simple_embed(text)

    def embed_texts(self, texts: List[str]) -> np.ndarray:
        """Birden fazla metni embedding'e çevir"""
        if self.embedder:
            return self.embedder.encode(texts, convert_to_numpy=True, show_progress_bar=True)
        else:
            return np.array([self._simple_embed(t) for t in texts])

    def _simple_embed(self, text: str, dim: int = 384) -> np.ndarray:
        """Basit hash tabanlı embedding (fallback)"""
        # Kelimeleri al
        words = text.lower().split()

        # Her kelime için hash tabanlı vektör
        embedding = np.zeros(dim)
        for i, word in enumerate(words):
            # Kelime hash'i
            h = hash(word) % dim
            embedding[h] += 1.0 / (i + 1)  # Position decay

        # Normalize
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm

        return embedding

    def add_document(self, document: Document) -> int:
        """Dokümanı vektör deposuna ekle"""
        if not document.chunks:
            logger.warning(f"Doküman boş: {document.filename}")
            return 0

        texts = [chunk.content for chunk in document.chunks]
        embeddings = self.embed_texts(texts)

        if self.use_fallback:
            # Numpy fallback
            for i, chunk in enumerate(document.chunks):
                if chunk.id not in self.fallback_ids:
                    self.fallback_embeddings.append(embeddings[i])
                    self.fallback_chunks.append(chunk)
                    self.fallback_ids.append(chunk.id)

            self._save_fallback_data()

        else:
            # ChromaDB
            ids = [chunk.id for chunk in document.chunks]
            metadatas = [
                {
                    "source": chunk.source,
                    "page": chunk.page,
                    "chunk_index": chunk.chunk_index,
                    "categories": ",".join(chunk.metadata.get("categories", []))
                }
                for chunk in document.chunks
            ]

            # Batch olarak ekle
            self.collection.add(
                ids=ids,
                embeddings=embeddings.tolist(),
                documents=texts,
                metadatas=metadatas
            )

        logger.info(f"Eklendi: {document.filename} ({len(document.chunks)} chunk)")
        return len(document.chunks)

    def add_documents(self, documents: List[Document]) -> int:
        """Birden fazla dokümanı ekle"""
        total = 0
        for doc in documents:
            total += self.add_document(doc)
        return total

    def search(
        self,
        query: str,
        top_k: int = 5,
        category_filter: Optional[str] = None,
        score_threshold: float = 0.3
    ) -> List[SearchResult]:
        """Semantik arama yap"""
        query_embedding = self.embed_text(query)

        if self.use_fallback:
            return self._fallback_search(query_embedding, top_k, category_filter, score_threshold)
        else:
            return self._chroma_search(query, query_embedding, top_k, category_filter, score_threshold)

    def _fallback_search(
        self,
        query_embedding: np.ndarray,
        top_k: int,
        category_filter: Optional[str],
        score_threshold: float
    ) -> List[SearchResult]:
        """Numpy fallback arama"""
        if not self.fallback_embeddings:
            return []

        # Cosine similarity hesapla
        embeddings_matrix = np.array(self.fallback_embeddings)
        similarities = np.dot(embeddings_matrix, query_embedding)

        # Sırala
        indices = np.argsort(similarities)[::-1]

        results = []
        for idx in indices:
            if len(results) >= top_k:
                break

            chunk = self.fallback_chunks[idx]
            score = float(similarities[idx])

            # Threshold kontrolü
            if score < score_threshold:
                continue

            # Kategori filtresi
            if category_filter:
                chunk_categories = chunk.metadata.get("categories", [])
                if category_filter not in chunk_categories:
                    continue

            results.append(SearchResult(
                chunk=chunk,
                score=score,
                distance=1.0 - score
            ))

        return results

    def _chroma_search(
        self,
        query: str,
        query_embedding: np.ndarray,
        top_k: int,
        category_filter: Optional[str],
        score_threshold: float
    ) -> List[SearchResult]:
        """ChromaDB arama"""
        where_filter = None
        if category_filter:
            where_filter = {"categories": {"$contains": category_filter}}

        results = self.collection.query(
            query_embeddings=[query_embedding.tolist()],
            n_results=top_k,
            where=where_filter,
            include=["documents", "metadatas", "distances"]
        )

        search_results = []

        if results and results['ids'] and results['ids'][0]:
            for i, chunk_id in enumerate(results['ids'][0]):
                distance = results['distances'][0][i] if results['distances'] else 0.0
                score = 1.0 - distance  # Cosine distance -> similarity

                if score < score_threshold:
                    continue

                # Chunk oluştur
                chunk = DocumentChunk(
                    id=chunk_id,
                    content=results['documents'][0][i],
                    source=results['metadatas'][0][i].get('source', ''),
                    page=results['metadatas'][0][i].get('page', 0),
                    chunk_index=results['metadatas'][0][i].get('chunk_index', 0),
                    metadata={
                        "categories": results['metadatas'][0][i].get('categories', '').split(',')
                    }
                )

                search_results.append(SearchResult(
                    chunk=chunk,
                    score=score,
                    distance=distance
                ))

        return search_results

    def get_stats(self) -> Dict[str, Any]:
        """Depo istatistikleri"""
        if self.use_fallback:
            return {
                "type": "numpy_fallback",
                "total_chunks": len(self.fallback_chunks),
                "embedding_dim": len(self.fallback_embeddings[0]) if self.fallback_embeddings else 0,
                "persist_directory": str(self.persist_directory)
            }
        else:
            return {
                "type": "chromadb",
                "collection": self.collection_name,
                "total_chunks": self.collection.count(),
                "persist_directory": str(self.persist_directory)
            }

    def clear(self):
        """Tüm verileri sil"""
        if self.use_fallback:
            self.fallback_embeddings = []
            self.fallback_chunks = []
            self.fallback_ids = []
            self._save_fallback_data()
        else:
            self.chroma_client.delete_collection(self.collection_name)
            self.collection = self.chroma_client.create_collection(
                name=self.collection_name,
                metadata={"description": "Smart Factory RAG Documents"}
            )
        logger.info("Vektör deposu temizlendi")
