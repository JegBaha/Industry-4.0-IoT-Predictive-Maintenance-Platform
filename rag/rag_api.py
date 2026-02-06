"""
RAG API Endpoints for Smart Factory
FastAPI router for document management, Q&A, and alarm analysis
"""

import os
import time
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel

from .document_processor import DocumentProcessor
from .vector_store import VectorStore
from .rag_engine import RAGEngine, LLMProvider
from .knowledge_base import KnowledgeBase

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/rag", tags=["RAG"])

# ─── Singleton RAG bileşenleri ──────────────────────────────────────────────

_vector_store: Optional[VectorStore] = None
_rag_engine: Optional[RAGEngine] = None
_knowledge_base: Optional[KnowledgeBase] = None
_initialized = False


def get_vector_store() -> VectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStore(
            persist_directory=os.getenv("RAG_DATA_DIR", "./rag_data"),
            embedding_model=os.getenv("RAG_EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        )
    return _vector_store


def get_rag_engine() -> RAGEngine:
    global _rag_engine
    if _rag_engine is None:
        vs = get_vector_store()

        # LLM provider seçimi
        provider_name = os.getenv("RAG_LLM_PROVIDER", "ollama").lower()
        providers = {
            "openai": LLMProvider.OPENAI,
            "anthropic": LLMProvider.ANTHROPIC,
            "ollama": LLMProvider.OLLAMA,
            "local": LLMProvider.LOCAL,
        }
        provider = providers.get(provider_name, LLMProvider.OLLAMA)

        _rag_engine = RAGEngine(
            vector_store=vs,
            llm_provider=provider,
            model_name=os.getenv("RAG_MODEL_NAME", "llama2"),
            api_base=os.getenv("RAG_API_BASE", "http://localhost:11434"),
            api_key=os.getenv("RAG_API_KEY"),
        )
    return _rag_engine


def get_knowledge_base() -> KnowledgeBase:
    global _knowledge_base
    if _knowledge_base is None:
        vs = get_vector_store()
        _knowledge_base = KnowledgeBase(
            vector_store=vs,
            docs_directory=os.getenv("RAG_DOCS_DIR", "./rag_docs"),
            auto_load_builtin=True
        )
    return _knowledge_base


def init_rag():
    """RAG sistemini başlat"""
    global _initialized
    if _initialized:
        return
    try:
        get_knowledge_base()
        _initialized = True
        logger.info("RAG sistemi baslatildi")
    except Exception as e:
        logger.error(f"RAG baslatilamadi: {e}")


# ─── Request / Response modelleri ───────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    category: Optional[str] = None
    top_k: int = 5


class AlarmAnalysisRequest(BaseModel):
    machine_code: str
    sensor_type: str
    value: float
    unit: str = ""
    timestamp: str = ""


class OEEAnalysisRequest(BaseModel):
    machine_code: str
    availability: float
    performance: float
    quality: float
    mtbf: float = 0
    mttr: float = 0


class CustomKnowledgeRequest(BaseModel):
    title: str
    content: str
    category: str = "general"


# ─── Endpoints ──────────────────────────────────────────────────────────────

@router.get("/status")
def rag_status():
    """RAG sistemi durumu"""
    try:
        kb = get_knowledge_base()
        return {
            "status": "active",
            "initialized": _initialized,
            **kb.get_status()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/init")
def rag_init():
    """RAG sistemini başlat"""
    init_rag()
    return {"ok": True, "status": "initialized"}


@router.post("/query")
def rag_query(req: QueryRequest):
    """Genel soru-cevap"""
    engine = get_rag_engine()
    result = engine.query(
        question=req.question,
        top_k=req.top_k,
        category_filter=req.category
    )
    return {
        "question": result.query,
        "answer": result.answer,
        "sources": result.sources,
        "confidence": result.confidence
    }


@router.post("/analyze/alarm")
def rag_analyze_alarm(req: AlarmAnalysisRequest):
    """Alarm durumu analizi (RAG tabanlı)"""
    engine = get_rag_engine()
    result = engine.analyze_alarm(
        machine_code=req.machine_code,
        sensor_type=req.sensor_type,
        value=req.value,
        unit=req.unit,
        timestamp=req.timestamp
    )
    return result


@router.post("/analyze/oee")
def rag_analyze_oee(req: OEEAnalysisRequest):
    """OEE analizi (RAG tabanlı)"""
    engine = get_rag_engine()
    result = engine.analyze_oee(
        machine_code=req.machine_code,
        availability=req.availability,
        performance=req.performance,
        quality=req.quality,
        mtbf=req.mtbf,
        mttr=req.mttr
    )
    return result


@router.get("/vibration-limits")
def rag_vibration_limits(machine_type: str = "general"):
    """Titreşim limitleri sorgula"""
    engine = get_rag_engine()
    return engine.get_vibration_limits(machine_type)


@router.post("/documents/upload")
async def rag_upload_document(file: UploadFile = File(...)):
    """PDF/TXT doküman yükle"""
    kb = get_knowledge_base()

    # Dosyayı kaydet
    docs_dir = Path(os.getenv("RAG_DOCS_DIR", "./rag_docs"))
    docs_dir.mkdir(parents=True, exist_ok=True)

    save_path = docs_dir / file.filename
    content = await file.read()

    with open(save_path, 'wb') as f:
        f.write(content)

    # İşle ve vektör deposuna ekle
    result = kb.load_user_document(str(save_path))
    if result:
        return {"ok": True, **result}
    else:
        raise HTTPException(status_code=400, detail="Dosya islenemedi")


@router.post("/documents/load-directory")
def rag_load_directory(path: str = Form(None)):
    """Dizindeki tüm dokümanları yükle"""
    kb = get_knowledge_base()
    results = kb.load_user_directory(path)
    return {"ok": True, "loaded": results, "count": len(results)}


@router.post("/knowledge/add")
def rag_add_knowledge(req: CustomKnowledgeRequest):
    """Özel bilgi ekle"""
    kb = get_knowledge_base()
    chunks = kb.add_custom_knowledge(
        title=req.title,
        content=req.content,
        category=req.category
    )
    return {"ok": True, "title": req.title, "chunks": chunks}


@router.post("/knowledge/save-builtin")
def rag_save_builtin():
    """Yerleşik dokümanları dosya olarak kaydet"""
    kb = get_knowledge_base()
    kb.save_builtin_as_files()
    return {"ok": True, "message": "Builtin dokumanlar kaydedildi"}


@router.post("/search")
def rag_search(req: QueryRequest):
    """Semantik arama (LLM olmadan, sadece vektör arama)"""
    vs = get_vector_store()
    results = vs.search(
        query=req.question,
        top_k=req.top_k,
        category_filter=req.category
    )
    return {
        "query": req.question,
        "results": [
            {
                "content": r.chunk.content,
                "source": r.chunk.source,
                "score": r.score,
                "categories": r.chunk.metadata.get("categories", [])
            }
            for r in results
        ]
    }


@router.delete("/clear")
def rag_clear():
    """Tüm vektör verilerini sil"""
    vs = get_vector_store()
    vs.clear()
    return {"ok": True, "message": "Vektor deposu temizlendi"}
