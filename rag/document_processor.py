"""
Document Processor for RAG System
PDF, TXT, MD dosyalarını işleyip chunk'lara ayırır
"""

import os
import re
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunk:
    """Tek bir doküman parçası"""
    id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    page: int = 0
    chunk_index: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self.content,
            "metadata": self.metadata,
            "source": self.source,
            "page": self.page,
            "chunk_index": self.chunk_index
        }


@dataclass
class Document:
    """İşlenmiş doküman"""
    id: str
    filename: str
    filepath: str
    doc_type: str
    chunks: List[DocumentChunk] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def total_chunks(self) -> int:
        return len(self.chunks)


class DocumentProcessor:
    """
    Doküman işleme sınıfı
    - PDF, TXT, MD dosyalarını okur
    - Akıllı chunking yapar
    - Metadata çıkarır
    """

    # Doküman kategorileri
    DOC_CATEGORIES = {
        "vibration": ["titreşim", "vibration", "bearing", "rms", "velocity", "acceleration", "iso 10816", "iso 20816"],
        "oee": ["oee", "availability", "performance", "quality", "mtbf", "mttr", "downtime", "uptime"],
        "alarm": ["alarm", "threshold", "limit", "warning", "critical", "fault", "error"],
        "plc": ["plc", "opc", "opc-ua", "modbus", "siemens", "s7", "twincat", "beckhoff"],
        "maintenance": ["maintenance", "bakım", "arıza", "failure", "predictive", "preventive"],
        "sensor": ["sensor", "sensör", "temperature", "current", "pressure", "flow"]
    }

    def __init__(
        self,
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        min_chunk_size: int = 100
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    def generate_id(self, content: str, source: str) -> str:
        """Unique ID oluştur"""
        hash_input = f"{source}:{content[:100]}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:12]

    def detect_category(self, text: str) -> List[str]:
        """Doküman kategorisini tespit et"""
        text_lower = text.lower()
        categories = []

        for category, keywords in self.DOC_CATEGORIES.items():
            for keyword in keywords:
                if keyword in text_lower:
                    if category not in categories:
                        categories.append(category)
                    break

        return categories if categories else ["general"]

    def extract_tables(self, text: str) -> List[Dict[str, Any]]:
        """Basit tablo çıkarma (markdown/text tabloları için)"""
        tables = []
        lines = text.split('\n')
        current_table = []
        in_table = False

        for line in lines:
            # Tablo satırı tespiti (| veya tab ile ayrılmış)
            if '|' in line or '\t' in line:
                in_table = True
                current_table.append(line)
            elif in_table and line.strip():
                current_table.append(line)
            elif in_table:
                if len(current_table) >= 2:
                    tables.append({
                        "type": "table",
                        "content": '\n'.join(current_table)
                    })
                current_table = []
                in_table = False

        return tables

    def extract_thresholds(self, text: str) -> List[Dict[str, Any]]:
        """Alarm eşik değerlerini çıkar"""
        thresholds = []

        # Pattern: değer + birim + durum (warning, alarm, critical vs.)
        patterns = [
            # "5.0 mm/s warning", "10 mm/s alarm"
            r'(\d+\.?\d*)\s*(mm/s|m/s²|g|°C|A|V|Hz|RPM)\s*(warning|alarm|critical|danger|fault)',
            # "warning: 5.0 mm/s", "alarm limit: 10"
            r'(warning|alarm|critical|danger)\s*:?\s*(\d+\.?\d*)\s*(mm/s|m/s²|g|°C|A|V|Hz|RPM)?',
            # "threshold = 0.5", "limit = 10"
            r'(threshold|limit|eşik)\s*[=:]\s*(\d+\.?\d*)',
        ]

        for pattern in patterns:
            matches = re.finditer(pattern, text.lower())
            for match in matches:
                groups = match.groups()
                thresholds.append({
                    "raw": match.group(0),
                    "groups": groups
                })

        return thresholds

    def chunk_text(self, text: str, source: str = "") -> List[DocumentChunk]:
        """Metni akıllı chunk'lara ayır"""
        chunks = []

        # Önce paragraflara ayır
        paragraphs = re.split(r'\n\s*\n', text)

        current_chunk = ""
        chunk_index = 0

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            # Paragraf zaten çok uzunsa, cümlelere böl
            if len(para) > self.chunk_size:
                sentences = re.split(r'(?<=[.!?])\s+', para)
                for sentence in sentences:
                    if len(current_chunk) + len(sentence) <= self.chunk_size:
                        current_chunk += " " + sentence if current_chunk else sentence
                    else:
                        if len(current_chunk) >= self.min_chunk_size:
                            chunk_id = self.generate_id(current_chunk, source)
                            categories = self.detect_category(current_chunk)
                            thresholds = self.extract_thresholds(current_chunk)

                            chunks.append(DocumentChunk(
                                id=chunk_id,
                                content=current_chunk.strip(),
                                metadata={
                                    "categories": categories,
                                    "thresholds": thresholds,
                                    "char_count": len(current_chunk)
                                },
                                source=source,
                                chunk_index=chunk_index
                            ))
                            chunk_index += 1

                        # Overlap için son kısmı tut
                        overlap_text = current_chunk[-self.chunk_overlap:] if len(current_chunk) > self.chunk_overlap else ""
                        current_chunk = overlap_text + " " + sentence
            else:
                # Normal paragraf
                if len(current_chunk) + len(para) <= self.chunk_size:
                    current_chunk += "\n\n" + para if current_chunk else para
                else:
                    if len(current_chunk) >= self.min_chunk_size:
                        chunk_id = self.generate_id(current_chunk, source)
                        categories = self.detect_category(current_chunk)
                        thresholds = self.extract_thresholds(current_chunk)

                        chunks.append(DocumentChunk(
                            id=chunk_id,
                            content=current_chunk.strip(),
                            metadata={
                                "categories": categories,
                                "thresholds": thresholds,
                                "char_count": len(current_chunk)
                            },
                            source=source,
                            chunk_index=chunk_index
                        ))
                        chunk_index += 1

                    overlap_text = current_chunk[-self.chunk_overlap:] if len(current_chunk) > self.chunk_overlap else ""
                    current_chunk = overlap_text + "\n\n" + para

        # Son chunk
        if len(current_chunk) >= self.min_chunk_size:
            chunk_id = self.generate_id(current_chunk, source)
            categories = self.detect_category(current_chunk)
            thresholds = self.extract_thresholds(current_chunk)

            chunks.append(DocumentChunk(
                id=chunk_id,
                content=current_chunk.strip(),
                metadata={
                    "categories": categories,
                    "thresholds": thresholds,
                    "char_count": len(current_chunk)
                },
                source=source,
                chunk_index=chunk_index
            ))

        return chunks

    def process_text_file(self, filepath: str) -> Document:
        """TXT/MD dosyası işle"""
        path = Path(filepath)

        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        doc_id = self.generate_id(content, filepath)
        chunks = self.chunk_text(content, source=path.name)

        # Genel metadata
        all_categories = set()
        for chunk in chunks:
            all_categories.update(chunk.metadata.get("categories", []))

        return Document(
            id=doc_id,
            filename=path.name,
            filepath=filepath,
            doc_type=path.suffix[1:],
            chunks=chunks,
            metadata={
                "categories": list(all_categories),
                "total_chars": len(content),
                "total_chunks": len(chunks)
            }
        )

    def process_pdf(self, filepath: str) -> Document:
        """PDF dosyası işle"""
        path = Path(filepath)

        try:
            # PyMuPDF (fitz) kullan
            import fitz

            doc = fitz.open(filepath)
            all_text = ""
            page_texts = []

            for page_num, page in enumerate(doc):
                text = page.get_text()
                page_texts.append(text)
                all_text += f"\n\n[Page {page_num + 1}]\n{text}"

            doc.close()

        except ImportError:
            # PyMuPDF yoksa, pdfplumber dene
            try:
                import pdfplumber

                with pdfplumber.open(filepath) as pdf:
                    page_texts = []
                    all_text = ""
                    for page_num, page in enumerate(pdf.pages):
                        text = page.extract_text() or ""
                        page_texts.append(text)
                        all_text += f"\n\n[Page {page_num + 1}]\n{text}"

            except ImportError:
                logger.warning("PDF okumak için fitz veya pdfplumber gerekli. pip install pymupdf veya pip install pdfplumber")
                return Document(
                    id=self.generate_id(filepath, filepath),
                    filename=path.name,
                    filepath=filepath,
                    doc_type="pdf",
                    chunks=[],
                    metadata={"error": "PDF library not installed"}
                )

        doc_id = self.generate_id(all_text, filepath)
        chunks = self.chunk_text(all_text, source=path.name)

        # Sayfa bilgisi ekle
        for chunk in chunks:
            # Chunk içinde sayfa referansı ara
            page_match = re.search(r'\[Page (\d+)\]', chunk.content)
            if page_match:
                chunk.page = int(page_match.group(1))

        all_categories = set()
        for chunk in chunks:
            all_categories.update(chunk.metadata.get("categories", []))

        return Document(
            id=doc_id,
            filename=path.name,
            filepath=filepath,
            doc_type="pdf",
            chunks=chunks,
            metadata={
                "categories": list(all_categories),
                "total_pages": len(page_texts),
                "total_chars": len(all_text),
                "total_chunks": len(chunks)
            }
        )

    def process_file(self, filepath: str) -> Optional[Document]:
        """Dosya türüne göre işle"""
        path = Path(filepath)

        if not path.exists():
            logger.error(f"Dosya bulunamadı: {filepath}")
            return None

        suffix = path.suffix.lower()

        if suffix == '.pdf':
            return self.process_pdf(filepath)
        elif suffix in ['.txt', '.md', '.markdown']:
            return self.process_text_file(filepath)
        else:
            logger.warning(f"Desteklenmeyen dosya türü: {suffix}")
            return None

    def process_directory(self, dirpath: str, recursive: bool = True) -> List[Document]:
        """Dizindeki tüm dokümanları işle"""
        documents = []
        path = Path(dirpath)

        if not path.exists():
            logger.error(f"Dizin bulunamadı: {dirpath}")
            return documents

        pattern = '**/*' if recursive else '*'
        supported_extensions = ['.pdf', '.txt', '.md', '.markdown']

        for file_path in path.glob(pattern):
            if file_path.suffix.lower() in supported_extensions:
                doc = self.process_file(str(file_path))
                if doc:
                    documents.append(doc)
                    logger.info(f"İşlendi: {file_path.name} ({doc.total_chunks} chunk)")

        return documents
