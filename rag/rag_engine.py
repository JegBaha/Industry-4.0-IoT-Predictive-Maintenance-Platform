"""
RAG Engine for Smart Factory
LLM entegrasyonu ve akıllı analiz
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .vector_store import VectorStore, SearchResult
from .document_processor import DocumentChunk

logger = logging.getLogger(__name__)


class LLMProvider(Enum):
    """Desteklenen LLM sağlayıcıları"""
    OPENAI = "openai"
    OLLAMA = "ollama"
    ANTHROPIC = "anthropic"
    LOCAL = "local"  # Transformers tabanlı yerel model


@dataclass
class AnalysisResult:
    """Analiz sonucu"""
    query: str
    answer: str
    sources: List[Dict[str, Any]]
    context_used: str
    metadata: Dict[str, Any]
    confidence: float = 0.0


class RAGEngine:
    """
    RAG Engine sınıfı
    - Vektör aramadan context oluşturma
    - LLM ile analiz
    - Smart Factory'ye özel prompt'lar
    """

    # Prompt şablonları
    SYSTEM_PROMPT = """Sen bir endüstriyel IoT ve akıllı fabrika (Industry 4.0) uzmanısın.
Görevin, verilen dokümanlardan elde edilen bilgileri kullanarak fabrika operatörlerine
ve mühendislere teknik destek sağlamaktır.

Uzmanlık alanların:
- Titreşim analizi ve condition monitoring (ISO 10816, ISO 20816)
- OEE (Overall Equipment Effectiveness) hesaplamaları
- Prediktif bakım ve arıza tahmini
- Alarm yönetimi ve eşik değerler
- PLC, OPC-UA ve endüstriyel iletişim protokolleri
- MTBF, MTTR hesaplamaları

Cevaplarında:
1. Teknik ve kesin ol
2. Mümkünse sayısal değerler ve limitler ver
3. ISO/IEC standartlarına referans ver
4. Pratik öneriler sun
5. Türkçe cevap ver (teknik terimler İngilizce kalabilir)"""

    ANALYSIS_PROMPT_TEMPLATE = """
Aşağıdaki doküman bilgilerine dayanarak soruyu cevapla:

---DOKÜMAN BİLGİLERİ---
{context}
---

SORU: {query}

Not: Cevabını yalnızca verilen doküman bilgilerine dayandır.
Bilgi yoksa "Bu konuda yeterli doküman bilgisi bulunamadı" de.
"""

    ALARM_ANALYSIS_TEMPLATE = """
Sensör verisi alarm durumu analizi:

---SENSÖR VERİSİ---
Makine: {machine_code}
Sensör Tipi: {sensor_type}
Değer: {value} {unit}
Zaman: {timestamp}
---

---İLGİLİ DOKÜMAN BİLGİLERİ---
{context}
---

Bu sensör değerini analiz et ve şunları belirt:
1. Bu değer normal aralıkta mı? (ilgili standartlara göre)
2. Alarm seviyesi nedir? (normal/warning/critical)
3. OEE'ye potansiyel etkisi
4. Muhtemel kök neden (root cause)
5. Önerilen aksiyon

Cevabı JSON formatında ver:
{{
    "status": "normal|warning|critical",
    "standard_reference": "ISO/standart referansı",
    "limit_info": "ilgili limit bilgisi",
    "oee_impact": "OEE etkisi açıklaması",
    "root_cause": "olası kök neden",
    "action": "önerilen aksiyon",
    "explanation": "detaylı açıklama"
}}
"""

    OEE_ANALYSIS_TEMPLATE = """
OEE (Overall Equipment Effectiveness) analizi:

---ÜRETİM VERİLERİ---
Makine: {machine_code}
Availability: {availability}%
Performance: {performance}%
Quality: {quality}%
OEE: {oee}%
MTBF: {mtbf} saat
MTTR: {mttr} dakika
---

---İLGİLİ DOKÜMAN BİLGİLERİ---
{context}
---

Bu OEE verilerini analiz et:
1. Hangi faktör en çok iyileştirme potansiyeli taşıyor?
2. Benchmark değerlerine göre durum (World Class: OEE>85%)
3. MTBF/MTTR oranı değerlendirmesi
4. İyileştirme önerileri

Cevabı JSON formatında ver:
{{
    "oee_class": "Poor|Average|Good|World Class",
    "weakest_factor": "availability|performance|quality",
    "improvement_potential": yüzde değer,
    "mtbf_mttr_ratio": sayı,
    "recommendations": ["öneri1", "öneri2", ...],
    "explanation": "detaylı açıklama"
}}
"""

    def __init__(
        self,
        vector_store: VectorStore,
        llm_provider: LLMProvider = LLMProvider.OLLAMA,
        model_name: str = "llama2",
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 1000
    ):
        self.vector_store = vector_store
        self.llm_provider = llm_provider
        self.model_name = model_name
        self.api_key = api_key or os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
        self.api_base = api_base or "http://localhost:11434"  # Ollama default
        self.temperature = temperature
        self.max_tokens = max_tokens

        self.llm_client = None
        self._init_llm()

    def _init_llm(self):
        """LLM client'ı başlat"""
        if self.llm_provider == LLMProvider.OPENAI:
            try:
                from openai import OpenAI
                self.llm_client = OpenAI(api_key=self.api_key)
                logger.info("OpenAI client başlatıldı")
            except ImportError:
                logger.warning("openai paketi bulunamadı")

        elif self.llm_provider == LLMProvider.ANTHROPIC:
            try:
                from anthropic import Anthropic
                self.llm_client = Anthropic(api_key=self.api_key)
                logger.info("Anthropic client başlatıldı")
            except ImportError:
                logger.warning("anthropic paketi bulunamadı")

        elif self.llm_provider == LLMProvider.OLLAMA:
            # Ollama HTTP API kullanır, özel client gerekmez
            logger.info(f"Ollama kullanılacak: {self.api_base}")

        elif self.llm_provider == LLMProvider.LOCAL:
            try:
                from transformers import pipeline
                self.llm_client = pipeline(
                    "text-generation",
                    model=self.model_name,
                    device_map="auto"
                )
                logger.info(f"Local model yüklendi: {self.model_name}")
            except ImportError:
                logger.warning("transformers paketi bulunamadı")

    def _build_context(self, search_results: List[SearchResult], max_length: int = 3000) -> str:
        """Arama sonuçlarından context oluştur"""
        context_parts = []
        total_length = 0

        for i, result in enumerate(search_results):
            chunk_text = f"[Kaynak {i+1}: {result.chunk.source}]\n{result.chunk.content}\n"

            if total_length + len(chunk_text) > max_length:
                break

            context_parts.append(chunk_text)
            total_length += len(chunk_text)

        return "\n---\n".join(context_parts)

    def _call_llm(self, prompt: str, system_prompt: str = None) -> str:
        """LLM'i çağır"""
        system = system_prompt or self.SYSTEM_PROMPT

        if self.llm_provider == LLMProvider.OPENAI:
            response = self.llm_client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            return response.choices[0].message.content

        elif self.llm_provider == LLMProvider.ANTHROPIC:
            response = self.llm_client.messages.create(
                model=self.model_name,
                max_tokens=self.max_tokens,
                system=system,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            return response.content[0].text

        elif self.llm_provider == LLMProvider.OLLAMA:
            import requests

            response = requests.post(
                f"{self.api_base}/api/generate",
                json={
                    "model": self.model_name,
                    "prompt": f"{system}\n\n{prompt}",
                    "stream": False,
                    "options": {
                        "temperature": self.temperature,
                        "num_predict": self.max_tokens
                    }
                },
                timeout=120
            )
            response.raise_for_status()
            return response.json().get("response", "")

        elif self.llm_provider == LLMProvider.LOCAL:
            full_prompt = f"{system}\n\n{prompt}"
            outputs = self.llm_client(
                full_prompt,
                max_new_tokens=self.max_tokens,
                temperature=self.temperature,
                do_sample=True
            )
            return outputs[0]["generated_text"][len(full_prompt):]

        else:
            return self._fallback_response(prompt)

    def _fallback_response(self, prompt: str) -> str:
        """LLM olmadan basit rule-based cevap"""
        # Anahtar kelime bazlı basit analiz
        prompt_lower = prompt.lower()

        if "titreşim" in prompt_lower or "vibration" in prompt_lower:
            return """Titreşim analizi için ISO 10816 standardına bakınız:
- İyi: 0-2.8 mm/s
- Kabul edilebilir: 2.8-4.5 mm/s
- Uyarı: 4.5-7.1 mm/s
- Tehlikeli: >7.1 mm/s

RMS velocity değeri önerilir."""

        elif "oee" in prompt_lower:
            return """OEE (Overall Equipment Effectiveness) hesaplaması:
OEE = Availability × Performance × Quality

World Class değerler:
- Availability: >90%
- Performance: >95%
- Quality: >99%
- OEE: >85%"""

        elif "alarm" in prompt_lower or "eşik" in prompt_lower:
            return """Genel alarm eşikleri:
- Sıcaklık: Warning 60°C, Critical 70°C
- Titreşim: Warning 4.5 mm/s, Critical 7.1 mm/s
- Akım: Warning +20%, Critical +50%

Makineye özel değerler için dokümanlara bakınız."""

        else:
            return "Bu konuda yeterli bilgi bulunamadı. Lütfen ilgili dokümanları yükleyin."

    def query(
        self,
        question: str,
        top_k: int = 5,
        category_filter: Optional[str] = None
    ) -> AnalysisResult:
        """Genel soru-cevap"""
        # Vektör araması
        search_results = self.vector_store.search(
            query=question,
            top_k=top_k,
            category_filter=category_filter
        )

        # Context oluştur
        context = self._build_context(search_results)

        # Prompt hazırla
        prompt = self.ANALYSIS_PROMPT_TEMPLATE.format(
            context=context if context else "İlgili doküman bulunamadı.",
            query=question
        )

        # LLM çağır
        try:
            answer = self._call_llm(prompt)
        except Exception as e:
            logger.error(f"LLM hatası: {e}")
            answer = self._fallback_response(question)

        # Kaynakları hazırla
        sources = [
            {
                "source": r.chunk.source,
                "page": r.chunk.page,
                "score": r.score,
                "preview": r.chunk.content[:200] + "..."
            }
            for r in search_results
        ]

        return AnalysisResult(
            query=question,
            answer=answer,
            sources=sources,
            context_used=context,
            metadata={"category_filter": category_filter, "top_k": top_k},
            confidence=search_results[0].score if search_results else 0.0
        )

    def analyze_alarm(
        self,
        machine_code: str,
        sensor_type: str,
        value: float,
        unit: str = "",
        timestamp: str = ""
    ) -> Dict[str, Any]:
        """Alarm durumu analizi"""
        # İlgili dokümanları ara
        query = f"{sensor_type} alarm threshold limit {unit}"
        search_results = self.vector_store.search(
            query=query,
            top_k=5,
            category_filter="alarm" if sensor_type == "vibration" else None
        )

        context = self._build_context(search_results)

        prompt = self.ALARM_ANALYSIS_TEMPLATE.format(
            machine_code=machine_code,
            sensor_type=sensor_type,
            value=value,
            unit=unit,
            timestamp=timestamp,
            context=context if context else "İlgili doküman bulunamadı."
        )

        try:
            response = self._call_llm(prompt)
            # JSON parse etmeyi dene
            try:
                # JSON bloğunu bul
                import re
                json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    result = {"explanation": response, "status": "unknown"}
            except json.JSONDecodeError:
                result = {"explanation": response, "status": "unknown"}

        except Exception as e:
            logger.error(f"Alarm analizi hatası: {e}")
            result = self._fallback_alarm_analysis(sensor_type, value)

        result["sources"] = [r.chunk.source for r in search_results[:3]]
        return result

    def _fallback_alarm_analysis(self, sensor_type: str, value: float) -> Dict[str, Any]:
        """Rule-based alarm analizi"""
        thresholds = {
            "temperature": {"warning": 60, "critical": 70, "unit": "°C"},
            "vibration": {"warning": 4.5, "critical": 7.1, "unit": "mm/s"},
            "current": {"warning": 12, "critical": 15, "unit": "A"},
            "pressure": {"warning": 8, "critical": 10, "unit": "bar"}
        }

        t = thresholds.get(sensor_type, {"warning": 100, "critical": 200, "unit": ""})

        if value >= t["critical"]:
            status = "critical"
        elif value >= t["warning"]:
            status = "warning"
        else:
            status = "normal"

        return {
            "status": status,
            "standard_reference": "Internal thresholds",
            "limit_info": f"Warning: {t['warning']} {t['unit']}, Critical: {t['critical']} {t['unit']}",
            "oee_impact": "Critical durumda availability düşer" if status == "critical" else "Normal",
            "root_cause": "Analiz için daha fazla doküman gerekli",
            "action": "Acil müdahale gerekli" if status == "critical" else "İzlemeye devam",
            "explanation": f"Değer: {value}, Durum: {status}"
        }

    def analyze_oee(
        self,
        machine_code: str,
        availability: float,
        performance: float,
        quality: float,
        mtbf: float = 0,
        mttr: float = 0
    ) -> Dict[str, Any]:
        """OEE analizi"""
        oee = availability * performance * quality / 10000

        query = "OEE improvement availability performance quality MTBF MTTR"
        search_results = self.vector_store.search(query=query, top_k=5)
        context = self._build_context(search_results)

        prompt = self.OEE_ANALYSIS_TEMPLATE.format(
            machine_code=machine_code,
            availability=availability,
            performance=performance,
            quality=quality,
            oee=round(oee, 1),
            mtbf=mtbf,
            mttr=mttr,
            context=context if context else "İlgili doküman bulunamadı."
        )

        try:
            response = self._call_llm(prompt)
            try:
                import re
                json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    result = {"explanation": response}
            except json.JSONDecodeError:
                result = {"explanation": response}

        except Exception as e:
            logger.error(f"OEE analizi hatası: {e}")
            result = self._fallback_oee_analysis(availability, performance, quality, oee)

        result["sources"] = [r.chunk.source for r in search_results[:3]]
        return result

    def _fallback_oee_analysis(
        self,
        availability: float,
        performance: float,
        quality: float,
        oee: float
    ) -> Dict[str, Any]:
        """Rule-based OEE analizi"""
        # En düşük faktörü bul
        factors = {"availability": availability, "performance": performance, "quality": quality}
        weakest = min(factors, key=factors.get)

        # OEE sınıflandırması
        if oee >= 85:
            oee_class = "World Class"
        elif oee >= 70:
            oee_class = "Good"
        elif oee >= 55:
            oee_class = "Average"
        else:
            oee_class = "Poor"

        recommendations = []
        if availability < 90:
            recommendations.append("Planlı bakım süresini azaltın")
            recommendations.append("Arıza sürelerini analiz edin")
        if performance < 95:
            recommendations.append("Küçük duruşları minimize edin")
            recommendations.append("Hız kayıplarını inceleyin")
        if quality < 99:
            recommendations.append("Hurda ve yeniden işleme azaltın")
            recommendations.append("Kalite kontrol süreçlerini iyileştirin")

        return {
            "oee_class": oee_class,
            "weakest_factor": weakest,
            "improvement_potential": round(85 - oee, 1) if oee < 85 else 0,
            "recommendations": recommendations,
            "explanation": f"OEE: {oee:.1f}% - {oee_class}. En zayıf faktör: {weakest}"
        }

    def get_vibration_limits(self, machine_type: str = "general") -> Dict[str, Any]:
        """Titreşim limitleri sorgula"""
        query = f"vibration limits thresholds ISO 10816 {machine_type} mm/s RMS velocity"
        search_results = self.vector_store.search(query=query, top_k=5, category_filter="vibration")

        if search_results:
            context = self._build_context(search_results)
            prompt = f"""Aşağıdaki doküman bilgilerine göre {machine_type} tipi makineler için
titreşim limitlerini özetle (mm/s RMS velocity cinsinden):

{context}

Cevabı şu formatta ver:
- Good (İyi): X-Y mm/s
- Acceptable (Kabul edilebilir): X-Y mm/s
- Warning (Uyarı): X-Y mm/s
- Unacceptable (Kabul edilemez): >X mm/s
"""
            try:
                answer = self._call_llm(prompt)
            except:
                answer = self._fallback_vibration_limits()
        else:
            answer = self._fallback_vibration_limits()

        return {
            "machine_type": machine_type,
            "limits": answer,
            "sources": [r.chunk.source for r in search_results[:3]] if search_results else []
        }

    def _fallback_vibration_limits(self) -> str:
        """ISO 10816-3 tabanlı varsayılan limitler"""
        return """ISO 10816-3 Sınıf I Makineler (genel):
- Good (İyi): 0 - 2.8 mm/s
- Acceptable (Kabul edilebilir): 2.8 - 4.5 mm/s
- Warning (Uyarı): 4.5 - 7.1 mm/s
- Unacceptable (Kabul edilemez): > 7.1 mm/s

Not: Gerçek değerler makine tipine göre değişir."""
