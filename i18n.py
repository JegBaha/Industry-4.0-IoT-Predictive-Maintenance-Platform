"""Simple i18n helper with inline bundles."""
from config import locales

_BUNDLES = {
    "tr": {
        "welcome": "Akıllı Fabrika Simülatörü",
        "health_ok": "Sistem çalışıyor",
    },
    "en": {
        "welcome": "Smart Factory Simulator",
        "health_ok": "System healthy",
    },
    "de": {
        "welcome": "Smart Factory Simulator",
        "health_ok": "System läuft",
    },
}


def t(key: str, lang: str | None = None) -> str:
    lang = lang or locales.default
    bundle = _BUNDLES.get(lang, _BUNDLES[locales.default])
    return bundle.get(key, key)
