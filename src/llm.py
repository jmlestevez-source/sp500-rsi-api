# src/llm.py
"""
Clientes LLM: Groq (primario) y Gemini (backup).
Sin cambios respecto al original salvo limpieza.
"""

import os
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv()

GROQ_MODELS = [
    "llama-3.3-70b-versatile",
    "llama-3.1-70b-versatile",
    "mixtral-8x7b-32768",
    "llama3-70b-8192",
    "llama-3.1-8b-instant",
    "gemma2-9b-it",
]

GEMINI_MODELS = [
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
    "gemini-2.0-flash-lite",
]

# Contador global de llamadas API
request_counts: dict[str, int] = {}


# ── Groq ──────────────────────────────────────────────────────────────────────

def call_groq(
    prompt: str,
    system: str = "",
    max_tokens: int = 1000,
    temperature: float = 0.1,
) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY no encontrada")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for model in GROQ_MODELS:
        try:
            time.sleep(2)
            messages = []
            if system:
                messages.append(
                    {"role": "system", "content": system}
                )
            messages.append(
                {"role": "user", "content": prompt}
            )

            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json={
                    "model":       model,
                    "messages":    messages,
                    "max_tokens":  max_tokens,
                    "temperature": temperature,
                },
                timeout=60,
            )

            key = f"groq/{model}"
            request_counts[key] = request_counts.get(key, 0) + 1

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 15))
                print(f"    Groq rate limit, esperando {retry}s...")
                time.sleep(retry + 2)
                continue

            if r.status_code == 200:
                content = (
                    r.json()["choices"][0]["message"]["content"]
                )
                if content and content.strip():
                    print(f"    groq/{model.split('-')[0]} OK")
                    return content.strip()

            print(f"    Groq {model}: HTTP {r.status_code}")

        except requests.exceptions.Timeout:
            print(f"    Groq {model}: timeout")
            continue
        except Exception as e:
            print(f"    Groq {model}: {e}")
            continue

    raise ValueError("Groq: todos los modelos fallaron")


# ── Gemini ────────────────────────────────────────────────────────────────────

def call_gemini(
    prompt: str,
    max_tokens: int = 1000,
    temperature: float = 0.1,
) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY no encontrada")

    for model in GEMINI_MODELS:
        try:
            time.sleep(1)
            r = requests.post(
                "https://generativelanguage.googleapis.com"
                f"/v1beta/models/{model}:generateContent"
                f"?key={api_key}",
                json={
                    "contents": [
                        {"parts": [{"text": prompt}]}
                    ],
                    "generationConfig": {
                        "maxOutputTokens": max_tokens,
                        "temperature":     temperature,
                    },
                },
                timeout=60,
            )

            key = f"gemini/{model}"
            request_counts[key] = request_counts.get(key, 0) + 1

            if r.status_code == 429:
                print("    Gemini rate limit, esperando 15s...")
                time.sleep(15)
                continue

            if r.status_code == 200:
                data    = r.json()
                content = (
                    data
                    .get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                )
                if content and content.strip():
                    print(f"    gemini/{model} OK")
                    return content.strip()

            print(f"    Gemini {model}: HTTP {r.status_code}")

        except requests.exceptions.Timeout:
            print(f"    Gemini {model}: timeout")
            continue
        except Exception as e:
            print(f"    Gemini {model}: {e}")
            continue

    raise ValueError("Gemini: todos los modelos fallaron")


# ── Router ────────────────────────────────────────────────────────────────────

def call_llm(
    prompt: str,
    task: str = "general",
    system: str = "",
    max_tokens: int = 1000,
    temperature: float = 0.1,
) -> str:
    groq_key   = os.getenv("GROQ_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    errors     = []

    if groq_key:
        try:
            return call_groq(prompt, system, max_tokens, temperature)
        except Exception as e:
            errors.append(f"Groq: {e}")
            print("    Groq falló, probando Gemini...")

    if gemini_key:
        try:
            full = f"{system}\n\n{prompt}" if system else prompt
            return call_gemini(full, max_tokens, temperature)
        except Exception as e:
            errors.append(f"Gemini: {e}")

    raise Exception(
        f"Todos los LLMs fallaron para '{task}'.\nErrores: {errors}"
    )


def call_llm_json(
    prompt: str,
    task: str = "general",
    max_tokens: int = 300,
    temperature: float = 0.1,
) -> dict:
    system = (
        "You are a financial analyst assistant. "
        "You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, markdown, "
        "or text before or after the JSON. "
        "Start your response directly with { and end with }. "
        "All numbers must be numeric, not strings."
    )
    full_prompt = (
        f"{prompt}\n\n"
        "IMPORTANT: Your entire response must be a single valid "
        "JSON object. Start with { and end with }. No other text."
    )

    groq_key   = os.getenv("GROQ_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")

    if groq_key:
        try:
            text = call_groq(full_prompt, system, max_tokens, temperature)
            return extract_json(text)
        except Exception as e:
            print(f"    Groq JSON falló: {e}")

    if gemini_key:
        try:
            full = f"{system}\n\n{full_prompt}"
            text = call_gemini(full, max_tokens, temperature)
            return extract_json(text)
        except Exception as e:
            print(f"    Gemini JSON falló: {e}")

    raise Exception(f"No se pudo obtener JSON válido para '{task}'")


def extract_json(text: str) -> dict:
    """Extrae JSON aunque el modelo añada texto extra."""
    try:
        return json.loads(text)
    except Exception:
        pass

    for marker in ["```json", "```"]:
        if marker in text:
            start = text.find(marker) + len(marker)
            end   = text.find("```", start)
            if end > start:
                try:
                    return json.loads(text[start:end].strip())
                except Exception:
                    pass

    start = text.find("{")
    end   = text.rfind("}") + 1
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end])
        except Exception:
            pass

    raise Exception(f"JSON no encontrado en:\n{text[:300]}")
