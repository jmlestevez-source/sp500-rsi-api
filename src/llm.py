# src/llm.py
"""
Clientes LLM: Groq (primario) y Gemini (backup).
Cambio clave: máximo 30s de espera por rate limit,
nunca más de 1000s.
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

request_counts: dict[str, int] = {}

# Tiempo máximo que esperaremos un rate limit
MAX_RETRY_WAIT = 30  # segundos


def call_groq(
    prompt:      str,
    system:      str = "",
    max_tokens:  int = 1000,
    temperature: float = 0.1,
) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY no encontrada")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    for model in GROQ_MODELS:
        try:
            time.sleep(1)  # pausa mínima entre llamadas

            messages = []
            if system:
                messages.append(
                    {"role": "system", "content": system}
                )
            messages.append(
                {"role": "user", "content": prompt}
            )

            r = requests.post(
                "https://api.groq.com/openai/v1/"
                "chat/completions",
                headers=headers,
                json={
                    "model":       model,
                    "messages":    messages,
                    "max_tokens":  max_tokens,
                    "temperature": temperature,
                },
                timeout=30,
            )

            key = f"groq/{model}"
            request_counts[key] = (
                request_counts.get(key, 0) + 1
            )

            if r.status_code == 429:
                # Leer el Retry-After pero limitar a MAX
                retry_raw = int(
                    r.headers.get("Retry-After", 15)
                )
                retry = min(retry_raw, MAX_RETRY_WAIT)

                if retry_raw > MAX_RETRY_WAIT:
                    # Rate limit demasiado largo:
                    # saltar a siguiente modelo
                    print(
                        f"    Groq {model}: rate limit "
                        f"{retry_raw}s → saltando modelo"
                    )
                    continue

                print(
                    f"    Groq rate limit, "
                    f"esperando {retry}s..."
                )
                time.sleep(retry)
                continue

            if r.status_code == 400:
                # Error del modelo (contexto, etc.)
                # Saltar directamente al siguiente
                print(f"    Groq {model}: HTTP 400, saltando")
                continue

            if r.status_code == 200:
                content = (
                    r.json()
                    ["choices"][0]["message"]["content"]
                )
                if content and content.strip():
                    short = model.split("-")[0]
                    print(f"    groq/{short} OK")
                    return content.strip()

            print(
                f"    Groq {model}: HTTP {r.status_code}"
            )

        except requests.exceptions.Timeout:
            print(f"    Groq {model}: timeout")
            continue
        except Exception as e:
            print(f"    Groq {model}: {e}")
            continue

    raise ValueError("Groq: todos los modelos fallaron")


def call_gemini(
    prompt:      str,
    max_tokens:  int = 1000,
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
                timeout=30,
            )

            key = f"gemini/{model}"
            request_counts[key] = (
                request_counts.get(key, 0) + 1
            )

            if r.status_code == 429:
                retry_raw = int(
                    r.headers.get("Retry-After", 15)
                )
                retry = min(retry_raw, MAX_RETRY_WAIT)
                if retry_raw > MAX_RETRY_WAIT:
                    print(
                        f"    Gemini {model}: rate limit "
                        f"{retry_raw}s → saltando"
                    )
                    continue
                print(
                    f"    Gemini rate limit, "
                    f"esperando {retry}s..."
                )
                time.sleep(retry)
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

            print(
                f"    Gemini {model}: "
                f"HTTP {r.status_code}"
            )

        except requests.exceptions.Timeout:
            print(f"    Gemini {model}: timeout")
            continue
        except Exception as e:
            print(f"    Gemini {model}: {e}")
            continue

    raise ValueError("Gemini: todos los modelos fallaron")


def call_llm(
    prompt:      str,
    task:        str   = "general",
    system:      str   = "",
    max_tokens:  int   = 1000,
    temperature: float = 0.1,
) -> str:
    groq_key   = os.getenv("GROQ_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    errors     = []

    if groq_key:
        try:
            return call_groq(
                prompt, system, max_tokens, temperature
            )
        except Exception as e:
            errors.append(f"Groq: {e}")
            print("    Groq falló, probando Gemini...")

    if gemini_key:
        try:
            full = (
                f"{system}\n\n{prompt}"
                if system else prompt
            )
            return call_gemini(
                full, max_tokens, temperature
            )
        except Exception as e:
            errors.append(f"Gemini: {e}")

    raise Exception(
        f"Todos los LLMs fallaron para '{task}'. "
        f"Errores: {errors}"
    )


def call_llm_json(
    prompt:      str,
    task:        str   = "general",
    max_tokens:  int   = 300,
    temperature: float = 0.1,
) -> dict:
    system = (
        "You are a financial analyst assistant. "
        "You MUST respond with ONLY a valid JSON object. "
        "Do NOT include any explanation, markdown, "
        "or text before or after the JSON. "
        "Start your response directly with { "
        "and end with }. "
        "All numbers must be numeric, not strings."
    )
    full_prompt = (
        f"{prompt}\n\n"
        "IMPORTANT: Your entire response must be "
        "a single valid JSON object. "
        "Start with { and end with }. No other text."
    )

    groq_key   = os.getenv("GROQ_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")

    if groq_key:
        try:
            text = call_groq(
                full_prompt, system,
                max_tokens, temperature,
            )
            return extract_json(text)
        except Exception as e:
            print(f"    Groq JSON falló: {e}")

    if gemini_key:
        try:
            full = f"{system}\n\n{full_prompt}"
            text = call_gemini(
                full, max_tokens, temperature
            )
            return extract_json(text)
        except Exception as e:
            print(f"    Gemini JSON falló: {e}")

    raise Exception(
        f"No se pudo obtener JSON válido "
        f"para '{task}'"
    )


def extract_json(text: str) -> dict:
    """Extrae JSON aunque el modelo añada texto."""
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
                    return json.loads(
                        text[start:end].strip()
                    )
                except Exception:
                    pass

    start = text.find("{")
    end   = text.rfind("}") + 1
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end])
        except Exception:
            pass

    raise Exception(
        f"JSON no encontrado en:\n{text[:300]}"
    )
