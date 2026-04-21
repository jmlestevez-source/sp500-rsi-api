# diagnose.py
import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENROUTER_API_KEY")

def check_account():
    print("=== CUENTA ===")
    r = requests.get(
        "https://openrouter.ai/api/v1/auth/key",
        headers={"Authorization": f"Bearer {API_KEY}"}
    )
    print(f"Status: {r.status_code}")
    print(f"Response: {r.json()}")

def check_free_models():
    print("\n=== MODELOS GRATUITOS ===")
    r = requests.get(
        "https://openrouter.ai/api/v1/models",
        headers={"Authorization": f"Bearer {API_KEY}"}
    )
    models = r.json().get("data", [])
    free   = [m for m in models if ":free" in m.get("id","")]
    print(f"Total modelos: {len(models)}")
    print(f"Modelos :free: {len(free)}")
    for m in free[:10]:
        print(f"  - {m['id']}")

def test_simple_call(model: str):
    print(f"\n=== TEST: {model} ===")
    r = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": model,
            "messages": [{"role": "user", "content": "Say OK"}],
            "max_tokens": 10
        },
        timeout=30
    )
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text[:300]}")
    return r.status_code == 200

if __name__ == "__main__":
    check_account()
    check_free_models()

    models_to_test = [
        "meta-llama/llama-3.1-8b-instruct:free",
        "mistralai/mistral-7b-instruct:free",
        "google/gemma-2-9b-it:free",
    ]

    for model in models_to_test:
        ok = test_simple_call(model)
        time.sleep(5)
