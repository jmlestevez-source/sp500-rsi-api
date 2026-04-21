# check_models.py
import requests
import os
from dotenv import load_dotenv

load_dotenv()

def check_free_models():
    headers = {
        "Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY')}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(
        "https://openrouter.ai/api/v1/models",
        headers=headers
    )
    
    models = response.json().get("data", [])
    
    free_models = [
        m for m in models
        if (
            m.get("pricing", {}).get("prompt") == "0"
            or m.get("pricing", {}).get("prompt") == 0
            or ":free" in m.get("id", "")
        )
    ]
    
    print(f"Modelos gratuitos disponibles ({len(free_models)}):\n")
    for m in sorted(free_models, key=lambda x: x["id"]):
        ctx = m.get("context_length", "?")
        print(f"  {m['id']} (ctx: {ctx})")

if __name__ == "__main__":
    check_free_models()
