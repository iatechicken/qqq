import os, time, json, base64
from urllib.parse import urlencode, urlparse, parse_qs
import requests
from dotenv import load_dotenv

load_dotenv()

AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize"
TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
TOKENS_PATH = "tokens.json"

def basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")

def main():
    client_id = os.environ["SCHWAB_CLIENT_ID"].strip()
    client_secret = os.environ["SCHWAB_CLIENT_SECRET"].strip()
    redirect_uri = os.environ.get("SCHWAB_REDIRECT_URI", "https://127.0.0.1").strip()

    url = AUTH_URL + "?" + urlencode({"client_id": client_id, "redirect_uri": redirect_uri})
    print("\n1) Open this URL in a browser and approve:\n")
    print(url)
    print("\n2) You’ll be redirected to your redirect URI (may show an error page).")
    print("   Copy the FULL redirect URL from the address bar and paste it here.\n")

    redirect = input("Redirect URL: ").strip()
    qs = parse_qs(urlparse(redirect).query)
    code = qs.get("code", [None])[0]
    if not code:
        raise SystemExit("No `code` param found in redirect URL.")

    headers = {
        "Authorization": basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    tok = r.json()
    tok["obtained_at"] = int(time.time())

    with open(TOKENS_PATH, "w") as f:
        json.dump(tok, f, indent=2, sort_keys=True)

    print(f"\n✅ Saved {TOKENS_PATH}. You can now run ingestion.\n")

if __name__ == "__main__":
    main()
