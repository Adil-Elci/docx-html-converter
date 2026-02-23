import json
import requests

BASE_URL = "http://localhost:8000"

SAMPLE_GOOGLE_DOC = "https://docs.google.com/document/d/GOOGLE_DOC_ID/edit"
SAMPLE_DOCX_URL = "https://example.com/sample.docx"


def run_request(source_url: str):
    payload = {
        "publishing_site": "example.com",
        "source_url": source_url,
        "post_status": "draft",
        "language": "de",
        "options": {
            "remove_images": True,
            "fix_headings": True,
            "max_slug_length": 80,
            "max_meta_length": 155,
            "max_excerpt_length": 180,
        },
    }
    response = requests.post(f"{BASE_URL}/convert", json=payload, timeout=60)
    print(f"Status: {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    except Exception:
        print(response.text)


if __name__ == "__main__":
    print("Testing Google Doc placeholder...")
    run_request(SAMPLE_GOOGLE_DOC)
    print("\nTesting DOCX URL placeholder...")
    run_request(SAMPLE_DOCX_URL)
