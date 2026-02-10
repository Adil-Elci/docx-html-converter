import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.server import slugify


def test_german_slug_rules():
    slug, warning = slugify("Übermäßige Größe: Spaß & Fußball ß", 80)
    assert slug == "uebermaessige-groesse-spass-und-fussball-ss"
    assert warning is None
