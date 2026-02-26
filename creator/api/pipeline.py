from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

from .llm import LLMError, call_llm_json, call_llm_text
from .validators import (
    count_h2,
    count_hyperlinks,
    locate_backlink,
    validate_backlink_placement,
    validate_hyperlink_count,
    validate_word_count,
    word_count_from_html,
)
from .web import (
    extract_internal_links,
    extract_canonical_link,
    extract_meta_content,
    extract_page_title,
    fetch_url,
    sanitize_html,
)

logger = logging.getLogger("creator.pipeline")

DEFAULT_LLM_BASE_URL = "https://api.openai.com/v1"
DEFAULT_LLM_MODEL = "gpt-4.1-mini"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_HTTP_RETRIES = 2
DEFAULT_LEONARDO_BASE_URL = "https://cloud.leonardo.ai/api/rest/v1"
DEFAULT_LEONARDO_MODEL_ID = "1dd50843-d653-4516-a8e3-f0238ee453ff"
DEFAULT_IMAGE_WIDTH = 1024
DEFAULT_IMAGE_HEIGHT = 576
DEFAULT_POLL_SECONDS = 2
DEFAULT_POLL_TIMEOUT_SECONDS = 90

NEGATIVE_PROMPT = "text, watermark, logo, letters, UI, low quality, blurry, deformed"

STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "your", "you", "our", "are", "was", "were",
    "what", "when", "where", "why", "how", "who", "about", "into", "out", "over", "under", "between", "while",
    "their", "they", "them", "these", "those", "also", "but", "not", "can", "will", "just", "than", "then",
    "such", "use", "using", "used", "more", "most", "some", "any", "each", "other", "its", "it's", "our", "we",
}


class CreatorError(RuntimeError):
    pass


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _read_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        return float(raw)
    except ValueError:
        return default


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    parsed = urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        return cleaned
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"
    return normalized.rstrip("/")


def _extract_keywords(text: str, max_terms: int = 10) -> List[str]:
    words = re.findall(r"\b[a-zA-Z][a-zA-Z-]{2,}\b", (text or "").lower())
    counts: Dict[str, int] = {}
    for word in words:
        if word in STOPWORDS:
            continue
        counts[word] = counts.get(word, 0) + 1
    sorted_terms = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [term for term, _ in sorted_terms[:max_terms]]


def _guess_brand_name(target_url: str, html: str) -> str:
    meta = extract_meta_content(html, [("property", "og:site_name"), ("name", "application-name")])
    if meta:
        return meta
    title = extract_page_title(html)
    if title:
        for sep in ["|", "-", "–", ":"]:
            if sep in title:
                return title.split(sep)[0].strip()
        return title.strip()
    host = urlparse(target_url).netloc
    return host.replace("www.", "").split(".")[0].replace("-", " ").title()


def _pick_backlink_url(target_url: str, html: str) -> str:
    canonical = extract_canonical_link(html)
    if canonical:
        return canonical
    links = extract_internal_links(html, target_url)
    for keyword in ("services", "solutions", "product", "pricing", "about", "platform"):
        for link in links:
            if keyword in link:
                return link
    return target_url


def _is_anchor_safe(anchor: Optional[str]) -> bool:
    if not anchor:
        return False
    cleaned = anchor.strip()
    if len(cleaned) < 2 or len(cleaned) > 80:
        return False
    if re.search(r"https?://", cleaned):
        return False
    lowered = cleaned.lower()
    if any(term in lowered for term in ["visit our", "buy now", "click here", "limited time"]):
        return False
    return True


def _build_anchor_text(anchor_type: str, brand_name: str, keyword_cluster: List[str]) -> str:
    if anchor_type == "brand" and brand_name:
        return brand_name
    if anchor_type == "partial_match" and keyword_cluster:
        return " ".join(keyword_cluster[:3]).title()
    return "this resource"


def _strip_code_fences(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:html)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _wrap_paragraphs(text: str) -> str:
    cleaned = _strip_code_fences(text)
    if "<p" in cleaned or "<h2" in cleaned:
        return cleaned
    parts = [part.strip() for part in re.split(r"\n{2,}", cleaned) if part.strip()]
    if not parts:
        return ""
    return "".join(f"<p>{part}</p>" for part in parts)


def _normalize_section_html(h2: str, h3s: List[str], raw: str) -> str:
    cleaned = _strip_code_fences(raw)
    if "<h2" in cleaned:
        return cleaned
    body = _wrap_paragraphs(cleaned)
    html = f"<h2>{h2}</h2>"
    if h3s:
        for h3 in h3s:
            html += f"<h3>{h3}</h3>"
    if body:
        html += body
    return html


def _strip_non_backlinks(html: str, backlink_url: str) -> str:
    if not backlink_url:
        return re.sub(r"<a[^>]*>(.*?)</a>", r"\1", html, flags=re.IGNORECASE | re.DOTALL)

    def replacer(match: re.Match[str]) -> str:
        href = match.group(1) or ""
        inner = match.group(2) or ""
        if backlink_url in href:
            return match.group(0)
        return inner

    return re.sub(
        r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        replacer,
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )


def _insert_backlink(html: str, backlink_url: str, anchor_text: str, placement: str) -> str:
    anchor_html = f'<a href="{backlink_url}">{anchor_text}</a>'
    if placement == "intro":
        match = re.search(r"</p>", html, flags=re.IGNORECASE)
        if match:
            return html[:match.start()] + f" {anchor_html}" + html[match.start():]
        return anchor_html + html

    index = 0
    try:
        index = max(0, int(placement.split("_")[1]) - 2)
    except Exception:
        index = 0

    matches = list(re.finditer(r"<h2[^>]*>", html, flags=re.IGNORECASE))
    if not matches:
        return html + anchor_html

    if index >= len(matches):
        index = len(matches) - 1

    start = matches[index].end()
    after = html[start:]
    p_match = re.search(r"</p>", after, flags=re.IGNORECASE)
    if p_match:
        insert_at = start + p_match.start()
        return html[:insert_at] + f" {anchor_html}" + html[insert_at:]
    return html[:start] + anchor_html + html[start:]


def _generate_article_by_sections(
    *,
    phase4: Dict[str, Any],
    phase3: Dict[str, Any],
    backlink_url: str,
    llm_api_key: str,
    llm_base_url: str,
    llm_model: str,
    http_timeout: int,
) -> Optional[Dict[str, Any]]:
    outline_items = phase4.get("outline") or []
    if not isinstance(outline_items, list) or not outline_items:
        return None

    h2_count = len(outline_items)
    intro_target = 150
    target_total = 1050
    per_section = max(150, int((target_total - intro_target) / max(1, h2_count)))
    per_min = max(130, per_section - 20)
    per_max = min(280, per_section + 50)

    backlink_placement = phase4.get("backlink_placement") or "intro"
    anchor_text = phase4.get("anchor_text_final") or "this resource"

    intro_system = "Write a short introduction paragraph in HTML. Return only HTML."
    intro_user = (
        f"Topic: {phase3.get('final_article_topic','')}\n"
        f"H1: {phase4.get('h1','')}\n"
        f"Length: {intro_target - 20}-{intro_target + 20} words.\n"
        "No hyperlinks unless explicitly requested."
    )
    if backlink_placement == "intro":
        intro_user += f"\nInclude exactly one hyperlink to {backlink_url} with anchor text: {anchor_text}."

    try:
        intro_raw = call_llm_text(
            system_prompt=intro_system,
            user_prompt=intro_user,
            api_key=llm_api_key,
            base_url=llm_base_url,
            model=llm_model,
            timeout_seconds=http_timeout,
            max_tokens=500,
            temperature=0.2,
        )
    except LLMError:
        intro_raw = ""
    intro_html = _wrap_paragraphs(intro_raw) or "<p></p>"

    sections_html: List[str] = []
    for index, item in enumerate(outline_items, start=1):
        h2 = (item.get("h2") or "").strip() if isinstance(item, dict) else str(item)
        h3s = item.get("h3") if isinstance(item, dict) else []
        h3s_list = [str(h3).strip() for h3 in (h3s or []) if str(h3).strip()]
        placement_index = None
        if backlink_placement.startswith("section_"):
            try:
                placement_index = int(backlink_placement.split("_")[1]) - 1
            except Exception:
                placement_index = None
        include_backlink = placement_index == (index - 1)

        section_system = "Write HTML for a single H2 section of a guest post. Return only HTML."
        section_user = (
            f"H2: {h2}\n"
            f"H3s: {h3s_list}\n"
            f"Length: {per_min}-{per_max} words.\n"
            "Write in a neutral authoritative tone. Do not use bullet lists unless necessary."
            "\nDo not include any hyperlinks unless explicitly requested."
        )
        if include_backlink:
            section_user += f"\nInclude exactly one hyperlink to {backlink_url} with anchor text: {anchor_text}."

        try:
            raw = call_llm_text(
                system_prompt=section_system,
                user_prompt=section_user,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=1200,
                temperature=0.2,
            )
        except LLMError:
            raw = ""

        sections_html.append(_normalize_section_html(h2, h3s_list, raw))

    article_html = f"<h1>{phase4.get('h1','')}</h1>" + intro_html + "".join(sections_html)
    article_html = _strip_non_backlinks(article_html, backlink_url)
    if backlink_url and anchor_text and backlink_url not in article_html:
        article_html = _insert_backlink(article_html, backlink_url, anchor_text, backlink_placement)

    word_count = word_count_from_html(article_html)
    for _expand_pass in range(3):
        if word_count >= 800:
            break
        expand_system = "Write an additional paragraph for a blog post in HTML. Return only HTML."
        expand_user = (
            f"Topic: {phase3.get('final_article_topic','')}\n"
            f"Current word count: {word_count}. Need at least 800 words.\n"
            f"Write one additional paragraph of 120-180 words that fits the article. "
            "No hyperlinks."
        )
        try:
            extra = call_llm_text(
                system_prompt=expand_system,
                user_prompt=expand_user,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=400,
                temperature=0.2,
            )
            article_html += _wrap_paragraphs(extra)
            word_count = word_count_from_html(article_html)
        except LLMError:
            break

    meta_title = phase4.get("h1") or ""
    excerpt = ""
    match = re.search(r"<p[^>]*>(.*?)</p>", article_html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        excerpt = re.sub(r"<[^>]+>", "", match.group(1)).strip()[:200]

    return {
        "meta_title": meta_title,
        "meta_description": "",
        "slug": "",
        "excerpt": excerpt,
        "article_html": article_html,
    }


def _call_leonardo(
    *,
    prompt: str,
    api_key: str,
    base_url: str,
    model_id: str,
    width: int,
    height: int,
    timeout_seconds: int,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
) -> str:
    if not api_key:
        raise CreatorError("LEONARDO_API_KEY is required for image generation.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    create_url = base_url.rstrip("/") + "/generations"
    payload = {
        "prompt": prompt,
        "modelId": model_id,
        "width": width,
        "height": height,
        "num_images": 1,
        "negative_prompt": NEGATIVE_PROMPT,
        "guidance_scale": 6,
    }
    try:
        response = requests.post(create_url, headers=headers, json=payload, timeout=timeout_seconds)
    except Exception as exc:  # pragma: no cover - network
        raise CreatorError(f"Leonardo request failed: {exc}") from exc
    if response.status_code >= 400:
        raise CreatorError(f"Leonardo HTTP {response.status_code}: {response.text[:300]}")
    body = response.json()
    generation_id = body.get("sdGenerationJob", {}).get("generationId") or body.get("generationId")
    if not generation_id:
        raise CreatorError("Leonardo response missing generationId.")

    poll_url = base_url.rstrip("/") + f"/generations/{generation_id}"
    deadline = time.time() + poll_timeout_seconds
    while time.time() < deadline:
        try:
            poll = requests.get(poll_url, headers=headers, timeout=timeout_seconds)
        except Exception as exc:  # pragma: no cover - network
            raise CreatorError(f"Leonardo poll failed: {exc}") from exc
        if poll.status_code >= 400:
            raise CreatorError(f"Leonardo poll HTTP {poll.status_code}: {poll.text[:300]}")
        data = poll.json()
        generations = data.get("generations") or data.get("sdGenerationJob", {}).get("generations") or []
        if generations:
            url = generations[0].get("url") or generations[0].get("imageUrl")
            if url:
                return url
        time.sleep(poll_interval_seconds)
    raise CreatorError("Leonardo generation timed out.")


def run_creator_pipeline(*, target_site_url: str, publishing_site_url: str, anchor: Optional[str], topic: Optional[str], dry_run: bool) -> Dict[str, Any]:
    warnings: List[str] = []
    debug: Dict[str, Any] = {"dry_run": dry_run, "timings_ms": {}, "fetched_pages": []}

    http_timeout = _read_int_env("CREATOR_HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    http_retries = _read_int_env("CREATOR_HTTP_RETRIES", DEFAULT_HTTP_RETRIES)
    explicit_llm_key = os.getenv("CREATOR_LLM_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    llm_api_key = explicit_llm_key or openai_key or anthropic_key

    explicit_base_url = os.getenv("CREATOR_LLM_BASE_URL", "").strip()
    if explicit_base_url:
        llm_base_url = explicit_base_url
    elif anthropic_key and not openai_key:
        llm_base_url = "https://api.anthropic.com/v1"
    else:
        llm_base_url = DEFAULT_LLM_BASE_URL

    explicit_model = os.getenv("CREATOR_LLM_MODEL", "").strip()
    if explicit_model:
        llm_model = explicit_model
    elif "anthropic" in llm_base_url.lower():
        llm_model = "claude-haiku-4-20250414"
    else:
        llm_model = DEFAULT_LLM_MODEL

    phase_start = time.time()
    logger.info("creator.phase1.start target=%s", target_site_url)
    target_html = fetch_url(
        target_site_url,
        purpose="target_home",
        warnings=warnings,
        debug=debug,
        timeout_seconds=http_timeout,
        retries=http_retries,
    )
    target_text = sanitize_html(target_html)
    brand_name = _guess_brand_name(target_site_url, target_html)
    keyword_cluster = _extract_keywords(target_text, max_terms=10)
    backlink_url = _pick_backlink_url(target_site_url, target_html) or target_site_url
    anchor_type = "brand" if brand_name else "contextual_generic"
    if not brand_name and keyword_cluster:
        anchor_type = "partial_match"
    if not keyword_cluster:
        warnings.append("target_keywords_missing")
    phase1 = {
        "brand_name": brand_name,
        "backlink_url": backlink_url,
        "anchor_type": anchor_type,
        "keyword_cluster": keyword_cluster,
    }
    debug["timings_ms"]["phase1"] = int((time.time() - phase_start) * 1000)

    phase_start = time.time()
    logger.info("creator.phase2.start publishing=%s", publishing_site_url)
    publishing_html = fetch_url(
        publishing_site_url,
        purpose="host_home",
        warnings=warnings,
        debug=debug,
        timeout_seconds=http_timeout,
        retries=http_retries,
    )
    publishing_text = sanitize_html(publishing_html)
    if not publishing_text:
        warnings.append("publishing_site_fetch_empty")
    phase2 = {
        "allowed_topics": [],
        "content_style_constraints": [],
        "internal_linking_opportunities": [],
    }
    if publishing_text:
        system_prompt = (
            "You analyze publishing site content for safe guest post topics. "
            "Use only the provided site text. Return JSON with allowed_topics (5-10), "
            "content_style_constraints (3-6), internal_linking_opportunities (optional, internal only)."
        )
        user_prompt = (
            "Publishing site text:\n"
            f"{publishing_text[:4000]}\n\n"
            "Return JSON: {\"allowed_topics\":[...],\"content_style_constraints\":[...],\"internal_linking_opportunities\":[...]}."
        )
        try:
            llm_out = call_llm_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=900,
            )
            phase2["allowed_topics"] = llm_out.get("allowed_topics") or []
            phase2["content_style_constraints"] = llm_out.get("content_style_constraints") or []
            phase2["internal_linking_opportunities"] = llm_out.get("internal_linking_opportunities") or []
        except LLMError as exc:
            warnings.append(f"phase2_llm_failed:{exc}")
            phase2["allowed_topics"] = _extract_keywords(publishing_text, max_terms=8)
            phase2["content_style_constraints"] = ["Neutral, authoritative tone", "Avoid promotional language"]
    else:
        phase2["allowed_topics"] = []
        phase2["content_style_constraints"] = []

    debug["timings_ms"]["phase2"] = int((time.time() - phase_start) * 1000)

    phase_start = time.time()
    logger.info("creator.phase3.start")
    system_prompt = (
        "You select a guest post topic that fits publishing site authority and allows a natural backlink. "
        "Avoid promotional topics and exact match money keywords. Return JSON only."
    )
    user_prompt = (
        f"Allowed topics: {phase2['allowed_topics']}\n"
        f"Target keyword cluster: {keyword_cluster}\n"
        f"Requested topic (optional): {topic or ''}\n"
        "Return JSON: {\"final_article_topic\":\"...\",\"search_intent_type\":\"informational|commercial|navigational\","
        "\"primary_keyword\":\"...\",\"secondary_keywords\":[\"...\",\"...\"]}"
    )
    try:
        llm_out = call_llm_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=llm_api_key,
            base_url=llm_base_url,
            model=llm_model,
            timeout_seconds=http_timeout,
            max_tokens=500,
        )
        phase3 = {
            "final_article_topic": llm_out.get("final_article_topic") or (topic or ""),
            "search_intent_type": llm_out.get("search_intent_type") or "informational",
            "primary_keyword": llm_out.get("primary_keyword") or (keyword_cluster[0] if keyword_cluster else ""),
            "secondary_keywords": llm_out.get("secondary_keywords") or [],
        }
    except LLMError as exc:
        warnings.append(f"phase3_llm_failed:{exc}")
        fallback_topic = topic or (phase2["allowed_topics"][0] if phase2["allowed_topics"] else "Industry insights")
        phase3 = {
            "final_article_topic": fallback_topic,
            "search_intent_type": "informational",
            "primary_keyword": keyword_cluster[0] if keyword_cluster else fallback_topic,
            "secondary_keywords": keyword_cluster[1:3] if len(keyword_cluster) > 1 else [],
        }
    debug["timings_ms"]["phase3"] = int((time.time() - phase_start) * 1000)

    phase_start = time.time()
    logger.info("creator.phase4.start")
    anchor_safe = _is_anchor_safe(anchor)
    outline = None
    phase4 = {}
    outline_errors: List[str] = []
    for attempt in range(1, 3):
        system_prompt = (
            "Create an SEO article outline. Provide H1 and 4-6 H2 sections, optional H3. "
            "Choose backlink placement as intro or one specific section (section_2..section_6). "
            "Return JSON only."
        )
        user_prompt = (
            f"Topic: {phase3['final_article_topic']}\n"
            f"Allowed topics: {phase2['allowed_topics']}\n"
            f"Primary keyword: {phase3['primary_keyword']}\n"
            f"Secondary keywords: {phase3['secondary_keywords']}\n"
            f"Anchor provided: {anchor or ''}\n"
            f"Anchor safe: {anchor_safe}\n"
            "Return JSON: {\"h1\":\"...\",\"outline\":[{\"h2\":\"...\",\"h3\":[\"...\"]}],"
            "\"backlink_placement\":\"intro|section_2|section_3|section_4|section_5|section_6\",\"anchor_text_final\":\"...\"}"
        )
        try:
            llm_out = call_llm_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=700,
            )
        except LLMError as exc:
            outline_errors.append(str(exc))
            continue

        h1 = (llm_out.get("h1") or "").strip()
        outline_items = llm_out.get("outline") or []
        backlink_placement = (llm_out.get("backlink_placement") or "").strip()
        anchor_text_final = (llm_out.get("anchor_text_final") or "").strip()
        if not h1 or not isinstance(outline_items, list) or not (4 <= len(outline_items) <= 6):
            outline_errors.append("invalid_outline_structure")
            continue
        if backlink_placement not in {"intro", "section_2", "section_3", "section_4", "section_5", "section_6"}:
            outline_errors.append("invalid_backlink_placement")
            continue
        if not anchor_text_final:
            anchor_text_final = anchor if anchor_safe else _build_anchor_text(anchor_type, brand_name, keyword_cluster)
        outline = {
            "h1": h1,
            "outline": outline_items,
            "backlink_placement": backlink_placement,
            "anchor_text_final": anchor_text_final,
        }
        break

    if not outline:
        raise CreatorError(f"Outline validation failed: {outline_errors}")

    phase4 = outline
    debug["timings_ms"]["phase4"] = int((time.time() - phase_start) * 1000)

    phase_start = time.time()
    logger.info("creator.phase5.start")
    article_payload = None
    errors: List[str] = []
    backlink_url = phase1["backlink_url"]
    last_article_html = ""
    last_validation_errors: List[str] = []
    for attempt in range(1, 4):
        if attempt == 1:
            system_prompt = (
                "Write an SEO blog post in clean HTML. CRITICAL: the article body MUST be 800-1100 words "
                "(aim for 900+ words). Use neutral authoritative tone, "
                "exactly one hyperlink in the entire HTML, no CTA spam, no 'visit our site' language. "
                "Include H1 and 4-6 H2 sections. Each section should have 2-3 substantial paragraphs. "
                "Return JSON only."
            )
            user_prompt = (
                f"H1: {phase4['h1']}\n"
                f"Outline: {phase4['outline']}\n"
                f"Backlink placement: {phase4['backlink_placement']}\n"
                f"Backlink URL: {backlink_url}\n"
                f"Anchor text: {phase4['anchor_text_final']}\n"
                f"Primary keyword: {phase3['primary_keyword']}\n"
                f"Secondary keywords: {phase3['secondary_keywords']}\n"
                "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
                "\"excerpt\":\"...\",\"article_html\":\"...\"}"
            )
            max_tokens = 3500
            temperature = 0.3
        elif attempt == 2 and last_article_html:
            system_prompt = (
                "Fix or rewrite the HTML to satisfy all constraints. Do not return markdown fences. "
                "Return JSON only."
            )
            user_prompt = (
                f"Current article_html:\n{last_article_html}\n\n"
                f"Issues: {last_validation_errors}\n"
                f"Required H1: {phase4['h1']}\n"
                f"Required outline: {phase4['outline']}\n"
                "Constraints: 800-1100 words, H1 + 4-6 H2 sections, exactly one hyperlink in the HTML.\n"
                f"Backlink URL: {backlink_url}\n"
                f"Backlink placement: {phase4['backlink_placement']}\n"
                f"Anchor text (use exactly): {phase4['anchor_text_final']}\n"
                "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
                "\"excerpt\":\"...\",\"article_html\":\"...\"}"
            )
            max_tokens = 3500
            temperature = 0.2
        else:
            system_prompt = (
                "Write a NEW article from scratch. CRITICAL: the article body MUST be 800-1100 words "
                "(aim for 900+ words). Each H2 section needs 2-3 substantial paragraphs. "
                "Do not return markdown fences. Return JSON only."
            )
            user_prompt = (
                f"H1: {phase4['h1']}\n"
                f"Outline: {phase4['outline']}\n"
                f"Backlink placement: {phase4['backlink_placement']}\n"
                f"Backlink URL: {backlink_url}\n"
                f"Anchor text (use exactly): {phase4['anchor_text_final']}\n"
                "Constraints: 800-1100 words (aim for 900+), H1 + 4-6 H2 sections, exactly one hyperlink in the HTML, "
                "neutral authoritative tone, no CTA spam, no 'visit our site' language.\n"
                f"Primary keyword: {phase3['primary_keyword']}\n"
                f"Secondary keywords: {phase3['secondary_keywords']}\n"
                "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
                "\"excerpt\":\"...\",\"article_html\":\"...\"}"
            )
            max_tokens = 3500
            temperature = 0.2
        try:
            llm_out = call_llm_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=max_tokens,
                temperature=temperature,
                allow_html_fallback=True,
            )
        except LLMError as exc:
            errors.append(str(exc))
            continue

        html_fallback = bool(llm_out.pop("_html_fallback", False))
        article_html = (llm_out.get("article_html") or "").strip()
        if not article_html:
            errors.append("missing_article_html")
            continue

        wc = word_count_from_html(article_html)
        logger.info(
            "creator.phase5.attempt attempt=%s word_count=%s html_fallback=%s",
            attempt, wc, html_fallback,
        )

        if html_fallback:
            warnings.append("llm_html_fallback")

        validation_errors: List[str] = []
        for check in (
            validate_word_count(article_html, 750, 1100),
            validate_hyperlink_count(article_html, 1),
            validate_backlink_placement(article_html, backlink_url, phase4["backlink_placement"]),
        ):
            if check:
                validation_errors.append(check)
        if not (4 <= count_h2(article_html) <= 6):
            validation_errors.append("h2_count_invalid")

        if validation_errors:
            errors.extend(validation_errors)
            last_article_html = article_html
            last_validation_errors = validation_errors
            continue

        article_payload = {
            "meta_title": llm_out.get("meta_title") or phase4["h1"],
            "meta_description": llm_out.get("meta_description") or "",
            "slug": llm_out.get("slug") or "",
            "excerpt": llm_out.get("excerpt") or "",
            "article_html": article_html,
        }
        break

    if not article_payload:
        fallback_payload = _generate_article_by_sections(
            phase4=phase4,
            phase3=phase3,
            backlink_url=backlink_url,
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
            http_timeout=http_timeout,
        )
        if fallback_payload:
            article_html = (fallback_payload.get("article_html") or "").strip()
            validation_errors: List[str] = []
            for check in (
                validate_word_count(article_html, 750, 1100),
                validate_hyperlink_count(article_html, 1),
                validate_backlink_placement(article_html, backlink_url, phase4["backlink_placement"]),
            ):
                if check:
                    validation_errors.append(check)
            if not (4 <= count_h2(article_html) <= 6):
                validation_errors.append("h2_count_invalid")
            if not validation_errors:
                article_payload = fallback_payload
            else:
                errors.extend(validation_errors)

    if not article_payload:
        raise CreatorError(f"Article generation failed: {errors}")

    # ── post-generation repairs ──────────────────────────────────────
    art_html = (article_payload.get("article_html") or "").strip()
    # Strip stray hyperlinks (keep only the backlink)
    art_html = _strip_non_backlinks(art_html, backlink_url)
    # Insert backlink if missing
    if backlink_url and backlink_url not in art_html:
        anchor_text = phase4.get("anchor_text_final") or "this resource"
        art_html = _insert_backlink(art_html, backlink_url, anchor_text, phase4["backlink_placement"])
        warnings.append("backlink_inserted_post_generation")
    article_payload["article_html"] = art_html

    phase5 = article_payload
    debug["timings_ms"]["phase5"] = int((time.time() - phase_start) * 1000)

    phase_start = time.time()
    logger.info("creator.phase6.start")
    phase6 = {
        "image_model": "Leonardo Flux Schnell",
        "featured_image": {},
        "in_content_image": {},
    }

    image_prompts = None
    system_prompt = (
        "Generate image prompts for a blog post. Featured image is required. "
        "Optional in-content image if helpful. Return JSON only."
    )
    user_prompt = (
        f"Article topic: {phase3['final_article_topic']}\n"
        f"Outline: {phase4['outline']}\n"
        "Return JSON: {\"featured_prompt\":\"...\",\"featured_alt\":\"...\","
        "\"include_in_content\":true|false,\"in_content_prompt\":\"...\",\"in_content_alt\":\"...\"}"
    )
    try:
        image_prompts = call_llm_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=llm_api_key,
            base_url=llm_base_url,
            model=llm_model,
            timeout_seconds=http_timeout,
            max_tokens=500,
        )
    except LLMError as exc:
        warnings.append(f"phase6_llm_failed:{exc}")
        image_prompts = {
            "featured_prompt": f"Editorial photo illustrating: {phase3['final_article_topic']}",
            "featured_alt": phase3["final_article_topic"],
            "include_in_content": False,
            "in_content_prompt": "",
            "in_content_alt": "",
        }

    featured_prompt = (image_prompts.get("featured_prompt") or "").strip()
    featured_alt = (image_prompts.get("featured_alt") or "").strip()
    include_in_content = bool(image_prompts.get("include_in_content"))
    in_content_prompt = (image_prompts.get("in_content_prompt") or "").strip()
    in_content_alt = (image_prompts.get("in_content_alt") or "").strip()

    if not featured_prompt:
        featured_prompt = f"Editorial photo illustrating: {phase3['final_article_topic']}"
    if not featured_alt:
        featured_alt = phase3["final_article_topic"]

    leonardo_api_key = os.getenv("LEONARDO_API_KEY", "").strip()
    leonardo_base_url = os.getenv("LEONARDO_BASE_URL", DEFAULT_LEONARDO_BASE_URL).strip()
    width = _read_int_env("CREATOR_IMAGE_WIDTH", DEFAULT_IMAGE_WIDTH)
    height = _read_int_env("CREATOR_IMAGE_HEIGHT", DEFAULT_IMAGE_HEIGHT)
    poll_timeout = _read_int_env("CREATOR_IMAGE_POLL_TIMEOUT_SECONDS", DEFAULT_POLL_TIMEOUT_SECONDS)
    poll_interval = _read_int_env("CREATOR_IMAGE_POLL_INTERVAL_SECONDS", DEFAULT_POLL_SECONDS)
    image_required = _read_bool_env("CREATOR_IMAGE_REQUIRED", False)

    featured_image_url = ""
    in_content_image_url = ""
    if not dry_run:
        try:
            featured_image_url = _call_leonardo(
                prompt=featured_prompt,
                api_key=leonardo_api_key,
                base_url=leonardo_base_url,
                model_id=DEFAULT_LEONARDO_MODEL_ID,
                width=width,
                height=height,
                timeout_seconds=http_timeout,
                poll_timeout_seconds=poll_timeout,
                poll_interval_seconds=poll_interval,
            )
        except CreatorError as exc:
            warnings.append(f"phase6_featured_image_failed:{exc}")
            featured_image_url = ""
            include_in_content = False

        if include_in_content and in_content_prompt:
            try:
                in_content_image_url = _call_leonardo(
                    prompt=in_content_prompt,
                    api_key=leonardo_api_key,
                    base_url=leonardo_base_url,
                    model_id=DEFAULT_LEONARDO_MODEL_ID,
                    width=width,
                    height=height,
                    timeout_seconds=http_timeout,
                    poll_timeout_seconds=poll_timeout,
                    poll_interval_seconds=poll_interval,
                )
            except CreatorError as exc:
                warnings.append(f"phase6_in_content_image_failed:{exc}")
                in_content_image_url = ""

    if image_required and not featured_image_url and not dry_run:
        raise CreatorError("Featured image generation failed.")

    phase6["featured_image"] = {
        "prompt": featured_prompt,
        "negative_prompt": NEGATIVE_PROMPT,
        "alt_text": featured_alt,
    }
    phase6["in_content_image"] = {
        "prompt": in_content_prompt,
        "negative_prompt": NEGATIVE_PROMPT,
        "alt_text": in_content_alt,
    }
    debug["timings_ms"]["phase6"] = int((time.time() - phase_start) * 1000)

    phase_start = time.time()
    p7_wc = word_count_from_html(phase5["article_html"])
    logger.info("creator.phase7.start word_count=%s", p7_wc)
    phase7_errors: List[str] = []
    allowed_topics = [t.lower() for t in phase2.get("allowed_topics") or [] if isinstance(t, str)]
    if allowed_topics:
        topic_lower = (phase3["final_article_topic"] or "").lower()
        if not any(topic in topic_lower for topic in allowed_topics):
            phase7_errors.append("topic_not_in_allowed_topics")
    if validate_hyperlink_count(phase5["article_html"], 1):
        phase7_errors.append("hyperlink_count_invalid")
    if validate_word_count(phase5["article_html"], 750, 1100):
        phase7_errors.append("word_count_invalid")
    if not (4 <= count_h2(phase5["article_html"]) <= 6):
        phase7_errors.append("h2_count_invalid")

    if phase7_errors:
        # one fix pass
        current_wc = word_count_from_html(phase5["article_html"])
        system_prompt = (
            "Fix the HTML article to satisfy all SEO checks. "
            "The article MUST have between 800 and 1100 words (currently it has "
            f"{current_wc} words). Expand or rewrite sections as needed to reach "
            "at least 850 words. Keep exactly one hyperlink. Keep 4-6 H2 sections. "
            "Return JSON only."
        )
        user_prompt = (
            f"Article_html: {phase5['article_html']}\n"
            f"Issues: {phase7_errors}\n"
            f"Current word count: {current_wc}\n"
            f"Required word count: 800-1100\n"
            f"Backlink URL: {backlink_url}\n"
            f"Placement: {phase4['backlink_placement']}\n"
            f"Anchor text: {phase4['anchor_text_final']}\n"
            "Return JSON: {\"meta_title\":\"...\",\"meta_description\":\"...\",\"slug\":\"...\","
            "\"excerpt\":\"...\",\"article_html\":\"...\"}"
        )
        try:
            llm_out = call_llm_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=4500,
                allow_html_fallback=True,
            )
            fixed_html = (llm_out.get("article_html") or "").strip()
            fixed_wc = word_count_from_html(fixed_html) if fixed_html else 0
            logger.info("creator.phase7.fix_result before=%s after=%s", current_wc, fixed_wc)
            # Only accept the fix if it improved or at least maintained word count
            if fixed_html and fixed_wc >= current_wc:
                phase5["article_html"] = fixed_html
            phase5["meta_title"] = llm_out.get("meta_title") or phase5["meta_title"]
            phase5["meta_description"] = llm_out.get("meta_description") or phase5["meta_description"]
            phase5["slug"] = llm_out.get("slug") or phase5["slug"]
            phase5["excerpt"] = llm_out.get("excerpt") or phase5["excerpt"]
            phase7_errors = []
            if validate_hyperlink_count(phase5["article_html"], 1):
                phase7_errors.append("hyperlink_count_invalid")
            if validate_word_count(phase5["article_html"], 750, 1100):
                phase7_errors.append("word_count_invalid")
            if not (4 <= count_h2(phase5["article_html"]) <= 6):
                phase7_errors.append("h2_count_invalid")
        except LLMError as exc:
            phase7_errors.append(f"phase7_fix_failed:{exc}")

    if phase7_errors:
        raise CreatorError(f"Final SEO checks failed: {phase7_errors}")

    debug["timings_ms"]["phase7"] = int((time.time() - phase_start) * 1000)

    images: List[Dict[str, str]] = []
    if featured_image_url:
        images.append({"type": "featured", "id_or_url": featured_image_url})
    if in_content_image_url:
        images.append({"type": "in_content", "id_or_url": in_content_image_url})

    return {
        "ok": True,
        "target_site_url": target_site_url,
        "host_site_url": publishing_site_url,
        "phase1": phase1,
        "phase2": phase2,
        "phase3": phase3,
        "phase4": phase4,
        "phase5": phase5,
        "phase6": phase6,
        "images": images,
        "warnings": warnings,
        "debug": debug,
    }
