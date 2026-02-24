from typing import List, Tuple
from playwright.sync_api import sync_playwright

from config import (
    BASE, LIST_URL, START_PAGE, END_PAGE,
    NAV_TIMEOUT, WAIT_TIMEOUT, NETWORKIDLE_TIMEOUT,
    HEADLESS,
)
from models import EvaluationRow
from utils import run_function_with_retries, safe_text


# Carga de los links internos de la página N
def get_eval_links_from_page_n(page) -> list[str]:
    hrefs = page.locator('h3 a[href^="/evaluation/"]').evaluate_all(
        "els => [...new Set(els.map(e => e.getAttribute('href')).filter(Boolean))]"
    )
    return [BASE + h for h in hrefs]

# Extrae datos de una evaluación (es una "row")
def extract_detail(page) -> EvaluationRow:
    slug = page.url.rstrip("/").split("/")[-1]
    title = safe_text(page.locator("h1"))

    # helper para campos multivalor (extrae todos los <a>)
    def multival_text(label: str) -> str:
        try:
            block = page.get_by_text(label, exact=True).locator("xpath=..")
            links = block.locator("a").all_inner_texts()
            links = [t.strip() for t in links if t.strip()]
            if links:
                return "; ".join(links)
            # fallback si no hay links
            raw = block.inner_text(timeout=2000)
            raw = " ".join(raw.split())
            return raw.replace(label, "").strip(" :")
        except Exception:
            return ""

    def list_to_colons(myarr: list) -> str:
        return "; ".join([s.strip() for s in myarr if s.strip()])

    def multival_selector(selector: str) -> str:
        elements = page.locator(selector).all_inner_texts()
        return list_to_colons(elements)

    # valores de la página
    sectors     = multival_selector(".evaluation-full-sectors a")
    researchers = multival_selector(".evaluation-full-researchers .summary-content")
    fieldwork   = multival_selector(".evaluation-full-fieldwork a")

    location = multival_text("Location:")
    sample   = multival_text("Sample:")

    # separar timeline
    timeline_raw = multival_text("Timeline:")
    timeline_start, timeline_end = "", ""
    if " - " in timeline_raw:
        parts = timeline_raw.split(" - ")
        if len(parts) == 2:
            timeline_start = parts[0].strip()
            timeline_end = parts[1].strip()

    target_group        = multival_selector(".evaluation-full-target-group li")
    outcome_of_interest = multival_selector(".evaluation-full-outcome-interest li")
    intervention_type   = multival_selector(".evaluation-full-intervention-type li")
    research_papers     = multival_selector(".evaluation-full-research-papers a")
    partners            = multival_selector(".evaluation-full-partners a")

    txt_abstract         = safe_text(page.locator(".evaluation-full-abstract").first)
    txt_policy_issue     = safe_text(page.locator(".evaluation-full-policy-issue .text-full-body").first)
    txt_context_of_eval  = safe_text(page.locator(".evaluation-full-evaluation-context .text-full-body").first)
    txt_details_inter    = safe_text(page.locator(".evaluation-full-intervention-details .text-full-body").first)
    txt_results_lessons  = safe_text(page.locator(".evaluation-full-results-policy-lessons .text-full-body").first)
    txt_citations        = safe_text(page.locator(".evaluation-full-citations").first)

    return EvaluationRow(
        id=slug,
        title=title,
        fieldwork=fieldwork,
        researchers=researchers,
        sectors=sectors,
        location=location,
        sample=sample,
        timeline_start=timeline_start,
        timeline_end=timeline_end,
        target_group=target_group,
        outcome_of_interest=outcome_of_interest,
        intervention_type=intervention_type,
        research_papers=research_papers,
        partners=partners,
        abstract=txt_abstract,
        policy_issue=txt_policy_issue,
        context_of_eval=txt_context_of_eval,
        details_inter=txt_details_inter,
        results_lessons=txt_results_lessons,
        citations=txt_citations
    )


def run_scrape() -> Tuple[List[EvaluationRow], List[dict]]:
    """
    Orquesta Playwright:
      - itera pages START_PAGE..END_PAGE
      - recoge links de cada list page
      - abre cada evaluación en un tab
      - extrae EvaluationRow
    Retorna:
      rows: lista de EvaluationRow
      errors: lista de dicts con errores por URL/item
    """
    rows: List[EvaluationRow] = []
    errors: List[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        list_page = context.new_page()

        for page_idx in range(START_PAGE, END_PAGE + 1):
            url = LIST_URL.format(page=page_idx)

            # ---------------------------------------
            # Carga de la página número N
            # ---------------------------------------
            def load_page_n():
                list_page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                list_page.locator('h3 a[href^="/evaluation/"]').first.wait_for(timeout=WAIT_TIMEOUT)

            run_function_with_retries(load_page_n, label=f"load list page {page_idx}")

            eval_links = run_function_with_retries(
                lambda: get_eval_links_from_page_n(list_page),
                label=f"collect links page {page_idx}"
            )

            # Si por alguna razón no salen 10 exactos (como en la última pag.) igual se sigue
            print(f"[page {page_idx}] links found: {len(eval_links)}")

            # ---------------------------------------
            # Iteración sobre cada uno de los links internos
            # ---------------------------------------
            for i, link in enumerate(eval_links, start=1):

                def scrape_one():
                    tab = context.new_page()
                    try:
                        tab.goto(link, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                        tab.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
                        return extract_detail(tab)
                    finally:
                        tab.close()

                try:
                    row = run_function_with_retries(
                        scrape_one,
                        label=f"scrape detail p{page_idx} #{i}"
                    )
                    rows.append(row)
                    print(f"  -> scraped: {row.title[:60]}")
                
                # capturar errores y seguir loop
                except Exception as e:
                    errors.append({
                        "page_idx": page_idx,
                        "i": i,
                        "url": link,
                        "error_type": type(e).__name__,
                        "error": str(e),
                    })
                    print(f"[error] failed detail p{page_idx} #{i}: {type(e).__name__}")
                    continue

        browser.close()

    return rows, errors