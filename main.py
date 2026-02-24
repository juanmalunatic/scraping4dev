import time
import csv
from dataclasses import dataclass, asdict
from typing import List
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

BASE = "https://www.povertyactionlab.org"
LIST_URL = "https://www.povertyactionlab.org/evaluations?page={page}"  # si no funciona, cambiamos a "Load more"
START_PAGE = 0
END_PAGE = 2
#END_PAGE = 129  # inclusive
MAX_RETRIES = 4

@dataclass
class EvaluationRow:
    id: str
    title: str
    researchers: str
    sectors: str
    fieldwork: str
    location: str
    sample: str
    timeline_start: str
    timeline_end: str
    target_group: str
    outcome_of_interest: str
    intervention_type: str
    research_papers: str
    partners: str
    abstract: str
    fulltext: str


def run_function_with_retries(callable_fn, *, tries=MAX_RETRIES, base_sleep=1.0, label="op"):
    last_err = None
    for attempt in range(1, tries + 1):
        try:
            return callable_fn()
        except (PWTimeoutError, Exception) as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1))
            print(f"[retry] {label} failed (attempt {attempt}/{tries}): {type(e).__name__} -> sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise last_err

def safe_text(locator) -> str:
    try:
        t = locator.first.inner_text(timeout=2000)
        return " ".join(t.split())
    except Exception:
        return ""

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

    abstract = safe_text(page.locator(".evaluation-full-abstract").first)
    fulltext = safe_text(page.locator(".node--type-evaluation").first)

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
        abstract=abstract,
        fulltext=fulltext
    )


# noinspection SpellCheckingInspection
def main():
    # typehinteamos que hay una lista de EvaluationRows
    rows: List[EvaluationRow] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()

        list_page = context.new_page()

        # loop principal
        for page_idx in range(START_PAGE, END_PAGE + 1):

            url = LIST_URL.format(page=page_idx)

            # ---------------------------------------
            # Carga de la página número N
            # ---------------------------------------
            def load_page_n():
                # carga
                list_page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # esperar los links
                list_page.locator('h3 a[href^="/evaluation/"]').first.wait_for(timeout=20000)

            run_function_with_retries(
                load_page_n,
                label=f"load list page {page_idx}"
            )

            # ---------------------------------------
            # Carga de los links internos de la página N
            # ---------------------------------------
            def get_eval_links_from_page_n(page) -> list[str]:
                hrefs = page.locator('h3 a[href^="/evaluation/"]').evaluate_all(
                    "els => [...new Set(els.map(e => e.getAttribute('href')).filter(Boolean))]"
                )
                return [BASE + h for h in hrefs]

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
                        tab.goto(link, wait_until="domcontentloaded", timeout=30000)
                        tab.wait_for_load_state("networkidle", timeout=20000)
                        row_one = extract_detail(tab)
                        return row_one
                    finally:
                        tab.close()

                row = run_function_with_retries(
                    scrape_one,
                    label=f"scrape detail p{page_idx} #{i}"
                )
                rows.append(row)

                print(f"  -> scraped: {row.title[:60]}")

        browser.close()

    # guardar CSV
    out_file = "evaluations.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()) if rows else ["url","title","researchers","location","timeline","sample"])
        writer.writeheader()
        for r in rows:
            writer.writerow(asdict(r))
        print(f"Done. Rows: {len(rows)} -> {out_file}")


if __name__ == "__main__":
    main()