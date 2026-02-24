from datetime import datetime

from scraper import run_scrape
from pipeline import make_run_dir, write_run_artifacts


def main():
    started_at = datetime.now()
    run_id, run_dir = make_run_dir()

    rows, errors = run_scrape()

    finished_at = datetime.now()
    write_run_artifacts(
        rows=rows,
        errors=errors,
        run_id=run_id,
        run_dir=run_dir,
        started_at=started_at,
        finished_at=finished_at,
    )

    print(f"[run] saved to: {run_dir}")


if __name__ == "__main__":
    main()