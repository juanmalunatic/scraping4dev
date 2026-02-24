import json
import hashlib
from pathlib import Path
from datetime import datetime
from dataclasses import asdict
from typing import List, Optional

from config import DATA_DIR
from models import EvaluationRow
from utils import collapse_ws, normalize_multival, RETRY_EVENTS

MULTI_KEYS = {
    "researchers",
    "sectors",
    "fieldwork",
    "target_group",
    "outcome_of_interest",
    "intervention_type",
    "research_papers",
    "partners",
}

def make_run_dir() -> tuple[str, Path]:
    run_id = datetime.now().strftime("%Y-%m-%d %H%M%S")
    run_dir = Path(DATA_DIR) / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_id, run_dir

def record_hash(row_dict: dict) -> str:
    d = dict(row_dict)
    d.pop("hash", None)
    d.pop("scraped_at", None)
    d.pop("run_id", None)

    for k, v in list(d.items()):
        if isinstance(v, str):
            v = collapse_ws(v)
            if k in MULTI_KEYS and v:
                v = normalize_multival(v)
            d[k] = v

    payload = json.dumps(d, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

def write_jsonl(run_dir: Path, rows: List[EvaluationRow], run_id: str) -> Path:
    out_path = run_dir / "evaluations.jsonl"
    scraped_at = datetime.now().isoformat(timespec="seconds")

    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            d = asdict(r)
            d["run_id"] = run_id
            d["scraped_at"] = scraped_at
            d["hash"] = record_hash(d)
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    return out_path

def find_previous_run_dir(current_run_id: str) -> Optional[Path]:
    base = Path(DATA_DIR)
    if not base.exists():
        return None
    runs = [p for p in base.iterdir() if p.is_dir()]
    if not runs:
        return None

    runs_sorted = sorted(runs, key=lambda p: p.name)
    prev = None
    for r in runs_sorted:
        if r.name < current_run_id:
            prev = r
    return prev

def load_hash_index(run_dir: Path) -> dict[str, str]:
    path = run_dir / "evaluations.jsonl"
    idx: dict[str, str] = {}
    if not path.exists():
        return idx

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            idx[obj["id"]] = obj.get("hash", "")
    return idx

def make_diff_report(current_run_id: str, run_dir: Path) -> dict:
    prev_dir = find_previous_run_dir(current_run_id)
    if prev_dir is None:
        return {
            "current_run_id": current_run_id,
            "previous_run_id": None,
            "added": 0,
            "removed": 0,
            "changed": 0,
            "unchanged": 0,
            "added_ids": [],
            "removed_ids": [],
            "changed_ids": [],
        }

    prev_idx = load_hash_index(prev_dir)
    cur_idx = load_hash_index(run_dir)

    prev_ids = set(prev_idx.keys())
    cur_ids = set(cur_idx.keys())

    added = sorted(cur_ids - prev_ids)
    removed = sorted(prev_ids - cur_ids)

    common = prev_ids & cur_ids
    changed = sorted([i for i in common if prev_idx.get(i) != cur_idx.get(i)])
    unchanged = len(common) - len(changed)

    return {
        "current_run_id": current_run_id,
        "previous_run_id": prev_dir.name,
        "added": len(added),
        "removed": len(removed),
        "changed": len(changed),
        "unchanged": unchanged,
        "added_ids": added,
        "removed_ids": removed,
        "changed_ids": changed,
    }

def write_json(run_dir: Path, filename: str, obj: dict):
    path = run_dir / filename
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_run_artifacts(
    rows: List[EvaluationRow],
    errors: List[dict],
    run_id: str,
    run_dir: Path,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    jsonl_path = write_jsonl(run_dir, rows, run_id)

    diff_report = make_diff_report(run_id, run_dir)
    write_json(run_dir, "diff_report.json", diff_report)

    run_report = {
        "run_id": run_id,
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "duration_seconds": int((finished_at - started_at).total_seconds()),
        "evaluations_scraped": len(rows),
        "retries_count": RETRY_EVENTS,
        "errors_count": len(errors),
        "errors": errors[:25],
        "outputs": {
            "jsonl": str(jsonl_path),
            "diff_report": str(run_dir / "diff_report.json"),
            "run_report": str(run_dir / "run_report.json"),
        },
    }
    write_json(run_dir, "run_report.json", run_report)