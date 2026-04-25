import json
from pathlib import Path


def write(report: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str))
    return output_path


def to_string(report: dict) -> str:
    return json.dumps(report, indent=2, default=str)
