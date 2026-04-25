from pathlib import Path

from jinja2 import Environment, FileSystemLoader


def write(report: dict, output_path: Path, templates_dir: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = render(report, templates_dir)
    output_path.write_text(html)
    return output_path


def render(report: dict, templates_dir: Path) -> str:
    env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=True)
    template = env.get_template("member_report.html")
    return template.render(report=report)
