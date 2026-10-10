"""Package dbt docs artifacts for a static site (e.g. GitHub Pages)."""

from __future__ import annotations

import shutil
from pathlib import Path

# Files the dbt docs SPA needs at runtime. graph.html is optional (older dbt).
REQUIRED_DOC_FILES = ("index.html", "catalog.json", "manifest.json")
OPTIONAL_DOC_FILES = ("graph.html",)


def package_docs_site(target_dir: Path, out_dir: Path) -> list[Path]:
    """Copy dbt docs static assets from ``target_dir`` into ``out_dir``.

    Returns the list of files copied. Raises ``FileNotFoundError`` if any
    required artifact is missing (usually means ``dbt docs generate`` was not run).
    """
    target_dir = Path(target_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    missing = [name for name in REQUIRED_DOC_FILES if not (target_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f"dbt docs artifacts missing in {target_dir}: {', '.join(missing)}. "
            "Run `dbt docs generate` first."
        )

    copied: list[Path] = []
    for name in REQUIRED_DOC_FILES + OPTIONAL_DOC_FILES:
        src = target_dir / name
        if not src.is_file():
            continue
        dest = out_dir / name
        shutil.copy2(src, dest)
        copied.append(dest)
    return copied
