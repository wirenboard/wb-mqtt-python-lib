"""
Package rules that the code itself cannot enforce.
"""

import ast
from pathlib import Path

# From the repository, not from an import that could find another copy
PACKAGE_DIR = Path(__file__).resolve().parents[1] / "wb_mqtt_lib"


def test_library_imports_its_own_modules_only_relatively() -> None:
    """
    A service ships the library as a submodule inside its own package.

    An absolute import would then quietly take another copy of the library, for example a system-wide one.
    """
    absolute = []
    for path in sorted(PACKAGE_DIR.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            else:
                continue
            absolute += [
                f"{path.name}:{node.lineno} {name}"
                for name in names
                if name == PACKAGE_DIR.name or name.startswith(f"{PACKAGE_DIR.name}.")
            ]
    assert not absolute
