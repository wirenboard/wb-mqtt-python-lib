"""
Package rules that the code itself cannot enforce.
"""

import ast
from pathlib import Path

# From the repository, not from an import that could find another copy
PACKAGE_DIR = Path(__file__).resolve().parents[1] / "wb_mqtt_lib"


def get_relative_imports(path: Path) -> list[tuple[int, Path]]:
    """
    Files of the package that a module imports relatively, with the line of each import.
    """
    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if not isinstance(node, ast.ImportFrom) or not node.level:
            continue
        # One dot is the directory of the module, each next dot is one level up
        base = path.parents[node.level - 1].joinpath(*(node.module.split(".") if node.module else []))
        for alias in node.names:
            # The name is a module itself or a name inside the module the import points to
            for candidate in (base / alias.name, base):
                files = [candidate.with_suffix(".py"), candidate / "__init__.py"]
                module = next((file for file in files if file.is_file()), None)
                if module is not None:
                    found.append((node.lineno, module))
                    break
    return found


def test_library_imports_its_own_modules_only_relatively() -> None:
    """
    A service ships the library as a submodule inside its own package.

    An absolute import would then quietly take another copy of the library, for example a system-wide one.
    """
    absolute = []
    for path in sorted(PACKAGE_DIR.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            else:
                continue
            absolute += [
                f"{path.relative_to(PACKAGE_DIR)}:{node.lineno} {name}"
                for name in names
                if name == PACKAGE_DIR.name or name.startswith(f"{PACKAGE_DIR.name}.")
            ]
    assert not absolute


def test_mqtt_layer_does_not_import_the_convention() -> None:
    """
    _mqtt is plain MQTT and knows nothing of the Wiren Board convention, _convention is built over it.
    The modules of the package root that _mqtt imports, and what they import in turn, count as its part.
    """
    pending = sorted((PACKAGE_DIR / "_mqtt").rglob("*.py"))
    checked: set[Path] = set()
    imports = []
    while pending:
        path = pending.pop()
        if path in checked:
            continue
        checked.add(path)
        for line, module in get_relative_imports(path):
            if "_convention" in module.relative_to(PACKAGE_DIR).parts:
                imports.append(f"{path.relative_to(PACKAGE_DIR)}:{line}")
            else:
                pending.append(module)
    assert not imports
