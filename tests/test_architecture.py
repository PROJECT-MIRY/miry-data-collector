from __future__ import annotations

import ast
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src/miry"
FORBIDDEN_IMPORTS = {
    "contracts": ("miry.cli", "miry.collector", "miry.pipeline", "miry.universe"),
    "universe": ("miry.cli", "miry.collector", "miry.pipeline"),
    "collector": ("miry.cli", "miry.pipeline"),
    "pipeline": ("miry.cli", "miry.collector", "miry.universe"),
}


@pytest.mark.parametrize(("layer", "forbidden"), FORBIDDEN_IMPORTS.items())
def test_layer_has_no_reverse_dependencies(layer: str, forbidden: tuple[str, ...]) -> None:
    violations: list[str] = []
    for path in sorted((PACKAGE_ROOT / layer).rglob("*.py")):
        tree = ast.parse(path.read_bytes(), filename=str(path))
        for node in ast.walk(tree):
            for module in _imported_modules(node):
                if any(
                    module == prefix or module.startswith(f"{prefix}.")
                    for prefix in forbidden
                ):
                    relative = path.relative_to(PROJECT_ROOT)
                    violations.append(f"{relative}:{node.lineno}: {module}")
    assert violations == []


def _imported_modules(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.ImportFrom):
        return () if node.module is None else (node.module,)
    if isinstance(node, ast.Import):
        return tuple(alias.name for alias in node.names)
    return ()
