from __future__ import annotations

import ast
from typing import Any, Generator


class Plugin:
    name = "flake8-pyspark-with-columm"
    version = "0.1.0"

    def __init__(self, tree: ast.AST) -> None:
        self._tree = tree

    def _contains_with_column(self, node: ast.AST) -> bool:
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr == "withColumn":
                    return True
        return False

    def run(self) -> Generator[tuple[int, int, str, type], Any, None]:
        for node in ast.walk(self._tree):
            if isinstance(node, (ast.For, ast.While)):
                for child in ast.walk(node):
                    if self._contains_with_column(child):
                        yield (
                            node.lineno,
                            node.col_offset,
                            "PSPRK001 Usage of withColumn in a loop detected, use withColumns or select instead!",
                            type(self),
                        )
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id == "reduce":
                        for arg in node.args:
                            if isinstance(arg, ast.Lambda):
                                for child in ast.walk(arg.body):
                                    if self._contains_with_column(child):
                                        yield (
                                            arg.lineno,
                                            arg.col_offset,
                                            "PSPRK002 Usage of withColumn in reduce detected, use withColumns or select instead!",
                                            type(self),
                                        )
