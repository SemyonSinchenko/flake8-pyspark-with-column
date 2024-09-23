from __future__ import annotations

import ast
from typing import Any, Generator


class Plugin:
    name = "flake8-pyspark-with-columm"
    version = "0.1.0"

    def __init__(self, tree: ast.AST) -> None:
        self._tree = tree
        self._visited_nodes = set()  # To avoid multiple messages for nested loops

    def _contains_with_column(self, node: ast.AST) -> bool:
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr == "withColumn":
                    return True
        return False

    def _contains_with_column_renamed(self, node: ast.AST) -> bool:
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr == "withColumnRenamed":
                    return True
        return False

    def _yield_if_not_present(
        self, coord: tuple[int, int, str]
    ) -> tuple[int, int, str, type] | None:
        if coord in self._visited_nodes:
            return None
        else:
            self._visited_nodes.add(coord)
            return (coord[0], coord[1], coord[2], type(self))

    def run(self) -> Generator[tuple[int, int, str, type], Any, None]:
        for node in ast.walk(self._tree):
            if isinstance(node, (ast.For, ast.While)):
                for child in ast.walk(node):
                    if self._contains_with_column(child):
                        message = self._yield_if_not_present(
                            (
                                child.lineno,
                                child.col_offset,
                                "PSPRK001 Usage of withColumn in a loop detected, use withColumns or select instead!",
                            )
                        )
                        if message:
                            yield message
                    if self._contains_with_column_renamed(child):
                        message = self._yield_if_not_present(
                            (
                                child.lineno,
                                child.col_offset,
                                "PSPRK003 Usage of withColumnRenamed in a loop detected, use withColumnsRenamed or select instead!",
                            )
                        )
                        if message:
                            yield message

            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id == "reduce":
                        for arg in node.args:
                            if isinstance(arg, ast.Lambda):
                                for child in ast.walk(arg.body):
                                    if self._contains_with_column(child):
                                        message = self._yield_if_not_present(
                                            (
                                                child.lineno,
                                                child.col_offset,
                                                "PSPRK002 Usage of withColumn in reduce detected, use withColumns or select instead!",
                                            )
                                        )
                                        if message:
                                            yield message
                                    if self._contains_with_column_renamed(child):
                                        message = self._yield_if_not_present(
                                            (
                                                child.lineno,
                                                child.col_offset,
                                                "PSPRK004 Usage of withColumnRenamed in reduce detected, use withColumnsRenamed or select instead!",
                                            )
                                        )
                                        if message:
                                            yield message
