from __future__ import annotations
from dataclasses import dataclass, field
from typing import Union, Optional

# A binary node is manipulated as a plain dict: {'tag': str, 'attrs': dict[str,str], 'content': ...}
# content: list[BinaryNode] | str | bytes | None
BinaryNode = dict
BinaryNodeAttributes = dict
