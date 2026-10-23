#!/usr/bin/env python3
"""Non-blank, non-comment lines between each `// region <name>` and `// endregion` in the contestants' sources: what a
contestant's code costs to write, including the failure, stop and interrupt handling the semantics tests require."""
import re
from pathlib import Path

src = Path(__file__).resolve().parent.parent / "src/main/kotlin/bvr"
for f in ("Contestants.kt", "Overload.kt"):
    for name, body in re.findall(r"// region (\S+)\n(.*?)// endregion", (src / f).read_text(), re.S):
        lines = [l for l in body.splitlines() if l.strip() and not l.strip().startswith(("//", "*", "/**"))]
        print(f"{name}: {len(lines)}")
