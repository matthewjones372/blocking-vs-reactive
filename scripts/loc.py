#!/usr/bin/env python3
"""Non-blank, non-comment lines between each `// region <name>` and `// endregion` in IoScenario.kt."""
import re
from pathlib import Path

src = (Path(__file__).resolve().parent.parent / "src/main/kotlin/bvr/IoScenario.kt").read_text()
for name, body in re.findall(r"// region (\S+)\n(.*?)// endregion", src, re.S):
    lines = [l for l in body.splitlines() if l.strip() and not l.strip().startswith("//")]
    print(f"{name}: {len(lines)}")
