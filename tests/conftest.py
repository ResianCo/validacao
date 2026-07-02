"""Pass 5c: pytest fixtures and path setup.

Adds the bin/ directory to sys.path so test files can do
`import pipeline_csv` / `import passo0_validacao` directly without
the scripts being a real Python package.
"""

import os
import sys

# Resolve the repo root and bin/ relative to this file.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
_BIN = os.path.join(_REPO_ROOT, "bin")

# Insert at front so the bin scripts take precedence over any
# same-named module on the path.
if _BIN not in sys.path:
    sys.path.insert(0, _BIN)