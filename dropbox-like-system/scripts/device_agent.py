from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.device_agent import main


if __name__ == "__main__":
    main()

