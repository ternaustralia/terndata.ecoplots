from __future__ import annotations

import sys

from packaging.version import InvalidVersion, Version


def main() -> None:
    base = sys.argv[1]
    run_number = sys.argv[2]

    try:
        version = Version(base)
    except InvalidVersion:
        version = Version("0.0.0")

    release = list(version.release)
    while len(release) < 3:
        release.append(0)
    release[2] += 1

    print(f"{release[0]}.{release[1]}.{release[2]}.dev{run_number}")


if __name__ == "__main__":
    main()
