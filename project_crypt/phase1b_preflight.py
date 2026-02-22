from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def has_cmd(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def check_gh() -> Check:
    if not has_cmd("gh"):
        return Check("github_cli", False, "gh not installed")
    p = subprocess.run(["gh", "auth", "status"], capture_output=True, text=True)
    return Check("github_auth", p.returncode == 0, (p.stdout + p.stderr).strip()[:300])


def check_env(name: str) -> Check:
    val = os.getenv(name)
    return Check(name, bool(val), "SET" if val else "MISSING")


def main() -> None:
    checks = [
        check_gh(),
        check_env("COINGECKO_API_KEY"),
        check_env("REDDIT_CLIENT_ID"),
        check_env("REDDIT_CLIENT_SECRET"),
        check_env("REDDIT_USER_AGENT"),
        check_env("X_API_KEY"),
        check_env("X_API_SECRET"),
        check_env("X_BEARER_TOKEN"),
        check_env("CRYPTOCOM_API_KEY"),
        check_env("CRYPTOCOM_API_SECRET"),
    ]

    print("Phase-1B readiness preflight")
    for c in checks:
        tag = "OK" if c.ok else "FAIL"
        print(f"- {c.name}: {tag} ({c.detail})")


if __name__ == "__main__":
    main()
