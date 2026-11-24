"""`poetry run fpl-publish`: put the database where the hosted app can read it.

The file is uploaded to a GitHub Release asset on a fixed tag, so its download
URL never changes. The hosted dashboard reads that URL from `FPL_DB_URL`.
Uses the `gh` CLI, which is already signed in locally and in GitHub Actions.
"""

import argparse
import subprocess
import sys

from fpl_analytics.config import DB_PATH

RELEASE_TAG = "data-latest"
RELEASE_TITLE = "Latest data"
RELEASE_NOTES = (
    "The newest FPL database, replaced by every scheduled refresh. "
    "This is published data, not a software release."
)


def _gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(["gh", *args], capture_output=True, text=True)
    except FileNotFoundError:
        sys.exit("The GitHub CLI (gh) is not installed. See https://cli.github.com.")
    if check and result.returncode != 0:
        sys.exit(f"gh {' '.join(args)} failed: {result.stderr.strip() or result.stdout.strip()}")
    return result


def publish(tag: str = RELEASE_TAG) -> str:
    """Upload the database, creating the release on first use. Returns the URL."""
    if not DB_PATH.exists():
        sys.exit(f"{DB_PATH} does not exist. Run `poetry run fpl-pipeline` first.")

    if _gh("release", "view", tag, check=False).returncode != 0:
        print(f"Creating the {tag} release...")
        _gh("release", "create", tag, "--title", RELEASE_TITLE, "--notes", RELEASE_NOTES)

    print(f"Uploading {DB_PATH.name} ({DB_PATH.stat().st_size / 1e6:.1f} MB)...")
    _gh("release", "upload", tag, str(DB_PATH), "--clobber")

    repo = _gh("repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner").stdout.strip()
    url = f"https://github.com/{repo}/releases/download/{tag}/{DB_PATH.name}"
    print(f"Published. The hosted app reads it from:\n  {url}")
    return url


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish the database for the hosted dashboard.")
    parser.add_argument("--tag", default=RELEASE_TAG, help="Release tag to upload to (default %(default)s).")
    publish(parser.parse_args().tag)


if __name__ == "__main__":
    main()
