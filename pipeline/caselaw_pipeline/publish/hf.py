from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import httpx
from huggingface_hub import HfApi

log = logging.getLogger(__name__)


def resolve_url(repo_id: str, path_in_repo: str, revision: str = "main") -> str:
    # Public resolve URL (no auth required for public repos)
    return f"https://huggingface.co/datasets/{repo_id}/resolve/{revision}/{path_in_repo}"


def download_file(url: str, out_path: Path, timeout_s: float = 120.0) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, follow_redirects=True, timeout=timeout_s) as r:
        r.raise_for_status()
        with out_path.open("wb") as f:
            for chunk in r.iter_bytes():
                if chunk:
                    f.write(chunk)


def download_text(url: str, timeout_s: float = 60.0) -> str:
    with httpx.Client(follow_redirects=True, timeout=timeout_s) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.text


def download_file_if_exists(url: str, out_path: Path, timeout_s: float = 120.0) -> bool:
    """Download a file, returning True on success or False if 404."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=timeout_s) as r:
            if r.status_code == 404:
                return False
            r.raise_for_status()
            with out_path.open("wb") as f:
                for chunk in r.iter_bytes():
                    if chunk:
                        f.write(chunk)
        return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return False
        raise


def upload_file(local_path: Path, repo_id: str, path_in_repo: str, token: str, commit_message: str) -> None:
    api = HfApi(token=token)
    api.upload_file(
        path_or_fileobj=str(local_path),
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type="dataset",
        commit_message=commit_message,
    )
