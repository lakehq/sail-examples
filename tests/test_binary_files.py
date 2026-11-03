"""Binary inputs are stored with Git LFS, and a checkout holds their content, not LFS pointers."""

import fnmatch
import os
import subprocess

import pytest
from catalog import ROOT

LFS_POINTER_PREFIX = b"version https://git-lfs.github.com/spec/v1"


def git(*args):
    return subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True, check=True).stdout


def lfs_patterns():
    patterns = []
    for line in (ROOT / ".gitattributes").read_text().splitlines():
        fields = line.split()
        if fields and not fields[0].startswith("#") and "filter=lfs" in fields[1:]:
            patterns.append(fields[0])
    return patterns


def lfs_tracked_files():
    patterns = lfs_patterns()
    for directory, subdirectories, files in os.walk(ROOT):
        subdirectories[:] = [
            name
            for name in subdirectories
            if not name.startswith(".")
            and name not in {"__pycache__", "spark-warehouse"}
            and not (directory == str(ROOT) and name == "internal")
        ]
        for name in files:
            if any(fnmatch.fnmatch(name, pattern) for pattern in patterns):
                yield os.path.join(directory, name)


def test_binary_files_are_stored_with_git_lfs():
    # A copy without `.git`, such as one made with `git archive`, has no index to inspect.
    try:
        toplevel = git("rev-parse", "--show-toplevel").decode().strip()
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a Git checkout")
    if os.path.realpath(toplevel) != os.path.realpath(ROOT):
        pytest.skip("not a Git checkout of this repository")
    # `i/-text` marks a file whose committed or staged content is binary, so it bypassed Git LFS.
    entries = [entry for entry in git("ls-files", "--eol", "-z").split(b"\0") if entry]
    stored_in_git = [entry.split(b"\t", 1)[1].decode() for entry in entries if entry.startswith(b"i/-text")]
    assert stored_in_git == [], (
        f"binary files stored directly in Git: {stored_in_git}. "
        "Add their extensions to .gitattributes with `git lfs track`, then add them again."
    )


def test_checkout_has_git_lfs_content():
    pointers = []
    for path in lfs_tracked_files():
        with open(path, "rb") as file:
            if file.read(len(LFS_POINTER_PREFIX)) == LFS_POINTER_PREFIX:
                pointers.append(os.path.relpath(path, ROOT))
    assert pointers == [], (
        f"Git LFS pointers instead of content in {len(pointers)} file(s), for example {sorted(pointers)[:3]}. "
        "Install Git LFS (https://git-lfs.com), run `git lfs install` once, then run `git lfs pull`."
    )
