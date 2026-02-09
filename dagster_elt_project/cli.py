"""CLI for running dagster-elt-project dev server."""

import sys
import os
import subprocess
from pathlib import Path


def dev():
    """Run the Dagster dev server."""
    # Parse port from command line
    port = "3000"
    for i, arg in enumerate(sys.argv[1:]):
        if arg in ["-p", "--port"] and i + 1 < len(sys.argv) - 1:
            port = sys.argv[i + 2]
            break
        elif arg.startswith("--port="):
            port = arg.split("=")[1]
            break
        elif arg.isdigit():
            port = arg
            break

    # Get the project root directory
    project_root = Path(__file__).parent.parent

    print(f"\n🚀 Starting Dagster ELT Project")
    print(f"   Port: {port}")
    print(f"   UI: http://127.0.0.1:{port}")
    print(f"   Project: {project_root}")
    print("\nPress Ctrl+C to stop\n")

    # Run dagster dev from the project root
    os.chdir(project_root)
    os.execvp("dagster", [
        "dagster",
        "dev",
        "-m",
        "dagster_elt_project.definitions",
        "-p",
        port
    ])


if __name__ == "__main__":
    dev()
