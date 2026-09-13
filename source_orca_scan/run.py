import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceOrcaScan


def run() -> None:
    # Airbyte messages are UTF-8 JSON. Airbyte's containers already are, but a Windows
    # console defaults to a legacy code page and would crash on the first non-ASCII row.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    source = SourceOrcaScan()
    launch(source, sys.argv[1:])
