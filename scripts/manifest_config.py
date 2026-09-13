"""Build secrets/config_manifest.json = secrets/config.json + the declarative manifest, so the
low-code version of the connector can be run locally with the CDK's generic runner:

    poetry run python scripts/manifest_config.py
    poetry run source-declarative-manifest check    --config secrets/config_manifest.json
    poetry run source-declarative-manifest discover --config secrets/config_manifest.json
    poetry run source-declarative-manifest read     --config secrets/config_manifest.json --catalog <catalog>
"""

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "connector-builder" / "manifest.yaml"


def main(config_path: str, out_path: str) -> None:
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    config["__injected_declarative_manifest"] = manifest
    Path(out_path).parent.mkdir(exist_ok=True)
    Path(out_path).write_text(json.dumps(config, indent=2), encoding="utf-8")
    print(f"wrote {out_path} ({len(manifest['streams'])} fixed streams, {len(manifest.get('dynamic_streams', []))} dynamic stream template(s))")


if __name__ == "__main__":
    main(
        sys.argv[1] if len(sys.argv) > 1 else str(ROOT / "secrets" / "config.json"),
        sys.argv[2] if len(sys.argv) > 2 else str(ROOT / "secrets" / "config_manifest.json"),
    )
