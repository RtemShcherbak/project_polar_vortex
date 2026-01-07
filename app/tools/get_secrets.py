import os
import yaml
from pathlib import Path

_secrets = None

def load_secrets():
    global _secrets
    if _secrets is not None:
        return _secrets

    # 1) ENV
    secrets = dict(os.environ)

    # 2) secrets.yaml
    path = Path("/Users/artem/Documents/Zerich/polar_vortex/project_polar_vortex/app/secrets.yaml")
    if path.exists():
        with open(path) as f:
            data = yaml.safe_load(f)
            secrets.update(data)

    _secrets = secrets
    return secrets


def get_secret(name: str) -> str:
    secrets = load_secrets()
    if name not in secrets:
        raise RuntimeError(f"Secret {name} not found")
    return secrets[name]

