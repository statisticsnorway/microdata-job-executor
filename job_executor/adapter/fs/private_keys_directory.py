import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PrivateKeysDirectory:
    path_with_rdn: Path

    def create(self) -> bool:
        if not self.path_with_rdn.exists():
            os.makedirs(self.path_with_rdn)
            return True
        return False

    def save_private_key(self, microdata_private_key_pem: bytes) -> None:
        fd = os.open(
            self._get_private_key_location(),
            os.O_CREAT | os.O_WRONLY | os.O_EXCL,
            0o600,
        )
        with os.fdopen(fd, "wb") as file:
            file.write(microdata_private_key_pem)

    def clean_up(self) -> bool:
        if self._get_private_key_location().exists():
            os.remove(self._get_private_key_location())
            return True
        return False

    def _get_private_key_location(self) -> Path:
        return self.path_with_rdn / "microdata_private_key.pem"
