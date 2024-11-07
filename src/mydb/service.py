import dataclasses
import pathlib


@dataclasses.dataclass()
class DbService:
    file_path: pathlib.Path
    database_name: str
