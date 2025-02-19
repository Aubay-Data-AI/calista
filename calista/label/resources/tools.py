import pathlib
import calista


def get_file_path(file_name: str) -> str:
    calista_p = pathlib.Path(calista.__file__)
    return f"{str(calista_p.parent)}/label/resources/{file_name}"