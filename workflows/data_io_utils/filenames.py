from __future__ import annotations

from pathlib import Path

from django.utils.text import slugify as djangoslugify


def file_path_shortener(
    filepath: str | Path,
    shorten_dirs_to: int = 1,
    shorten_name_to: int = 10,
    slugify: bool = False,
) -> str:
    """
    Take a potentially long filepath, and make a somewhat meaningful short version of it for printing.
    Useful for e.g. naming cluster jobs or prefect flows.

    :param filepath: Full or relative path to file, e.g. /nfs/public/my_file.csv
    :param shorten_dirs_to: Limit directory names to this number of characters.
    :param shorten_name_to: Limit file names to this number of characters.
    :param slugify: If True, slugify the file name as well, suitable for URLs etc.

    Examples:
        shorten_dirs_to=1
        shorten_name_to=10
        /nfs/production/long/path/to/nested/directory/my_big_file_name_is_long.csv
            -> /n/p/l/p/t/n/d/m..ong.csv

        shorten_dirs_to=3
        shorten_name_to=20
        /nfs/production/long/path/to/nested/directory/my_big_file_name_is_long.csv
            -> /nfs/pro/lon/pat/to/nes/dir/my_big_file_name_is_long.csv
    """
    entire_path = Path(filepath)
    short_name = entire_path.name

    if len(short_name) > shorten_name_to:
        short_name = short_name[0] + ".." + short_name[-(shorten_name_to - 3) :]

    short_path = ""
    for dir in entire_path.parts[:-1]:
        short_path += dir[:shorten_dirs_to] + ("/" if dir != "/" else "")
    short_path += short_name

    if slugify:
        return djangoslugify(short_path.replace("/", "_").replace(".", "_"))

    return short_path
