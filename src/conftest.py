from pathlib import Path

root_src_dir = "src"

pytest_plugins = [
    str(Path(fixture.parent, fixture.stem).relative_to(root_src_dir)).replace(
        "/", "."
    )
    for fixture in Path(root_src_dir).rglob("*_fixtures.py")
]
