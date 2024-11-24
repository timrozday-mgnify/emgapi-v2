import pytest
from pydantic import ValidationError

from workflows.data_io_utils.file_rules import (
    Directory,
    DirectoryExistsRule,
    File,
    FileExistsRule,
    FileIsNotEmptyRule,
    GlobHasFilesRule,
    _FileRule,
    _GlobRule,
)


def test_file_rules_utils(tmp_path):
    # no rules applied, no error should be raised
    f = File(
        path=tmp_path / "non-exist.tsv",
    )
    assert f.path == tmp_path / "non-exist.tsv"

    # apply existence rule
    with pytest.raises(ValidationError):
        File(path=tmp_path / "non-exist.tsv", rules=[FileExistsRule])

    # empty file should be okay
    (tmp_path / "empty.tsv").touch()
    File(path=tmp_path / "empty.tsv", rules=[FileExistsRule])

    # apply size rule
    with pytest.raises(ValidationError):
        File(path=tmp_path / "empty.tsv", rules=[FileExistsRule, FileIsNotEmptyRule])

    # new custom rule to check content
    MyRule = _FileRule(
        rule_name="Text contains hello", test=lambda p: "hello" in p.read_text()
    )

    with pytest.raises(ValidationError) as exc_info:
        File(path=tmp_path / "empty.tsv", rules=[MyRule])
        assert "Text contains hello" in exc_info.value

    hello_file = tmp_path / "hello.tsv"
    hello_file.write_text("hello world")
    File(path=hello_file, rules=[MyRule])


def test_dir_rules_utils(tmp_path):
    # non exist directory with no rules is fine
    d = Directory(
        path=tmp_path / "non-exist",
    )
    assert d.path == tmp_path / "non-exist"

    with pytest.raises(ValidationError):
        d = Directory(path=tmp_path / "non-exist", rules=[DirectoryExistsRule])

    # empty dir fine if it exists
    empty = tmp_path / "empty"
    empty.mkdir(exist_ok=True, parents=True)

    Directory(path=empty, rules=[DirectoryExistsRule])

    # but should fail glob test if no files
    with pytest.raises(ValidationError):
        Directory(
            path=empty, rules=[DirectoryExistsRule], glob_rules=[GlobHasFilesRule]
        )

    # adding a file should fix it
    (empty / "something.txt").touch()

    Directory(path=empty, rules=[DirectoryExistsRule], glob_rules=[GlobHasFilesRule])

    # test custom glob test
    ShouldContainHelloRule = _GlobRule(
        rule_name="Directory contains a file matching hello",
        glob_patten="*hello*",
        test=lambda matches: len(list(matches)) > 0,
    )

    with pytest.raises(ValidationError) as exc_info:
        Directory(path=empty, glob_rules=[ShouldContainHelloRule])
        assert "Directory contains a file matching hello" in exc_info.value

    (empty / "hello.txt").touch()
    Directory(path=empty, glob_rules=[ShouldContainHelloRule])
