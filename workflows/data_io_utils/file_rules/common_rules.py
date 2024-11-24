from workflows.data_io_utils.file_rules.base_rules import (
    DirectoryRule,
    FileRule,
    GlobRule,
)

FileExistsRule = FileRule(
    rule_name="File should exist",
    test=lambda f: f.is_file(),
)
FileIsNotEmptyRule = FileRule(
    rule_name="File should not be empty",
    test=lambda f: f.stat().st_size > 0,
)
DirectoryExistsRule = DirectoryRule(
    rule_name="Directory should exist",
    test=lambda f: f.is_dir(),
)
GlobHasFilesRule = GlobRule(
    rule_name="Dir should have at least one file",
    glob_patten="*",
    test=lambda matches: len(list(matches)) > 0,
)
