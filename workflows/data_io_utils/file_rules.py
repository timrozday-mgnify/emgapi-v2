import logging
from pathlib import Path
from typing import Callable, Iterable, List

from pydantic import BaseModel, Field, model_validator


class _FileRule(BaseModel):
    rule_name: str = Field(..., description="Nice name of the rule to be applied")
    test: Callable[[Path], bool] = Field(
        ..., description="Function to call on file to check if rule passes"
    )


FileExistsRule = _FileRule(
    rule_name="File should exist",
    test=lambda f: f.is_file(),
)

FileIsNotEmptyRule = _FileRule(
    rule_name="File should not be empty",
    test=lambda f: f.stat().st_size > 0,
)


class File(BaseModel):
    """
    A data file from a pipeline output, say.
    """

    path: Path = Field(..., description="pathlib.Path pointer to the file")
    rules: List[_FileRule] = Field(
        default_factory=list, description="List of rules to be applied", repr=False
    )

    @model_validator(mode="after")
    def passes_all_rules(self):
        failures = []
        for rule in self.rules:
            try:
                passes = rule.test(self.path)
            except Exception as e:
                logging.error(
                    f"Unexpected failure applying rule <<{rule.__class__.__name__}: {rule.rule_name}>> to {self.path}. Treating as rule failure. {e}"
                )
                failures.append(rule)
            else:
                if not passes:
                    failures.append(rule)
        if failures:
            raise ValueError(
                f"Rules {[f.rule_name for f in failures]} failed for {self.path}"
            )
        return self


class _DirectoryRule(_FileRule): ...


DirectoryExistsRule = _DirectoryRule(
    rule_name="Directory should exist",
    test=lambda f: f.is_dir(),
)


class _GlobRule(_DirectoryRule):
    glob_patten: str = Field(
        default="*", description="Glob pattern to match within the dir"
    )
    test: Callable[[Iterable[Path]], bool] = Field(
        ...,
        description="Function to call on the result of glob pattern to check it passes",
    )


GlobHasFilesRule = _GlobRule(
    rule_name="Dir should have at least one file",
    glob_patten="*",
    test=lambda matches: len(list(matches)) > 0,
)


class Directory(File):
    files: List[File] = Field(
        default_factory=list,
        description="File objects to specifically check in the directory",
    )
    rules: List[_DirectoryRule] = Field(
        default_factory=list,
        description="List of rules to be applied to the directory path",
        repr=False,
    )
    glob_rules: List[_GlobRule] = Field(
        default_factory=list,
        description="List of glob rules to be applied to the dir",
        repr=False,
    )

    @model_validator(mode="after")
    def passes_all_glob_rules(self):
        failures = []
        for rule in self.glob_rules:
            try:
                passes = rule.test(self.path.glob(rule.glob_patten))
            except Exception as e:
                logging.error(
                    f"Unexpected failure applying rule <<{rule.__class__.__name__}: {rule.rule_name}>> to files of {self}. Treating as rule failure. {e}"
                )
                failures.append(rule)
            else:
                if not passes:
                    failures.append(rule)
        if failures:
            raise ValueError(
                f"Rules {[f.rule_name for f in failures]} failed for {self}"
            )
        return self
