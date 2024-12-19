from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from workflows.data_io_utils.csv.csv_comment_handler import CSVDelimiter
from workflows.data_io_utils.file_rules.base_rules import GlobRule
from workflows.data_io_utils.file_rules.rule_factories import (
    generate_csv_schema_file_rule,
)


class TaxonomyTSVRow(BaseModel):
    otu_id: int = Field(..., alias="OTU ID")
    taxonomy: str
    taxid: Optional[int]

    model_config = ConfigDict(extra="allow")  # e.g. for SSU, PR2 etc columns


FileConformsToTaxonomyTSVSchemaRule = generate_csv_schema_file_rule(
    TaxonomyTSVRow, delimiter=CSVDelimiter.TAB
)

GlobOfTaxonomyFolderHasHtmlAndMseqRule = GlobRule(
    rule_name="Folder should contain html and mseq files",
    glob_patten="*",
    test=lambda files: sum(f.suffix in [".html", ".mseq"] for f in files) == 2,
)

GlobOfQcFolderHasFastpAndMultiqc = GlobRule(
    rule_name="Folder should contain fastp and multiqc files",
    glob_patten="*",
    test=lambda files: sum(
        f.name.endswith("multiqc_report.html") or f.name.endswith("fastp.json")
        for f in files
    )
    == 2,
)
