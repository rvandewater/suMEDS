from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sumeds import SummaryConfig
from sumeds.cli import main


def test_yaml_and_cli_override(meds_dataset: Path, tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "summary.yaml"
    config_path.write_text(
        """summary:
  per_split: false
  split_columns: false
privacy:
  min_subjects: 3
  rare_code_action: drop
  round_counts_to: null
"""
    )
    config = SummaryConfig.from_yaml(config_path)
    assert config == SummaryConfig(min_subjects=3, rare_code_action="drop")

    output = tmp_path / "cli.parquet"
    assert (
        main(
            [
                str(meds_dataset),
                "-o",
                str(output),
                "--config",
                str(config_path),
                "--rare-code-action",
                "bucket",
                "--split-columns",
                "--min-split-subjects",
                "2",
            ]
        )
        == 0
    )
    assert f"Wrote {output}" in capsys.readouterr().out
    result = pl.read_parquet(output)
    assert result["code"].to_list() == ["A", "__RARE__"]
    assert "event_count_train" in result.columns
    assert result["event_count_held_out"].null_count() == result.height


def test_config_rejects_typos_and_invalid_values(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("privacy:\n  min_subject: 5\n")
    with pytest.raises(ValueError, match="Unknown configuration options"):
        SummaryConfig.from_yaml(path)
    path.write_text("summary:\n  min_subjects: 5\n")
    with pytest.raises(ValueError, match="wrong sections"):
        SummaryConfig.from_yaml(path)
    with pytest.raises(ValueError, match="positive integer"):
        SummaryConfig(min_subjects=0)
    with pytest.raises(ValueError, match="min_split_subjects"):
        SummaryConfig(min_split_subjects=0)
    with pytest.raises(ValueError, match="bucket.*drop"):
        SummaryConfig(rare_code_action="mask")
    with pytest.raises(ValueError, match="mutually exclusive"):
        SummaryConfig(per_split=True, split_columns=True)
