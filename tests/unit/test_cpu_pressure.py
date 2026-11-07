from pathlib import Path

import pytest

from docker_test_toolkit.application.lifecycle import host_cpu_pressure
from docker_test_toolkit.domain.policy import (
    CPU_PRESSURE_JOIN_LIMIT,
    cpu_pressure_blocks_join,
    parse_cpu_pressure,
    parse_df,
    split_probe_output,
)

_PSI = "some avg10={avg10} avg60={avg60} avg300=12.00 total=123456\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"


def test_a_joiner_waits_while_the_minute_average_cpu_pressure_is_at_the_limit() -> None:
    # CI host: with only one toolkit run holding, the CI jobs kept the 4 cores 77 % busy and tasks waited
    # for a core 34 % of the time; a second stack then stalled Neo4j past its 30 s transaction timeout.
    # 40 % kept the CI host one run at a time (minute averages 31-38 % with one run); 50 % lets a second run join
    # until the host is clearly saturated.
    assert CPU_PRESSURE_JOIN_LIMIT == 50.0
    assert cpu_pressure_blocks_join(parse_cpu_pressure(_PSI.format(avg10="80.00", avg60="50.00")))
    assert not cpu_pressure_blocks_join(parse_cpu_pressure(_PSI.format(avg10="80.00", avg60="49.99")))


def test_a_host_without_cpu_pressure_figures_never_blocks_a_join() -> None:
    # macOS and kernels without PSI publish no figures; memory admission alone decides there.
    assert not cpu_pressure_blocks_join(None)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        _PSI.format(avg10="1.00", avg60="lots"),
        _PSI.format(avg10="1.00", avg60="-1.00"),
        _PSI.format(avg10="1.00", avg60="100.01"),
        "some avg10=1.00 avg300=1.00 total=1\n",
    ],
)
def test_an_unrecognized_pressure_file_reads_as_no_figures(text: str) -> None:
    assert parse_cpu_pressure(text) is None


def test_the_minute_average_is_read() -> None:
    assert parse_cpu_pressure(_PSI.format(avg10="5.50", avg60="33.96")) == 33.96


def test_a_present_but_unreadable_pressure_file_holds_joins_back(tmp_path: Path) -> None:
    # Absent (macOS, no PSI): memory decides alone. Present but broken on Linux: full pressure, so joins wait; a run
    # alone is never held back by CPU pressure.
    psi = tmp_path / "cpu"
    assert host_cpu_pressure(psi) is None
    psi.write_text("garbage\n")
    assert host_cpu_pressure(psi) == 100.0
    psi.write_text(_PSI.format(avg10="1.00", avg60="12.50"))
    assert host_cpu_pressure(psi) == 12.5
    assert host_cpu_pressure(tmp_path) == 100.0  # a directory where the file belongs: unreadable


_DF = "Filesystem     1024-blocks     Used Available Capacity Mounted on\n{row}\n"


def test_parse_df_reads_total_and_available_in_bytes() -> None:
    # Measured: the probe's df of the bind-mounted Docker data root inside Docker Desktop's VM.
    text = _DF.format(row="/dev/vda1         474095688 21114988 428824492       5% /dtt-data")

    assert parse_df(text) == (474095688 * 1024, 428824492 * 1024)


def test_parse_df_accepts_a_wrapped_device_row() -> None:
    # POSIX df puts a long device name on its own line and the figures on the next.
    text = _DF.format(row="/dev/mapper/a-very-long-volume-group-name\n  474095688 21114988 428824492 5% /dtt-data")

    assert parse_df(text) == (474095688 * 1024, 428824492 * 1024)


@pytest.mark.parametrize(
    "text",
    ["", "nonsense", _DF.format(row="/dev/vda1 -1 0 0 0% /dtt-data"), _DF.format(row="/dev/vda1 10 5 11 50% /x")],
)
def test_parse_df_rejects_anything_but_one_sane_row(text: str) -> None:
    # A negative size or more available than total is not a figure a decision may use.
    assert parse_df(text) is None


def test_split_probe_output_keeps_the_two_figures_independent() -> None:
    df = _DF.format(row="/dev/vda1 100 40 60 40% /dtt-data")
    psi = _PSI.format(avg10="1.00", avg60="1.56")

    garbage_psi, good_df = split_probe_output("cat: /proc/pressure/cpu: No such file\n::dtt::\n" + df)
    good_psi, garbage_df = split_probe_output(psi + "::dtt::\ndf: /dtt-data: not found\n")

    assert parse_cpu_pressure(garbage_psi) is None and parse_df(good_df) == (100 * 1024, 60 * 1024)
    assert parse_cpu_pressure(good_psi) == 1.56 and parse_df(garbage_df) is None
    assert split_probe_output("no sentinel at all") == ("", "")
    # A kernel without PSI: `cat` writes only to stderr, so the probe's stdout starts with the sentinel.
    assert parse_df(split_probe_output("::dtt::\n" + df)[1]) == (100 * 1024, 60 * 1024)
