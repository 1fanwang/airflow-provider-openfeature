import pytest

from openfeature_airflow.switchback import switchback_context, time_bucket, window_seconds


class TestWindowSeconds:
    def test_units(self):
        assert window_seconds("30s") == 30
        assert window_seconds("5m") == 300
        assert window_seconds("1h") == 3600
        assert window_seconds("2d") == 172800

    def test_bad_window(self):
        for bad in ("1w", "h", "abc", "1.5h", ""):
            with pytest.raises(ValueError):
                window_seconds(bad)


class TestTimeBucket:
    def test_stable_within_window_changes_across(self):
        base = 3_600_000.0  # aligned to a 1h boundary (1000 * 3600)
        assert time_bucket("1h", base) == time_bucket("1h", base + 3599)
        assert time_bucket("1h", base) != time_bucket("1h", base + 3600)

    def test_bucket_encodes_window(self):
        assert time_bucket("1h", 1_000_000.0).startswith("1h#")

    def test_monotonic_across_windows(self):
        base = 1_000_000.0
        b0 = time_bucket("30m", base)
        b1 = time_bucket("30m", base + 1800)
        assert b0 != b1


class TestSwitchbackContext:
    def test_cluster_wide_key_is_the_bucket(self):
        ctx = switchback_context("1h", now=1_000_000.0)
        assert ctx.targeting_key == time_bucket("1h", 1_000_000.0)
        assert ctx.attributes["time_bucket"] == ctx.targeting_key

    def test_per_entity_scoping(self):
        ctx = switchback_context("1h", now=1_000_000.0, entity="etl_alpha")
        assert ctx.targeting_key == f"etl_alpha:{time_bucket('1h', 1_000_000.0)}"

    def test_extra_attrs_attached(self):
        ctx = switchback_context("1h", now=1_000_000.0, team="data")
        assert ctx.attributes["team"] == "data"
