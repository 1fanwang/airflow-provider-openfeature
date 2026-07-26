import math

from openfeature_airflow.analysis import _chi2_sf, _gammap_series, _gammaq, _gammaq_cf, lift, srm_check


class TestChiSquareSF:
    def test_known_critical_values(self):
        # chi-square critical values: P(X > crit) == alpha
        assert math.isclose(_chi2_sf(3.8415, 1), 0.05, abs_tol=1e-3)
        assert math.isclose(_chi2_sf(6.635, 1), 0.01, abs_tol=1e-3)
        assert math.isclose(_chi2_sf(10.828, 1), 0.001, abs_tol=1e-3)
        assert math.isclose(_chi2_sf(5.991, 2), 0.05, abs_tol=1e-3)
        assert math.isclose(_chi2_sf(11.345, 3), 0.01, abs_tol=1e-3)

    def test_edges(self):
        assert _chi2_sf(0.0, 1) == 1.0
        assert _chi2_sf(-1.0, 1) == 1.0
        assert _chi2_sf(1000.0, 1) < 1e-6

    def test_uses_series_for_small_x(self):
        assert math.isclose(_gammaq(5.0, 1.0), 0.9963401531726563)

    def test_series_can_exhaust_iteration_limit(self):
        assert _gammap_series(2.0, 1.0, iters=1, eps=0.0) > 0.0

    def test_continued_fraction_can_exhaust_iteration_limit(self):
        assert _gammaq_cf(2.0, 5.0, iters=1) > 0.0


class TestSRM:
    def test_balanced_split_passes(self):
        r = srm_check({"canary": 500, "control": 500})
        assert r.ok and r.p_value > 0.9 and r.chi_square < 1e-9

    def test_configured_ratio_passes(self):
        # 10/90 split observed against a configured 10/90
        r = srm_check({"canary": 100, "control": 900}, {"canary": 10, "control": 90})
        assert r.ok and r.p_value > 0.9

    def test_mismatch_flagged(self):
        # expected 50/50 but observed 600/400 over 1000 -> strong SRM
        r = srm_check({"canary": 600, "control": 400})
        assert not r.ok and r.p_value < 0.001

    def test_small_ratio_mismatch(self):
        # configured 25% but 40% landed in canary
        r = srm_check({"canary": 400, "control": 600}, {"canary": 25, "control": 75})
        assert not r.ok

    def test_expected_counts_reported(self):
        r = srm_check({"a": 30, "b": 70}, {"a": 50, "b": 50})
        assert r.expected == {"a": 50.0, "b": 50.0}

    def test_degenerate_inputs(self):
        assert srm_check({}).ok
        assert srm_check({"only": 10}).ok  # single group cannot mismatch
        assert srm_check({"a": 0, "b": 0}).ok


class TestLift:
    def test_relative_change(self):
        assert math.isclose(lift(100.0, 112.0), 0.12)
        assert math.isclose(lift(200.0, 100.0), -0.5)

    def test_zero_control(self):
        assert lift(0.0, 5.0) is None
