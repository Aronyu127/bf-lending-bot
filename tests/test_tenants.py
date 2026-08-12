"""Unit tests for the env-backed tenant registry (no network)."""

import os
import unittest
from unittest import mock

import tenants as tn


def _env(**overrides):
    """Env with every TENANT*/TENANTS key cleared, plus the given overrides."""
    base = {k: v for k, v in os.environ.items() if not k.startswith(("TENANTS", "TENANT_"))}
    base.update(overrides)
    return mock.patch.dict(os.environ, base, clear=True)


_TWO = {
    "TENANTS": "aron,bob",
    "TENANT_ARON_BF_API_KEY": "ak",
    "TENANT_ARON_BF_API_SECRET": "as",
    "TENANT_ARON_PASSWORD": "pw-aron",
    "TENANT_BOB_BF_API_KEY": "bk",
    "TENANT_BOB_BF_API_SECRET": "bs",
    "TENANT_BOB_PASSWORD": "pw-bob",
}


class TestResolveFundCurrency(unittest.TestCase):
    def test_defaults_when_blank(self):
        self.assertEqual(tn.resolve_fund_currency(None), "fUSD")
        self.assertEqual(tn.resolve_fund_currency("  "), "fUSD")

    def test_normalises_prefix_and_case(self):
        self.assertEqual(tn.resolve_fund_currency("ust"), "fUST")
        self.assertEqual(tn.resolve_fund_currency("fust"), "fUST")
        self.assertEqual(tn.resolve_fund_currency("fUSD"), "fUSD")


class TestLoadTenants(unittest.TestCase):
    def test_loads_both_with_defaults(self):
        with _env(**_TWO):
            ts = tn.load_tenants()
        self.assertEqual([t.name for t in ts], ["aron", "bob"])
        self.assertEqual(ts[0].api_key, "ak")
        self.assertEqual(ts[0].fund_currency, "fUSD")
        self.assertFalse(ts[0].dry_run)

    def test_per_tenant_currency_and_dry_run(self):
        with _env(**_TWO, TENANT_BOB_FUND_CURRENCY="ust", TENANT_BOB_DRY_RUN="true"):
            bob = tn.load_tenants()[1]
        self.assertEqual(bob.fund_currency, "fUST")
        self.assertTrue(bob.dry_run)

    def test_missing_tenants_var_raises(self):
        with _env(), self.assertRaises(tn.TenantConfigError):
            tn.load_tenants()

    def test_missing_credentials_raises_and_names_the_var(self):
        broken = dict(_TWO)
        del broken["TENANT_BOB_BF_API_SECRET"]
        with _env(**broken), self.assertRaises(tn.TenantConfigError) as cm:
            tn.load_tenants()
        self.assertIn("TENANT_BOB_BF_API_SECRET", str(cm.exception))

    def test_duplicate_password_raises(self):
        dup = dict(_TWO, TENANT_BOB_PASSWORD="pw-aron")
        with _env(**dup), self.assertRaises(tn.TenantConfigError):
            tn.load_tenants()

    def test_invalid_tenant_name_raises(self):
        with _env(TENANTS="a-b"), self.assertRaises(tn.TenantConfigError):
            tn.load_tenants()


class TestPasswordLookup(unittest.TestCase):
    def test_resolves_matching_tenant(self):
        with _env(**_TWO):
            self.assertEqual(tn.find_tenant_by_password("pw-bob").name, "bob")

    def test_rejects_wrong_and_empty_password(self):
        with _env(**_TWO):
            self.assertIsNone(tn.find_tenant_by_password("nope"))
            self.assertIsNone(tn.find_tenant_by_password(""))

    def test_get_tenant_by_name(self):
        with _env(**_TWO):
            self.assertEqual(tn.get_tenant("aron").api_secret, "as")
            self.assertIsNone(tn.get_tenant("carol"))


if __name__ == "__main__":
    unittest.main()
