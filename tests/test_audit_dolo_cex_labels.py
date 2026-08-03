import importlib
import unittest
from unittest import mock


class AuditDoloCexLabelsTest(unittest.TestCase):
    def audit_module(self):
        return importlib.import_module("audit_dolo_cex_labels")

    def test_public_explorer_title_extracts_confirmed_binance_metadata(self):
        module = self.audit_module()
        html = "<html><head><title>Binance Deposit: 0x06fd... | Address | Etherscan</title></head></html>"

        metadata = module.extract_public_explorer_metadata(html)

        self.assertEqual("Binance Deposit", metadata["nametag"])
        self.assertTrue(module.is_cex_metadata(metadata))

    def test_regular_address_title_is_not_a_cex_suggestion(self):
        module = self.audit_module()
        html = "<html><head><title>Address: 0x1234 | Etherscan</title></head></html>"

        metadata = module.extract_public_explorer_metadata(html)

        self.assertFalse(module.is_cex_metadata(metadata))

    def test_public_page_audit_reports_source_without_promoting_heuristics(self):
        module = self.audit_module()
        response = mock.Mock(status_code=200, text="<title>Binance Deposit | Etherscan</title>")
        response.raise_for_status.return_value = None
        session = mock.Mock()
        session.get.return_value = response
        candidate = {
            "address": "0x06fd4ba7973a0d39a91734bbc35bc2bcaa99e3b0",
            "label": "",
            "labelType": "",
        }

        report = module.run_public_page_audit([candidate], delay=0, session=session)

        self.assertEqual(1, report["queriedCount"])
        self.assertEqual(1, len(report["confirmedCexSuggestions"]))
        suggestion = report["confirmedCexSuggestions"][0]
        self.assertEqual("Binance Deposit", suggestion["suggestedLabel"])
        self.assertEqual("etherscan-public-page", suggestion["source"])
        self.assertEqual("", candidate["labelType"])


if __name__ == "__main__":
    unittest.main()
