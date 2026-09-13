import importlib
import unittest
import tempfile
from pathlib import Path
from unittest import mock


class AuditDoloCexLabelsTest(unittest.TestCase):
    def test_direct_gate_deposit_owner_is_recognized_but_generic_gateway_is_not(self):
        module = self.audit_module()
        self.assertTrue(module.is_cex_metadata({'nametag':'Gate Deposit'}))
        self.assertFalse(module.is_cex_metadata({'nametag':'Bridge Gateway'}))

    def test_discovery_includes_zero_balance_historical_cex_and_deposit_funnels(self):
        module = self.audit_module()
        old = '0x' + '1'*40
        funnel = '0x' + '2'*40
        historical = '0x' + '3'*40
        payload = {'cex_watch':{'depositCandidates':[{'address':funnel,'sentToCexDolo':10000000,'txCount':20}]},
                   'cex_supply_history':[{'walletBalances':[{'address':historical,'balance':500000}]}]}
        with mock.patch.object(module,'load_json',side_effect=[{'holders':[]},payload]):
            rows = module.collect_candidates({old:{'type':'cex','evidenceStatus':'review_needed'}},10000,10000,0,False)
        self.assertEqual({r['address'] for r in rows}, {old,funnel,historical})
        self.assertEqual(next(r for r in rows if r['address']==funnel)['maxGrossFlow'],10000000)

    MEXC = "0xf61a30978ecb7cccb30eb97f9ba94b8b35675034"

    def profile_html(self, address, badge="MEXC"):
        return (f'<div class="HeaderInfo_headerInfoWrap__test"><div><span>{address}</span>'
                f'<img src="avatar"><div class="db-user-tag is-cex" title="{badge}"></div></div></div>')

    def test_mexc_is_in_the_canonical_registry_with_direct_evidence(self):
        info = self.audit_module().load_labels()[self.MEXC]
        self.assertEqual((info["label"], info["type"], info["confidence"]), ("MEXC", "cex", "confirmed"))
        self.assertEqual(info["evidenceStatus"], "public_label")

    def test_direct_badge_must_belong_to_the_requested_profile(self):
        module = self.audit_module()
        document = self.profile_html(self.MEXC)
        self.assertEqual(module.extract_debank_cex_metadata(document, self.MEXC)["profileAddress"], self.MEXC)
        other = "0x1111111111111111111111111111111111111111"
        self.assertEqual(module.extract_debank_cex_metadata(document, other), {})
        outside_badge = f'<div class="HeaderInfo_headerInfoWrap__test">{self.MEXC}</div><div class="db-user-tag is-cex" title="MEXC"></div>'
        self.assertEqual(module.extract_debank_cex_metadata(outside_badge, self.MEXC), {})

    def test_unloaded_debank_profile_is_an_error_not_no_cex_badge(self):
        module = self.audit_module()
        proc = mock.Mock(returncode=0, stdout='<div id="root"></div>')
        with mock.patch.object(module.subprocess, "run", return_value=proc):
            metadata, error = module.fetch_debank_cex_metadata(self.MEXC, "chrome")
        self.assertIsNone(metadata)
        self.assertEqual(error, "debank_profile_not_loaded")

    def test_loaded_unlabelled_profile_is_a_valid_negative_check(self):
        module = self.audit_module()
        proc = mock.Mock(returncode=0, stdout=f'<div class="HeaderInfo_headerInfoWrap__test">{self.MEXC}</div>')
        with mock.patch.object(module.subprocess, "run", return_value=proc):
            self.assertEqual(module.fetch_debank_cex_metadata(self.MEXC, "chrome"), (None, None))

    def test_rotation_reaches_addresses_outside_top_120_and_prioritizes_recent_activity(self):
        module = self.audit_module()
        holders = [{"address": f"0x{i:040x}", "balance": 1000000-i} for i in range(1,151)]
        with mock.patch.object(module, "load_json", side_effect=[{"holders": holders}, {"periods": {}}]):
            pool = module.collect_candidates({}, 10000, 10000, 0, False)
        state = {"debank": {"addresses": {r["address"]:{"lastAttemptAt":"2026-09-01"} for r in pool[:120]}}}
        pool[-1]["periods"] = ["1d"]
        chosen, report = module.select_debank_rotation_candidates(pool, state, set(), 20)
        self.assertEqual(len(pool), 150)
        self.assertEqual(chosen[0]["address"], pool[-1]["address"])
        self.assertEqual(report["newCandidates"], 20)

    def test_overlapping_periods_do_not_multiply_candidate_transaction_count(self):
        module = self.audit_module()
        rows = {}
        module.add_candidate(rows, self.MEXC, "7d", tx_count=3)
        module.add_candidate(rows, self.MEXC, "30d", tx_count=5)
        module.add_candidate(rows, self.MEXC, "all", tx_count=5)
        self.assertEqual(rows[self.MEXC]["txCount"], 5)

    def test_nearly_flat_transit_wallet_still_enters_discovery_from_gross_flow(self):
        module = self.audit_module()
        payload = {"periods":{"30d":{"eth":{"accumulators":[{
            "address":self.MEXC,"net_flow":1,"balance":0,"gross_inflow":350000,"gross_outflow":349999,
        }]}}}}
        with mock.patch.object(module, "load_json", side_effect=[{"holders":[]},payload]):
            rows = module.collect_candidates({},10000,10000,0,False)
        self.assertEqual(rows[0]["address"], self.MEXC)
        self.assertEqual(rows[0]["maxGrossFlow"],350000)

    def test_only_direct_profile_evidence_is_published_and_existing_labels_survive(self):
        module = self.audit_module()
        def suggestion(address, **extra):
            return {"address":address, "suggestedLabel":"MEXC", "source":"debank-public-label",
                    "debank":{"nametag":"MEXC", "profileAddress":address}, **extra}
        existing = "0x1111111111111111111111111111111111111111"
        indirect = "0x2222222222222222222222222222222222222222"
        wrong_profile = "0x3333333333333333333333333333333333333333"
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"dolo-address-labels.js"
            original = '  const DOLO_ADDRESS_LABELS = {\n    "'+existing+'": {label:"Known User", type:"eoa"}\n  };\n'
            path.write_text(original)
            result = module.apply_confirmed_cex_labels([
                suggestion(self.MEXC), suggestion(existing),
                suggestion(indirect, source="flow-audit"),
                suggestion(wrong_profile, debank={"nametag":"MEXC", "profileAddress":self.MEXC}),
            ], {existing:{"label":"Known User", "type":"eoa"}}, path=path, verified_at="2026-09-12")
            with mock.patch.object(module, "LABELS_JS", path):
                loaded = module.load_labels()
            self.assertEqual(loaded[self.MEXC]["type"], "cex")
            self.assertEqual(loaded[existing]["label"], "Known User")
            self.assertNotIn(indirect, loaded)
            self.assertNotIn(wrong_profile, loaded)
            self.assertEqual(len(result["added"]), 1)
            before = path.read_text()
            module.apply_confirmed_cex_labels([suggestion(self.MEXC)], loaded, path=path, verified_at="2026-09-12")
            self.assertEqual(path.read_text(), before)

    def test_debank_direct_evidence_replaces_explorer_keyword_suggestion(self):
        module = self.audit_module()
        primary = {"confirmedCexSuggestions":[{"address":self.MEXC,"source":"etherscan-public-page"}]}
        direct = {"address":self.MEXC,"source":"debank-public-label","debank":{"profileAddress":self.MEXC}}
        merged = module.merge_audit_reports(primary, {"confirmedCexSuggestions":[direct]})
        self.assertEqual(merged["confirmedCexSuggestions"], [direct])

    def test_label_audit_reads_overrides_without_inventing_confirmed_confidence(self):
        module = self.audit_module()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'dolo-address-labels.js'
            path.write_text('"0x1111111111111111111111111111111111111111": {label:"Old CEX",type:"cex"}')
            (Path(folder)/'dolo-address-overrides.js').write_text(
                '"0x1111111111111111111111111111111111111111": {label:"Linked Wallet",type:"cex",source:"flow-audit",confidence:"high"},'
                '"0x2222222222222222222222222222222222222222": {label:"Legacy CEX",type:"cex"}')
            with mock.patch.object(module,'LABELS_JS',path):
                labels = module.load_labels()
        self.assertEqual(labels['0x1111111111111111111111111111111111111111']['label'],'Linked Wallet')
        self.assertEqual(labels['0x2222222222222222222222222222222222222222']['confidence'],'unknown')
        self.assertEqual(labels['0x1111111111111111111111111111111111111111']['evidenceStatus'],'review_needed')

    def test_existing_cex_without_direct_evidence_is_not_skipped_by_discovery(self):
        module = self.audit_module()
        address='0x1111111111111111111111111111111111111111'
        with mock.patch.object(module,'load_json',side_effect=[
            {'holders':[{'address':address,'balance':500000}]}, {'periods':{}}]):
            rows=module.collect_candidates({address:{'type':'cex','label':'Linked','evidenceStatus':'review_needed'}},100000,100000,120,False)
        self.assertEqual([r['address'] for r in rows],[address])

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

    def test_debank_direct_cex_badge_extracts_coinbase(self):
        module = self.audit_module()
        html = '''
        <div class="db-user-tag is-cex" title="Coinbase">
          <span class="db-user-tag-content">Coinbase</span>
        </div>
        '''

        metadata = module.extract_debank_cex_metadata(html)

        self.assertEqual("Coinbase", metadata["nametag"])
        self.assertTrue(module.is_cex_metadata(metadata))

    def test_funded_by_coinbase_is_not_a_direct_cex_badge(self):
        module = self.audit_module()
        html = '<div class="funded-by">Funded By Coinbase 10</div>'

        metadata = module.extract_debank_cex_metadata(html)

        self.assertEqual({}, metadata)

    def test_debank_direct_cex_badge_accepts_new_exchange_names(self):
        module = self.audit_module()
        html = '<div class="is-cex db-user-tag" title="New Exchange"></div>'

        metadata = module.extract_debank_cex_metadata(html)

        self.assertEqual("New Exchange", metadata["nametag"])

    def test_debank_audit_is_advisory_and_preserves_candidate_label_type(self):
        module = self.audit_module()
        candidate = {
            "address": "0x906bd3aff2700f0d1aaf937d9c8dbf6024102e19",
            "label": "",
            "labelType": "",
        }
        with mock.patch.object(
            module,
            "fetch_debank_cex_metadata",
            return_value=({"nametag": "Coinbase"}, None),
        ):
            report = module.run_debank_page_audit(
                [candidate],
                delay=0,
                chrome_binary="chrome",
            )

        self.assertEqual(1, report["queriedCount"])
        self.assertEqual("Coinbase", report["confirmedCexSuggestions"][0]["suggestedLabel"])
        self.assertEqual("debank-public-label", report["confirmedCexSuggestions"][0]["source"])
        self.assertEqual("", candidate["labelType"])

    def test_merged_report_removes_confirmed_address_from_no_tag_rows(self):
        module = self.audit_module()
        address = "0x906bd3aff2700f0d1aaf937d9c8dbf6024102e19"
        primary = {
            "confirmedCexSuggestions": [],
            "nonCexTagged": [],
            "noPublicTag": [address],
            "errors": {},
            "queriedCount": 1,
        }
        secondary = {
            "confirmedCexSuggestions": [{"address": address, "suggestedLabel": "Coinbase"}],
            "nonCexTagged": [],
            "noPublicTag": [],
            "errors": {},
            "queriedCount": 1,
        }

        merged = module.merge_audit_reports(primary, secondary)

        self.assertEqual([], merged["noPublicTag"])

    def test_debank_rotation_prioritizes_unchecked_candidates_and_skips_confirmed(self):
        module = self.audit_module()
        fresh = "0x0000000000000000000000000000000000000001"
        retried = "0x0000000000000000000000000000000000000002"
        confirmed = "0x0000000000000000000000000000000000000003"
        candidates = [{"address": address} for address in (fresh, retried, confirmed)]
        state = {
            "schemaVersion": 1,
            "debank": {
                "addresses": {
                    retried: {"lastAttemptAt": "2026-08-31T00:00:00Z", "outcome": "no_cex_badge"},
                },
            },
        }

        selected, summary = module.select_debank_rotation_candidates(
            candidates,
            state,
            {confirmed},
            limit=2,
        )

        self.assertEqual([fresh, retried], [row["address"] for row in selected])
        self.assertEqual(1, summary["newCandidates"])
        self.assertEqual(1, summary["rechecks"])
        self.assertEqual(1, summary["excludedConfirmed"])

    def test_debank_rotation_records_errors_without_treating_them_as_no_cex(self):
        module = self.audit_module()
        no_tag = "0x0000000000000000000000000000000000000001"
        confirmed = "0x0000000000000000000000000000000000000002"
        errored = "0x0000000000000000000000000000000000000003"
        report = {
            "confirmedCexSuggestions": [{"address": confirmed}],
            "noPublicTag": [no_tag],
            "errors": {errored: "debank_timeout"},
        }

        state = module.record_debank_rotation_results(
            {},
            report,
            attempted_at="2026-09-06T08:00:00Z",
        )

        addresses = state["debank"]["addresses"]
        self.assertEqual("no_cex_badge", addresses[no_tag]["outcome"])
        self.assertEqual("confirmed_cex", addresses[confirmed]["outcome"])
        self.assertEqual("error", addresses[errored]["outcome"])
        self.assertEqual("2026-09-06T08:00:00Z", addresses[errored]["lastAttemptAt"])


if __name__ == "__main__":
    unittest.main()
