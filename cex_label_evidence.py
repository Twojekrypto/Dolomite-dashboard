"""Recorded label provenance, independent of retained CEX membership."""


def cex_label_evidence_status(info):
    public_sources = {
        "etherscan-public-label", "etherscan-public-page", "debank-public-label",
        "etherscan-coinex-deposit-factory",
    }
    if (info.get("source") in public_sources
            and info.get("confidence") not in {"potential", "high", "unknown"}):
        return "public_label"
    return "review_needed"
