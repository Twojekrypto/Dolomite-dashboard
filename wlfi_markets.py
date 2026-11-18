"""Strict account-family classification; this never identifies wallet ownership."""
import re

MINIMUM = int('47' * 32, 16)
MAXIMUM = MINIMUM + 100
_DECIMAL = re.compile(r'[0-9]+')
_ACCOUNT_ID = re.compile(r'0x[0-9a-fA-F]{40}-([0-9]+)')
_CHAINS = {
    'ethereum': 'ethereum', 'eth': 'ethereum', '1': 'ethereum',
    'arbitrum': 'arbitrum', 'arb': 'arbitrum', '42161': 'arbitrum',
    'berachain': 'berachain', 'bera': 'berachain', '80094': 'berachain',
    'mantle': 'mantle', '5000': 'mantle', 'xlayer': 'xlayer', '196': 'xlayer',
    'botanix': 'botanix', '3637': 'botanix',
    'polygon_zkevm': 'polygon_zkevm', 'polygon-zkevm': 'polygon_zkevm', '1101': 'polygon_zkevm',
}


def account_number(account):
    """Return the exact decimal subaccount, or None for missing/malformed evidence."""
    if isinstance(account, dict):
        for field in ('accountNumber', 'accountId', 'account', 'id'):
            if field in account:
                return account_number(account[field])
        return None
    if isinstance(account, bool) or not isinstance(account, (str, int)):
        return None
    text = str(account)
    match = _ACCOUNT_ID.fullmatch(text)
    if match:
        text = match.group(1)
    if not _DECIMAL.fullmatch(text):
        return None
    text = text.lstrip('0') or '0'
    if len(text) > 78:
        return None
    value = int(text)
    return str(value) if value < 2**256 else None


def account_origin(chain, account):
    if isinstance(chain, bool):
        return 'unknown'
    normalized = _CHAINS.get(str(chain).lower())
    if normalized is None:
        return 'unknown'
    if normalized != 'ethereum':
        return 'other'
    number = account_number(account)
    if number is None:
        return 'unknown'
    return 'wlfi' if MINIMUM <= int(number) <= MAXIMUM else 'other'
