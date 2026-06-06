#!/usr/bin/env python3
"""
Backward-compatible entrypoint for oDOLO reward claims.

The implementation now indexes all supported Dolomite RewardClaimed logs in
generate_reward_claim_events.py and also writes the legacy Berachain
data/odolo-claim-events.json file used by older History builds.
"""
from generate_reward_claim_events import main


if __name__ == "__main__":
    main()
