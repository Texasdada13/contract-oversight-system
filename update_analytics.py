"""
Update analytics for all contracts.
Calculates and updates:
- Construction costs (total - land)
- Inflation-adjusted costs
- Burn rates
- Forecast budget at completion
- Expected completion dates
- Risk probability scores
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.executive_analytics import get_executive_analytics

if __name__ == "__main__":
    print("Updating executive analytics for all contracts...")

    analytics = get_executive_analytics()

    # Update all contracts
    count = analytics.update_all_analytics()

    print(f"[OK] Analytics updated for {count} contracts")

    # Generate executive summary
    summary = analytics.generate_executive_summary()
    print("\nExecutive Summary:")
    print(f"  {summary}")

    print("\nAnalytics update complete!")
