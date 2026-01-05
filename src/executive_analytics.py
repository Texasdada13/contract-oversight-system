"""
Executive Analytics Module
Provides predictive analytics, risk scoring, and executive-level insights
for the Contract Oversight System.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / 'data' / 'contracts.db'


class ExecutiveAnalytics:
    """Provides executive-level analytics and predictions."""

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DB_PATH

    def _get_connection(self):
        """Get database connection with row factory."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def calculate_construction_cost(self, contract_id: str) -> float:
        """
        Calculate construction cost by subtracting land cost from total.

        Args:
            contract_id: Contract ID

        Returns:
            Construction cost (current_amount - land_cost)
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT current_amount, land_cost
            FROM contracts
            WHERE contract_id = ?
        ''', (contract_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return 0.0

        current_amount = row['current_amount'] or 0
        land_cost = row['land_cost'] or 0

        return current_amount - land_cost

    def calculate_inflation_adjusted_cost(self, contract_id: str, base_year: int = 2026) -> float:
        """
        Calculate inflation-adjusted cost using construction cost index.

        Args:
            contract_id: Contract ID
            base_year: Year to adjust to (default 2026)

        Returns:
            Inflation-adjusted cost in base year dollars
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Get contract fiscal year and current amount
        cursor.execute('''
            SELECT fiscal_year, current_amount
            FROM contracts
            WHERE contract_id = ?
        ''', (contract_id,))

        contract_row = cursor.fetchone()
        if not contract_row or not contract_row['fiscal_year']:
            conn.close()
            return 0.0

        try:
            contract_year = int(contract_row['fiscal_year'])
        except (ValueError, TypeError):
            conn.close()
            return contract_row['current_amount'] or 0.0

        current_amount = contract_row['current_amount'] or 0

        # Get inflation indices
        cursor.execute('''
            SELECT construction_cost_index
            FROM inflation_index
            WHERE year = ?
        ''', (contract_year,))
        contract_index_row = cursor.fetchone()

        cursor.execute('''
            SELECT construction_cost_index
            FROM inflation_index
            WHERE year = ?
        ''', (base_year,))
        base_index_row = cursor.fetchone()

        conn.close()

        if not contract_index_row or not base_index_row:
            # No inflation data available
            return current_amount

        contract_index = contract_index_row['construction_cost_index']
        base_index = base_index_row['construction_cost_index']

        # Adjust to base year: amount * (base_index / contract_index)
        inflation_adjusted = current_amount * (base_index / contract_index)

        return round(inflation_adjusted, 2)

    def calculate_burn_rate(self, contract_id: str) -> float:
        """
        Calculate monthly burn rate (spending rate).

        Args:
            contract_id: Contract ID

        Returns:
            Monthly burn rate in dollars
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT start_date, total_paid
            FROM contracts
            WHERE contract_id = ?
        ''', (contract_id,))

        row = cursor.fetchone()
        conn.close()

        if not row or not row['start_date']:
            return 0.0

        total_paid = row['total_paid'] or 0
        start_date_str = row['start_date']

        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except (ValueError, TypeError):
            return 0.0

        # Calculate months elapsed
        today = datetime.now()
        months_elapsed = max((today.year - start_date.year) * 12 + (today.month - start_date.month), 1)

        burn_rate = total_paid / months_elapsed

        return round(burn_rate, 2)

    def forecast_budget_at_completion(self, contract_id: str) -> float:
        """
        Forecast final budget at completion based on current burn rate and progress.

        Uses Earned Value Management (EVM) formula:
        Estimate at Completion (EAC) = BAC / CPI
        where CPI = Cost Performance Index

        Args:
            contract_id: Contract ID

        Returns:
            Forecasted budget at completion
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT current_amount, total_paid, percent_complete
            FROM contracts
            WHERE contract_id = ?
        ''', (contract_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return 0.0

        budget_at_completion = row['current_amount'] or 0
        total_paid = row['total_paid'] or 0
        percent_complete = (row['percent_complete'] or 0) / 100.0

        if percent_complete == 0:
            # No progress yet, return original budget
            return budget_at_completion

        # Calculate Cost Performance Index (CPI)
        # CPI = Earned Value / Actual Cost
        # Earned Value = BAC * % Complete
        earned_value = budget_at_completion * percent_complete
        cpi = earned_value / total_paid if total_paid > 0 else 1.0

        # Estimate at Completion = BAC / CPI
        if cpi > 0:
            forecast = budget_at_completion / cpi
        else:
            forecast = budget_at_completion

        return round(forecast, 2)

    def predict_expected_completion_date(self, contract_id: str) -> Optional[str]:
        """
        Predict expected completion date based on current progress velocity.

        Args:
            contract_id: Contract ID

        Returns:
            Expected completion date as ISO string (YYYY-MM-DD) or None
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT start_date, percent_complete, current_end_date
            FROM contracts
            WHERE contract_id = ?
        ''', (contract_id,))

        row = cursor.fetchone()
        conn.close()

        if not row or not row['start_date']:
            return None

        start_date_str = row['start_date']
        percent_complete = row['percent_complete'] or 0
        planned_end_str = row['current_end_date']

        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except (ValueError, TypeError):
            return None

        if percent_complete == 0:
            # No progress, return planned end date
            return planned_end_str

        # Calculate days elapsed
        today = datetime.now()
        days_elapsed = (today - start_date).days

        if days_elapsed <= 0:
            return planned_end_str

        # Calculate velocity (% per day)
        velocity = percent_complete / days_elapsed

        if velocity <= 0:
            # No velocity, can't predict
            return planned_end_str

        # Calculate remaining work
        remaining_work = 100 - percent_complete

        # Predict remaining days
        remaining_days = remaining_work / velocity

        # Expected completion date
        expected_completion = today + timedelta(days=remaining_days)

        return expected_completion.strftime('%Y-%m-%d')

    def calculate_risk_probability(self, contract_id: str) -> Tuple[float, str]:
        """
        Calculate risk probability score based on multiple factors:
        - Cost variance
        - Schedule variance
        - Vendor performance
        - Change order frequency
        - Complexity (contract value)

        Args:
            contract_id: Contract ID

        Returns:
            Tuple of (risk_probability_score 0-100, risk_category)
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                c.overall_health_score,
                c.cost_variance_score,
                c.schedule_variance_score,
                c.change_order_count,
                c.current_amount,
                c.percent_complete,
                v.performance_score
            FROM contracts c
            LEFT JOIN vendors v ON c.vendor_id = v.vendor_id
            WHERE c.contract_id = ?
        ''', (contract_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return 50.0, 'Medium'

        # Extract factors
        health_score = row['overall_health_score'] or 50
        cost_variance_score = row['cost_variance_score'] or 50
        schedule_variance_score = row['schedule_variance_score'] or 50
        change_order_count = row['change_order_count'] or 0
        contract_value = row['current_amount'] or 0
        percent_complete = row['percent_complete'] or 0
        vendor_score = row['performance_score'] or 50

        # Calculate risk factors (0-100, where 100 = high risk)

        # 1. Inverse of health score (low health = high risk)
        health_risk = 100 - health_score

        # 2. Cost risk (low cost variance score = high risk)
        cost_risk = 100 - cost_variance_score

        # 3. Schedule risk
        schedule_risk = 100 - schedule_variance_score

        # 4. Change order risk (more change orders = higher risk)
        change_order_risk = min(change_order_count * 15, 100)

        # 5. Complexity risk (higher value = higher risk)
        if contract_value > 10000000:  # >$10M
            complexity_risk = 80
        elif contract_value > 5000000:  # >$5M
            complexity_risk = 60
        elif contract_value > 1000000:  # >$1M
            complexity_risk = 40
        else:
            complexity_risk = 20

        # 6. Vendor risk (low vendor score = high risk)
        vendor_risk = 100 - vendor_score

        # 7. Progress risk (early stage = higher risk)
        if percent_complete < 25:
            progress_risk = 60
        elif percent_complete < 50:
            progress_risk = 40
        elif percent_complete < 75:
            progress_risk = 20
        else:
            progress_risk = 10

        # Weighted average of risk factors
        risk_score = (
            health_risk * 0.25 +
            cost_risk * 0.20 +
            schedule_risk * 0.20 +
            change_order_risk * 0.10 +
            complexity_risk * 0.10 +
            vendor_risk * 0.10 +
            progress_risk * 0.05
        )

        # Determine risk category
        if risk_score >= 70:
            category = 'High'
        elif risk_score >= 50:
            category = 'Medium'
        elif risk_score >= 30:
            category = 'Low'
        else:
            category = 'Very Low'

        return round(risk_score, 1), category

    def generate_executive_summary(self) -> str:
        """
        Generate auto-generated executive summary text.

        Returns:
            Executive summary as natural language text
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Get portfolio statistics
        cursor.execute('''
            SELECT
                COUNT(*) as total_contracts,
                SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) as active_count,
                SUM(current_amount) as total_value,
                SUM(total_paid) as total_paid,
                AVG(overall_health_score) as avg_health,
                SUM(CASE WHEN overall_health_score < 50 THEN 1 ELSE 0 END) as at_risk_count,
                SUM(CASE WHEN overall_health_score < 30 THEN 1 ELSE 0 END) as critical_count
            FROM contracts
            WHERE is_deleted = 0
        ''')

        stats = cursor.fetchone()

        # Get budget variance
        cursor.execute('''
            SELECT
                SUM(current_amount - original_amount) as total_variance
            FROM contracts
            WHERE is_deleted = 0 AND original_amount > 0
        ''')

        variance_row = cursor.fetchone()
        conn.close()

        if not stats:
            return "No contract data available."

        total_contracts = stats['total_contracts'] or 0
        active_count = stats['active_count'] or 0
        total_value = stats['total_value'] or 0
        total_paid = stats['total_paid'] or 0
        avg_health = stats['avg_health'] or 0
        at_risk_count = stats['at_risk_count'] or 0
        critical_count = stats['critical_count'] or 0

        total_variance = (variance_row['total_variance'] or 0) if variance_row else 0
        variance_pct = (total_variance / total_value * 100) if total_value > 0 else 0

        # Determine health status
        if avg_health >= 70:
            health_status = "healthy"
            health_assessment = "Portfolio is performing well."
        elif avg_health >= 50:
            health_status = "stable with some concerns"
            health_assessment = "Portfolio requires attention to at-risk projects."
        else:
            health_status = "at significant risk"
            health_assessment = "Immediate action required on multiple projects."

        # Build summary
        summary_parts = []

        # Part 1: Overall status
        summary_parts.append(
            f"Portfolio health is {health_status} at {avg_health:.0f}/100."
        )

        # Part 2: Projects requiring attention
        if critical_count > 0:
            summary_parts.append(
                f"{critical_count} project{'s' if critical_count != 1 else ''} in critical status requiring immediate board action."
            )
        elif at_risk_count > 0:
            summary_parts.append(
                f"{at_risk_count} project{'s' if at_risk_count != 1 else ''} at risk and requiring monitoring."
            )
        else:
            summary_parts.append("All projects are on track.")

        # Part 3: Budget performance
        if variance_pct > 10:
            summary_parts.append(
                f"Portfolio is {variance_pct:.1f}% over budget—cost controls needed."
            )
        elif variance_pct > 5:
            summary_parts.append(
                f"Portfolio is {variance_pct:.1f}% over budget—monitoring recommended."
            )
        elif variance_pct < -5:
            summary_parts.append(
                f"Portfolio is under budget by {abs(variance_pct):.1f}%."
            )
        else:
            summary_parts.append("Budget performance is on target.")

        return " ".join(summary_parts)

    def update_all_analytics(self):
        """
        Update all analytics fields for all contracts.
        This should be run periodically to refresh predictions.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Get all active contracts
        cursor.execute('''
            SELECT contract_id FROM contracts
            WHERE is_deleted = 0
        ''')

        contracts = cursor.fetchall()
        updated_count = 0

        for row in contracts:
            contract_id = row['contract_id']

            # Calculate all metrics
            construction_cost = self.calculate_construction_cost(contract_id)
            inflation_adjusted = self.calculate_inflation_adjusted_cost(contract_id)
            burn_rate = self.calculate_burn_rate(contract_id)
            forecast_budget = self.forecast_budget_at_completion(contract_id)
            expected_date = self.predict_expected_completion_date(contract_id)
            risk_score, risk_category = self.calculate_risk_probability(contract_id)

            # Update contract with calculated values
            cursor.execute('''
                UPDATE contracts
                SET
                    construction_cost = ?,
                    inflation_adjusted_cost = ?,
                    burn_rate_monthly = ?,
                    forecast_budget_at_completion = ?,
                    expected_completion_date = ?,
                    risk_probability_score = ?,
                    risk_category = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE contract_id = ?
            ''', (construction_cost, inflation_adjusted, burn_rate, forecast_budget,
                  expected_date, risk_score, risk_category, contract_id))

            updated_count += 1

        conn.commit()
        conn.close()

        logger.info(f"Updated analytics for {updated_count} contracts")
        return updated_count


# Convenience function
def get_executive_analytics(db_path: Path = None) -> ExecutiveAnalytics:
    """Get ExecutiveAnalytics instance."""
    return ExecutiveAnalytics(db_path)
