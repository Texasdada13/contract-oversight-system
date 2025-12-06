"""
Contract Oversight System - Scoring Engine
Calculates performance scores, risk levels, and health metrics for contracts and vendors.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ContractScoringEngine:
    """Calculates contract performance and health scores."""

    def __init__(self):
        # Score weights for overall health
        self.health_weights = {
            'cost_variance': 0.30,      # Budget adherence
            'schedule_variance': 0.25,   # Timeline adherence
            'performance': 0.25,         # Quality/deliverables
            'compliance': 0.20           # Documentation, insurance, etc.
        }

        # Risk thresholds
        self.risk_thresholds = {
            'critical': 30,
            'high': 50,
            'medium': 70,
            'low': 100
        }

    def calculate_cost_variance_score(self, contract: Dict) -> float:
        """
        Calculate cost variance score (0-100).
        Higher score = better budget adherence.
        """
        original = contract.get('original_amount', 0) or 0
        current = contract.get('current_amount', 0) or 0

        if original <= 0:
            return 50  # No baseline

        # Calculate variance percentage
        variance_pct = ((current - original) / original) * 100

        # Score based on variance
        # 0% variance = 100, 10% over = 80, 20% over = 60, 30%+ over = 40 or less
        if variance_pct <= 0:
            return 100  # Under budget
        elif variance_pct <= 5:
            return 95
        elif variance_pct <= 10:
            return 85
        elif variance_pct <= 15:
            return 70
        elif variance_pct <= 20:
            return 55
        elif variance_pct <= 30:
            return 40
        else:
            return max(20, 40 - (variance_pct - 30))

    def calculate_schedule_variance_score(self, contract: Dict) -> float:
        """
        Calculate schedule variance score (0-100).
        Higher score = better timeline adherence.
        """
        original_end = contract.get('original_end_date')
        current_end = contract.get('current_end_date')
        start_date = contract.get('start_date')

        if not original_end or not start_date:
            return 50  # No baseline

        try:
            original_end_dt = datetime.fromisoformat(str(original_end)[:10])
            current_end_dt = datetime.fromisoformat(str(current_end or original_end)[:10])
            start_dt = datetime.fromisoformat(str(start_date)[:10])

            # Original duration
            original_duration = (original_end_dt - start_dt).days
            if original_duration <= 0:
                return 50

            # Days extended
            extension_days = (current_end_dt - original_end_dt).days
            extension_pct = (extension_days / original_duration) * 100

            # Score based on extension
            if extension_pct <= 0:
                return 100  # On or ahead of schedule
            elif extension_pct <= 5:
                return 90
            elif extension_pct <= 10:
                return 80
            elif extension_pct <= 20:
                return 65
            elif extension_pct <= 30:
                return 50
            else:
                return max(20, 50 - (extension_pct - 30))

        except (ValueError, TypeError):
            return 50

    def calculate_performance_score(self, contract: Dict, milestones: List[Dict] = None) -> float:
        """
        Calculate performance score based on milestone completion and quality.
        """
        base_score = 50

        # Check milestone completion if available
        if milestones:
            total = len(milestones)
            if total > 0:
                completed_on_time = 0
                completed_late = 0
                overdue = 0

                for m in milestones:
                    status = m.get('status', '')
                    due_date = m.get('due_date')
                    completed_date = m.get('completed_date')

                    if status == 'Completed':
                        if due_date and completed_date:
                            try:
                                due = datetime.fromisoformat(str(due_date)[:10])
                                completed = datetime.fromisoformat(str(completed_date)[:10])
                                if completed <= due:
                                    completed_on_time += 1
                                else:
                                    completed_late += 1
                            except:
                                completed_on_time += 1
                        else:
                            completed_on_time += 1
                    elif status == 'Overdue':
                        overdue += 1

                # Calculate score
                on_time_pct = (completed_on_time / total) * 100
                late_pct = (completed_late / total) * 100
                overdue_pct = (overdue / total) * 100

                base_score = on_time_pct - (late_pct * 0.3) - (overdue_pct * 0.5)
                base_score = max(0, min(100, base_score))

        # Factor in percent complete vs expected
        percent_complete = contract.get('percent_complete', 0) or 0

        # Blend with base score
        return (base_score * 0.7) + (percent_complete * 0.3)

    def calculate_compliance_score(self, contract: Dict) -> float:
        """
        Calculate compliance score based on documentation and requirements.
        """
        score = 100

        # Check insurance
        if contract.get('requires_insurance') and not contract.get('insurance_verified'):
            score -= 20

        # Check bond
        if contract.get('requires_bond') and not contract.get('bond_verified'):
            score -= 15

        # Check for board approval if required
        board_date = contract.get('board_approval_date')
        award_date = contract.get('award_date')
        if award_date and not board_date:
            # Award without board approval might be an issue for large contracts
            amount = contract.get('current_amount', 0) or 0
            if amount > 50000:  # Threshold for board approval
                score -= 25

        # Penalize sole source without justification
        if contract.get('is_sole_source') and not contract.get('justification'):
            score -= 15

        return max(0, score)

    def calculate_overall_health(self, contract: Dict, milestones: List[Dict] = None) -> float:
        """Calculate overall contract health score."""
        cost_score = self.calculate_cost_variance_score(contract)
        schedule_score = self.calculate_schedule_variance_score(contract)
        performance_score = self.calculate_performance_score(contract, milestones)
        compliance_score = self.calculate_compliance_score(contract)

        # Weighted average
        health = (
            cost_score * self.health_weights['cost_variance'] +
            schedule_score * self.health_weights['schedule_variance'] +
            performance_score * self.health_weights['performance'] +
            compliance_score * self.health_weights['compliance']
        )

        return round(health, 1)

    def determine_risk_level(self, health_score: float) -> str:
        """Determine risk level from health score."""
        if health_score < self.risk_thresholds['critical']:
            return 'Critical'
        elif health_score < self.risk_thresholds['high']:
            return 'High'
        elif health_score < self.risk_thresholds['medium']:
            return 'Medium'
        else:
            return 'Low'

    def score_contract(self, contract: Dict, milestones: List[Dict] = None) -> Dict:
        """Score a single contract and return updated data."""
        contract['cost_variance_score'] = self.calculate_cost_variance_score(contract)
        contract['schedule_variance_score'] = self.calculate_schedule_variance_score(contract)
        contract['performance_score'] = self.calculate_performance_score(contract, milestones)
        contract['compliance_score'] = self.calculate_compliance_score(contract)
        contract['overall_health_score'] = self.calculate_overall_health(contract, milestones)
        contract['risk_level'] = self.determine_risk_level(contract['overall_health_score'])

        return contract

    def batch_score_contracts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Score all contracts in a DataFrame."""
        if df.empty:
            return df

        df = df.copy()

        # Calculate scores for each contract
        df['cost_variance_score'] = df.apply(
            lambda row: self.calculate_cost_variance_score(row.to_dict()), axis=1
        )
        df['schedule_variance_score'] = df.apply(
            lambda row: self.calculate_schedule_variance_score(row.to_dict()), axis=1
        )
        df['performance_score'] = df.apply(
            lambda row: self.calculate_performance_score(row.to_dict()), axis=1
        )
        df['compliance_score'] = df.apply(
            lambda row: self.calculate_compliance_score(row.to_dict()), axis=1
        )
        df['overall_health_score'] = df.apply(
            lambda row: self.calculate_overall_health(row.to_dict()), axis=1
        )
        df['risk_level'] = df['overall_health_score'].apply(self.determine_risk_level)

        return df


class VendorScoringEngine:
    """Calculates vendor performance scores."""

    def __init__(self):
        self.score_weights = {
            'on_time_delivery': 0.30,
            'budget_adherence': 0.25,
            'quality': 0.25,
            'responsiveness': 0.20
        }

    def calculate_vendor_score(self, vendor: Dict, contracts: List[Dict]) -> float:
        """
        Calculate overall vendor performance score based on contract history.
        """
        if not contracts:
            return 50  # No history

        total_weight = 0
        weighted_score = 0

        for contract in contracts:
            # Weight by contract value
            amount = contract.get('current_amount', 0) or 1
            health = contract.get('overall_health_score', 50)

            weighted_score += health * amount
            total_weight += amount

        if total_weight > 0:
            return round(weighted_score / total_weight, 1)

        return 50

    def get_vendor_metrics(self, vendor: Dict, contracts: List[Dict]) -> Dict:
        """Get detailed vendor metrics."""
        if not contracts:
            return {
                'total_contracts': 0,
                'total_value': 0,
                'avg_health_score': 50,
                'on_time_rate': 0,
                'budget_adherence_rate': 0,
                'active_contracts': 0,
                'completed_contracts': 0
            }

        total_value = sum(c.get('current_amount', 0) or 0 for c in contracts)
        avg_health = np.mean([c.get('overall_health_score', 50) for c in contracts])

        on_time = sum(1 for c in contracts if c.get('schedule_variance_score', 50) >= 70)
        on_budget = sum(1 for c in contracts if c.get('cost_variance_score', 50) >= 70)

        active = sum(1 for c in contracts if c.get('status') == 'Active')
        completed = sum(1 for c in contracts if c.get('status') == 'Completed')

        return {
            'total_contracts': len(contracts),
            'total_value': total_value,
            'avg_health_score': round(avg_health, 1),
            'on_time_rate': round((on_time / len(contracts)) * 100, 1) if contracts else 0,
            'budget_adherence_rate': round((on_budget / len(contracts)) * 100, 1) if contracts else 0,
            'active_contracts': active,
            'completed_contracts': completed
        }


class AlertGenerator:
    """Generates alerts and warnings for contracts."""

    def __init__(self):
        self.alert_rules = [
            {
                'id': 'cost_overrun_warning',
                'name': 'Cost Overrun Warning',
                'severity': 'High',
                'condition': lambda c: self._cost_overrun(c, 10, 20)
            },
            {
                'id': 'cost_overrun_critical',
                'name': 'Critical Cost Overrun',
                'severity': 'Critical',
                'condition': lambda c: self._cost_overrun(c, 20, 100)
            },
            {
                'id': 'schedule_delay_warning',
                'name': 'Schedule Delay Warning',
                'severity': 'Medium',
                'condition': lambda c: self._schedule_delay(c, 10, 20)
            },
            {
                'id': 'schedule_delay_critical',
                'name': 'Critical Schedule Delay',
                'severity': 'High',
                'condition': lambda c: self._schedule_delay(c, 20, 100)
            },
            {
                'id': 'expiring_soon',
                'name': 'Contract Expiring Soon',
                'severity': 'Medium',
                'condition': lambda c: self._expiring_soon(c, 30)
            },
            {
                'id': 'insurance_expired',
                'name': 'Insurance Not Verified',
                'severity': 'High',
                'condition': lambda c: c.get('requires_insurance') and not c.get('insurance_verified')
            },
            {
                'id': 'low_health_score',
                'name': 'Low Health Score',
                'severity': 'High',
                'condition': lambda c: (c.get('overall_health_score', 100) or 100) < 50
            },
            {
                'id': 'multiple_change_orders',
                'name': 'Excessive Change Orders',
                'severity': 'Medium',
                'condition': lambda c: (c.get('change_order_count', 0) or 0) >= 3
            },
            {
                'id': 'no_activity',
                'name': 'No Recent Activity',
                'severity': 'Low',
                'condition': lambda c: self._no_recent_activity(c, 60)
            }
        ]

    def _cost_overrun(self, contract: Dict, min_pct: float, max_pct: float) -> bool:
        original = contract.get('original_amount', 0) or 0
        current = contract.get('current_amount', 0) or 0
        if original <= 0:
            return False
        variance_pct = ((current - original) / original) * 100
        return min_pct <= variance_pct < max_pct

    def _schedule_delay(self, contract: Dict, min_pct: float, max_pct: float) -> bool:
        original_end = contract.get('original_end_date')
        current_end = contract.get('current_end_date')
        start_date = contract.get('start_date')

        if not original_end or not start_date:
            return False

        try:
            original_end_dt = datetime.fromisoformat(str(original_end)[:10])
            current_end_dt = datetime.fromisoformat(str(current_end or original_end)[:10])
            start_dt = datetime.fromisoformat(str(start_date)[:10])

            original_duration = (original_end_dt - start_dt).days
            if original_duration <= 0:
                return False

            extension_days = (current_end_dt - original_end_dt).days
            extension_pct = (extension_days / original_duration) * 100

            return min_pct <= extension_pct < max_pct
        except:
            return False

    def _expiring_soon(self, contract: Dict, days: int) -> bool:
        end_date = contract.get('current_end_date') or contract.get('original_end_date')
        if not end_date:
            return False

        try:
            end_dt = datetime.fromisoformat(str(end_date)[:10])
            days_until = (end_dt - datetime.now()).days
            return 0 < days_until <= days
        except:
            return False

    def _no_recent_activity(self, contract: Dict, days: int) -> bool:
        updated = contract.get('updated_at')
        if not updated:
            return False

        try:
            updated_dt = datetime.fromisoformat(str(updated)[:19])
            days_since = (datetime.now() - updated_dt).days
            return days_since > days and contract.get('status') == 'Active'
        except:
            return False

    def generate_alerts(self, contracts: List[Dict]) -> List[Dict]:
        """Generate alerts for a list of contracts."""
        alerts = []

        for contract in contracts:
            for rule in self.alert_rules:
                try:
                    if rule['condition'](contract):
                        alerts.append({
                            'alert_id': f"{contract.get('contract_id')}_{rule['id']}",
                            'contract_id': contract.get('contract_id'),
                            'contract_title': contract.get('title'),
                            'vendor_name': contract.get('vendor_name'),
                            'alert_type': rule['id'],
                            'title': rule['name'],
                            'severity': rule['severity'],
                            'generated_at': datetime.now().isoformat()
                        })
                except Exception as e:
                    logger.error(f"Error checking rule {rule['id']}: {e}")

        # Sort by severity
        severity_order = {'Critical': 0, 'High': 1, 'Medium': 2, 'Low': 3}
        alerts.sort(key=lambda a: severity_order.get(a['severity'], 4))

        return alerts
