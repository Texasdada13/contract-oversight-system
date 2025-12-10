"""
Procurement Benchmarking Module
Based on Coupa 2025 Total Spend Management Benchmark Report.

This module provides:
1. Industry benchmark KPIs for procurement performance
2. Entity scoring against benchmarks
3. Procurement Health Score calculation
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# ============================================
# COUPA 2025 BENCHMARK KPIS (Top Quartile Medians)
# ============================================

BENCHMARK_CATEGORIES = {
    'spend_analysis': 'Spend Analysis',
    'supplier_management': 'Supplier Risk & Performance Management',
    'source_to_contract': 'Source-to-Contract',
    'procurement': 'Procurement',
    'cash_liquidity': 'Cash & Liquidity Management',
    'e_invoicing': 'E-Invoicing',
    'expenses': 'Expenses',
    'payments': 'Payments'
}

# Benchmark definitions with industry top-quartile medians
COUPA_BENCHMARKS = {
    # Spend Analysis
    'increase_visibility_managed_spend': {
        'name': 'Increase in Visibility of Managed Spend',
        'category': 'spend_analysis',
        'benchmark_value': 24.4,
        'unit': 'percent',
        'direction': 'higher',  # higher is better
        'description': 'Percentage increase in spend visibility after implementation',
        'importance': 'high'
    },

    # Supplier Risk & Performance Management
    'supplier_info_mgmt_cycle_time': {
        'name': 'Supplier Information Management Cycle Time',
        'category': 'supplier_management',
        'benchmark_value': 6.6,
        'unit': 'business_hours',
        'direction': 'lower',  # lower is better
        'description': 'Time to complete supplier onboarding/updates',
        'importance': 'medium'
    },
    'spend_with_primary_suppliers': {
        'name': 'Spend with Primary Suppliers',
        'category': 'supplier_management',
        'benchmark_value': 17.5,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Percentage of spend concentrated with strategic suppliers',
        'importance': 'medium'
    },

    # Source-to-Contract
    'contract_mgmt_cycle_time': {
        'name': 'Contract Management Cycle Time',
        'category': 'source_to_contract',
        'benchmark_value': 11.3,
        'unit': 'business_days',
        'direction': 'lower',
        'description': 'Days from contract request to execution',
        'importance': 'high'
    },
    'on_contract_spend': {
        'name': 'On-Contract Spend',
        'category': 'source_to_contract',
        'benchmark_value': 81.1,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Percentage of spend under contract coverage',
        'importance': 'high'
    },

    # Procurement
    'structured_spend': {
        'name': 'Structured Spend',
        'category': 'procurement',
        'benchmark_value': 55.3,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Spend processed through structured procurement channels',
        'importance': 'high'
    },
    'pre_approved_spend': {
        'name': 'Pre-Approved Spend',
        'category': 'procurement',
        'benchmark_value': 96.4,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Spend that goes through proper approval workflows',
        'importance': 'critical'
    },
    'electronic_po_processing': {
        'name': 'Electronic PO Processing Rate',
        'category': 'procurement',
        'benchmark_value': 98.8,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Purchase orders processed electronically',
        'importance': 'medium'
    },
    'requisition_to_order_cycle_time': {
        'name': 'Requisition-to-Order Cycle Time',
        'category': 'procurement',
        'benchmark_value': 4.0,
        'unit': 'business_hours',
        'direction': 'lower',
        'description': 'Hours from requisition submission to PO creation',
        'importance': 'high'
    },

    # Cash & Liquidity Management
    'touchless_cash_reconciliation': {
        'name': 'Touchless Cash Flow Reconciliation',
        'category': 'cash_liquidity',
        'benchmark_value': 99.97,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Automated cash reconciliation rate',
        'importance': 'medium'
    },
    'cash_concentration_index': {
        'name': 'Cash Concentration Index',
        'category': 'cash_liquidity',
        'benchmark_value': 76.0,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Effectiveness of cash pooling and concentration',
        'importance': 'medium'
    },

    # E-Invoicing
    'electronic_invoice_processing': {
        'name': 'Electronic Invoice Processing Rate',
        'category': 'e_invoicing',
        'benchmark_value': 86.2,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Invoices processed electronically',
        'importance': 'high'
    },
    'invoice_approval_cycle_time': {
        'name': 'Invoice Approval Cycle Time',
        'category': 'e_invoicing',
        'benchmark_value': 10.5,
        'unit': 'business_hours',
        'direction': 'lower',
        'description': 'Hours to approve an invoice',
        'importance': 'high'
    },
    'first_time_match_rate': {
        'name': 'First-Time Match Rate',
        'category': 'e_invoicing',
        'benchmark_value': 97.1,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Invoices that match PO on first attempt',
        'importance': 'high'
    },

    # Expenses
    'expense_report_approval_cycle_time': {
        'name': 'Expense Report Approval Cycle Time',
        'category': 'expenses',
        'benchmark_value': 7.5,
        'unit': 'business_hours',
        'direction': 'lower',
        'description': 'Hours to approve expense reports',
        'importance': 'medium'
    },
    'expense_lines_within_policy': {
        'name': 'Expense Lines Within Policy',
        'category': 'expenses',
        'benchmark_value': 98.9,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Expense line items compliant with policy',
        'importance': 'high'
    },

    # Payments
    'invoices_paid_digitally': {
        'name': 'Invoices Paid Digitally',
        'category': 'payments',
        'benchmark_value': 96.0,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Payments made through digital channels',
        'importance': 'medium'
    },
    'suppliers_using_digital_payments': {
        'name': 'Suppliers Using Digital Payments',
        'category': 'payments',
        'benchmark_value': 96.2,
        'unit': 'percent',
        'direction': 'higher',
        'description': 'Suppliers accepting digital payments',
        'importance': 'medium'
    },
    'payment_batch_approval_cycle_time': {
        'name': 'Payment Batch Approval Cycle Time',
        'category': 'payments',
        'benchmark_value': 4.2,
        'unit': 'business_hours',
        'direction': 'lower',
        'description': 'Hours to approve payment batches',
        'importance': 'medium'
    }
}

# Weight multipliers for importance levels
IMPORTANCE_WEIGHTS = {
    'critical': 2.0,
    'high': 1.5,
    'medium': 1.0,
    'low': 0.5
}


@dataclass
class KPIScore:
    """Individual KPI score result."""
    kpi_id: str
    name: str
    category: str
    actual_value: float
    benchmark_value: float
    unit: str
    direction: str
    score: float  # 0-100 score
    gap: float  # Gap from benchmark (positive = better, negative = worse)
    gap_percent: float
    rating: str  # 'Excellent', 'Good', 'Fair', 'Poor', 'Critical'
    recommendation: str


@dataclass
class CategoryScore:
    """Category-level score."""
    category_id: str
    category_name: str
    score: float
    kpi_scores: List[KPIScore]
    strengths: List[str]
    improvement_areas: List[str]


@dataclass
class ProcurementHealthScore:
    """Overall procurement health score."""
    overall_score: float
    grade: str  # A, B, C, D, F
    rating: str  # 'Excellent', 'Good', 'Fair', 'Poor', 'Critical'
    category_scores: Dict[str, CategoryScore]
    top_strengths: List[str]
    priority_improvements: List[str]
    peer_comparison: Dict
    calculated_at: datetime


class BenchmarkingEngine:
    """Engine for calculating procurement benchmarks and health scores."""

    def __init__(self):
        self.benchmarks = COUPA_BENCHMARKS
        self.categories = BENCHMARK_CATEGORIES
        self.importance_weights = IMPORTANCE_WEIGHTS

    def score_kpi(self, kpi_id: str, actual_value: float) -> KPIScore:
        """Score a single KPI against its benchmark."""
        if kpi_id not in self.benchmarks:
            raise ValueError(f"Unknown KPI: {kpi_id}")

        kpi = self.benchmarks[kpi_id]
        benchmark = kpi['benchmark_value']
        direction = kpi['direction']

        # Calculate gap
        if direction == 'higher':
            gap = actual_value - benchmark
            gap_percent = (gap / benchmark * 100) if benchmark > 0 else 0
            # Score: 100 if at or above benchmark, scaled down if below
            if actual_value >= benchmark:
                score = min(100, 100 + (gap_percent * 0.1))  # Bonus for exceeding
            else:
                score = max(0, (actual_value / benchmark) * 100)
        else:  # lower is better
            gap = benchmark - actual_value
            gap_percent = (gap / benchmark * 100) if benchmark > 0 else 0
            # Score: 100 if at or below benchmark, scaled down if above
            if actual_value <= benchmark:
                score = min(100, 100 + (gap_percent * 0.1))  # Bonus for exceeding
            else:
                score = max(0, (benchmark / actual_value) * 100) if actual_value > 0 else 0

        # Determine rating
        if score >= 90:
            rating = 'Excellent'
        elif score >= 75:
            rating = 'Good'
        elif score >= 60:
            rating = 'Fair'
        elif score >= 40:
            rating = 'Poor'
        else:
            rating = 'Critical'

        # Generate recommendation
        recommendation = self._generate_recommendation(kpi_id, actual_value, benchmark, direction, rating)

        return KPIScore(
            kpi_id=kpi_id,
            name=kpi['name'],
            category=kpi['category'],
            actual_value=actual_value,
            benchmark_value=benchmark,
            unit=kpi['unit'],
            direction=direction,
            score=round(score, 1),
            gap=round(gap, 2),
            gap_percent=round(gap_percent, 1),
            rating=rating,
            recommendation=recommendation
        )

    def _generate_recommendation(self, kpi_id: str, actual: float, benchmark: float,
                                  direction: str, rating: str) -> str:
        """Generate improvement recommendation for a KPI."""
        if rating in ['Excellent', 'Good']:
            return "Maintain current performance. Consider documenting best practices."

        kpi = self.benchmarks[kpi_id]
        name = kpi['name']

        if direction == 'higher':
            gap = benchmark - actual
            if 'cycle_time' in kpi_id.lower():
                return f"Reduce {name} by {gap:.1f} {kpi['unit'].replace('_', ' ')} to meet benchmark."
            else:
                return f"Increase {name} by {gap:.1f}% to reach the industry benchmark of {benchmark}%."
        else:
            gap = actual - benchmark
            return f"Reduce {name} by {gap:.1f} {kpi['unit'].replace('_', ' ')} to meet industry best practices."

    def score_category(self, category_id: str, kpi_values: Dict[str, float]) -> CategoryScore:
        """Score all KPIs in a category."""
        category_kpis = {k: v for k, v in self.benchmarks.items() if v['category'] == category_id}

        kpi_scores = []
        weighted_sum = 0
        total_weight = 0

        for kpi_id in category_kpis:
            if kpi_id in kpi_values:
                kpi_score = self.score_kpi(kpi_id, kpi_values[kpi_id])
                kpi_scores.append(kpi_score)

                # Apply importance weight
                weight = self.importance_weights.get(
                    self.benchmarks[kpi_id].get('importance', 'medium'), 1.0
                )
                weighted_sum += kpi_score.score * weight
                total_weight += weight

        category_score = weighted_sum / total_weight if total_weight > 0 else 0

        # Identify strengths and improvement areas
        strengths = [s.name for s in kpi_scores if s.rating in ['Excellent', 'Good']]
        improvements = [s.name for s in kpi_scores if s.rating in ['Poor', 'Critical']]

        return CategoryScore(
            category_id=category_id,
            category_name=self.categories.get(category_id, category_id),
            score=round(category_score, 1),
            kpi_scores=kpi_scores,
            strengths=strengths,
            improvement_areas=improvements
        )

    def calculate_health_score(self, kpi_values: Dict[str, float],
                                peer_data: Optional[Dict] = None) -> ProcurementHealthScore:
        """Calculate overall procurement health score."""
        category_scores = {}
        all_kpi_scores = []

        # Score each category
        for category_id in self.categories:
            category_score = self.score_category(category_id, kpi_values)
            category_scores[category_id] = category_score
            all_kpi_scores.extend(category_score.kpi_scores)

        # Calculate overall weighted score
        category_weights = {
            'spend_analysis': 1.0,
            'supplier_management': 1.0,
            'source_to_contract': 1.5,  # Higher weight for contract management
            'procurement': 2.0,  # Highest weight for core procurement
            'cash_liquidity': 0.8,
            'e_invoicing': 1.2,
            'expenses': 0.8,
            'payments': 1.0
        }

        weighted_sum = 0
        total_weight = 0
        for cat_id, cat_score in category_scores.items():
            weight = category_weights.get(cat_id, 1.0)
            if cat_score.kpi_scores:  # Only include if we have data
                weighted_sum += cat_score.score * weight
                total_weight += weight

        overall_score = weighted_sum / total_weight if total_weight > 0 else 0

        # Determine grade and rating
        if overall_score >= 90:
            grade, rating = 'A', 'Excellent'
        elif overall_score >= 80:
            grade, rating = 'B', 'Good'
        elif overall_score >= 70:
            grade, rating = 'C', 'Fair'
        elif overall_score >= 60:
            grade, rating = 'D', 'Poor'
        else:
            grade, rating = 'F', 'Critical'

        # Top strengths (top 5 highest scoring KPIs)
        sorted_scores = sorted(all_kpi_scores, key=lambda x: x.score, reverse=True)
        top_strengths = [f"{s.name}: {s.score:.0f}/100" for s in sorted_scores[:5] if s.rating in ['Excellent', 'Good']]

        # Priority improvements (bottom 5 or those rated Poor/Critical)
        priority_improvements = [
            f"{s.name}: {s.recommendation}"
            for s in sorted_scores
            if s.rating in ['Poor', 'Critical']
        ][:5]

        # Peer comparison
        peer_comparison = self._calculate_peer_comparison(overall_score, peer_data) if peer_data else {}

        return ProcurementHealthScore(
            overall_score=round(overall_score, 1),
            grade=grade,
            rating=rating,
            category_scores=category_scores,
            top_strengths=top_strengths,
            priority_improvements=priority_improvements,
            peer_comparison=peer_comparison,
            calculated_at=datetime.now()
        )

    def _calculate_peer_comparison(self, score: float, peer_data: Dict) -> Dict:
        """Compare score against peer entities."""
        peer_scores = peer_data.get('scores', [])
        if not peer_scores:
            return {}

        peer_scores.append(score)
        peer_scores.sort(reverse=True)
        rank = peer_scores.index(score) + 1

        return {
            'rank': rank,
            'total_peers': len(peer_scores),
            'percentile': round((len(peer_scores) - rank) / len(peer_scores) * 100, 1),
            'peer_average': round(sum(peer_scores) / len(peer_scores), 1),
            'peer_median': round(peer_scores[len(peer_scores) // 2], 1),
            'vs_average': round(score - sum(peer_scores) / len(peer_scores), 1)
        }

    def get_benchmark_summary(self) -> Dict:
        """Get summary of all benchmarks for display."""
        summary = {
            'total_kpis': len(self.benchmarks),
            'categories': {},
            'kpis': []
        }

        for category_id, category_name in self.categories.items():
            category_kpis = [
                {
                    'id': k,
                    'name': v['name'],
                    'benchmark': v['benchmark_value'],
                    'unit': v['unit'],
                    'direction': v['direction'],
                    'importance': v.get('importance', 'medium'),
                    'description': v.get('description', '')
                }
                for k, v in self.benchmarks.items()
                if v['category'] == category_id
            ]
            summary['categories'][category_id] = {
                'name': category_name,
                'kpi_count': len(category_kpis),
                'kpis': category_kpis
            }
            summary['kpis'].extend(category_kpis)

        return summary

    def estimate_kpis_from_contracts(self, contracts: List[Dict], payments: List[Dict] = None,
                                      vendors: List[Dict] = None) -> Dict[str, float]:
        """Estimate KPI values from existing contract/payment/vendor data."""
        estimated = {}

        if not contracts:
            return estimated

        total_contracts = len(contracts)
        total_value = sum(c.get('current_amount', 0) or 0 for c in contracts)

        # On-Contract Spend (assume all tracked contracts are "on-contract")
        # In reality, this would compare against total organizational spend
        estimated['on_contract_spend'] = 75.0  # Placeholder - would need total spend data

        # Structured Spend (contracts through formal procurement)
        formal_procurement = sum(1 for c in contracts
                                  if c.get('procurement_method') in ['Competitive Bid', 'RFP', 'ITB', 'RFQ'])
        if total_contracts > 0:
            estimated['structured_spend'] = (formal_procurement / total_contracts) * 100

        # Pre-Approved Spend (contracts with board approval)
        approved = sum(1 for c in contracts if c.get('board_approval_date'))
        if total_contracts > 0:
            estimated['pre_approved_spend'] = (approved / total_contracts) * 100

        # Contract Management Cycle Time (average days from solicitation to award)
        cycle_times = []
        for c in contracts:
            if c.get('solicitation_date') and c.get('award_date'):
                try:
                    from datetime import datetime
                    sol = datetime.fromisoformat(c['solicitation_date'][:10])
                    award = datetime.fromisoformat(c['award_date'][:10])
                    days = (award - sol).days
                    if days > 0:
                        cycle_times.append(days)
                except:
                    pass
        if cycle_times:
            estimated['contract_mgmt_cycle_time'] = sum(cycle_times) / len(cycle_times)

        # Spend with Primary Suppliers
        if vendors and total_value > 0:
            vendor_spend = {}
            for c in contracts:
                vid = c.get('vendor_id')
                if vid:
                    vendor_spend[vid] = vendor_spend.get(vid, 0) + (c.get('current_amount', 0) or 0)

            # Top 20% of vendors by spend
            sorted_vendors = sorted(vendor_spend.values(), reverse=True)
            top_count = max(1, len(sorted_vendors) // 5)
            primary_spend = sum(sorted_vendors[:top_count])
            estimated['spend_with_primary_suppliers'] = (primary_spend / total_value) * 100

        # Electronic PO Processing (assume electronic if in system)
        estimated['electronic_po_processing'] = 85.0  # Reasonable default

        # Payment metrics
        if payments:
            digital_payments = sum(1 for p in payments
                                   if p.get('payment_type') in ['ACH', 'Wire', 'EFT', 'Digital'])
            if len(payments) > 0:
                estimated['invoices_paid_digitally'] = (digital_payments / len(payments)) * 100

        return estimated


# Global instance
_benchmarking_engine = None


def get_benchmarking_engine() -> BenchmarkingEngine:
    """Get or create benchmarking engine instance."""
    global _benchmarking_engine
    if _benchmarking_engine is None:
        _benchmarking_engine = BenchmarkingEngine()
    return _benchmarking_engine
