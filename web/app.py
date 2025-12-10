"""
Contract Oversight System - Web Application
A transparency dashboard for school boards and city councils to monitor contracts and spending.
"""

import os
import sys
import io
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
import base64

from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, session, Response
from flask_cors import CORS
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_database
from src.scoring_engine import ContractScoringEngine, VendorScoringEngine, AlertGenerator
from src.benchmarking import get_benchmarking_engine, COUPA_BENCHMARKS, BENCHMARK_CATEGORIES

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'contract-oversight-dev-key')
CORS(app)

# Initialize components
db = get_database()
contract_scorer = ContractScoringEngine()
vendor_scorer = VendorScoringEngine()
alert_generator = AlertGenerator()

# Global data cache
current_contracts = None
current_vendors = None


def load_data():
    """Load and score all contracts."""
    global current_contracts, current_vendors

    current_contracts = db.get_all_contracts()
    if not current_contracts.empty:
        current_contracts = contract_scorer.batch_score_contracts(current_contracts)
        logger.info(f"Loaded {len(current_contracts)} contracts")

    current_vendors = db.get_all_vendors()
    logger.info(f"Loaded {len(current_vendors)} vendors")


def get_portfolio_summary(df: pd.DataFrame) -> Dict:
    """Generate summary statistics."""
    if df is None or df.empty:
        return {}

    total_value = float(df['current_amount'].sum())
    total_paid = float(df['total_paid'].sum())
    original_value = float(df['original_amount'].sum())

    return {
        'total_contracts': len(df),
        'total_value': total_value,
        'total_paid': total_paid,
        'total_remaining': total_value - total_paid,
        'original_value': original_value,
        'total_overrun': total_value - original_value,
        'overrun_pct': ((total_value - original_value) / original_value * 100) if original_value > 0 else 0,
        'avg_health_score': float(df['overall_health_score'].mean()) if 'overall_health_score' in df.columns else 50,
        'at_risk_count': len(df[df['overall_health_score'] < 50]) if 'overall_health_score' in df.columns else 0,
        'critical_count': len(df[df['overall_health_score'] < 30]) if 'overall_health_score' in df.columns else 0,
        'active_count': len(df[df['status'] == 'Active']),
        'completed_count': len(df[df['status'] == 'Completed']),
        'status_distribution': df['status'].value_counts().to_dict() if 'status' in df.columns else {},
        'department_distribution': df['department'].value_counts().to_dict() if 'department' in df.columns else {},
        'type_distribution': df['contract_type'].value_counts().to_dict() if 'contract_type' in df.columns else {}
    }


# Initialize data on startup
load_data()


# ==========================
# ROUTES - DASHBOARD
# ==========================

@app.route('/')
def index():
    return redirect(url_for('dashboard'))


@app.route('/dashboard')
def dashboard():
    """Main oversight dashboard."""
    global current_contracts

    if current_contracts is None:
        load_data()

    summary = get_portfolio_summary(current_contracts)

    # Get alerts
    contracts_list = current_contracts.to_dict('records') if current_contracts is not None else []
    alerts = alert_generator.generate_alerts(contracts_list)

    # Get at-risk contracts
    at_risk = []
    if current_contracts is not None and 'overall_health_score' in current_contracts.columns:
        at_risk_df = current_contracts[current_contracts['overall_health_score'] < 50].sort_values('overall_health_score')
        at_risk = at_risk_df.head(10).to_dict('records')

    # Recent changes
    recent_changes = db.get_recent_changes(limit=10)

    # Stats
    stats = db.get_statistics()

    return render_template('dashboard.html',
                          summary=summary,
                          alerts=alerts[:10],
                          at_risk_contracts=at_risk,
                          recent_changes=recent_changes,
                          stats=stats,
                          title='Contract Oversight Dashboard')


@app.route('/executive')
def executive_dashboard():
    """Executive dashboard for commissioners and board members."""
    global current_contracts, current_vendors

    if current_contracts is None:
        load_data()

    summary = get_portfolio_summary(current_contracts)

    # Calculate overall health score
    overall_health = int(summary.get('avg_health_score', 50))

    # Get alerts for critical items
    contracts_list_data = current_contracts.to_dict('records') if current_contracts is not None else []
    all_alerts = alert_generator.generate_alerts(contracts_list_data)

    # Critical items requiring immediate action
    critical_items = []
    for alert in all_alerts:
        if alert.get('severity') in ['Critical', 'High']:
            critical_items.append({
                'title': alert.get('contract_title', 'Unknown Contract')[:40],
                'reason': alert.get('title', 'Issue detected')
            })

    # Top contracts by value
    top_contracts = []
    if current_contracts is not None and not current_contracts.empty:
        top_df = current_contracts.nlargest(10, 'current_amount')
        top_contracts = top_df.to_dict('records')

    # Calculate on-time rate (contracts not delayed)
    on_time_rate = 85  # Placeholder - would calculate from actual milestone data
    if current_contracts is not None and 'schedule_health' in current_contracts.columns:
        good_schedule = len(current_contracts[current_contracts['schedule_health'] >= 70])
        on_time_rate = int(good_schedule / len(current_contracts) * 100) if len(current_contracts) > 0 else 85

    # Vendor count
    vendor_count = len(current_vendors) if current_vendors is not None else 0

    # Expiring contracts (within 90 days)
    expiring_soon = 0
    upcoming_deadlines = []
    if current_contracts is not None and 'end_date' in current_contracts.columns:
        from datetime import datetime, timedelta
        now = datetime.now()
        ninety_days = now + timedelta(days=90)
        for _, row in current_contracts.iterrows():
            try:
                end_date = pd.to_datetime(row['end_date'])
                if pd.notna(end_date) and now <= end_date <= ninety_days:
                    expiring_soon += 1
                    days_until = (end_date - now).days
                    upcoming_deadlines.append({
                        'title': row.get('title', 'Unknown'),
                        'days_until': days_until
                    })
            except:
                pass
        upcoming_deadlines = sorted(upcoming_deadlines, key=lambda x: x['days_until'])[:10]

    # Change order count
    change_order_count = 0
    if current_contracts is not None and 'change_order_count' in current_contracts.columns:
        change_order_count = int(current_contracts['change_order_count'].sum())

    # Pending approvals (placeholder)
    pending_approvals = 3

    # Pending board items (placeholder - would come from approval workflow)
    pending_board_items = []
    if current_contracts is not None and not current_contracts.empty:
        # Show contracts with status 'Draft' as pending
        draft_df = current_contracts[current_contracts['status'] == 'Draft'] if 'status' in current_contracts.columns else pd.DataFrame()
        for _, row in draft_df.head(5).iterrows():
            pending_board_items.append({
                'title': row.get('title', 'Unknown'),
                'amount': row.get('current_amount', 0)
            })

    # Monthly spending trend
    monthly_spending = {'months': [], 'amounts': []}
    # Generate sample monthly data for visualization
    import calendar
    from datetime import datetime
    now = datetime.now()
    for i in range(6, 0, -1):
        month_idx = (now.month - i) % 12
        if month_idx == 0:
            month_idx = 12
        month_name = calendar.month_abbr[month_idx]
        monthly_spending['months'].append(month_name)
        # Sample amounts based on total value distributed
        base_amount = (summary.get('total_value', 0) or 0) / 12
        variance = 0.8 + (i * 0.05)  # Slight upward trend
        monthly_spending['amounts'].append(base_amount * variance)

    # School district stats
    school_stats = {'active_contracts': 0, 'total_value': 0, 'top_contracts': []}
    if current_contracts is not None and 'department' in current_contracts.columns:
        school_df = current_contracts[current_contracts['department'].str.contains('School|Education', case=False, na=False)]
        if not school_df.empty:
            school_stats['active_contracts'] = len(school_df[school_df['status'] == 'Active']) if 'status' in school_df.columns else len(school_df)
            school_stats['total_value'] = float(school_df['current_amount'].sum())
            school_stats['top_contracts'] = school_df.nlargest(3, 'current_amount').to_dict('records')

    # County comparison data
    county_comparison = None
    try:
        comparison_data = db.get_county_comparison_data()
        if comparison_data and 'counties' in comparison_data:
            counties = comparison_data['counties']
            marion_data = next((c for c in counties if c.get('county_id') == 'marion'), None)
            if marion_data:
                per_capita_values = [c.get('expenditures_per_capita', 0) for c in counties if c.get('expenditures_per_capita')]
                avg_per_capita = sum(per_capita_values) / len(per_capita_values) if per_capita_values else 0

                # Calculate rank
                sorted_counties = sorted(counties, key=lambda x: x.get('expenditures_per_capita', 0))
                rank = next((i+1 for i, c in enumerate(sorted_counties) if c.get('county_id') == 'marion'), 0)

                county_comparison = {
                    'per_capita': marion_data.get('expenditures_per_capita', 0),
                    'peer_avg': avg_per_capita,
                    'per_capita_rank': rank,
                    'total_counties': len(counties)
                }
    except Exception as e:
        logger.warning(f"Could not load county comparison: {e}")

    # Key Performance Indicators
    kpis = {
        'avg_duration_months': 24,
        'change_order_rate': 0,
        'unique_vendors': vendor_count,
        'local_vendor_pct': 65,
        'competitive_bid_pct': 78,
        'avg_vendor_score': 72
    }

    if current_contracts is not None and len(current_contracts) > 0:
        contracts_with_co = len(current_contracts[current_contracts['change_order_count'] > 0]) if 'change_order_count' in current_contracts.columns else 0
        kpis['change_order_rate'] = int(contracts_with_co / len(current_contracts) * 100)

        # Average duration
        if 'start_date' in current_contracts.columns and 'end_date' in current_contracts.columns:
            durations = []
            for _, row in current_contracts.iterrows():
                try:
                    start = pd.to_datetime(row['start_date'])
                    end = pd.to_datetime(row['end_date'])
                    if pd.notna(start) and pd.notna(end):
                        durations.append((end - start).days / 30)
                except:
                    pass
            if durations:
                kpis['avg_duration_months'] = int(sum(durations) / len(durations))

    return render_template('executive_dashboard.html',
                          summary=summary,
                          overall_health=overall_health,
                          critical_items=critical_items[:5],
                          top_contracts=top_contracts,
                          on_time_rate=on_time_rate,
                          vendor_count=vendor_count,
                          expiring_soon=expiring_soon,
                          change_order_count=change_order_count,
                          pending_approvals=pending_approvals,
                          upcoming_deadlines=upcoming_deadlines,
                          pending_board_items=pending_board_items,
                          monthly_spending=monthly_spending,
                          school_stats=school_stats,
                          county_comparison=county_comparison,
                          kpis=kpis,
                          report_date=datetime.now().strftime('%B %d, %Y'),
                          last_updated=datetime.now().strftime('%I:%M %p'),
                          title='Executive Dashboard')


@app.route('/contracts')
def contracts_list():
    """All contracts view with filtering and saved searches."""
    global current_contracts, current_vendors

    if current_contracts is None:
        load_data()

    df = current_contracts.copy() if current_contracts is not None else pd.DataFrame()

    # Apply filters
    search = request.args.get('search', '').strip()
    status = request.args.get('status', '')
    department = request.args.get('department', '')
    health = request.args.get('health', '')

    if search and not df.empty:
        mask = df['title'].str.contains(search, case=False, na=False) | \
               df['contract_number'].str.contains(search, case=False, na=False)
        df = df[mask]

    if status and not df.empty:
        df = df[df['status'] == status]

    if department and not df.empty:
        df = df[df['department'] == department]

    if health and not df.empty and 'overall_health_score' in df.columns:
        if health == 'critical':
            df = df[df['overall_health_score'] < 30]
        elif health == 'at_risk':
            df = df[(df['overall_health_score'] >= 30) & (df['overall_health_score'] < 50)]
        elif health == 'warning':
            df = df[(df['overall_health_score'] >= 50) & (df['overall_health_score'] < 70)]
        elif health == 'healthy':
            df = df[df['overall_health_score'] >= 70]

    contracts = df.to_dict('records') if not df.empty else []

    # Calculate summary stats
    total_value = float(df['current_amount'].sum()) if not df.empty else 0
    at_risk_count = len(df[df['overall_health_score'] < 50]) if not df.empty and 'overall_health_score' in df.columns else 0
    overrun_count = len(df[df['current_amount'] > df['original_amount']]) if not df.empty else 0

    # Get unique departments for filter
    departments = current_contracts['department'].unique().tolist() if current_contracts is not None and 'department' in current_contracts.columns else []

    # Get vendors for add contract form
    vendors = current_vendors.to_dict('records') if current_vendors is not None else []

    # Get saved searches from localStorage (passed via cookies/session in real app)
    saved_searches = session.get('saved_searches', [])

    return render_template('contracts.html',
                          contracts=contracts,
                          departments=departments,
                          vendors=vendors,
                          total_value=total_value,
                          at_risk_count=at_risk_count,
                          overrun_count=overrun_count,
                          saved_searches=saved_searches,
                          title='All Contracts')


@app.route('/contract/<contract_id>')
def contract_detail(contract_id):
    """Single contract detail view."""
    contract = db.get_contract(contract_id)
    if not contract:
        return "Contract not found", 404

    # Score the contract
    contract = contract_scorer.score_contract(contract)

    # Get related data
    milestones = db.get_milestones(contract_id)
    milestone_stats = db.get_milestone_stats(contract_id)
    change_orders = db.get_change_orders(contract_id)
    payments = db.get_payments(contract_id)
    comments = db.get_comments(contract_id)
    audit_log = db.get_audit_log(table_name='contracts', record_id=contract_id, limit=20)
    issues = db.get_issues(contract_id=contract_id)

    # Get vendor info if exists
    vendor = None
    if contract.get('vendor_id'):
        vendor = db.get_vendor(contract['vendor_id'])

    # Extract scores for template
    health_score = contract.get('overall_health_score', 50)
    scores = {
        'cost_variance_score': contract.get('cost_variance_score', 50),
        'schedule_variance_score': contract.get('schedule_variance_score', 50),
        'performance_score': contract.get('performance_score', 50),
        'compliance_score': contract.get('compliance_score', 50)
    }

    # Calculate total change order amount
    total_co_amount = sum(co.get('amount', 0) for co in change_orders)

    return render_template('contract_detail.html',
                          contract=contract,
                          milestones=milestones,
                          milestone_stats=milestone_stats,
                          change_orders=change_orders,
                          payments=payments,
                          comments=comments,
                          audit_log=audit_log,
                          issues=issues,
                          vendor=vendor,
                          health_score=health_score,
                          scores=scores,
                          total_co_amount=total_co_amount,
                          title=contract.get('title', 'Contract Detail'))


@app.route('/vendors')
def vendors_list():
    """All vendors view."""
    global current_vendors

    if current_vendors is None:
        load_data()

    vendors = current_vendors.to_dict('records') if current_vendors is not None else []

    # Add contract counts and metrics
    performance_scores = []
    for vendor in vendors:
        contracts = db.get_vendor_contracts(vendor['vendor_id'])
        vendor['contract_count'] = len(contracts)
        vendor['total_value'] = sum(c.get('current_amount', 0) or 0 for c in contracts)
        metrics = vendor_scorer.get_vendor_metrics(vendor, contracts)
        vendor.update(metrics)
        if 'performance_score' in vendor:
            performance_scores.append(vendor['performance_score'])

    # Calculate summary stats for template
    active_count = sum(1 for v in vendors if v.get('status') == 'Active')

    # Check insurance expiring (within 30 days)
    from datetime import datetime, timedelta
    today = datetime.now().date()
    expiring_threshold = today + timedelta(days=30)
    expiring_insurance = 0
    for v in vendors:
        if v.get('insurance_expiry'):
            try:
                exp_date = datetime.strptime(str(v['insurance_expiry']), '%Y-%m-%d').date()
                if today <= exp_date <= expiring_threshold:
                    expiring_insurance += 1
            except:
                pass

    avg_score = sum(performance_scores) / len(performance_scores) if performance_scores else 0

    # Get unique categories
    categories = list(set(v.get('category', '') for v in vendors if v.get('category')))

    return render_template('vendors.html',
                          vendors=vendors,
                          active_count=active_count,
                          expiring_insurance=expiring_insurance,
                          avg_score=avg_score,
                          categories=categories,
                          title='Vendor Management')


@app.route('/vendor/<vendor_id>')
def vendor_detail(vendor_id):
    """Single vendor detail view."""
    vendor = db.get_vendor(vendor_id)
    if not vendor:
        return "Vendor not found", 404

    contracts = db.get_vendor_contracts(vendor_id)
    metrics = vendor_scorer.get_vendor_metrics(vendor, contracts)
    vendor.update(metrics)

    # Calculate stats for template
    stats = {
        'total_contracts': len(contracts),
        'active_contracts': sum(1 for c in contracts if c.get('status') == 'Active'),
        'completed_contracts': sum(1 for c in contracts if c.get('status') == 'Completed'),
        'total_value': sum(c.get('current_amount', 0) or 0 for c in contracts),
        'total_paid': sum(c.get('total_paid', 0) or 0 for c in contracts)
    }

    # Performance score
    performance_score = vendor.get('performance_score', 70)

    # Check insurance/license status
    from datetime import datetime, timedelta
    today = datetime.now().date()
    expiring_threshold = today + timedelta(days=30)

    def get_status(expiry_date_str):
        if not expiry_date_str:
            return 'missing'
        try:
            exp_date = datetime.strptime(str(expiry_date_str), '%Y-%m-%d').date()
            if exp_date < today:
                return 'expired'
            elif exp_date <= expiring_threshold:
                return 'expiring'
            else:
                return 'valid'
        except:
            return 'missing'

    insurance_status = get_status(vendor.get('insurance_expiry'))
    license_status = get_status(vendor.get('license_expiry'))

    return render_template('vendor_detail.html',
                          vendor=vendor,
                          contracts=contracts,
                          stats=stats,
                          metrics=metrics,
                          performance_score=performance_score,
                          insurance_status=insurance_status,
                          license_status=license_status,
                          title=vendor.get('vendor_name', 'Vendor Detail'))


@app.route('/alerts')
def alerts_page():
    """Alerts and issues view."""
    global current_contracts

    if current_contracts is None:
        load_data()

    contracts_list = current_contracts.to_dict('records') if current_contracts is not None else []
    alerts = alert_generator.generate_alerts(contracts_list)

    # Get open issues from database and enrich with contract title
    issues = db.get_issues(status='Open')
    for issue in issues:
        if issue.get('contract_id'):
            contract = db.get_contract(issue['contract_id'])
            issue['contract_title'] = contract.get('title', 'Unknown') if contract else 'Unknown'
        else:
            issue['contract_title'] = 'N/A'

    return render_template('alerts.html',
                          alerts=alerts,
                          issues=issues,
                          title='Alerts & Issues')


@app.route('/public')
def public_dashboard():
    """Public transparency dashboard (read-only)."""
    global current_contracts

    if current_contracts is None:
        load_data()

    summary = get_portfolio_summary(current_contracts)
    contracts = current_contracts.to_dict('records') if current_contracts is not None else []

    # Filter to show only non-sensitive info
    public_contracts = []
    for c in contracts:
        public_contracts.append({
            'contract_id': c.get('contract_id'),
            'title': c.get('title'),
            'vendor_name': c.get('vendor_name'),
            'department': c.get('department'),
            'contract_type': c.get('contract_type'),
            'original_amount': c.get('original_amount'),
            'current_amount': c.get('current_amount'),
            'total_paid': c.get('total_paid'),
            'status': c.get('status'),
            'start_date': c.get('start_date'),
            'current_end_date': c.get('current_end_date'),
            'percent_complete': c.get('percent_complete'),
            'overall_health_score': c.get('overall_health_score'),
            'risk_level': c.get('risk_level'),
            'change_order_count': c.get('change_order_count', 0),
            'total_change_order_amount': c.get('total_change_order_amount', 0)
        })

    return render_template('public_dashboard.html',
                          summary=summary,
                          contracts=public_contracts,
                          title='Public Contract Transparency Dashboard')


# ==========================
# API ROUTES
# ==========================

@app.route('/api/contracts')
def api_contracts():
    """Get all contracts."""
    global current_contracts

    if current_contracts is None:
        load_data()

    return jsonify(current_contracts.to_dict('records') if current_contracts is not None else [])


@app.route('/api/contract/<contract_id>')
def api_contract(contract_id):
    """Get single contract."""
    contract = db.get_contract(contract_id)
    if contract:
        contract = contract_scorer.score_contract(contract)
        return jsonify(contract)
    return jsonify({'error': 'Not found'}), 404


@app.route('/api/contract/<contract_id>', methods=['PUT'])
def api_update_contract(contract_id):
    """Update a contract."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data'}), 400

    data['contract_id'] = contract_id
    db.save_contract(data, changed_by=data.get('changed_by', 'api'))
    load_data()

    return jsonify({'success': True})


@app.route('/api/contract', methods=['POST'])
def api_create_contract():
    """Create a new contract."""
    data = request.get_json()
    if not data or not data.get('contract_id'):
        return jsonify({'error': 'Contract ID required'}), 400

    existing = db.get_contract(data['contract_id'])
    if existing:
        return jsonify({'error': 'Contract ID already exists'}), 400

    db.save_contract(data, changed_by=data.get('changed_by', 'api'))
    load_data()

    return jsonify({'success': True, 'contract_id': data['contract_id']})


@app.route('/api/vendors')
def api_vendors():
    """Get all vendors."""
    global current_vendors

    if current_vendors is None:
        load_data()

    return jsonify(current_vendors.to_dict('records') if current_vendors is not None else [])


@app.route('/api/summary')
def api_summary():
    """Get portfolio summary."""
    global current_contracts

    if current_contracts is None:
        load_data()

    return jsonify(get_portfolio_summary(current_contracts))


@app.route('/api/alerts')
def api_alerts():
    """Get current alerts."""
    global current_contracts

    if current_contracts is None:
        load_data()

    contracts_list = current_contracts.to_dict('records') if current_contracts is not None else []
    alerts = alert_generator.generate_alerts(contracts_list)

    return jsonify(alerts)


@app.route('/api/contract/<contract_id>/milestones')
def api_milestones(contract_id):
    """Get milestones for a contract."""
    milestones = db.get_milestones(contract_id)
    return jsonify(milestones)


@app.route('/api/contract/<contract_id>/milestones', methods=['POST'])
def api_add_milestone(contract_id):
    """Add a milestone."""
    data = request.get_json()
    data['contract_id'] = contract_id
    milestone_id = db.add_milestone(data)
    return jsonify({'success': True, 'milestone_id': milestone_id})


@app.route('/api/contract/<contract_id>/milestones/stats')
def api_milestone_stats(contract_id):
    """Get milestone statistics for a contract."""
    stats = db.get_milestone_stats(contract_id)
    return jsonify(stats)


@app.route('/api/milestone/<int:milestone_id>', methods=['PUT'])
def api_update_milestone(milestone_id):
    """Update a milestone."""
    data = request.get_json()
    success = db.update_milestone(milestone_id, data)
    return jsonify({'success': success})


@app.route('/api/milestone/<int:milestone_id>', methods=['DELETE'])
def api_delete_milestone(milestone_id):
    """Delete a milestone."""
    success = db.delete_milestone(milestone_id)
    return jsonify({'success': success})


@app.route('/api/contract/<contract_id>/change-orders')
def api_change_orders(contract_id):
    """Get change orders for a contract."""
    change_orders = db.get_change_orders(contract_id)
    return jsonify(change_orders)


@app.route('/api/contract/<contract_id>/change-orders', methods=['POST'])
def api_add_change_order(contract_id):
    """Add a change order."""
    data = request.get_json()
    data['contract_id'] = contract_id
    co_id = db.add_change_order(data)
    load_data()  # Refresh to update contract totals
    return jsonify({'success': True, 'change_order_id': co_id})


@app.route('/api/contract/<contract_id>/comments')
def api_get_comments(contract_id):
    """Get comments for a contract."""
    comments = db.get_comments(contract_id)
    return jsonify(comments)


@app.route('/api/contract/<contract_id>/comments', methods=['POST'])
def api_add_comment(contract_id):
    """Add a comment."""
    data = request.get_json()
    comment_id = db.add_comment(
        contract_id=contract_id,
        content=data.get('content'),
        user_id=data.get('user_id'),
        user_name=data.get('user_name', 'Anonymous'),
        parent_id=data.get('parent_id')
    )
    return jsonify({'success': True, 'comment_id': comment_id})


@app.route('/api/export')
def api_export():
    """Export contracts to CSV."""
    global current_contracts

    if current_contracts is None:
        load_data()

    output = io.StringIO()
    current_contracts.to_csv(output, index=False)
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'contracts_export_{datetime.now().strftime("%Y%m%d")}.csv'
    )


@app.route('/api/export/excel')
def api_export_excel():
    """Export contracts to Excel with multiple sheets."""
    global current_contracts, current_vendors

    if current_contracts is None:
        load_data()

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Contracts sheet
        if current_contracts is not None and not current_contracts.empty:
            # Select columns for export
            export_cols = ['contract_id', 'contract_number', 'title', 'vendor_name', 'department',
                          'status', 'original_amount', 'current_amount', 'total_paid',
                          'start_date', 'end_date', 'contract_type', 'overall_health_score']
            available_cols = [c for c in export_cols if c in current_contracts.columns]
            contracts_df = current_contracts[available_cols].copy()
            contracts_df.to_excel(writer, sheet_name='Contracts', index=False)

        # Vendors sheet
        if current_vendors is not None and not current_vendors.empty:
            current_vendors.to_excel(writer, sheet_name='Vendors', index=False)

        # Summary sheet
        summary_data = {
            'Metric': [
                'Total Contracts',
                'Active Contracts',
                'Total Contract Value',
                'Total Paid',
                'Average Health Score',
                'At-Risk Contracts',
                'Total Vendors',
                'Report Date'
            ],
            'Value': [
                len(current_contracts) if current_contracts is not None else 0,
                len(current_contracts[current_contracts['status'] == 'Active']) if current_contracts is not None and 'status' in current_contracts.columns else 0,
                f"${current_contracts['current_amount'].sum():,.2f}" if current_contracts is not None else '$0',
                f"${current_contracts['total_paid'].sum():,.2f}" if current_contracts is not None else '$0',
                f"{current_contracts['overall_health_score'].mean():.1f}" if current_contracts is not None and 'overall_health_score' in current_contracts.columns else 'N/A',
                len(current_contracts[current_contracts['overall_health_score'] < 50]) if current_contracts is not None and 'overall_health_score' in current_contracts.columns else 0,
                len(current_vendors) if current_vendors is not None else 0,
                datetime.now().strftime('%Y-%m-%d %H:%M')
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

        # Department breakdown sheet
        if current_contracts is not None and 'department' in current_contracts.columns:
            dept_summary = current_contracts.groupby('department').agg({
                'contract_id': 'count',
                'current_amount': 'sum',
                'total_paid': 'sum'
            }).reset_index()
            dept_summary.columns = ['Department', 'Contract Count', 'Total Value', 'Total Paid']
            dept_summary.to_excel(writer, sheet_name='By Department', index=False)

    output.seek(0)

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'contracts_report_{datetime.now().strftime("%Y%m%d")}.xlsx'
    )


@app.route('/api/export/pdf')
def api_export_pdf():
    """Generate PDF report of contracts using HTML template."""
    global current_contracts

    if current_contracts is None:
        load_data()

    summary = get_portfolio_summary(current_contracts)

    # Get top contracts
    top_contracts = []
    if current_contracts is not None and not current_contracts.empty:
        top_df = current_contracts.nlargest(20, 'current_amount')
        top_contracts = top_df.to_dict('records')

    # Get at-risk contracts
    at_risk = []
    if current_contracts is not None and 'overall_health_score' in current_contracts.columns:
        at_risk_df = current_contracts[current_contracts['overall_health_score'] < 50].sort_values('overall_health_score')
        at_risk = at_risk_df.head(10).to_dict('records')

    # Department breakdown
    dept_breakdown = []
    if current_contracts is not None and 'department' in current_contracts.columns:
        for dept in current_contracts['department'].unique():
            dept_df = current_contracts[current_contracts['department'] == dept]
            dept_breakdown.append({
                'department': dept,
                'count': len(dept_df),
                'total_value': float(dept_df['current_amount'].sum()),
                'total_paid': float(dept_df['total_paid'].sum())
            })
        dept_breakdown = sorted(dept_breakdown, key=lambda x: x['total_value'], reverse=True)

    return render_template('report_pdf.html',
                          summary=summary,
                          top_contracts=top_contracts,
                          at_risk_contracts=at_risk,
                          dept_breakdown=dept_breakdown,
                          report_date=datetime.now().strftime('%B %d, %Y'),
                          title='Contract Portfolio Report')


@app.route('/api/statistics')
def api_statistics():
    """Get database statistics."""
    return jsonify(db.get_statistics())


# ==========================
# COMPARISON FEATURE
# ==========================

@app.route('/compare')
def compare_contracts():
    """Compare multiple contracts side by side."""
    contract_ids = request.args.getlist('contracts')

    if len(contract_ids) < 2:
        return redirect(url_for('contracts_list'))

    contracts_to_compare = []
    for cid in contract_ids[:4]:  # Max 4 contracts
        contract = db.get_contract(cid)
        if contract:
            contract = contract_scorer.score_contract(contract)
            # Get additional metrics
            change_orders = db.get_change_orders(cid)
            milestones = db.get_milestones(cid)
            payments = db.get_payments(cid)
            issues = db.get_issues(contract_id=cid)

            contract['change_order_count'] = len(change_orders)
            contract['total_co_amount'] = sum(co.get('amount', 0) for co in change_orders)
            contract['milestone_count'] = len(milestones)
            contract['completed_milestones'] = len([m for m in milestones if m.get('status') == 'Completed'])
            contract['payment_count'] = len(payments)
            contract['issue_count'] = len([i for i in issues if i.get('status') == 'Open'])

            contracts_to_compare.append(contract)

    return render_template('compare.html',
                          contracts=contracts_to_compare,
                          title='Contract Comparison')


# ==========================
# ANALYTICS DASHBOARD
# ==========================

@app.route('/analytics')
def analytics_dashboard():
    """Advanced analytics dashboard."""
    global current_contracts, current_vendors

    if current_contracts is None:
        load_data()

    df = current_contracts

    # Time-based analysis
    today = datetime.now()

    # Monthly spending trend (last 12 months)
    monthly_data = []
    for i in range(11, -1, -1):
        month_start = (today.replace(day=1) - timedelta(days=30*i)).replace(day=1)
        month_name = month_start.strftime('%b %Y')
        # Simulate spending based on contract dates
        monthly_data.append({
            'month': month_name,
            'spending': float(df['total_paid'].sum() / 12 * (1 + np.random.uniform(-0.2, 0.2)))
        })

    # Department analysis
    dept_analysis = []
    if 'department' in df.columns:
        for dept in df['department'].unique():
            dept_df = df[df['department'] == dept]
            dept_analysis.append({
                'department': dept,
                'contract_count': len(dept_df),
                'total_value': float(dept_df['current_amount'].sum()),
                'avg_health': float(dept_df['overall_health_score'].mean()) if 'overall_health_score' in dept_df.columns else 50,
                'overrun_amount': float((dept_df['current_amount'] - dept_df['original_amount']).sum()),
                'at_risk_count': len(dept_df[dept_df['overall_health_score'] < 50]) if 'overall_health_score' in dept_df.columns else 0
            })

    # Vendor performance ranking
    vendor_rankings = []
    if current_vendors is not None:
        for _, vendor in current_vendors.iterrows():
            contracts = db.get_vendor_contracts(vendor['vendor_id'])
            if contracts:
                metrics = vendor_scorer.get_vendor_metrics(vendor.to_dict(), contracts)
                vendor_rankings.append({
                    'vendor_id': vendor['vendor_id'],
                    'name': vendor.get('vendor_name', vendor.get('name', 'Unknown')),
                    'contract_count': len(contracts),
                    'total_value': sum(c.get('current_amount', 0) or 0 for c in contracts),
                    'performance_score': metrics.get('performance_score', 50),
                    'on_time_pct': metrics.get('on_time_pct', 0),
                    'budget_adherence': metrics.get('budget_adherence', 0)
                })
        vendor_rankings.sort(key=lambda x: x['performance_score'], reverse=True)

    # Risk distribution
    risk_distribution = {
        'Critical': len(df[df['overall_health_score'] < 30]) if 'overall_health_score' in df.columns else 0,
        'High': len(df[(df['overall_health_score'] >= 30) & (df['overall_health_score'] < 50)]) if 'overall_health_score' in df.columns else 0,
        'Medium': len(df[(df['overall_health_score'] >= 50) & (df['overall_health_score'] < 70)]) if 'overall_health_score' in df.columns else 0,
        'Low': len(df[df['overall_health_score'] >= 70]) if 'overall_health_score' in df.columns else 0
    }

    # Contract type analysis
    type_analysis = []
    if 'contract_type' in df.columns:
        for ctype in df['contract_type'].unique():
            type_df = df[df['contract_type'] == ctype]
            type_analysis.append({
                'type': ctype,
                'count': len(type_df),
                'total_value': float(type_df['current_amount'].sum()),
                'avg_overrun': float(((type_df['current_amount'] - type_df['original_amount']) / type_df['original_amount'] * 100).mean())
            })

    # Budget forecast (simple projection)
    total_budget = float(df['current_amount'].sum())
    total_paid = float(df['total_paid'].sum())
    avg_monthly_burn = total_paid / 6  # Assume 6 months of data
    months_remaining = (total_budget - total_paid) / avg_monthly_burn if avg_monthly_burn > 0 else 0

    forecast = {
        'total_budget': total_budget,
        'spent_to_date': total_paid,
        'remaining': total_budget - total_paid,
        'avg_monthly_burn': avg_monthly_burn,
        'projected_completion_months': months_remaining,
        'burn_rate_pct': (total_paid / total_budget * 100) if total_budget > 0 else 0
    }

    return render_template('analytics.html',
                          monthly_data=monthly_data,
                          dept_analysis=dept_analysis,
                          vendor_rankings=vendor_rankings[:10],
                          risk_distribution=risk_distribution,
                          type_analysis=type_analysis,
                          forecast=forecast,
                          summary=get_portfolio_summary(df),
                          title='Analytics Dashboard')


# ==========================
# PDF REPORTS
# ==========================

@app.route('/report/contract/<contract_id>')
def contract_report(contract_id):
    """Generate PDF report for a contract."""
    contract = db.get_contract(contract_id)
    if not contract:
        return "Contract not found", 404

    contract = contract_scorer.score_contract(contract)
    milestones = db.get_milestones(contract_id)
    change_orders = db.get_change_orders(contract_id)
    payments = db.get_payments(contract_id)
    issues = db.get_issues(contract_id=contract_id)

    vendor = None
    if contract.get('vendor_id'):
        vendor = db.get_vendor(contract['vendor_id'])

    return render_template('report_contract.html',
                          contract=contract,
                          milestones=milestones,
                          change_orders=change_orders,
                          payments=payments,
                          issues=issues,
                          vendor=vendor,
                          generated_at=datetime.now().strftime('%Y-%m-%d %H:%M'),
                          title=f"Contract Report - {contract.get('title')}")


@app.route('/report/portfolio')
def portfolio_report():
    """Generate portfolio summary report."""
    global current_contracts, current_vendors

    if current_contracts is None:
        load_data()

    summary = get_portfolio_summary(current_contracts)
    contracts = current_contracts.to_dict('records') if current_contracts is not None else []

    # Get alerts
    alerts = alert_generator.generate_alerts(contracts)

    # At-risk contracts
    at_risk = []
    if current_contracts is not None and 'overall_health_score' in current_contracts.columns:
        at_risk_df = current_contracts[current_contracts['overall_health_score'] < 50].sort_values('overall_health_score')
        at_risk = at_risk_df.to_dict('records')

    return render_template('report_portfolio.html',
                          summary=summary,
                          contracts=contracts,
                          alerts=alerts[:20],
                          at_risk=at_risk,
                          generated_at=datetime.now().strftime('%Y-%m-%d %H:%M'),
                          title='Portfolio Summary Report')


# ==========================
# VENDOR RATINGS
# ==========================

@app.route('/api/vendor/<vendor_id>/rating', methods=['POST'])
def api_rate_vendor(vendor_id):
    """Submit a vendor rating."""
    data = request.get_json()

    # Store rating (in a real app, this would go to a ratings table)
    rating = {
        'vendor_id': vendor_id,
        'contract_id': data.get('contract_id'),
        'quality_rating': data.get('quality_rating', 3),
        'timeliness_rating': data.get('timeliness_rating', 3),
        'communication_rating': data.get('communication_rating', 3),
        'value_rating': data.get('value_rating', 3),
        'comments': data.get('comments', ''),
        'rated_by': data.get('rated_by', 'Anonymous'),
        'rated_at': datetime.now().isoformat()
    }

    # Log the rating
    db.log_audit('vendor_ratings', vendor_id, 'CREATE',
                 new_values=rating, changed_by=rating['rated_by'])

    return jsonify({'success': True})


# ==========================
# BUDGET FORECASTING
# ==========================

@app.route('/api/forecast/<contract_id>')
def api_contract_forecast(contract_id):
    """Get budget forecast for a contract."""
    contract = db.get_contract(contract_id)
    if not contract:
        return jsonify({'error': 'Not found'}), 404

    payments = db.get_payments(contract_id)
    milestones = db.get_milestones(contract_id)

    # Calculate burn rate
    total_paid = sum(p.get('amount', 0) for p in payments)
    total_budget = contract.get('current_amount', contract.get('original_amount', 0))

    # Simple forecast based on percent complete and payments
    pct_complete = contract.get('percent_complete', 0) or 0

    if pct_complete > 0:
        projected_total = (total_paid / pct_complete) * 100
        variance = projected_total - total_budget
    else:
        projected_total = total_budget
        variance = 0

    # Milestone-based projection
    upcoming_milestones = [m for m in milestones if m.get('status') != 'Completed']
    upcoming_costs = sum(m.get('payment_amount', 0) or 0 for m in upcoming_milestones)

    forecast = {
        'total_budget': total_budget,
        'spent_to_date': total_paid,
        'remaining_budget': total_budget - total_paid,
        'percent_complete': pct_complete,
        'percent_spent': (total_paid / total_budget * 100) if total_budget > 0 else 0,
        'projected_total': projected_total,
        'projected_variance': variance,
        'variance_pct': (variance / total_budget * 100) if total_budget > 0 else 0,
        'upcoming_milestone_costs': upcoming_costs,
        'status': 'On Track' if variance <= total_budget * 0.1 else 'At Risk' if variance <= total_budget * 0.25 else 'Critical'
    }

    return jsonify(forecast)


# ==========================
# DOCUMENT MANAGEMENT
# ==========================

@app.route('/api/contract/<contract_id>/documents')
def api_get_documents(contract_id):
    """Get documents for a contract."""
    documents = db.get_documents(contract_id)
    return jsonify(documents)


@app.route('/api/contract/<contract_id>/documents', methods=['POST'])
def api_upload_document(contract_id):
    """Upload a document for a contract."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    # Save document metadata
    doc_data = {
        'contract_id': contract_id,
        'filename': file.filename,
        'document_type': request.form.get('document_type', 'Other'),
        'description': request.form.get('description', ''),
        'uploaded_by': request.form.get('uploaded_by', 'System'),
        'uploaded_at': datetime.now().isoformat(),
        'file_size': len(file.read())
    }
    file.seek(0)

    # In a real app, save the file to storage
    # For now, just log the metadata
    doc_id = db.add_document(doc_data)

    return jsonify({'success': True, 'document_id': doc_id})


# ==========================
# NOTIFICATIONS
# ==========================

@app.route('/notifications')
def notifications_page():
    """View notification settings and history."""
    # Get recent notifications
    notifications = db.get_notifications()

    return render_template('notifications.html',
                          notifications=notifications,
                          title='Notifications')


@app.route('/api/notifications/subscribe', methods=['POST'])
def api_subscribe_notifications():
    """Subscribe to notifications."""
    data = request.get_json()

    subscription = {
        'email': data.get('email'),
        'alert_types': data.get('alert_types', ['Critical', 'High']),
        'departments': data.get('departments', []),
        'frequency': data.get('frequency', 'immediate'),
        'created_at': datetime.now().isoformat()
    }

    # In a real app, save to database
    return jsonify({'success': True})


# ==========================
# CONTRACT TEMPLATES
# ==========================

@app.route('/templates')
def contract_templates():
    """Contract templates management."""
    return render_template('templates.html', title='Contract Templates')


# ==========================
# TIMELINE VIEW
# ==========================

@app.route('/timeline')
def timeline_view():
    """Contract timeline/Gantt view."""
    global current_contracts

    if current_contracts is None:
        load_data()

    df = current_contracts
    contracts = df.to_dict('records') if df is not None else []

    # Get all milestones
    all_milestones = []
    today = datetime.now().date()
    for contract in contracts:
        milestones = db.get_milestones(contract['contract_id'])
        for m in milestones:
            m['contract_title'] = contract['title']
            m['contract_id'] = contract['contract_id']
            # Calculate days_until for template
            if m.get('due_date'):
                try:
                    due = datetime.strptime(m['due_date'], '%Y-%m-%d').date()
                    m['days_until'] = (due - today).days
                except:
                    m['days_until'] = 999
            else:
                m['days_until'] = 999
            all_milestones.append(m)

    # Sort by due date
    all_milestones.sort(key=lambda x: x.get('due_date', '9999-12-31'))

    # Upcoming milestones (next 30 days)
    upcoming = [m for m in all_milestones
                if m.get('due_date') and m.get('status') != 'Completed'
                and m.get('days_until', 999) <= 30]

    # Get departments for filter
    departments = df['department'].unique().tolist() if 'department' in df.columns else []

    return render_template('timeline.html',
                          contracts=contracts,
                          milestones=all_milestones,
                          upcoming_milestones=upcoming[:10],
                          departments=departments,
                          title='Contract Timeline')


# ==========================
# SPENDING DASHBOARD
# ==========================

@app.route('/spending')
def spending_dashboard():
    """Spending trends and budget tracking."""
    global current_contracts

    if current_contracts is None:
        load_data()

    df = current_contracts

    # Get fiscal year filter
    year = request.args.get('year', datetime.now().year)
    fiscal_years = list(range(datetime.now().year - 2, datetime.now().year + 2))

    # Calculate spending summary
    total_budget = float(df['current_amount'].sum())
    total_spent = float(df['total_paid'].sum())

    summary = {
        'total_budget': total_budget,
        'total_spent': total_spent,
        'remaining': total_budget - total_spent,
        'spent_pct': (total_spent / total_budget * 100) if total_budget > 0 else 0,
        'total_contracts': len(df),
        'avg_monthly': total_spent / 12,
        'payment_count': 0  # Will calculate from payments
    }

    # Monthly spending trend
    monthly_spending = []
    cumulative = 0
    for i in range(12):
        month_name = (datetime(int(year), 1, 1) + timedelta(days=30*i)).strftime('%b')
        amount = total_spent / 12 * (1 + np.random.uniform(-0.3, 0.3))
        cumulative += amount
        monthly_spending.append({
            'month': month_name,
            'amount': amount,
            'cumulative': cumulative
        })

    # Department spending
    department_spending = []
    department_breakdown = []
    if 'department' in df.columns:
        for dept in df['department'].unique():
            dept_df = df[df['department'] == dept]
            budget = float(dept_df['current_amount'].sum())
            spent = float(dept_df['total_paid'].sum())
            department_spending.append({
                'name': dept,
                'spent': spent
            })
            department_breakdown.append({
                'name': dept,
                'budget': budget,
                'spent': spent,
                'remaining': budget - spent,
                'utilization': (spent / budget * 100) if budget > 0 else 0,
                'contract_count': len(dept_df)
            })

    # Top vendors by spending
    top_vendors = []
    if current_vendors is not None:
        for _, vendor in current_vendors.iterrows():
            contracts = db.get_vendor_contracts(vendor['vendor_id'])
            total = sum(c.get('total_paid', 0) or 0 for c in contracts)
            if total > 0:
                top_vendors.append({
                    'name': vendor.get('vendor_name', vendor.get('name', 'Unknown')),
                    'spent': total
                })
        top_vendors.sort(key=lambda x: x['spent'], reverse=True)

    # Recent payments (aggregate from all contracts)
    recent_payments = []
    for _, row in df.iterrows():
        payments = db.get_payments(row['contract_id'])
        for p in payments:
            p['contract_title'] = row['title']
            p['contract_id'] = row['contract_id']
            p['vendor_name'] = row.get('vendor_name', 'Unknown')
            recent_payments.append(p)

    recent_payments.sort(key=lambda x: x.get('payment_date', ''), reverse=True)
    summary['payment_count'] = len(recent_payments)

    return render_template('spending.html',
                          summary=summary,
                          monthly_spending=monthly_spending,
                          department_spending=department_spending,
                          department_breakdown=department_breakdown,
                          top_vendors=top_vendors[:10],
                          recent_payments=recent_payments[:20],
                          fiscal_years=fiscal_years,
                          current_year=int(year),
                          title='Spending Dashboard')


# ==========================
# AUDIT TRAIL
# ==========================

@app.route('/audit')
def audit_trail():
    """Audit trail viewer."""
    # Get filter parameters
    page = request.args.get('page', 1, type=int)
    per_page = 50
    table_filter = request.args.get('table', '')
    action_filter = request.args.get('action', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    # Get audit logs
    logs = db.get_audit_log(limit=1000)

    # Apply filters
    if table_filter:
        logs = [l for l in logs if l.get('table_name') == table_filter]
    if action_filter:
        logs = [l for l in logs if l.get('action') == action_filter]
    if date_from:
        logs = [l for l in logs if l.get('changed_at', '') >= date_from]
    if date_to:
        logs = [l for l in logs if l.get('changed_at', '') <= date_to + ' 23:59:59']

    # Calculate stats
    stats = {
        'total_changes': len(logs),
        'creates': len([l for l in logs if l.get('action') == 'CREATE']),
        'updates': len([l for l in logs if l.get('action') == 'UPDATE']),
        'deletes': len([l for l in logs if l.get('action') == 'DELETE']),
        'unique_users': len(set(l.get('changed_by', 'Unknown') for l in logs))
    }

    # Paginate
    total_pages = (len(logs) + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    paginated_logs = logs[start_idx:start_idx + per_page]

    # Get unique values for filters
    all_tables = list(set(l.get('table_name', '') for l in db.get_audit_log(limit=1000)))
    all_actions = ['CREATE', 'UPDATE', 'DELETE']

    return render_template('audit.html',
                          logs=paginated_logs,
                          stats=stats,
                          page=page,
                          total_pages=total_pages,
                          tables=all_tables,
                          actions=all_actions,
                          current_table=table_filter,
                          current_action=action_filter,
                          date_from=date_from,
                          date_to=date_to,
                          title='Audit Trail')


# ==========================
# CONTRACT RENEWALS
# ==========================

@app.route('/renewals')
def renewals_dashboard():
    """Contract renewal tracking."""
    global current_contracts

    if current_contracts is None:
        load_data()

    df = current_contracts
    today = datetime.now()

    # Find contracts expiring soon
    expiring_soon = []
    expired = []
    upcoming_renewals = []

    for _, row in df.iterrows():
        end_date_str = row.get('current_end_date') or row.get('end_date')
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                days_until = (end_date - today).days

                contract_info = row.to_dict()
                contract_info['days_until_expiry'] = days_until
                contract_info['end_date_formatted'] = end_date.strftime('%b %d, %Y')

                if days_until < 0:
                    expired.append(contract_info)
                elif days_until <= 30:
                    expiring_soon.append(contract_info)
                elif days_until <= 90:
                    upcoming_renewals.append(contract_info)
            except:
                pass

    # Sort by days until expiry
    expiring_soon.sort(key=lambda x: x['days_until_expiry'])
    upcoming_renewals.sort(key=lambda x: x['days_until_expiry'])

    # Calculate renewal statistics
    stats = {
        'expired_count': len(expired),
        'expiring_30_days': len(expiring_soon),
        'expiring_90_days': len(upcoming_renewals),
        'expired_value': sum(c.get('current_amount', 0) or 0 for c in expired),
        'expiring_value': sum(c.get('current_amount', 0) or 0 for c in expiring_soon + upcoming_renewals)
    }

    return render_template('renewals.html',
                          expired=expired,
                          expiring_soon=expiring_soon,
                          upcoming_renewals=upcoming_renewals,
                          stats=stats,
                          title='Contract Renewals')


# ==========================
# VENDOR PERFORMANCE
# ==========================

@app.route('/vendor-performance')
def vendor_performance_dashboard():
    """Comprehensive vendor performance dashboard."""
    global current_vendors

    if current_vendors is None:
        load_data()

    vendor_data = []
    for _, vendor in current_vendors.iterrows():
        contracts = db.get_vendor_contracts(vendor['vendor_id'])
        if contracts:
            metrics = vendor_scorer.get_vendor_metrics(vendor.to_dict(), contracts)

            # Calculate additional metrics
            total_value = sum(c.get('current_amount', 0) or 0 for c in contracts)
            total_paid = sum(c.get('total_paid', 0) or 0 for c in contracts)

            vendor_data.append({
                'vendor_id': vendor['vendor_id'],
                'name': vendor.get('vendor_name', vendor.get('name', 'Unknown')),
                'category': vendor.get('category', 'General'),
                'contract_count': len(contracts),
                'active_contracts': len([c for c in contracts if c.get('status') == 'Active']),
                'total_value': total_value,
                'total_paid': total_paid,
                'performance_score': metrics.get('performance_score', 50),
                'on_time_pct': metrics.get('on_time_pct', 0),
                'budget_adherence': metrics.get('budget_adherence', 0),
                'quality_score': metrics.get('quality_score', 50),
                'insurance_status': 'Valid' if vendor.get('insurance_expiry') and vendor['insurance_expiry'] > datetime.now().strftime('%Y-%m-%d') else 'Expired',
                'license_status': 'Valid' if vendor.get('license_expiry') and vendor['license_expiry'] > datetime.now().strftime('%Y-%m-%d') else 'Expired'
            })

    # Sort by performance score
    vendor_data.sort(key=lambda x: x['performance_score'], reverse=True)

    # Calculate summary stats
    summary = {
        'total_vendors': len(vendor_data),
        'avg_performance': sum(v['performance_score'] for v in vendor_data) / len(vendor_data) if vendor_data else 0,
        'top_performers': len([v for v in vendor_data if v['performance_score'] >= 70]),
        'underperformers': len([v for v in vendor_data if v['performance_score'] < 50]),
        'expired_insurance': len([v for v in vendor_data if v['insurance_status'] == 'Expired']),
        'expired_license': len([v for v in vendor_data if v['license_status'] == 'Expired'])
    }

    # Performance distribution
    performance_dist = {
        'Excellent (80+)': len([v for v in vendor_data if v['performance_score'] >= 80]),
        'Good (60-79)': len([v for v in vendor_data if 60 <= v['performance_score'] < 80]),
        'Fair (40-59)': len([v for v in vendor_data if 40 <= v['performance_score'] < 60]),
        'Poor (<40)': len([v for v in vendor_data if v['performance_score'] < 40])
    }

    return render_template('vendor_performance.html',
                          vendors=vendor_data,
                          summary=summary,
                          performance_dist=performance_dist,
                          title='Vendor Performance')


# ==========================
# CONTRACT APPROVALS
# ==========================

@app.route('/approvals')
def approvals_page():
    """Contract approval workflow page."""
    global current_contracts

    if current_contracts is None:
        load_data()

    # Sample approval data (in real app, this would come from database)
    pending_approvals = [
        {
            'request_id': 'APR-001',
            'type': 'New Contract',
            'contract_id': 'CNT-001',
            'contract_title': 'School Renovation Project',
            'description': 'Request approval for new construction contract with BuildRight Inc.',
            'amount': 2500000,
            'requested_by': 'John Smith',
            'requested_at': '2024-12-01',
            'approval_chain': [
                {'approver': 'Dept. Manager', 'status': 'approved'},
                {'approver': 'Finance Dir.', 'status': 'pending'},
                {'approver': 'Executive Dir.', 'status': 'waiting'},
                {'approver': 'Board', 'status': 'waiting'}
            ]
        },
        {
            'request_id': 'APR-002',
            'type': 'Change Order',
            'contract_id': 'CNT-003',
            'contract_title': 'HVAC System Upgrade',
            'description': 'Additional ventilation requirements due to building code changes.',
            'amount': 85000,
            'requested_by': 'Sarah Johnson',
            'requested_at': '2024-12-03',
            'approval_chain': [
                {'approver': 'Dept. Manager', 'status': 'pending'},
                {'approver': 'Finance Dir.', 'status': 'waiting'}
            ]
        },
        {
            'request_id': 'APR-003',
            'type': 'Budget Increase',
            'contract_id': 'CNT-005',
            'contract_title': 'IT Infrastructure Modernization',
            'description': 'Request 15% budget increase due to supply chain cost increases.',
            'amount': 150000,
            'requested_by': 'Mike Chen',
            'requested_at': '2024-12-04',
            'approval_chain': [
                {'approver': 'Dept. Manager', 'status': 'approved'},
                {'approver': 'Finance Dir.', 'status': 'pending'},
                {'approver': 'Executive Dir.', 'status': 'waiting'}
            ]
        }
    ]

    my_requests = [
        {'request_id': 'APR-001', 'type': 'New Contract', 'contract_title': 'School Renovation Project', 'amount': 2500000, 'status': 'Pending', 'requested_at': '2024-12-01'},
        {'request_id': 'APR-004', 'type': 'Renewal', 'contract_title': 'Janitorial Services', 'amount': 125000, 'status': 'Approved', 'requested_at': '2024-11-28'},
        {'request_id': 'APR-005', 'type': 'Change Order', 'contract_title': 'Parking Lot Repair', 'amount': 25000, 'status': 'Rejected', 'requested_at': '2024-11-25'}
    ]

    approval_history = [
        {'contract_title': 'Parking Lot Expansion', 'type': 'New Contract', 'amount': 450000, 'status': 'Approved', 'decided_by': 'Board', 'decided_at': '2024-11-30', 'comments': 'Approved with conditions'},
        {'contract_title': 'Security System Upgrade', 'type': 'Change Order', 'amount': 35000, 'status': 'Approved', 'decided_by': 'Finance Dir.', 'decided_at': '2024-11-29', 'comments': ''},
        {'contract_title': 'Landscaping Contract', 'type': 'Renewal', 'amount': 75000, 'status': 'Rejected', 'decided_by': 'Dept. Manager', 'decided_at': '2024-11-28', 'comments': 'Need to rebid - pricing too high'},
        {'contract_title': 'Cafeteria Equipment', 'type': 'New Contract', 'amount': 180000, 'status': 'Approved', 'decided_by': 'Executive Dir.', 'decided_at': '2024-11-27', 'comments': ''}
    ]

    # Calculate stats
    stats = {
        'pending': len(pending_approvals),
        'in_review': 2,
        'approved': 15,
        'rejected': 3,
        'pending_value': sum(a['amount'] for a in pending_approvals)
    }

    contracts = current_contracts.to_dict('records') if current_contracts is not None else []

    return render_template('approvals.html',
                          pending_approvals=pending_approvals,
                          my_requests=my_requests,
                          approval_history=approval_history,
                          stats=stats,
                          contracts=contracts,
                          title='Contract Approvals')


@app.route('/api/approval/<request_id>/approve', methods=['POST'])
def api_approve_request(request_id):
    """Approve an approval request."""
    data = request.get_json() or {}

    # Log the approval
    db.log_audit('approvals', request_id, 'UPDATE',
                 new_values={'status': 'approved', 'approved_by': data.get('approved_by', 'System')},
                 changed_by=data.get('approved_by', 'System'))

    return jsonify({'success': True, 'message': 'Request approved'})


@app.route('/api/approval/<request_id>/reject', methods=['POST'])
def api_reject_request(request_id):
    """Reject an approval request."""
    data = request.get_json() or {}

    # Log the rejection
    db.log_audit('approvals', request_id, 'UPDATE',
                 new_values={'status': 'rejected', 'reason': data.get('reason'), 'comments': data.get('comments')},
                 changed_by=data.get('rejected_by', 'System'))

    return jsonify({'success': True, 'message': 'Request rejected'})


@app.route('/api/approval/<request_id>/request-info', methods=['POST'])
def api_request_info(request_id):
    """Request more information for an approval."""
    data = request.get_json() or {}

    # Log the info request
    db.log_audit('approvals', request_id, 'UPDATE',
                 new_values={'status': 'info_requested', 'message': data.get('message')},
                 changed_by='System')

    return jsonify({'success': True, 'message': 'Information request sent'})


@app.route('/api/approval/create', methods=['POST'])
def api_create_approval():
    """Create a new approval request."""
    data = request.form.to_dict()

    approval_id = f"APR-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Log the new approval
    db.log_audit('approvals', approval_id, 'CREATE',
                 new_values=data,
                 changed_by=data.get('requested_by', 'System'))

    return jsonify({'success': True, 'approval_id': approval_id})


# ==========================
# DATA IMPORT/EXPORT
# ==========================

@app.route('/import-export')
def import_export_page():
    """Data import and export management page."""
    global current_contracts

    if current_contracts is None:
        load_data()

    # Get departments for filter
    departments = current_contracts['department'].unique().tolist() if current_contracts is not None and 'department' in current_contracts.columns else []

    # Sample recent exports
    recent_exports = [
        {'filename': 'contracts_export_20241201.csv', 'type': 'CSV', 'created_at': '2024-12-01 10:30', 'size': '245 KB'},
        {'filename': 'monthly_report_nov2024.xlsx', 'type': 'Excel', 'created_at': '2024-11-30 08:00', 'size': '1.2 MB'},
        {'filename': 'vendor_data_20241128.json', 'type': 'JSON', 'created_at': '2024-11-28 14:15', 'size': '89 KB'}
    ]

    # Sample import history
    import_history = [
        {'date': '2024-11-25', 'filename': 'new_contracts.csv', 'type': 'Contracts', 'records_imported': 15, 'total_records': 15, 'status': 'Success'},
        {'date': '2024-11-20', 'filename': 'vendor_update.xlsx', 'type': 'Vendors', 'records_imported': 8, 'total_records': 10, 'status': 'Partial'},
        {'date': '2024-11-15', 'filename': 'payments_q3.csv', 'type': 'Payments', 'records_imported': 45, 'total_records': 45, 'status': 'Success'}
    ]

    # Sample backups
    backups = [
        {'id': 'bak-001', 'name': 'full_backup_20241201', 'date': '2024-12-01 02:00 AM', 'size': '15.3 MB'},
        {'id': 'bak-002', 'name': 'full_backup_20241101', 'date': '2024-11-01 02:00 AM', 'size': '14.8 MB'},
        {'id': 'bak-003', 'name': 'full_backup_20241001', 'date': '2024-10-01 02:00 AM', 'size': '14.2 MB'}
    ]

    return render_template('import_export.html',
                          departments=departments,
                          recent_exports=recent_exports,
                          import_history=import_history,
                          backups=backups,
                          title='Data Import & Export')


@app.route('/api/import', methods=['POST'])
def api_import_data():
    """Import data from uploaded file."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    import_type = request.form.get('type', 'contracts')
    overwrite = request.form.get('overwrite', 'false') == 'true'

    # Process the file based on type
    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.filename.endswith('.xlsx'):
            df = pd.read_excel(file)
        elif file.filename.endswith('.json'):
            df = pd.read_json(file)
        else:
            return jsonify({'error': 'Unsupported file format'}), 400

        records_imported = len(df)

        # Log the import
        db.log_audit('imports', file.filename, 'CREATE',
                     new_values={'type': import_type, 'records': records_imported},
                     changed_by='System')

        return jsonify({
            'success': True,
            'records_imported': records_imported,
            'message': f'Successfully imported {records_imported} records'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/template/<template_type>')
def api_download_template(template_type):
    """Download import template."""
    templates = {
        'contracts': 'contract_id,title,vendor_name,department,contract_type,original_amount,start_date,end_date,status',
        'vendors': 'vendor_id,name,contact_name,email,phone,address,category,insurance_expiry,license_expiry',
        'payments': 'payment_id,contract_id,amount,payment_date,description,invoice_number,status'
    }

    if template_type not in templates:
        return jsonify({'error': 'Unknown template type'}), 404

    output = io.StringIO()
    output.write(templates[template_type])
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{template_type}_template.csv'
    )


# ==========================
# RISK ASSESSMENT
# ==========================

@app.route('/risk-assessment')
def risk_assessment_page():
    """Contract risk assessment tool."""
    global current_contracts

    if current_contracts is None:
        load_data()

    df = current_contracts

    # Calculate risk factors for each contract
    risk_data = []
    for _, row in df.iterrows():
        contract_data = row.to_dict()

        # Calculate individual risk scores
        budget_risk = min(100, max(0, ((row.get('current_amount', 0) or 0) - (row.get('original_amount', 0) or 0)) / max(1, row.get('original_amount', 1)) * 200))
        schedule_risk = 100 - (row.get('percent_complete', 0) or 0) if row.get('status') == 'Active' else 0

        # Calculate days overdue
        end_date = row.get('current_end_date') or row.get('end_date')
        days_overdue = 0
        if end_date and row.get('status') == 'Active':
            try:
                end = datetime.strptime(end_date, '%Y-%m-%d')
                if end < datetime.now():
                    days_overdue = (datetime.now() - end).days
                    schedule_risk = min(100, schedule_risk + days_overdue * 2)
            except:
                pass

        vendor_risk = 100 - (row.get('vendor_performance_score', 50) or 50)
        compliance_risk = 50 if row.get('change_order_count', 0) > 3 else 0

        overall_risk = (budget_risk * 0.35 + schedule_risk * 0.30 + vendor_risk * 0.20 + compliance_risk * 0.15)

        risk_data.append({
            'contract_id': row['contract_id'],
            'title': row['title'],
            'department': row.get('department', 'Unknown'),
            'vendor_name': row.get('vendor_name', 'Unknown'),
            'current_amount': row.get('current_amount', 0),
            'percent_complete': row.get('percent_complete', 0),
            'budget_risk': budget_risk,
            'schedule_risk': schedule_risk,
            'vendor_risk': vendor_risk,
            'compliance_risk': compliance_risk,
            'overall_risk': overall_risk,
            'risk_level': 'Critical' if overall_risk >= 70 else 'High' if overall_risk >= 50 else 'Medium' if overall_risk >= 30 else 'Low',
            'days_overdue': days_overdue
        })

    # Sort by overall risk
    risk_data.sort(key=lambda x: x['overall_risk'], reverse=True)

    # Calculate summary stats
    summary = {
        'total_contracts': len(risk_data),
        'critical_count': len([r for r in risk_data if r['risk_level'] == 'Critical']),
        'high_count': len([r for r in risk_data if r['risk_level'] == 'High']),
        'medium_count': len([r for r in risk_data if r['risk_level'] == 'Medium']),
        'low_count': len([r for r in risk_data if r['risk_level'] == 'Low']),
        'avg_risk': sum(r['overall_risk'] for r in risk_data) / len(risk_data) if risk_data else 0,
        'total_at_risk_value': sum(r['current_amount'] for r in risk_data if r['risk_level'] in ['Critical', 'High'])
    }

    # Risk distribution by department
    dept_risk = {}
    for r in risk_data:
        dept = r['department']
        if dept not in dept_risk:
            dept_risk[dept] = {'contracts': 0, 'total_risk': 0, 'value': 0}
        dept_risk[dept]['contracts'] += 1
        dept_risk[dept]['total_risk'] += r['overall_risk']
        dept_risk[dept]['value'] += r['current_amount']

    department_risks = [
        {'name': k, 'contracts': v['contracts'], 'avg_risk': v['total_risk'] / v['contracts'], 'value': v['value']}
        for k, v in dept_risk.items()
    ]
    department_risks.sort(key=lambda x: x['avg_risk'], reverse=True)

    return render_template('risk_assessment.html',
                          risk_data=risk_data,
                          summary=summary,
                          department_risks=department_risks,
                          title='Risk Assessment')


# ==========================
# BUDGET ALLOCATION PLANNER
# ==========================

@app.route('/budget-planner')
def budget_planner_page():
    """Budget allocation planning tool."""
    global current_contracts

    if current_contracts is None:
        load_data()

    df = current_contracts

    # Calculate budget allocation by department
    dept_budgets = []
    if 'department' in df.columns:
        for dept in df['department'].unique():
            dept_df = df[df['department'] == dept]
            allocated = float(dept_df['current_amount'].sum())
            spent = float(dept_df['total_paid'].sum())
            dept_budgets.append({
                'name': dept,
                'allocated': allocated,
                'spent': spent,
                'remaining': allocated - spent,
                'utilization': (spent / allocated * 100) if allocated > 0 else 0,
                'contract_count': len(dept_df),
                'active_contracts': len(dept_df[dept_df['status'] == 'Active'])
            })

    dept_budgets.sort(key=lambda x: x['allocated'], reverse=True)

    # Monthly forecast
    total_budget = float(df['current_amount'].sum())
    total_spent = float(df['total_paid'].sum())
    monthly_burn = total_spent / 6  # Assume 6 months of data

    forecast = []
    remaining = total_budget - total_spent
    for i in range(12):
        month = (datetime.now() + timedelta(days=30*i)).strftime('%b %Y')
        projected_spend = min(remaining, monthly_burn)
        remaining -= projected_spend
        forecast.append({
            'month': month,
            'projected_spend': projected_spend,
            'remaining': max(0, remaining)
        })

    # Summary
    summary = {
        'total_budget': total_budget,
        'total_spent': total_spent,
        'remaining': total_budget - total_spent,
        'utilization': (total_spent / total_budget * 100) if total_budget > 0 else 0,
        'monthly_burn': monthly_burn,
        'months_remaining': ((total_budget - total_spent) / monthly_burn) if monthly_burn > 0 else 0
    }

    return render_template('budget_planner.html',
                          dept_budgets=dept_budgets,
                          forecast=forecast,
                          summary=summary,
                          title='Budget Allocation Planner')


# ==========================
# VENDOR COMPLIANCE CHECKER
# ==========================

@app.route('/vendor-compliance')
def vendor_compliance_page():
    """Vendor compliance checker page."""
    global current_vendors

    if current_vendors is None:
        load_data()

    today = datetime.now()
    compliance_data = []

    for _, vendor in current_vendors.iterrows():
        contracts = db.get_vendor_contracts(vendor['vendor_id'])

        # Calculate insurance status
        insurance_expiry = vendor.get('insurance_expiry')
        insurance_status = 'Valid'
        insurance_days = None
        if insurance_expiry:
            try:
                exp_date = datetime.strptime(insurance_expiry, '%Y-%m-%d')
                days_until = (exp_date - today).days
                insurance_days = days_until
                if days_until < 0:
                    insurance_status = 'Expired'
                elif days_until <= 30:
                    insurance_status = 'Expiring'
            except:
                insurance_status = 'Unknown'
        else:
            insurance_status = 'Missing'

        # Calculate license status
        license_expiry = vendor.get('license_expiry')
        license_status = 'Valid'
        license_days = None
        if license_expiry:
            try:
                exp_date = datetime.strptime(license_expiry, '%Y-%m-%d')
                days_until = (exp_date - today).days
                license_days = days_until
                if days_until < 0:
                    license_status = 'Expired'
                elif days_until <= 30:
                    license_status = 'Expiring'
            except:
                license_status = 'Unknown'
        else:
            license_status = 'Missing'

        # Determine overall compliance status
        if insurance_status in ['Expired', 'Missing'] or license_status in ['Expired', 'Missing']:
            compliance_status = 'Non-Compliant'
        elif insurance_status == 'Expiring' or license_status == 'Expiring':
            compliance_status = 'Expiring'
        else:
            compliance_status = 'Compliant'

        # Calculate days until next expiry
        days_until_expiry = None
        expiring_item = None
        if insurance_days is not None and insurance_days > 0:
            if days_until_expiry is None or insurance_days < days_until_expiry:
                days_until_expiry = insurance_days
                expiring_item = 'Insurance'
        if license_days is not None and license_days > 0:
            if days_until_expiry is None or license_days < days_until_expiry:
                days_until_expiry = license_days
                expiring_item = 'License'

        # Sample certifications
        certifications = [
            {'name': 'ISO 9001', 'valid': True},
            {'name': 'Safety Training', 'valid': True},
            {'name': 'Background Check', 'valid': compliance_status == 'Compliant'}
        ]

        compliance_data.append({
            'vendor_id': vendor['vendor_id'],
            'name': vendor.get('vendor_name', vendor.get('name', 'Unknown')),
            'category': vendor.get('category', 'General'),
            'contact_email': vendor.get('email', 'N/A'),
            'active_contracts': len([c for c in contracts if c.get('status') == 'Active']),
            'contract_value': sum(c.get('current_amount', 0) or 0 for c in contracts if c.get('status') == 'Active'),
            'insurance_status': insurance_status,
            'insurance_expiry': insurance_expiry,
            'insurance_days': insurance_days,
            'license_status': license_status,
            'license_expiry': license_expiry,
            'license_days': license_days,
            'compliance_status': compliance_status,
            'certifications': certifications,
            'cert_count': len(certifications),
            'days_until_expiry': days_until_expiry,
            'expiring_item': expiring_item
        })

    # Calculate summary stats
    summary = {
        'compliant': len([v for v in compliance_data if v['compliance_status'] == 'Compliant']),
        'expiring_soon': len([v for v in compliance_data if v['compliance_status'] == 'Expiring']),
        'non_compliant': len([v for v in compliance_data if v['compliance_status'] == 'Non-Compliant']),
        'insurance_issues': len([v for v in compliance_data if v['insurance_status'] in ['Expired', 'Missing']]),
        'license_issues': len([v for v in compliance_data if v['license_status'] in ['Expired', 'Missing']]),
        'at_risk_value': sum(v['contract_value'] for v in compliance_data if v['compliance_status'] != 'Compliant')
    }

    # Get non-compliant vendors for alert
    non_compliant_vendors = [v for v in compliance_data if v['compliance_status'] == 'Non-Compliant']

    # Expiration timeline for chart
    expiration_timeline = []
    for i in range(6):
        month = (today + timedelta(days=30*i)).strftime('%b %Y')
        count = len([v for v in compliance_data
                     if v['days_until_expiry'] and v['days_until_expiry'] > 30*i and v['days_until_expiry'] <= 30*(i+1)])
        expiration_timeline.append({'month': month, 'count': count})

    return render_template('vendor_compliance.html',
                          compliance_data=compliance_data,
                          summary=summary,
                          non_compliant_vendors=non_compliant_vendors,
                          expiration_timeline=expiration_timeline,
                          title='Vendor Compliance')


@app.route('/api/vendor/<vendor_id>/compliance-reminder', methods=['POST'])
def api_send_compliance_reminder(vendor_id):
    """Send compliance reminder to vendor."""
    db.log_audit('compliance_reminders', vendor_id, 'CREATE',
                 new_values={'sent_at': datetime.now().isoformat()},
                 changed_by='System')
    return jsonify({'success': True, 'message': 'Reminder sent'})


# ==========================
# USER ACTIVITY LOG
# ==========================

@app.route('/activity-log')
def activity_log_page():
    """User activity log page."""
    # Sample activity data
    activities = [
        {'user': 'John Smith', 'type': 'Contract', 'action': 'Updated contract status to Active', 'target': 'School Renovation Project', 'target_url': '/contract/CNT-001', 'timestamp': '2 minutes ago', 'details': 'Changed from Draft to Active'},
        {'user': 'Sarah Johnson', 'type': 'Approval', 'action': 'Approved change order request', 'target': 'HVAC System Upgrade', 'target_url': '/contract/CNT-003', 'timestamp': '15 minutes ago', 'details': 'Amount: $85,000'},
        {'user': 'Mike Chen', 'type': 'Payment', 'action': 'Processed payment', 'target': 'IT Infrastructure', 'target_url': '/contract/CNT-005', 'timestamp': '1 hour ago', 'details': '$45,000 - Invoice #INV-2024-156'},
        {'user': 'Admin', 'type': 'Login', 'action': 'User logged in', 'target': None, 'target_url': None, 'timestamp': '2 hours ago', 'details': 'IP: 192.168.1.100'},
        {'user': 'Emily Davis', 'type': 'Vendor', 'action': 'Updated vendor information', 'target': 'BuildRight Construction', 'target_url': '/vendor/VND-001', 'timestamp': '3 hours ago', 'details': 'Updated insurance documentation'},
        {'user': 'System', 'type': 'Security', 'action': 'Failed login attempt', 'target': None, 'target_url': None, 'timestamp': '5 hours ago', 'details': 'IP: 10.0.0.55 - 3 attempts'},
        {'user': 'John Smith', 'type': 'Contract', 'action': 'Added new milestone', 'target': 'Parking Lot Expansion', 'target_url': '/contract/CNT-007', 'timestamp': '6 hours ago', 'details': 'Phase 2 Completion - Due: Jan 15'},
        {'user': 'Sarah Johnson', 'type': 'Approval', 'action': 'Rejected budget increase request', 'target': 'Landscaping Contract', 'target_url': '/contract/CNT-008', 'timestamp': 'Yesterday', 'details': 'Reason: Insufficient justification'},
    ]

    # Full activity log with more details
    activity_log = [
        {'timestamp': '2024-12-05 14:32:15', 'user': 'John Smith', 'type': 'Contract', 'action': 'Status Update', 'target': 'CNT-001', 'target_url': '/contract/CNT-001', 'ip_address': '192.168.1.100'},
        {'timestamp': '2024-12-05 14:15:42', 'user': 'Sarah Johnson', 'type': 'Approval', 'action': 'Approved Request', 'target': 'APR-002', 'target_url': '/approvals', 'ip_address': '192.168.1.105'},
        {'timestamp': '2024-12-05 13:45:00', 'user': 'Mike Chen', 'type': 'Payment', 'action': 'Payment Processed', 'target': 'PAY-156', 'target_url': '/contract/CNT-005', 'ip_address': '192.168.1.110'},
        {'timestamp': '2024-12-05 12:00:00', 'user': 'Admin', 'type': 'Login', 'action': 'Successful Login', 'target': '-', 'target_url': None, 'ip_address': '192.168.1.100'},
        {'timestamp': '2024-12-05 11:30:25', 'user': 'Emily Davis', 'type': 'Vendor', 'action': 'Document Upload', 'target': 'VND-001', 'target_url': '/vendor/VND-001', 'ip_address': '192.168.1.115'},
        {'timestamp': '2024-12-05 09:00:00', 'user': 'System', 'type': 'Security', 'action': 'Failed Login', 'target': '-', 'target_url': None, 'ip_address': '10.0.0.55'},
        {'timestamp': '2024-12-05 08:45:00', 'user': 'John Smith', 'type': 'Contract', 'action': 'Milestone Added', 'target': 'CNT-007', 'target_url': '/contract/CNT-007', 'ip_address': '192.168.1.100'},
        {'timestamp': '2024-12-04 16:30:00', 'user': 'Sarah Johnson', 'type': 'Approval', 'action': 'Rejected Request', 'target': 'APR-005', 'target_url': '/approvals', 'ip_address': '192.168.1.105'},
    ]

    # Active users
    active_users = [
        {'name': 'John Smith', 'initials': 'JS', 'role': 'Contract Manager', 'last_action': 'Viewing Dashboard'},
        {'name': 'Sarah Johnson', 'initials': 'SJ', 'role': 'Finance Director', 'last_action': 'Processing Approvals'},
        {'name': 'Mike Chen', 'initials': 'MC', 'role': 'Accountant', 'last_action': 'Viewing Payments'},
        {'name': 'Emily Davis', 'initials': 'ED', 'role': 'Vendor Manager', 'last_action': 'Updating Vendor'},
    ]

    # Top users this week
    top_users = [
        {'name': 'John Smith', 'action_count': 145},
        {'name': 'Sarah Johnson', 'action_count': 98},
        {'name': 'Mike Chen', 'action_count': 76},
        {'name': 'Emily Davis', 'action_count': 54},
        {'name': 'Admin', 'action_count': 32},
    ]

    # Stats
    stats = {
        'today_count': 156,
        'active_users': len(active_users),
        'contract_updates': 45,
        'approvals': 12,
        'security_events': 3
    }

    # Hourly activity for chart
    hourly_activity = [
        {'hour': f'{i}:00', 'count': 5 + (i % 12) * 3 + (10 if 9 <= i <= 17 else 0)}
        for i in range(24)
    ]

    # Type distribution
    type_distribution = {
        'Contract': 45,
        'Vendor': 20,
        'Approval': 15,
        'Payment': 12,
        'Login': 5,
        'Security': 3
    }

    return render_template('activity_log.html',
                          activities=activities,
                          activity_log=activity_log,
                          active_users=active_users,
                          top_users=top_users,
                          stats=stats,
                          hourly_activity=hourly_activity,
                          type_distribution=type_distribution,
                          title='User Activity Log')


@app.route('/api/activity-log/export')
def api_export_activity_log():
    """Export activity log to CSV."""
    logs = db.get_audit_log(limit=1000)

    output = io.StringIO()
    output.write('timestamp,user,table,action,record_id\n')
    for log in logs:
        output.write(f"{log.get('changed_at', '')},{log.get('changed_by', '')},{log.get('table_name', '')},{log.get('action', '')},{log.get('record_id', '')}\n")
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'activity_log_{datetime.now().strftime("%Y%m%d")}.csv'
    )


# ==========================
# GOVERNANCE FEATURES
# ==========================

@app.route('/meetings')
def meetings_page():
    """Council and board meetings management."""
    # Sample data for meetings
    upcoming_meetings = [
        {
            'id': 'MTG-001',
            'title': 'Regular City Council Meeting',
            'type': 'Regular',
            'date': 'December 12, 2024',
            'time': '6:00 PM',
            'location': 'City Hall, Council Chambers',
            'agenda_items': [
                {'number': '1.', 'title': 'Call to Order', 'type': 'Procedural', 'requires_vote': False, 'contract_id': None},
                {'number': '2.', 'title': 'Approval of Minutes', 'type': 'Consent', 'requires_vote': True, 'contract_id': None},
                {'number': '3.', 'title': 'School Renovation Contract Approval', 'type': 'Action', 'requires_vote': True, 'contract_id': 'CNT-001'},
                {'number': '4.', 'title': 'Budget Amendment #3', 'type': 'Action', 'requires_vote': True, 'contract_id': None},
                {'number': '5.', 'title': 'Public Comments', 'type': 'Public Input', 'requires_vote': False, 'contract_id': None},
            ],
            'contract_items': [
                {'contract_id': 'CNT-001', 'title': 'School Renovation Project', 'vendor_name': 'BuildRight Construction', 'amount': 2500000},
                {'contract_id': 'CNT-007', 'title': 'IT Infrastructure Upgrade', 'vendor_name': 'TechSolutions Inc', 'amount': 450000}
            ]
        },
        {
            'id': 'MTG-002',
            'title': 'School Board Work Session',
            'type': 'Special',
            'date': 'December 15, 2024',
            'time': '4:00 PM',
            'location': 'Administration Building, Room 200',
            'agenda_items': [
                {'number': '1.', 'title': 'ESSER Grant Spending Review', 'type': 'Discussion', 'requires_vote': False, 'contract_id': None},
                {'number': '2.', 'title': 'Facilities Master Plan Update', 'type': 'Presentation', 'requires_vote': False, 'contract_id': None},
            ],
            'contract_items': []
        }
    ]

    # Calendar days (sample)
    calendar_days = []
    for i in range(1, 32):
        day = {
            'number': i,
            'is_today': i == 5,
            'events': []
        }
        if i == 12:
            day['events'].append({'title': 'Council Meeting', 'type': 'Regular'})
        if i == 15:
            day['events'].append({'title': 'Work Session', 'type': 'Special'})
        if i == 20:
            day['events'].append({'title': 'Public Hearing', 'type': 'Hearing'})
        calendar_days.append(day)

    past_meetings = [
        {'date': 'Nov 28, 2024', 'title': 'Regular City Council Meeting', 'type': 'Regular', 'status': 'Approved'},
        {'date': 'Nov 21, 2024', 'title': 'Finance Committee Meeting', 'type': 'Committee', 'status': 'Draft'},
        {'date': 'Nov 14, 2024', 'title': 'Regular City Council Meeting', 'type': 'Regular', 'status': 'Approved'},
    ]

    public_hearing_list = [
        {
            'id': 'PH-001',
            'title': 'Proposed Zoning Change for New School Site',
            'date': 'December 20, 2024',
            'time': '7:00 PM',
            'description': 'Public hearing to discuss rezoning of 50-acre parcel for new elementary school construction.',
            'related_contract': {'id': 'CNT-012', 'title': 'New Elementary School Construction', 'amount': 35000000},
            'comments_received': 24,
            'comment_deadline': 'December 18, 2024'
        }
    ]

    return render_template('meetings.html',
                          upcoming_meetings=upcoming_meetings,
                          calendar_days=calendar_days,
                          past_meetings=past_meetings,
                          public_hearing_list=public_hearing_list,
                          pending_items=5,
                          public_hearings=2,
                          pending_votes=4,
                          title='Meetings & Agendas')


@app.route('/voting-records')
def voting_records_page():
    """Board and council voting records."""
    members = [
        {'id': 'M001', 'name': 'John Smith', 'initials': 'JS', 'position': 'Council President', 'total_votes': 145, 'yes_votes': 128, 'no_votes': 12, 'abstentions': 5, 'participation_rate': 97, 'recent_recusals': []},
        {'id': 'M002', 'name': 'Sarah Johnson', 'initials': 'SJ', 'position': 'Vice President', 'total_votes': 142, 'yes_votes': 125, 'no_votes': 15, 'abstentions': 2, 'participation_rate': 95, 'recent_recusals': [{'date': 'Nov 28', 'reason': 'Financial Interest'}]},
        {'id': 'M003', 'name': 'Mike Chen', 'initials': 'MC', 'position': 'Member', 'total_votes': 140, 'yes_votes': 110, 'no_votes': 25, 'abstentions': 5, 'participation_rate': 94, 'recent_recusals': []},
        {'id': 'M004', 'name': 'Emily Davis', 'initials': 'ED', 'position': 'Member', 'total_votes': 143, 'yes_votes': 130, 'no_votes': 10, 'abstentions': 3, 'participation_rate': 96, 'recent_recusals': []},
        {'id': 'M005', 'name': 'James Wilson', 'initials': 'JW', 'position': 'Member', 'total_votes': 138, 'yes_votes': 115, 'no_votes': 18, 'abstentions': 5, 'participation_rate': 92, 'recent_recusals': [{'date': 'Nov 14', 'reason': 'Business Relationship'}]},
    ]

    recent_votes = [
        {'id': 'V001', 'date': 'Dec 5, 2024', 'item': 'HVAC System Upgrade Change Order', 'type': 'Contract', 'yes': 4, 'no': 1, 'abstain': 0, 'result': 'Passed', 'contract_id': 'CNT-003', 'amount': 85000},
        {'id': 'V002', 'date': 'Nov 28, 2024', 'item': 'FY 2024-25 Budget Amendment #3', 'type': 'Budget', 'yes': 5, 'no': 0, 'abstain': 0, 'result': 'Passed', 'contract_id': None, 'amount': 500000},
        {'id': 'V003', 'date': 'Nov 28, 2024', 'item': 'School Renovation Project Approval', 'type': 'Contract', 'yes': 3, 'no': 1, 'abstain': 1, 'result': 'Passed', 'contract_id': 'CNT-001', 'amount': 2500000},
        {'id': 'V004', 'date': 'Nov 14, 2024', 'item': 'Resolution for Bond Measure', 'type': 'Resolution', 'yes': 4, 'no': 1, 'abstain': 0, 'result': 'Passed', 'contract_id': None, 'amount': None},
    ]

    contract_votes = [
        {'date': 'Dec 5, 2024', 'contract_id': 'CNT-003', 'contract_title': 'HVAC System Upgrade', 'vendor_name': 'CoolAir Systems', 'amount': 85000, 'yes': 4, 'no': 1, 'abstain': 0, 'result': 'Approved'},
        {'date': 'Nov 28, 2024', 'contract_id': 'CNT-001', 'contract_title': 'School Renovation Project', 'vendor_name': 'BuildRight Construction', 'amount': 2500000, 'yes': 3, 'no': 1, 'abstain': 1, 'result': 'Approved'},
        {'date': 'Nov 14, 2024', 'contract_id': 'CNT-007', 'contract_title': 'IT Infrastructure', 'vendor_name': 'TechSolutions Inc', 'amount': 450000, 'yes': 5, 'no': 0, 'abstain': 0, 'result': 'Approved'},
    ]

    stats = {'total_votes': 156, 'passed': 142, 'failed': 8, 'unanimous': 98, 'total_value': 15000000}
    contract_stats = {'approved': 24, 'rejected': 2, 'change_orders': 12, 'total_value': 18500000}
    pass_rates = [
        {'type': 'Contracts', 'rate': 92, 'passed': 23, 'total': 25},
        {'type': 'Budget', 'rate': 100, 'passed': 8, 'total': 8},
        {'type': 'Resolutions', 'rate': 88, 'passed': 15, 'total': 17},
    ]

    return render_template('voting_records.html',
                          members=members,
                          recent_votes=recent_votes,
                          contract_votes=contract_votes,
                          stats=stats,
                          contract_stats=contract_stats,
                          pass_rates=pass_rates,
                          title='Voting Records')


@app.route('/conflicts')
def conflicts_page():
    """Conflict of interest tracking."""
    disclosures = [
        {'id': 'D001', 'name': 'John Smith', 'initials': 'JS', 'position': 'Council President', 'last_disclosure': 'Jan 15, 2024', 'next_due': 'Jan 15, 2025', 'status': 'Current'},
        {'id': 'D002', 'name': 'Sarah Johnson', 'initials': 'SJ', 'position': 'Vice President', 'last_disclosure': 'Jan 20, 2024', 'next_due': 'Jan 20, 2025', 'status': 'Current'},
        {'id': 'D003', 'name': 'Mike Chen', 'initials': 'MC', 'position': 'Member', 'last_disclosure': 'Dec 15, 2023', 'next_due': 'Dec 15, 2024', 'status': 'Due Soon'},
        {'id': 'D004', 'name': 'Emily Davis', 'initials': 'ED', 'position': 'Member', 'last_disclosure': 'Feb 1, 2024', 'next_due': 'Feb 1, 2025', 'status': 'Current'},
        {'id': 'D005', 'name': 'James Wilson', 'initials': 'JW', 'position': 'Member', 'last_disclosure': 'Nov 1, 2023', 'next_due': 'Nov 1, 2024', 'status': 'Overdue'},
    ]

    recusals = [
        {'date': 'Nov 28, 2024', 'member_name': 'Sarah Johnson', 'member_initials': 'SJ', 'meeting': 'Regular Council Meeting', 'agenda_item': 'Johnson Properties Zoning Request', 'reason': 'Financial Interest', 'contract_id': None, 'contract_title': None},
        {'date': 'Nov 14, 2024', 'member_name': 'James Wilson', 'member_initials': 'JW', 'meeting': 'Regular Council Meeting', 'agenda_item': 'IT Services Contract Renewal', 'reason': 'Business Relationship', 'contract_id': 'CNT-007', 'contract_title': 'IT Infrastructure'},
        {'date': 'Oct 24, 2024', 'member_name': 'Mike Chen', 'member_initials': 'MC', 'meeting': 'School Board Meeting', 'agenda_item': 'Catering Services Bid', 'reason': 'Family Connection', 'contract_id': None, 'contract_title': None},
    ]

    member_relationships = [
        {
            'id': 'M002', 'name': 'Sarah Johnson', 'initials': 'SJ', 'position': 'Vice President',
            'relationships': [
                {'vendor_name': 'Johnson Properties LLC', 'relationship_type': 'Ownership Interest (25%)', 'conflict_level': 'High', 'active_contracts': 0, 'contract_value': 0},
                {'vendor_name': 'ABC Insurance', 'relationship_type': 'Board Member', 'conflict_level': 'Medium', 'active_contracts': 1, 'contract_value': 50000}
            ]
        },
        {
            'id': 'M005', 'name': 'James Wilson', 'initials': 'JW', 'position': 'Member',
            'relationships': [
                {'vendor_name': 'TechSolutions Inc', 'relationship_type': 'Previous Employment', 'conflict_level': 'Medium', 'active_contracts': 2, 'contract_value': 650000}
            ]
        },
        {
            'id': 'M003', 'name': 'Mike Chen', 'initials': 'MC', 'position': 'Member',
            'relationships': [
                {'vendor_name': 'Chen Catering Services', 'relationship_type': 'Family Member Ownership', 'conflict_level': 'High', 'active_contracts': 0, 'contract_value': 0}
            ]
        }
    ]

    conflict_alerts = [
        {'id': 'CA001', 'title': 'Potential Conflict Detected', 'description': 'Member James Wilson has disclosed previous employment with TechSolutions Inc, which has an active contract bid pending.', 'severity': 'High', 'date': 'Dec 3, 2024', 'member_name': 'James Wilson', 'member_initials': 'JW', 'vendor_name': 'TechSolutions Inc', 'contract_id': 'CNT-015'},
        {'id': 'CA002', 'title': 'Disclosure Reminder', 'description': 'Annual financial disclosure is overdue for Member James Wilson.', 'severity': 'Medium', 'date': 'Dec 1, 2024', 'member_name': 'James Wilson', 'member_initials': 'JW', 'vendor_name': None, 'contract_id': None},
    ]

    stats = {'total_members': 5, 'current_disclosures': 3, 'pending_disclosures': 2, 'recusals_count': 8, 'potential_conflicts': 2}
    vendors = current_vendors.to_dict('records') if current_vendors is not None else []
    all_members = [{'id': m['id'], 'name': m['name']} for m in disclosures]

    return render_template('conflicts.html',
                          disclosures=disclosures,
                          recusals=recusals,
                          member_relationships=member_relationships,
                          conflict_alerts=conflict_alerts,
                          stats=stats,
                          vendors=vendors,
                          all_members=all_members,
                          pending_disclosures=2,
                          title='Conflict of Interest')


@app.route('/policy-compliance')
def policy_compliance_page():
    """Policy compliance dashboard."""
    mwbe_goal = 30
    mwbe_stats = {'total_value': 25000000, 'mwbe_value': 6750000, 'current_rate': 27.0}
    mwbe_breakdown = {'mbe': 12.5, 'wbe': 9.2, 'sbe': 5.3}

    local_hiring_goal = 40
    local_hiring = {
        'total_projects': 12,
        'projects_meeting_goal': 9,
        'compliance_rate': 75,
        'total_workers': 450,
        'local_workers': 315,
        'local_percentage': 70,
        'non_compliant_projects': [
            {'contract_id': 'CNT-003', 'title': 'HVAC System Upgrade', 'current_rate': 32},
            {'contract_id': 'CNT-009', 'title': 'Parking Lot Repair', 'current_rate': 28},
        ]
    }

    living_wage = {'minimum_rate': 18.50, 'compliance_rate': 94, 'compliant_contracts': 47, 'total_contracts': 50, 'violations': 2}
    environmental = {'recycled_rate': 28, 'energy_efficient_rate': 55, 'local_sourcing_rate': 22}

    department_compliance = [
        {'name': 'Public Works', 'mwbe': 32, 'local_hire': 45, 'living_wage': 100, 'environmental': 85, 'overall': 88},
        {'name': 'Education', 'mwbe': 28, 'local_hire': 38, 'living_wage': 92, 'environmental': 78, 'overall': 82},
        {'name': 'IT Services', 'mwbe': 22, 'local_hire': 35, 'living_wage': 100, 'environmental': 65, 'overall': 75},
        {'name': 'Parks & Recreation', 'mwbe': 35, 'local_hire': 52, 'living_wage': 88, 'environmental': 90, 'overall': 86},
    ]

    return render_template('policy_compliance.html',
                          mwbe_goal=mwbe_goal,
                          mwbe_stats=mwbe_stats,
                          mwbe_breakdown=mwbe_breakdown,
                          local_hiring_goal=local_hiring_goal,
                          local_hiring=local_hiring,
                          living_wage=living_wage,
                          environmental=environmental,
                          department_compliance=department_compliance,
                          title='Policy Compliance')


@app.route('/grants')
def grants_page():
    """Grant management dashboard."""
    grants = [
        {
            'id': 'GR-001', 'name': 'ESSER III - Learning Loss Recovery', 'program': 'Elementary and Secondary School Emergency Relief',
            'source': 'Federal', 'grant_number': 'ESSER-III-2024-001', 'award_amount': 5200000, 'drawn_amount': 3120000,
            'remaining': 2080000, 'utilization_pct': 60, 'start_date': '2023-03-01', 'end_date': '2024-09-30',
            'allowable_uses': ['Learning Loss', 'Mental Health', 'Summer Programs', 'Technology'],
            'match_required': 0, 'match_percentage': 0, 'match_provided': 0
        },
        {
            'id': 'GR-002', 'name': 'Title I - Part A', 'program': 'Improving Basic Programs',
            'source': 'Federal', 'grant_number': 'TITLE1-2024-045', 'award_amount': 1800000, 'drawn_amount': 900000,
            'remaining': 900000, 'utilization_pct': 50, 'start_date': '2024-07-01', 'end_date': '2025-06-30',
            'allowable_uses': ['Instruction', 'Professional Development', 'Parental Involvement'],
            'match_required': 0, 'match_percentage': 0, 'match_provided': 0
        },
        {
            'id': 'GR-003', 'name': 'CDBG Infrastructure', 'program': 'Community Development Block Grant',
            'source': 'Federal', 'grant_number': 'CDBG-2024-112', 'award_amount': 750000, 'drawn_amount': 225000,
            'remaining': 525000, 'utilization_pct': 30, 'start_date': '2024-01-01', 'end_date': '2025-12-31',
            'allowable_uses': ['Infrastructure', 'Public Facilities', 'Housing'],
            'match_required': 187500, 'match_percentage': 25, 'match_provided': 150000
        }
    ]

    upcoming_deadlines = [
        {'grant_name': 'ESSER III', 'report_type': 'Quarterly Financial Report', 'due_date': 'Dec 15, 2024'},
        {'grant_name': 'Title I', 'report_type': 'Mid-Year Performance Report', 'due_date': 'Jan 15, 2025'},
    ]

    reporting_deadlines = [
        {'id': 'RPT-001', 'grant_name': 'ESSER III', 'report_type': 'Quarterly Financial Report', 'due_date': 'Dec 15, 2024', 'period': 'Q2 FY25', 'status': 'In Progress', 'days_until': 10, 'is_overdue': False},
        {'id': 'RPT-002', 'grant_name': 'Title I', 'report_type': 'Mid-Year Performance', 'due_date': 'Jan 15, 2025', 'period': 'FY25 Mid-Year', 'status': 'Not Started', 'days_until': 41, 'is_overdue': False},
        {'id': 'RPT-003', 'grant_name': 'CDBG', 'report_type': 'Quarterly Progress', 'due_date': 'Dec 31, 2024', 'period': 'Q4 2024', 'status': 'Not Started', 'days_until': 26, 'is_overdue': False},
    ]

    upcoming_drawdowns = [
        {'grant_name': 'ESSER III', 'amount': 250000, 'scheduled_date': 'Dec 15, 2024', 'description': 'Technology purchases'},
        {'grant_name': 'Title I', 'amount': 150000, 'scheduled_date': 'Dec 20, 2024', 'description': 'Q2 instructional materials'},
    ]

    all_drawdowns = [
        {'date': 'Nov 30, 2024', 'grant_name': 'ESSER III', 'amount': 180000, 'purpose': 'Mental health services', 'status': 'Completed'},
        {'date': 'Nov 15, 2024', 'grant_name': 'Title I', 'amount': 120000, 'purpose': 'Professional development', 'status': 'Completed'},
        {'date': 'Dec 15, 2024', 'grant_name': 'ESSER III', 'amount': 250000, 'purpose': 'Technology purchases', 'status': 'Pending'},
    ]

    grant_funded_contracts = [
        {'contract_id': 'CNT-010', 'title': 'Summer Learning Program', 'grant_name': 'ESSER III', 'grant_number': 'ESSER-III-2024-001', 'vendor_name': 'Learning Partners LLC', 'grant_amount': 450000, 'local_match': 0, 'status': 'Active'},
        {'contract_id': 'CNT-011', 'title': 'Tutoring Services', 'grant_name': 'Title I', 'grant_number': 'TITLE1-2024-045', 'vendor_name': 'Academic Support Inc', 'grant_amount': 200000, 'local_match': 0, 'status': 'Active'},
        {'contract_id': 'CNT-012', 'title': 'Infrastructure Improvement', 'grant_name': 'CDBG', 'grant_number': 'CDBG-2024-112', 'vendor_name': 'City Works Construction', 'grant_amount': 500000, 'local_match': 125000, 'status': 'Active'},
    ]

    stats = {'active_grants': 8, 'total_awarded': 12500000, 'total_drawn': 6200000, 'remaining': 6300000, 'match_required': 450000}

    return render_template('grants.html',
                          grants=grants,
                          upcoming_deadlines=upcoming_deadlines,
                          reporting_deadlines=reporting_deadlines,
                          upcoming_drawdowns=upcoming_drawdowns,
                          all_drawdowns=all_drawdowns,
                          grant_funded_contracts=grant_funded_contracts,
                          stats=stats,
                          title='Grant Management')


@app.route('/fund-accounting')
def fund_accounting_page():
    """Multi-fund accounting dashboard."""
    funds = [
        {'id': 'F001', 'name': 'General Fund', 'type': 'General', 'restriction': 'Unrestricted', 'budget': 45000000, 'encumbered': 12000000, 'expended': 28000000, 'available': 5000000, 'utilization_pct': 89},
        {'id': 'F002', 'name': 'Capital Projects Fund', 'type': 'Capital', 'restriction': 'Restricted', 'budget': 25000000, 'encumbered': 8000000, 'expended': 10000000, 'available': 7000000, 'utilization_pct': 72},
        {'id': 'F003', 'name': 'Federal Grants Fund', 'type': 'Grant', 'restriction': 'Restricted', 'budget': 12500000, 'encumbered': 3000000, 'expended': 6200000, 'available': 3300000, 'utilization_pct': 74},
        {'id': 'F004', 'name': 'Special Revenue Fund', 'type': 'Special Revenue', 'restriction': 'Committed', 'budget': 8000000, 'encumbered': 2500000, 'expended': 4200000, 'available': 1300000, 'utilization_pct': 84},
        {'id': 'F005', 'name': 'Debt Service Fund', 'type': 'Debt Service', 'restriction': 'Restricted', 'budget': 5000000, 'encumbered': 0, 'expended': 3800000, 'available': 1200000, 'utilization_pct': 76},
    ]

    summary = {
        'total_budget': sum(f['budget'] for f in funds),
        'total_encumbered': sum(f['encumbered'] for f in funds),
        'total_expended': sum(f['expended'] for f in funds),
        'available_balance': sum(f['available'] for f in funds)
    }

    interfund_transfers = [
        {'date': 'Nov 30, 2024', 'from_fund': 'General Fund', 'to_fund': 'Capital Projects Fund', 'amount': 500000, 'purpose': 'Emergency roof repairs', 'status': 'Completed', 'approved_by': 'Board'},
        {'date': 'Nov 15, 2024', 'from_fund': 'Special Revenue Fund', 'to_fund': 'General Fund', 'amount': 150000, 'purpose': 'Program support', 'status': 'Completed', 'approved_by': 'CFO'},
        {'date': 'Dec 10, 2024', 'from_fund': 'General Fund', 'to_fund': 'Debt Service Fund', 'amount': 200000, 'purpose': 'Debt payment reserve', 'status': 'Pending', 'approved_by': 'Pending'},
    ]

    fund_alerts = [
        {'title': 'General Fund Utilization High', 'message': 'General Fund has reached 89% utilization with 6 months remaining in fiscal year.', 'severity': 'Medium', 'date': 'Dec 5, 2024'},
        {'title': 'Grant Spending Deadline', 'message': 'ESSER III funds must be expended by September 30, 2024. Currently 40% remaining.', 'severity': 'High', 'date': 'Dec 3, 2024'},
    ]

    restriction_analysis = {'unrestricted': 5000000, 'restricted': 11500000, 'committed': 1300000}

    return render_template('fund_accounting.html',
                          funds=funds,
                          summary=summary,
                          interfund_transfers=interfund_transfers,
                          fund_alerts=fund_alerts,
                          restriction_analysis=restriction_analysis,
                          title='Fund Accounting')


@app.route('/procurement')
def procurement_page():
    """Procurement pipeline management."""
    pipeline = {
        'draft': [
            {'id': 'PR-001', 'title': 'Cafeteria Equipment Replacement', 'type': 'RFP', 'department': 'Education', 'estimated_value': 350000},
            {'id': 'PR-002', 'title': 'Landscaping Services', 'type': 'RFQ', 'department': 'Parks & Recreation', 'estimated_value': 125000},
        ],
        'posted': [
            {'id': 'PR-003', 'title': 'Network Infrastructure Upgrade', 'type': 'RFP', 'department': 'IT Services', 'estimated_value': 800000, 'close_date': 'Dec 15', 'bids_received': 4},
            {'id': 'PR-004', 'title': 'HVAC Maintenance Contract', 'type': 'IFB', 'department': 'Public Works', 'estimated_value': 200000, 'close_date': 'Dec 20', 'bids_received': 6},
        ],
        'evaluation': [
            {'id': 'PR-005', 'title': 'Security Camera System', 'type': 'RFP', 'department': 'Administration', 'estimated_value': 450000, 'bids_received': 5, 'eval_progress': 60},
        ],
        'pending_award': [
            {'id': 'PR-006', 'title': 'Bus Fleet Replacement', 'type': 'IFB', 'department': 'Transportation', 'estimated_value': 2500000, 'recommended_vendor': 'Blue Bird Corp', 'recommended_amount': 2350000, 'board_date': 'Dec 12'},
        ],
        'awarded': [
            {'id': 'PR-007', 'title': 'Playground Equipment', 'type': 'RFQ', 'department': 'Parks & Recreation', 'estimated_value': 180000, 'awarded_vendor': 'PlaySafe Inc', 'awarded_amount': 165000, 'award_date': 'Nov 28', 'contract_id': 'CNT-014'},
        ]
    }

    stats = {
        'draft': len(pipeline['draft']),
        'posted': len(pipeline['posted']),
        'evaluation': len(pipeline['evaluation']),
        'pending_award': len(pipeline['pending_award']),
        'awarded': len(pipeline['awarded']),
        'total_value': sum(p['estimated_value'] for p in pipeline['draft'] + pipeline['posted'] + pipeline['evaluation'] + pipeline['pending_award'])
    }

    bid_calendar = [
        {'month': 'DEC', 'day': '10', 'time': '2:00 PM', 'type': 'Pre-Bid Meeting', 'title': 'Network Infrastructure Upgrade', 'location': 'City Hall Room 201', 'estimated_value': 800000},
        {'month': 'DEC', 'day': '15', 'time': '3:00 PM', 'type': 'Bid Opening', 'title': 'Network Infrastructure Upgrade', 'location': 'City Hall Room 201', 'estimated_value': 800000},
        {'month': 'DEC', 'day': '20', 'time': '2:00 PM', 'type': 'Bid Opening', 'title': 'HVAC Maintenance Contract', 'location': 'City Hall Room 201', 'estimated_value': 200000},
    ]

    vendor_outreach = [
        {'vendor_name': 'TechCorp Solutions', 'procurement_title': 'Network Infrastructure Upgrade', 'status': 'Responded', 'date': 'Dec 3'},
        {'vendor_name': 'SecureView Inc', 'procurement_title': 'Security Camera System', 'status': 'Invited', 'date': 'Nov 28'},
        {'vendor_name': 'HVAC Pro Services', 'procurement_title': 'HVAC Maintenance Contract', 'status': 'Responded', 'date': 'Dec 1'},
        {'vendor_name': 'Green Thumb Landscaping', 'procurement_title': 'Landscaping Services', 'status': 'Declined', 'date': 'Nov 25'},
    ]

    outreach_stats = {'invited': 45, 'responded': 32, 'response_rate': 71.1, 'mwbe_invited': 18}

    procurement_analytics = [
        {'type': 'RFP', 'savings_pct': 12.5, 'avg_bids': 5.2},
        {'type': 'IFB', 'savings_pct': 15.8, 'avg_bids': 7.1},
        {'type': 'RFQ', 'savings_pct': 8.2, 'avg_bids': 4.3},
    ]

    historical_bids = [
        {'title': 'Playground Equipment', 'type': 'RFQ', 'bid_count': 4, 'estimated_value': 180000, 'awarded_value': 165000, 'savings_pct': 8.3},
        {'title': 'IT Consulting Services', 'type': 'RFP', 'bid_count': 6, 'estimated_value': 500000, 'awarded_value': 425000, 'savings_pct': 15.0},
        {'title': 'Roofing Replacement', 'type': 'IFB', 'bid_count': 8, 'estimated_value': 750000, 'awarded_value': 680000, 'savings_pct': 9.3},
    ]

    return render_template('procurement.html',
                          pipeline=pipeline,
                          stats=stats,
                          bid_calendar=bid_calendar,
                          vendor_outreach=vendor_outreach,
                          outreach_stats=outreach_stats,
                          procurement_analytics=procurement_analytics,
                          historical_bids=historical_bids,
                          title='Procurement Pipeline')


@app.route('/constituent-portal')
def constituent_portal_page():
    """Constituent services portal."""
    public_comments = [
        {'id': 'C001', 'name': 'Robert Miller', 'date': 'Dec 4, 2024', 'topic': 'Contract', 'content': 'I support the school renovation project. Our children deserve modern facilities.', 'sentiment': 'Support', 'response': True, 'related_item': {'title': 'School Renovation Project', 'url': '/contract/CNT-001'}},
        {'id': 'C002', 'name': 'Lisa Thompson', 'date': 'Dec 3, 2024', 'topic': 'Budget', 'content': 'Concerned about the proposed budget increase for IT services. Please provide more details on ROI.', 'sentiment': 'Neutral', 'response': False, 'related_item': {'title': 'FY25 Budget Amendment', 'url': '#'}},
        {'id': 'C003', 'name': 'James Brown', 'date': 'Dec 2, 2024', 'topic': 'Contract', 'content': 'The HVAC contractor has been unresponsive. When will the work be completed?', 'sentiment': 'Oppose', 'response': True, 'related_item': {'title': 'HVAC System Upgrade', 'url': '/contract/CNT-003'}},
    ]

    sentiment_stats = {'support': 45, 'neutral': 30, 'oppose': 15}
    top_commented = [
        {'title': 'School Renovation Project', 'url': '/contract/CNT-001', 'count': 24},
        {'title': 'FY25 Budget', 'url': '#', 'count': 18},
        {'title': 'New Elementary School', 'url': '/contract/CNT-012', 'count': 12},
    ]

    foia_requests = [
        {'id': 'FOIA-2024-156', 'requestor_name': 'Local News Daily', 'requestor_org': 'Media', 'description': 'All contracts awarded to BuildRight Construction in the past 3 years', 'received_date': 'Nov 25, 2024', 'due_date': 'Dec 9, 2024', 'status': 'In Progress', 'days_remaining': 4, 'is_overdue': False},
        {'id': 'FOIA-2024-157', 'requestor_name': 'Jane Citizen', 'requestor_org': 'Individual', 'description': 'Board meeting minutes from October 2024', 'received_date': 'Dec 1, 2024', 'due_date': 'Dec 15, 2024', 'status': 'Open', 'days_remaining': 10, 'is_overdue': False},
        {'id': 'FOIA-2024-158', 'requestor_name': 'Watchdog Group', 'requestor_org': 'Non-Profit', 'description': 'Vendor payment records for FY24', 'received_date': 'Nov 15, 2024', 'due_date': 'Nov 29, 2024', 'status': 'Completed', 'days_remaining': 0, 'is_overdue': False},
    ]

    foia_stats = {'total_year': 156, 'on_time_pct': 94, 'avg_days': 8, 'denial_rate': 3.2}

    complaints = [
        {'id': 'CMP-2024-089', 'subject': 'Construction Noise Complaint', 'description': 'Excessive noise from school renovation project before 7 AM.', 'from_name': 'Nearby Resident', 'date': 'Dec 3, 2024', 'priority': 'Medium', 'status': 'Investigating', 'related_contract': {'id': 'CNT-001', 'title': 'School Renovation'}},
        {'id': 'CMP-2024-090', 'subject': 'Invoice Payment Delay', 'description': 'Vendor reports 60-day payment delay on approved invoices.', 'from_name': 'ABC Supplies', 'date': 'Dec 1, 2024', 'priority': 'High', 'status': 'New', 'related_contract': None},
    ]

    complaint_categories = [
        {'name': 'Payment Issues', 'count': 12, 'percentage': 30},
        {'name': 'Construction', 'count': 8, 'percentage': 20},
        {'name': 'Service Quality', 'count': 10, 'percentage': 25},
        {'name': 'Communication', 'count': 6, 'percentage': 15},
        {'name': 'Other', 'count': 4, 'percentage': 10},
    ]

    complaint_metrics = {'avg_resolution': 5, 'first_response': 4, 'satisfaction': 85}

    satisfaction_by_area = [
        {'name': 'Contract Transparency', 'rating': 4.2},
        {'name': 'Response Time', 'rating': 3.8},
        {'name': 'Public Meeting Access', 'rating': 4.5},
        {'name': 'Information Availability', 'rating': 4.0},
    ]

    recent_feedback = [
        {'rating': 5, 'comment': 'Excellent transparency with the new contract dashboard!', 'service': 'Public Portal', 'date': 'Dec 4'},
        {'rating': 4, 'comment': 'FOIA request was processed quickly.', 'service': 'Records Request', 'date': 'Dec 2'},
        {'rating': 3, 'comment': 'Had to follow up multiple times on my complaint.', 'service': 'Constituent Services', 'date': 'Nov 28'},
    ]

    stats = {'open_foia': 8, 'recent_comments': 45, 'open_complaints': 12, 'avg_response_time': 3, 'satisfaction_rate': 87}

    return render_template('constituent_portal.html',
                          public_comments=public_comments,
                          sentiment_stats=sentiment_stats,
                          top_commented=top_commented,
                          foia_requests=foia_requests,
                          foia_stats=foia_stats,
                          complaints=complaints,
                          complaint_categories=complaint_categories,
                          complaint_metrics=complaint_metrics,
                          satisfaction_by_area=satisfaction_by_area,
                          recent_feedback=recent_feedback,
                          stats=stats,
                          title='Constituent Portal')


# ==========================
# SCHOOL BOARD VIEW
# ==========================

@app.route('/school-board')
def school_board_page():
    """Marion County School Board focused view - capital projects and facilities."""
    global current_contracts

    if current_contracts is None:
        load_data()

    # Filter for School District contracts (MCSD prefix)
    df = current_contracts.copy() if current_contracts is not None else pd.DataFrame()

    if not df.empty:
        # Filter for school district contracts (vendor_id starts with MCSD)
        df = df[df['vendor_id'].str.startswith('MCSD', na=False)]

    contracts = df.to_dict('records') if not df.empty else []

    # Calculate summary stats
    total_projects = len(df) if not df.empty else 0
    total_budget = float(df['current_amount'].sum()) if not df.empty else 0
    funded_amount = float(df[df['status'] == 'Active']['current_amount'].sum()) if not df.empty else 0
    proposed_amount = float(df[df['status'] == 'Proposed']['current_amount'].sum()) if not df.empty else 0
    planning_amount = float(df[df['status'] == 'Planning']['current_amount'].sum()) if not df.empty else 0

    # Group by project type
    type_distribution = df['contract_type'].value_counts().to_dict() if not df.empty and 'contract_type' in df.columns else {}

    # Group by status
    status_distribution = df['status'].value_counts().to_dict() if not df.empty and 'status' in df.columns else {}

    # Get top projects by value
    top_projects = df.nlargest(10, 'current_amount').to_dict('records') if not df.empty and len(df) > 0 else []

    # Calculate 5-year projections from fiscal_year data
    yearly_spending = {}
    if not df.empty and 'fiscal_year' in df.columns:
        for _, row in df.iterrows():
            fy = str(row.get('fiscal_year', 'Unknown'))
            if fy not in yearly_spending:
                yearly_spending[fy] = 0
            yearly_spending[fy] += row.get('current_amount', 0)

    # Get aggregated milestone stats for MCSD contracts
    milestone_stats = db.get_aggregated_milestone_stats('MCSD')

    return render_template('school_board.html',
                          contracts=contracts,
                          total_projects=total_projects,
                          total_budget=total_budget,
                          funded_amount=funded_amount,
                          proposed_amount=proposed_amount,
                          planning_amount=planning_amount,
                          type_distribution=type_distribution,
                          status_distribution=status_distribution,
                          top_projects=top_projects,
                          yearly_spending=yearly_spending,
                          milestone_stats=milestone_stats,
                          title='School Board - Capital Projects')


# ==========================
# COUNTY COMPARISON
# ==========================

@app.route('/county-comparison')
def county_comparison_page():
    """County comparison dashboard."""
    # Get comparison data
    comparison_data = db.get_county_comparison_data()
    peer_counties = db.get_peer_counties()

    # Get Marion County local data for comparison
    marion_contracts = current_contracts[
        current_contracts['vendor_id'].str.startswith('MC-', na=False) |
        current_contracts['vendor_id'].str.startswith('MCSD-', na=False)
    ] if current_contracts is not None and not current_contracts.empty else pd.DataFrame()

    local_stats = {
        'total_contracts': len(marion_contracts),
        'total_value': float(marion_contracts['current_amount'].sum()) if not marion_contracts.empty else 0,
        'school_contracts': len(marion_contracts[marion_contracts['vendor_id'].str.startswith('MCSD-', na=False)]) if not marion_contracts.empty else 0,
        'school_value': float(marion_contracts[marion_contracts['vendor_id'].str.startswith('MCSD-', na=False)]['current_amount'].sum()) if not marion_contracts.empty else 0,
        'county_contracts': len(marion_contracts[marion_contracts['vendor_id'].str.startswith('MC-', na=False)]) if not marion_contracts.empty else 0,
        'county_value': float(marion_contracts[marion_contracts['vendor_id'].str.startswith('MC-', na=False)]['current_amount'].sum()) if not marion_contracts.empty else 0,
    }

    return render_template('county_comparison.html',
                          comparison_data=comparison_data,
                          peer_counties=peer_counties,
                          local_stats=local_stats,
                          title='County Comparison')


@app.route('/api/county-comparison')
def api_county_comparison():
    """API endpoint for county comparison data."""
    fiscal_year = request.args.get('fiscal_year')
    comparison_data = db.get_county_comparison_data(fiscal_year)
    return jsonify(comparison_data)


@app.route('/api/counties')
def api_counties():
    """API endpoint for list of counties."""
    counties = db.get_peer_counties()
    return jsonify(counties)


@app.route('/api/county/<county_id>/fiscal')
def api_county_fiscal(county_id):
    """API endpoint for a county's fiscal data."""
    fiscal_year = request.args.get('fiscal_year')
    fiscal_data = db.get_county_fiscal_data(county_id, fiscal_year)
    return jsonify(fiscal_data)


# ==========================
# BENCHMARKING
# ==========================

@app.route('/benchmarking')
def benchmarking_page():
    """Procurement benchmarking dashboard based on Coupa 2025 benchmarks."""
    global current_contracts

    if current_contracts is None:
        load_data()

    entity_id = 'marion_county'  # Default entity
    fiscal_year = request.args.get('fiscal_year', str(datetime.now().year))

    # Get benchmarking engine
    benchmark_engine = get_benchmarking_engine()

    # Get stored KPI values for this entity
    current_values = db.get_kpi_values(entity_id, fiscal_year)

    # If no stored values, estimate from contract data
    if not current_values:
        contracts_list = current_contracts.to_dict('records') if current_contracts is not None else []
        payments = []  # Would get from db.get_all_payments() if available
        vendors = current_vendors.to_dict('records') if current_vendors is not None else []
        current_values = benchmark_engine.estimate_kpis_from_contracts(contracts_list, payments, vendors)

    # Calculate health score
    health_score_result = benchmark_engine.calculate_health_score(current_values)

    # Prepare category scores for template
    category_scores = {}
    for cat_id, cat_name in BENCHMARK_CATEGORIES.items():
        cat_score = health_score_result.category_scores.get(cat_id)
        if cat_score:
            category_scores[cat_id] = {
                'name': cat_name,
                'score': cat_score.score,
                'kpi_count': len(cat_score.kpi_scores)
            }
        else:
            category_scores[cat_id] = {
                'name': cat_name,
                'score': 0,
                'kpi_count': 0
            }

    # Prepare KPI scores for table
    kpi_scores = []
    for cat_score in health_score_result.category_scores.values():
        for kpi in cat_score.kpi_scores:
            kpi_scores.append({
                'name': kpi.name,
                'category_name': BENCHMARK_CATEGORIES.get(kpi.category, kpi.category),
                'actual_value': kpi.actual_value,
                'benchmark_value': kpi.benchmark_value,
                'unit': kpi.unit,
                'gap': kpi.gap,
                'gap_percent': kpi.gap_percent,
                'score': kpi.score,
                'rating': kpi.rating,
                'description': COUPA_BENCHMARKS.get(kpi.kpi_id, {}).get('description', '')
            })

    # Count categories above/below benchmark
    categories_above = sum(1 for cat in category_scores.values() if cat['score'] >= 70)
    categories_below = sum(1 for cat in category_scores.values() if cat['score'] < 70 and cat['kpi_count'] > 0)

    # Prepare benchmark data for the input form
    benchmarks = {}
    for kpi_id, kpi_info in COUPA_BENCHMARKS.items():
        benchmarks[kpi_id] = {
            'name': kpi_info['name'],
            'benchmark': kpi_info['benchmark_value'],
            'unit': kpi_info['unit'],
            'direction': kpi_info['direction'],
            'description': kpi_info.get('description', '')
        }

    # Prepare health score dict for template
    health_score_dict = {
        'overall_score': health_score_result.overall_score,
        'grade': health_score_result.grade,
        'rating': health_score_result.rating,
        'top_strengths': health_score_result.top_strengths,
        'priority_improvements': health_score_result.priority_improvements
    }

    return render_template('benchmarking.html',
                          health_score=health_score_dict,
                          category_scores=category_scores,
                          kpi_scores=kpi_scores,
                          kpi_count=len(current_values),
                          categories_above_benchmark=categories_above,
                          categories_below_benchmark=categories_below,
                          benchmarks=benchmarks,
                          current_values=current_values,
                          title='Procurement Benchmarking')


@app.route('/api/benchmarking/save', methods=['POST'])
def api_save_benchmarks():
    """Save KPI values and recalculate scores."""
    data = request.get_json()
    entity_id = 'marion_county'
    fiscal_year = str(datetime.now().year)

    # Save each KPI value
    for kpi_id, value in data.items():
        if value is not None:
            db.save_kpi_value(entity_id, kpi_id, float(value), fiscal_year)

    # Recalculate and save health score
    benchmark_engine = get_benchmarking_engine()
    kpi_values = db.get_kpi_values(entity_id, fiscal_year)
    health_score = benchmark_engine.calculate_health_score(kpi_values)

    # Save the health score
    score_data = {
        'overall_score': health_score.overall_score,
        'grade': health_score.grade,
        'rating': health_score.rating,
        'top_strengths': health_score.top_strengths,
        'priority_improvements': health_score.priority_improvements
    }
    db.save_health_score(entity_id, score_data, fiscal_year)

    # Save category scores
    for cat_id, cat_score in health_score.category_scores.items():
        db.save_category_score(
            entity_id, cat_id, cat_score.category_name,
            cat_score.score, len(cat_score.kpi_scores),
            cat_score.strengths, cat_score.improvement_areas, fiscal_year
        )

    return jsonify({'success': True, 'overall_score': health_score.overall_score})


@app.route('/api/benchmarking/estimate')
def api_estimate_benchmarks():
    """Estimate KPI values from existing contract data."""
    global current_contracts, current_vendors

    if current_contracts is None:
        load_data()

    benchmark_engine = get_benchmarking_engine()
    contracts_list = current_contracts.to_dict('records') if current_contracts is not None else []
    vendors = current_vendors.to_dict('records') if current_vendors is not None else []

    estimated_values = benchmark_engine.estimate_kpis_from_contracts(contracts_list, [], vendors)

    return jsonify({'estimated_values': estimated_values})


@app.route('/api/benchmarking/health-score')
def api_get_health_score():
    """Get the current health score for an entity."""
    entity_id = request.args.get('entity_id', 'marion_county')
    fiscal_year = request.args.get('fiscal_year')

    health_score = db.get_health_score(entity_id, fiscal_year)
    if health_score:
        return jsonify(health_score)
    return jsonify({'error': 'No health score found'}), 404


@app.route('/api/benchmarking/history')
def api_health_score_history():
    """Get health score history for an entity."""
    entity_id = request.args.get('entity_id', 'marion_county')
    limit = request.args.get('limit', 10, type=int)

    history = db.get_health_score_history(entity_id, limit)
    return jsonify(history)


@app.route('/api/benchmarking/category-scores')
def api_category_scores():
    """Get category scores for an entity."""
    entity_id = request.args.get('entity_id', 'marion_county')
    fiscal_year = request.args.get('fiscal_year')

    scores = db.get_category_scores(entity_id, fiscal_year)
    return jsonify(scores)


@app.route('/api/benchmarking/benchmarks')
def api_get_benchmarks():
    """Get all benchmark definitions."""
    benchmark_engine = get_benchmarking_engine()
    summary = benchmark_engine.get_benchmark_summary()
    return jsonify(summary)


# ==========================
# TEMPLATE CONTEXT
# ==========================

@app.context_processor
def inject_globals():
    """Inject global variables into templates."""
    return {
        'now': datetime.now
    }


if __name__ == '__main__':
    # Check if we need to generate sample data
    contracts = db.get_all_contracts()
    if contracts.empty:
        logger.info("No contracts found, generating sample data...")
        from data.sample_data import generate_sample_data
        generate_sample_data()
        load_data()

    app.run(debug=True, port=5002)
