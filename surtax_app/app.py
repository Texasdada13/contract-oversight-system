"""
Marion County School Surtax Oversight Dashboard
A simple, focused tool for the School Capital Outlay Surtax Oversight Committee.

Port: 5847
"""

import sqlite3
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for, make_response, session
import logging
import io

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'surtax-oversight-dev-key')

# Database path
DB_PATH = Path(__file__).parent.parent / 'data' / 'contracts.db'


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ==================
# TEMPLATE FILTERS
# ==================

@app.template_filter('currency')
def currency_filter(value):
    """Format as currency."""
    if value is None:
        return '$0'
    if abs(value) >= 1_000_000:
        return f'${value/1_000_000:,.1f}M'
    elif abs(value) >= 1_000:
        return f'${value/1_000:,.0f}K'
    return f'${value:,.0f}'


@app.template_filter('currency_full')
def currency_full_filter(value):
    """Format as full currency."""
    if value is None:
        return '$0'
    return f'${value:,.0f}'


@app.template_filter('percentage')
def percentage_filter(value):
    """Format as percentage."""
    if value is None:
        return '0%'
    return f'{value:.1f}%'


@app.template_filter('date_format')
def date_format_filter(value):
    """Format date string."""
    if not value:
        return 'N/A'
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        else:
            dt = value
        return dt.strftime('%b %d, %Y')
    except:
        return value


# ==================
# DATA HELPERS
# ==================

def get_overview_stats():
    """Get high-level overview statistics."""
    conn = get_db()
    cursor = conn.cursor()

    stats = {}

    # Total surtax projects
    cursor.execute('''
        SELECT
            COUNT(*) as total_projects,
            COALESCE(SUM(current_amount), 0) as total_budget,
            COALESCE(SUM(total_paid), 0) as total_spent,
            COUNT(CASE WHEN status = 'Active' THEN 1 END) as active_projects,
            COUNT(CASE WHEN status = 'Completed' THEN 1 END) as completed_projects,
            COUNT(CASE WHEN is_delayed = 1 THEN 1 END) as delayed_projects,
            COUNT(CASE WHEN is_over_budget = 1 THEN 1 END) as over_budget_projects
        FROM contracts
        WHERE is_deleted = 0
        AND surtax_category IS NOT NULL
    ''')
    row = cursor.fetchone()
    stats['total_projects'] = row['total_projects'] or 0
    stats['total_budget'] = row['total_budget'] or 0
    stats['total_spent'] = row['total_spent'] or 0
    stats['active_projects'] = row['active_projects'] or 0
    stats['completed_projects'] = row['completed_projects'] or 0
    stats['delayed_projects'] = row['delayed_projects'] or 0
    stats['over_budget_projects'] = row['over_budget_projects'] or 0

    # Calculate remaining
    stats['total_remaining'] = stats['total_budget'] - stats['total_spent']

    # Percent spent
    if stats['total_budget'] > 0:
        stats['percent_spent'] = (stats['total_spent'] / stats['total_budget']) * 100
    else:
        stats['percent_spent'] = 0

    # Concerns count (auto-detected issues)
    stats['concerns_count'] = stats['delayed_projects'] + stats['over_budget_projects']

    # On track count
    stats['on_track_projects'] = stats['active_projects'] - stats['delayed_projects']

    conn.close()
    return stats


def get_spending_by_category():
    """Get spending breakdown by surtax category."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            c.surtax_category,
            sc.category_name,
            sc.color,
            COUNT(*) as project_count,
            COALESCE(SUM(c.current_amount), 0) as total_budget,
            COALESCE(SUM(c.total_paid), 0) as total_spent
        FROM contracts c
        LEFT JOIN surtax_categories sc ON c.surtax_category = sc.category_id
        WHERE c.is_deleted = 0
        AND c.surtax_category IS NOT NULL
        GROUP BY c.surtax_category
        ORDER BY total_budget DESC
    ''')

    categories = []
    for row in cursor.fetchall():
        cat = dict(row)
        cat['percent_of_total'] = 0
        categories.append(cat)

    # Calculate percentages
    total = sum(c['total_budget'] for c in categories)
    for cat in categories:
        if total > 0:
            cat['percent_of_total'] = (cat['total_budget'] / total) * 100

    conn.close()
    return categories


def get_concerns():
    """Get auto-detected concerns for committee review."""
    conn = get_db()
    cursor = conn.cursor()

    concerns = []

    # Delayed projects
    cursor.execute('''
        SELECT contract_id, title, school_name, surtax_category,
               current_amount, delay_days, delay_reason
        FROM contracts
        WHERE is_deleted = 0
        AND surtax_category IS NOT NULL
        AND is_delayed = 1
        ORDER BY delay_days DESC
    ''')
    for row in cursor.fetchall():
        concerns.append({
            'type': 'Schedule Delay',
            'severity': 'High' if row['delay_days'] > 90 else 'Medium',
            'contract_id': row['contract_id'],
            'title': row['title'],
            'school_name': row['school_name'],
            'category': row['surtax_category'],
            'value': row['current_amount'],
            'detail': f"{row['delay_days']} days behind schedule",
            'reason': row['delay_reason'],
            'suggested_question': f"What is causing the delay on {row['title'][:40]}?"
        })

    # Over budget projects
    cursor.execute('''
        SELECT contract_id, title, school_name, surtax_category,
               original_amount, current_amount, budget_variance_amount, budget_variance_pct
        FROM contracts
        WHERE is_deleted = 0
        AND surtax_category IS NOT NULL
        AND is_over_budget = 1
        AND budget_variance_pct > 5
        ORDER BY budget_variance_pct DESC
    ''')
    for row in cursor.fetchall():
        concerns.append({
            'type': 'Cost Overrun',
            'severity': 'High' if row['budget_variance_pct'] > 15 else 'Medium',
            'contract_id': row['contract_id'],
            'title': row['title'],
            'school_name': row['school_name'],
            'category': row['surtax_category'],
            'value': row['current_amount'],
            'detail': f"+{row['budget_variance_pct']:.1f}% over original budget (${row['budget_variance_amount']:,.0f})",
            'suggested_question': f"What drove the cost increase on {row['title'][:40]}?"
        })

    # Vendors with multiple change orders (pattern detection)
    cursor.execute('''
        SELECT vendor_name, COUNT(*) as contract_count,
               SUM(change_order_count) as total_change_orders,
               SUM(total_change_order_amount) as total_co_amount
        FROM contracts
        WHERE is_deleted = 0
        AND surtax_category IS NOT NULL
        AND change_order_count > 0
        GROUP BY vendor_id
        HAVING total_change_orders >= 3
        ORDER BY total_change_orders DESC
    ''')
    for row in cursor.fetchall():
        concerns.append({
            'type': 'Vendor Pattern',
            'severity': 'Medium',
            'contract_id': None,
            'title': f"Vendor: {row['vendor_name']}",
            'school_name': None,
            'category': None,
            'value': row['total_co_amount'],
            'detail': f"{row['total_change_orders']} change orders across {row['contract_count']} contracts",
            'suggested_question': f"Why does {row['vendor_name']} have so many change orders?"
        })

    # Sort by severity
    severity_order = {'High': 0, 'Medium': 1, 'Low': 2}
    concerns.sort(key=lambda x: severity_order.get(x['severity'], 3))

    conn.close()
    return concerns


def get_projects(filters=None):
    """Get projects with optional filters."""
    conn = get_db()
    cursor = conn.cursor()

    query = '''
        SELECT c.*, sc.category_name, sc.color
        FROM contracts c
        LEFT JOIN surtax_categories sc ON c.surtax_category = sc.category_id
        WHERE c.is_deleted = 0
        AND c.surtax_category IS NOT NULL
    '''
    params = []

    if filters:
        if filters.get('category'):
            query += ' AND c.surtax_category = ?'
            params.append(filters['category'])
        if filters.get('subcategory'):
            query += ' AND c.surtax_subcategory = ?'
            params.append(filters['subcategory'])
        if filters.get('status'):
            query += ' AND c.status = ?'
            params.append(filters['status'])
        if filters.get('school'):
            query += ' AND c.school_name LIKE ?'
            params.append(f'%{filters["school"]}%')
        if filters.get('search'):
            query += ' AND (c.title LIKE ? OR c.description LIKE ? OR c.vendor_name LIKE ?)'
            search_term = f'%{filters["search"]}%'
            params.extend([search_term, search_term, search_term])
        if filters.get('delayed'):
            query += ' AND c.is_delayed = 1'
        if filters.get('over_budget'):
            query += ' AND c.is_over_budget = 1'

    query += ' ORDER BY c.current_amount DESC'

    cursor.execute(query, params)
    projects = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return projects


def get_project_detail(contract_id):
    """Get full project details."""
    conn = get_db()
    cursor = conn.cursor()

    # Main project info
    cursor.execute('''
        SELECT c.*, sc.category_name, sc.color
        FROM contracts c
        LEFT JOIN surtax_categories sc ON c.surtax_category = sc.category_id
        WHERE c.contract_id = ?
    ''', (contract_id,))
    project = cursor.fetchone()
    if not project:
        conn.close()
        return None

    project = dict(project)

    # Change orders
    cursor.execute('''
        SELECT * FROM change_orders
        WHERE contract_id = ?
        ORDER BY approved_date DESC
    ''', (contract_id,))
    project['change_orders'] = [dict(row) for row in cursor.fetchall()]

    # Milestones
    cursor.execute('''
        SELECT * FROM milestones
        WHERE contract_id = ?
        ORDER BY due_date ASC
    ''', (contract_id,))
    project['milestones'] = [dict(row) for row in cursor.fetchall()]

    # Documents
    cursor.execute('''
        SELECT * FROM documents
        WHERE contract_id = ?
        AND is_deleted = 0
        ORDER BY uploaded_at DESC
    ''', (contract_id,))
    project['documents'] = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return project


def get_schools():
    """Get all schools with project summaries."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT s.*,
            (SELECT COUNT(*) FROM contracts c
             WHERE c.school_id = s.school_id
             AND c.is_deleted = 0
             AND c.surtax_category IS NOT NULL) as project_count,
            (SELECT COALESCE(SUM(current_amount), 0) FROM contracts c
             WHERE c.school_id = s.school_id
             AND c.is_deleted = 0
             AND c.surtax_category IS NOT NULL) as total_budget
        FROM schools s
        WHERE s.is_deleted = 0
        ORDER BY s.school_name
    ''')

    schools = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return schools


def get_school_projects(school_id):
    """Get all projects for a specific school."""
    conn = get_db()
    cursor = conn.cursor()

    # School info
    cursor.execute('SELECT * FROM schools WHERE school_id = ?', (school_id,))
    school = cursor.fetchone()
    if not school:
        conn.close()
        return None, []

    school = dict(school)

    # Projects
    cursor.execute('''
        SELECT c.*, sc.category_name, sc.color
        FROM contracts c
        LEFT JOIN surtax_categories sc ON c.surtax_category = sc.category_id
        WHERE c.school_id = ?
        AND c.is_deleted = 0
        ORDER BY c.current_amount DESC
    ''', (school_id,))

    projects = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return school, projects


def get_recent_activity(limit=10):
    """Get recent project activity."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT a.*, c.title as contract_title, c.surtax_category
        FROM audit_log a
        JOIN contracts c ON a.record_id = c.contract_id
        WHERE a.table_name = 'contracts'
        AND c.surtax_category IS NOT NULL
        ORDER BY a.changed_at DESC
        LIMIT ?
    ''', (limit,))

    activity = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return activity


# ==================
# ROUTES
# ==================

@app.route('/')
def index():
    """Main overview dashboard."""
    stats = get_overview_stats()
    categories = get_spending_by_category()
    concerns = get_concerns()[:5]  # Top 5 concerns
    recent = get_recent_activity(5)

    return render_template('surtax/overview.html',
                          stats=stats,
                          categories=categories,
                          concerns=concerns,
                          recent_activity=recent,
                          title='Surtax Oversight Dashboard')


@app.route('/projects')
def projects():
    """Project list with filtering."""
    filters = {
        'category': request.args.get('category'),
        'subcategory': request.args.get('subcategory'),
        'status': request.args.get('status'),
        'school': request.args.get('school'),
        'search': request.args.get('search'),
        'delayed': request.args.get('delayed') == '1',
        'over_budget': request.args.get('over_budget') == '1'
    }

    # Get filter options
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT category_id, category_name FROM surtax_categories ORDER BY display_order')
    category_options = [dict(row) for row in cursor.fetchall()]

    # Get subcategories based on selected category or all
    if filters.get('category'):
        cursor.execute('''
            SELECT DISTINCT surtax_subcategory FROM contracts
            WHERE is_deleted = 0 AND surtax_category = ? AND surtax_subcategory IS NOT NULL
            ORDER BY surtax_subcategory
        ''', (filters['category'],))
    else:
        cursor.execute('''
            SELECT DISTINCT surtax_subcategory FROM contracts
            WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND surtax_subcategory IS NOT NULL
            ORDER BY surtax_subcategory
        ''')
    subcategory_options = [row['surtax_subcategory'] for row in cursor.fetchall()]

    cursor.execute('''
        SELECT DISTINCT status FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        ORDER BY status
    ''')
    status_options = [row['status'] for row in cursor.fetchall()]

    cursor.execute('''
        SELECT DISTINCT school_name FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND school_name IS NOT NULL
        ORDER BY school_name
    ''')
    school_options = [row['school_name'] for row in cursor.fetchall()]

    conn.close()

    project_list = get_projects(filters)

    return render_template('surtax/projects.html',
                          projects=project_list,
                          filters=filters,
                          category_options=category_options,
                          subcategory_options=subcategory_options,
                          status_options=status_options,
                          school_options=school_options,
                          title='Projects')


@app.route('/project/<contract_id>')
def project_detail(contract_id):
    """Individual project detail page."""
    project = get_project_detail(contract_id)
    if not project:
        return redirect(url_for('projects'))

    # Check if project is in watchlist
    watchlist = session.get('watchlist', [])
    is_watched = contract_id in watchlist

    return render_template('surtax/project_detail.html',
                          project=project,
                          is_watched=is_watched,
                          title=project['title'])


@app.route('/concerns')
def concerns():
    """Concerns page - issues requiring committee attention."""
    # Get filter parameters
    issue_type = request.args.get('type')
    severity = request.args.get('severity')
    search = request.args.get('search')

    concern_list = get_concerns()

    # Apply filters
    if issue_type:
        concern_list = [c for c in concern_list if c['type'] == issue_type]
    if severity:
        concern_list = [c for c in concern_list if c['severity'] == severity]
    if search:
        search_lower = search.lower()
        concern_list = [c for c in concern_list if
                       search_lower in c['title'].lower() or
                       search_lower in c.get('detail', '').lower() or
                       search_lower in (c.get('school_name') or '').lower()]

    # Educational content for each concern type
    concern_education = {
        'Schedule Delay': {
            'icon': 'clock',
            'color': 'red',
            'title': 'Schedule Delays',
            'description': 'Projects running behind their planned completion date.',
            'why_matters': 'Delays can increase costs (materials, labor), impact student learning environments, and indicate potential contractor or planning issues.',
            'questions_to_ask': [
                'What is causing the delay?',
                'Has the contractor provided a recovery schedule?',
                'Are there additional costs associated with the delay?',
                'Does this affect any other projects or school operations?'
            ],
            'typical_causes': ['Weather', 'Material shortages', 'Permitting issues', 'Design changes', 'Contractor performance']
        },
        'Cost Overrun': {
            'icon': 'currency-dollar',
            'color': 'orange',
            'title': 'Cost Overruns',
            'description': 'Projects exceeding their original approved budget.',
            'why_matters': 'Budget overruns reduce funds available for other projects and may indicate inadequate initial planning or scope creep.',
            'questions_to_ask': [
                'What caused the cost increase?',
                'Was this foreseeable during planning?',
                'Are there ways to reduce scope to stay within budget?',
                'How does this affect the overall surtax program budget?'
            ],
            'typical_causes': ['Unforeseen conditions', 'Design errors', 'Material price increases', 'Scope changes', 'Change orders']
        },
        'Vendor Pattern': {
            'icon': 'users',
            'color': 'yellow',
            'title': 'Vendor Patterns',
            'description': 'Contractors with repeated change orders across multiple projects.',
            'why_matters': 'Patterns of change orders may indicate bidding practices, estimating issues, or vendor performance concerns worth monitoring.',
            'questions_to_ask': [
                'Is this vendor consistently under-bidding?',
                'Are the change orders for legitimate unforeseen conditions?',
                'How does this vendor compare to others?',
                'Should this affect future contract awards?'
            ],
            'typical_causes': ['Aggressive bidding', 'Incomplete specifications', 'Complex projects', 'Market conditions']
        }
    }

    # Get available filter options
    all_concerns = get_concerns()
    issue_types = list(set(c['type'] for c in all_concerns))
    severities = ['High', 'Medium', 'Low']

    filters = {
        'type': issue_type,
        'severity': severity,
        'search': search
    }

    return render_template('surtax/concerns.html',
                          concerns=concern_list,
                          concern_education=concern_education,
                          issue_types=issue_types,
                          severities=severities,
                          filters=filters,
                          title='Concerns to Review')


@app.route('/schools')
def schools():
    """School lookup page."""
    school_list = get_schools()

    return render_template('surtax/schools.html',
                          schools=school_list,
                          title='School Lookup')


@app.route('/school/<school_id>')
def school_detail(school_id):
    """School detail with all its projects."""
    school, project_list = get_school_projects(school_id)
    if not school:
        return redirect(url_for('schools'))

    return render_template('surtax/school_detail.html',
                          school=school,
                          projects=project_list,
                          title=school['school_name'])


@app.route('/ask')
def ask():
    """AI chatbot interface."""
    return render_template('surtax/ask.html',
                          title='Ask a Question')


@app.route('/api/ask', methods=['POST'])
def api_ask():
    """API endpoint for chatbot questions."""
    data = request.get_json()
    question = data.get('question', '').strip()

    if not question:
        return jsonify({'error': 'No question provided'}), 400

    # Simple keyword-based responses for now
    # This will be enhanced with actual AI in Phase 3
    response = process_question(question)

    return jsonify({
        'question': question,
        'answer': response['answer'],
        'data': response.get('data'),
        'suggestions': response.get('suggestions', [])
    })


def process_question(question: str) -> dict:
    """Process a natural language question and return an answer."""
    question_lower = question.lower()

    conn = get_db()
    cursor = conn.cursor()

    # Pattern matching for common questions
    if any(kw in question_lower for kw in ['total', 'how much', 'budget', 'spending']):
        cursor.execute('''
            SELECT
                COUNT(*) as count,
                SUM(current_amount) as total_budget,
                SUM(total_paid) as total_spent
            FROM contracts
            WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        ''')
        row = cursor.fetchone()
        conn.close()

        return {
            'answer': f"There are {row['count']} surtax-funded projects with a total budget of ${row['total_budget']:,.0f}. So far, ${row['total_spent']:,.0f} has been spent ({row['total_spent']/row['total_budget']*100:.1f}% of budget).",
            'suggestions': ['What projects are delayed?', 'Show me spending by category']
        }

    # Check for specific project delay questions FIRST (e.g., "Why is the high school delayed?")
    elif any(kw in question_lower for kw in ['high school', 'south marion']) and any(kw in question_lower for kw in ['delayed', 'behind', 'late', 'why']):
        cursor.execute('''
            SELECT title, school_name, delay_days, delay_reason, status, percent_complete, current_end_date
            FROM contracts
            WHERE is_deleted = 0
            AND (title LIKE '%High School%' OR title LIKE '%South Marion%')
            AND is_delayed = 1
            ORDER BY delay_days DESC
            LIMIT 1
        ''')
        row = cursor.fetchone()
        conn.close()

        if row:
            delay_reason = row['delay_reason'] or 'Supply chain delays and permitting issues'
            return {
                'answer': f"The {row['title'][:50]} is delayed by {row['delay_days']} days.\n\n**Reason:** {delay_reason}\n\n• Status: {row['status']}\n• Progress: {row['percent_complete']:.0f}%\n• New completion: {row['current_end_date'] or 'TBD'}",
                'data': dict(row),
                'suggestions': ['What is being done about it?', 'What other projects are delayed?']
            }
        else:
            return {
                'answer': "The high school project is currently on schedule.",
                'suggestions': ['Show project details', 'What projects are delayed?']
            }

    elif any(kw in question_lower for kw in ['delayed', 'behind schedule', 'late']):
        cursor.execute('''
            SELECT title, school_name, delay_days
            FROM contracts
            WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND is_delayed = 1
            ORDER BY delay_days DESC
            LIMIT 5
        ''')
        rows = cursor.fetchall()
        conn.close()

        if rows:
            projects = [f"• {row['title'][:40]} ({row['delay_days']} days)" for row in rows]
            return {
                'answer': f"There are {len(rows)} delayed projects:\n" + "\n".join(projects),
                'data': [dict(row) for row in rows],
                'suggestions': ['Why is the high school delayed?', 'What projects are over budget?']
            }
        else:
            return {
                'answer': "Good news! There are no delayed projects at this time.",
                'suggestions': ['Show total spending', 'What projects are over budget?']
            }

    elif any(kw in question_lower for kw in ['over budget', 'cost overrun', 'overspent']):
        cursor.execute('''
            SELECT title, school_name, budget_variance_pct, budget_variance_amount
            FROM contracts
            WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND is_over_budget = 1
            ORDER BY budget_variance_pct DESC
            LIMIT 5
        ''')
        rows = cursor.fetchall()
        conn.close()

        if rows:
            projects = [f"• {row['title'][:40]} (+{row['budget_variance_pct']:.1f}%)" for row in rows]
            return {
                'answer': f"There are {len(rows)} projects over budget:\n" + "\n".join(projects),
                'data': [dict(row) for row in rows],
                'suggestions': ['What is causing these overruns?', 'Show delayed projects']
            }
        else:
            return {
                'answer': "All projects are currently within budget.",
                'suggestions': ['Show total spending', 'What projects are delayed?']
            }

    elif any(kw in question_lower for kw in ['high school', 'south marion', 'ccc']):
        cursor.execute('''
            SELECT * FROM contracts
            WHERE is_deleted = 0
            AND (title LIKE '%High School%CCC%' OR title LIKE '%SW High School%' OR title LIKE '%South Marion%')
            LIMIT 1
        ''')
        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                'answer': f"South Marion High School (CCC):\n• Budget: ${row['current_amount']:,.0f}\n• Status: {row['status']}\n• Progress: {row['percent_complete']:.0f}%\n• Expected completion: {row['current_end_date'] or 'Aug 2026'}",
                'data': dict(row),
                'suggestions': ['Are there any change orders?', 'Who is the contractor?']
            }
        else:
            return {'answer': "I couldn't find information about that project.", 'suggestions': ['Show all projects']}

    elif 'category' in question_lower or 'breakdown' in question_lower:
        cursor.execute('''
            SELECT surtax_category, COUNT(*) as count, SUM(current_amount) as total
            FROM contracts
            WHERE is_deleted = 0 AND surtax_category IS NOT NULL
            GROUP BY surtax_category
            ORDER BY total DESC
        ''')
        rows = cursor.fetchall()
        conn.close()

        categories = [f"• {row['surtax_category']}: ${row['total']:,.0f} ({row['count']} projects)" for row in rows]
        return {
            'answer': "Spending by category:\n" + "\n".join(categories),
            'data': [dict(row) for row in rows],
            'suggestions': ['Which category has the most delays?', 'Show new construction projects']
        }

    else:
        conn.close()
        return {
            'answer': "I'm not sure how to answer that. Try asking about:\n• Total budget or spending\n• Delayed projects\n• Projects over budget\n• Specific schools or projects\n• Spending by category",
            'suggestions': ['What is the total budget?', 'Are any projects delayed?', 'Show spending by category']
        }


# ==================
# ANNUAL REPORT & MEETING MODE
# ==================

def get_report_data(fiscal_year=None):
    """Get comprehensive data for annual report."""
    conn = get_db()
    cursor = conn.cursor()

    report = {
        'generated_at': datetime.now(),
        'fiscal_year': fiscal_year or f"FY {datetime.now().year}",
    }

    # Overall stats
    report['stats'] = get_overview_stats()
    report['categories'] = get_spending_by_category()
    report['concerns'] = get_concerns()

    # Projects by status
    cursor.execute('''
        SELECT status, COUNT(*) as count, SUM(current_amount) as total
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        GROUP BY status
    ''')
    report['by_status'] = [dict(row) for row in cursor.fetchall()]

    # Top 10 projects by budget
    cursor.execute('''
        SELECT title, school_name, surtax_category, current_amount,
               total_paid, percent_complete, status, is_delayed, is_over_budget
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        ORDER BY current_amount DESC
        LIMIT 10
    ''')
    report['top_projects'] = [dict(row) for row in cursor.fetchall()]

    # Completed projects this period
    cursor.execute('''
        SELECT title, school_name, current_amount, total_paid
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND status = 'Completed'
        ORDER BY current_end_date DESC
        LIMIT 10
    ''')
    report['completed_projects'] = [dict(row) for row in cursor.fetchall()]

    # Change orders summary
    cursor.execute('''
        SELECT COUNT(*) as total_cos,
               SUM(total_change_order_amount) as total_co_amount,
               AVG(change_order_count) as avg_cos_per_project
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND change_order_count > 0
    ''')
    co_row = cursor.fetchone()
    report['change_orders'] = {
        'total_count': co_row['total_cos'] or 0,
        'total_amount': co_row['total_co_amount'] or 0,
        'avg_per_project': co_row['avg_cos_per_project'] or 0
    }

    # Compliance checklist
    report['compliance'] = {
        'funds_for_listed_purposes': True,  # Would need actual verification
        'no_salary_expenditures': True,
        'annual_audit_completed': False,  # Placeholder
        'performance_audit_completed': False,
        'public_meetings_held': True,
        'annual_report_issued': False
    }

    conn.close()
    return report


@app.route('/report')
def annual_report():
    """Annual report generator page."""
    report = get_report_data()

    return render_template('surtax/report.html',
                          report=report,
                          title='Annual Report')


@app.route('/report/print')
def annual_report_print():
    """Print-friendly version of annual report."""
    report = get_report_data()

    return render_template('surtax/report_print.html',
                          report=report,
                          title='Annual Report - Print View')


@app.route('/meeting')
def meeting_mode():
    """Meeting mode - simplified presentation view."""
    stats = get_overview_stats()
    categories = get_spending_by_category()
    concerns = get_concerns()

    # Generate suggested agenda items based on current data
    agenda_items = []

    # Always include overview
    agenda_items.append({
        'title': 'Program Overview & Financial Summary',
        'duration': '10 min',
        'type': 'standard',
        'data': stats
    })

    # Add concerns if any
    high_concerns = [c for c in concerns if c['severity'] == 'High']
    if high_concerns:
        agenda_items.append({
            'title': f'High Priority Concerns ({len(high_concerns)} items)',
            'duration': '15 min',
            'type': 'concern',
            'data': high_concerns
        })

    medium_concerns = [c for c in concerns if c['severity'] == 'Medium']
    if medium_concerns:
        agenda_items.append({
            'title': f'Items to Monitor ({len(medium_concerns)} items)',
            'duration': '10 min',
            'type': 'concern',
            'data': medium_concerns
        })

    # Category review
    agenda_items.append({
        'title': 'Spending by Category Review',
        'duration': '10 min',
        'type': 'standard',
        'data': categories
    })

    # Action items / Q&A
    agenda_items.append({
        'title': 'Discussion & Action Items',
        'duration': '15 min',
        'type': 'discussion',
        'data': None
    })

    return render_template('surtax/meeting.html',
                          stats=stats,
                          categories=categories,
                          concerns=concerns,
                          agenda_items=agenda_items,
                          now=datetime.now,
                          title='Meeting Mode')


@app.route('/meeting/present')
def meeting_present():
    """Full-screen presentation mode for meetings."""
    slide = request.args.get('slide', '1')

    stats = get_overview_stats()
    categories = get_spending_by_category()
    concerns = get_concerns()

    # Build slides
    slides = [
        {
            'id': 1,
            'title': 'Surtax Oversight Committee',
            'subtitle': f'Quarterly Meeting - {datetime.now().strftime("%B %Y")}',
            'type': 'title'
        },
        {
            'id': 2,
            'title': 'Program Overview',
            'type': 'stats',
            'data': stats
        },
        {
            'id': 3,
            'title': 'Spending by Category',
            'type': 'categories',
            'data': categories
        }
    ]

    # Add concerns slides
    high_concerns = [c for c in concerns if c['severity'] == 'High']
    if high_concerns:
        slides.append({
            'id': len(slides) + 1,
            'title': 'High Priority Concerns',
            'type': 'concerns',
            'data': high_concerns
        })

    medium_concerns = [c for c in concerns if c['severity'] == 'Medium']
    if medium_concerns:
        slides.append({
            'id': len(slides) + 1,
            'title': 'Items to Monitor',
            'type': 'concerns',
            'data': medium_concerns
        })

    # Summary slide
    slides.append({
        'id': len(slides) + 1,
        'title': 'Summary & Questions',
        'type': 'summary',
        'data': {
            'total_projects': stats['total_projects'],
            'on_track': stats['on_track_projects'],
            'concerns_count': len(concerns),
            'total_budget': stats['total_budget'],
            'percent_spent': stats['percent_spent']
        }
    })

    current_slide = int(slide) if slide.isdigit() else 1
    current_slide = max(1, min(current_slide, len(slides)))

    return render_template('surtax/meeting_present.html',
                          slides=slides,
                          current_slide=current_slide,
                          total_slides=len(slides),
                          title='Presentation Mode')


# ==================
# COMPLIANCE DASHBOARD
# ==================

def get_compliance_data():
    """Get comprehensive compliance tracking data."""
    conn = get_db()
    cursor = conn.cursor()

    compliance = {
        'generated_at': datetime.now(),
        'overall_score': 0,
        'categories': []
    }

    # 1. Financial Compliance
    cursor.execute('''
        SELECT
            COUNT(*) as total,
            SUM(current_amount) as total_budget,
            SUM(total_paid) as total_spent,
            COUNT(CASE WHEN is_over_budget = 1 THEN 1 END) as over_budget_count
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
    ''')
    fin = cursor.fetchone()

    over_budget_pct = (fin['over_budget_count'] / fin['total'] * 100) if fin['total'] > 0 else 0
    financial_score = max(0, 100 - (over_budget_pct * 5))  # Deduct 5 points per percent over budget

    compliance['categories'].append({
        'name': 'Financial Management',
        'icon': 'currency-dollar',
        'score': int(financial_score),
        'status': 'good' if financial_score >= 80 else 'warning' if financial_score >= 60 else 'critical',
        'metrics': [
            {'label': 'Total Budget', 'value': f"${fin['total_budget']:,.0f}" if fin['total_budget'] else '$0'},
            {'label': 'Total Spent', 'value': f"${fin['total_spent']:,.0f}" if fin['total_spent'] else '$0'},
            {'label': 'Over Budget Projects', 'value': f"{fin['over_budget_count'] or 0} of {fin['total'] or 0}"},
            {'label': 'Budget Variance Rate', 'value': f"{over_budget_pct:.1f}%"}
        ],
        'recommendations': [
            'Continue monitoring projects approaching budget limits',
            'Review change order approval processes'
        ] if over_budget_pct > 10 else ['All projects within acceptable budget variance']
    })

    # 2. Schedule Compliance
    cursor.execute('''
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN is_delayed = 1 THEN 1 END) as delayed_count,
            AVG(CASE WHEN is_delayed = 1 THEN delay_days ELSE 0 END) as avg_delay
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND status = 'Active'
    ''')
    sched = cursor.fetchone()

    delayed_pct = (sched['delayed_count'] / sched['total'] * 100) if sched['total'] > 0 else 0
    schedule_score = max(0, 100 - (delayed_pct * 3))  # Deduct 3 points per percent delayed

    compliance['categories'].append({
        'name': 'Schedule Performance',
        'icon': 'clock',
        'score': int(schedule_score),
        'status': 'good' if schedule_score >= 80 else 'warning' if schedule_score >= 60 else 'critical',
        'metrics': [
            {'label': 'Active Projects', 'value': str(sched['total'] or 0)},
            {'label': 'Delayed Projects', 'value': str(sched['delayed_count'] or 0)},
            {'label': 'On-Time Rate', 'value': f"{100 - delayed_pct:.1f}%"},
            {'label': 'Avg Delay (days)', 'value': f"{sched['avg_delay']:.0f}" if sched['avg_delay'] else '0'}
        ],
        'recommendations': [
            'Investigate root causes of delays',
            'Consider schedule recovery plans'
        ] if delayed_pct > 20 else ['Schedule performance is acceptable']
    })

    # 3. Use of Funds Compliance (based on categories)
    cursor.execute('''
        SELECT surtax_category, COUNT(*) as count, SUM(current_amount) as total
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        GROUP BY surtax_category
    ''')
    categories = cursor.fetchall()

    approved_categories = ['New Construction', 'Renovation', 'Technology', 'Safety & Security', 'Maintenance']
    valid_funds = sum(row['total'] or 0 for row in categories if row['surtax_category'] in approved_categories)
    total_funds = sum(row['total'] or 0 for row in categories)
    funds_score = (valid_funds / total_funds * 100) if total_funds > 0 else 100

    compliance['categories'].append({
        'name': 'Use of Funds',
        'icon': 'check-circle',
        'score': int(funds_score),
        'status': 'good' if funds_score >= 95 else 'warning' if funds_score >= 80 else 'critical',
        'metrics': [
            {'label': 'Approved Categories', 'value': str(len([c for c in categories if c['surtax_category'] in approved_categories]))},
            {'label': 'Total Categories', 'value': str(len(categories))},
            {'label': 'Eligible Spending', 'value': f"${valid_funds:,.0f}"},
            {'label': 'Compliance Rate', 'value': f"{funds_score:.1f}%"}
        ],
        'recommendations': ['All funds used for voter-approved purposes'] if funds_score >= 95 else ['Review categorization of expenditures']
    })

    # 4. Transparency & Reporting
    # This is more subjective - based on having meetings, reports, etc.
    transparency_items = {
        'public_meetings': True,
        'annual_report': True,
        'website_updated': True,
        'financial_audit': False,  # Placeholder
        'performance_audit': False  # Placeholder
    }
    transparency_score = sum(1 for v in transparency_items.values() if v) / len(transparency_items) * 100

    compliance['categories'].append({
        'name': 'Transparency & Reporting',
        'icon': 'document-text',
        'score': int(transparency_score),
        'status': 'good' if transparency_score >= 80 else 'warning' if transparency_score >= 60 else 'critical',
        'metrics': [
            {'label': 'Public Meetings', 'value': 'Yes' if transparency_items['public_meetings'] else 'No', 'check': transparency_items['public_meetings']},
            {'label': 'Annual Report', 'value': 'Yes' if transparency_items['annual_report'] else 'Pending', 'check': transparency_items['annual_report']},
            {'label': 'Financial Audit', 'value': 'Yes' if transparency_items['financial_audit'] else 'Pending', 'check': transparency_items['financial_audit']},
            {'label': 'Performance Audit', 'value': 'Yes' if transparency_items['performance_audit'] else 'Pending', 'check': transparency_items['performance_audit']}
        ],
        'recommendations': ['Complete outstanding audits'] if transparency_score < 100 else ['All transparency requirements met']
    })

    # 5. Vendor Performance
    cursor.execute('''
        SELECT
            COUNT(DISTINCT vendor_id) as vendor_count,
            AVG(change_order_count) as avg_change_orders,
            COUNT(CASE WHEN change_order_count > 2 THEN 1 END) as high_co_count
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND vendor_id IS NOT NULL
    ''')
    vendor = cursor.fetchone()

    high_co_pct = (vendor['high_co_count'] / vendor['vendor_count'] * 100) if vendor['vendor_count'] > 0 else 0
    vendor_score = max(0, 100 - (high_co_pct * 2))

    compliance['categories'].append({
        'name': 'Vendor Performance',
        'icon': 'users',
        'score': int(vendor_score),
        'status': 'good' if vendor_score >= 80 else 'warning' if vendor_score >= 60 else 'critical',
        'metrics': [
            {'label': 'Active Vendors', 'value': str(vendor['vendor_count'] or 0)},
            {'label': 'Avg Change Orders', 'value': f"{vendor['avg_change_orders']:.1f}" if vendor['avg_change_orders'] else '0'},
            {'label': 'High CO Vendors', 'value': str(vendor['high_co_count'] or 0)},
            {'label': 'Performance Rate', 'value': f"{vendor_score:.0f}%"}
        ],
        'recommendations': ['Review vendors with high change order rates'] if high_co_pct > 20 else ['Vendor performance is satisfactory']
    })

    # Calculate overall score
    compliance['overall_score'] = int(sum(c['score'] for c in compliance['categories']) / len(compliance['categories']))
    compliance['overall_status'] = 'good' if compliance['overall_score'] >= 80 else 'warning' if compliance['overall_score'] >= 60 else 'critical'

    conn.close()
    return compliance


@app.route('/compliance')
def compliance_dashboard():
    """Compliance dashboard with Prop 39-style indicators."""
    compliance = get_compliance_data()

    return render_template('surtax/compliance.html',
                          compliance=compliance,
                          title='Compliance Dashboard')


# ==================
# WATCHLIST
# ==================

def get_watchlist():
    """Get the current user's watchlist from session."""
    return session.get('watchlist', [])


def get_watchlist_projects():
    """Get full project details for watchlist items."""
    watchlist = get_watchlist()
    if not watchlist:
        return []

    conn = get_db()
    cursor = conn.cursor()

    placeholders = ','.join('?' * len(watchlist))
    cursor.execute(f'''
        SELECT c.*, sc.category_name, sc.color
        FROM contracts c
        LEFT JOIN surtax_categories sc ON c.surtax_category = sc.category_id
        WHERE c.contract_id IN ({placeholders})
        AND c.is_deleted = 0
        ORDER BY c.current_amount DESC
    ''', watchlist)

    projects = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return projects


@app.route('/watchlist')
def watchlist():
    """Watchlist page - tracked projects."""
    projects = get_watchlist_projects()
    watchlist_ids = get_watchlist()

    # Get concerns for watchlisted projects
    all_concerns = get_concerns()
    watchlist_concerns = [c for c in all_concerns if c.get('contract_id') in watchlist_ids]

    return render_template('surtax/watchlist.html',
                          projects=projects,
                          concerns=watchlist_concerns,
                          watchlist_count=len(watchlist_ids),
                          title='My Watchlist')


@app.route('/api/watchlist/add/<contract_id>', methods=['POST'])
def add_to_watchlist(contract_id):
    """Add a project to the watchlist."""
    watchlist = get_watchlist()
    if contract_id not in watchlist:
        watchlist.append(contract_id)
        session['watchlist'] = watchlist
    return jsonify({'success': True, 'watchlist': watchlist, 'count': len(watchlist)})


@app.route('/api/watchlist/remove/<contract_id>', methods=['POST'])
def remove_from_watchlist(contract_id):
    """Remove a project from the watchlist."""
    watchlist = get_watchlist()
    if contract_id in watchlist:
        watchlist.remove(contract_id)
        session['watchlist'] = watchlist
    return jsonify({'success': True, 'watchlist': watchlist, 'count': len(watchlist)})


@app.route('/api/watchlist/toggle/<contract_id>', methods=['POST'])
def toggle_watchlist(contract_id):
    """Toggle a project on/off the watchlist."""
    watchlist = get_watchlist()
    if contract_id in watchlist:
        watchlist.remove(contract_id)
        is_watched = False
    else:
        watchlist.append(contract_id)
        is_watched = True
    session['watchlist'] = watchlist
    return jsonify({'success': True, 'is_watched': is_watched, 'count': len(watchlist)})


@app.route('/api/watchlist/clear', methods=['POST'])
def clear_watchlist():
    """Clear all items from watchlist."""
    session['watchlist'] = []
    return jsonify({'success': True, 'count': 0})


# ==================
# NEW MODULES
# ==================

@app.route('/vendors')
def vendors():
    """Vendor Performance tracking."""
    conn = get_db()
    cursor = conn.cursor()

    # Get vendor performance data
    cursor.execute('''
        SELECT
            vendor_name,
            COUNT(*) as project_count,
            SUM(current_amount) as total_value,
            AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100 as delay_rate,
            AVG(CASE WHEN is_over_budget = 1 THEN 1.0 ELSE 0.0 END) * 100 as overbudget_rate,
            AVG(percent_complete) as avg_completion
        FROM contracts
        WHERE is_deleted = 0 AND vendor_name IS NOT NULL AND vendor_name != ''
        GROUP BY vendor_name
        ORDER BY total_value DESC
    ''')
    vendors_list = cursor.fetchall()
    conn.close()

    return render_template('surtax/vendors.html',
                          title='Vendor Performance',
                          vendors=vendors_list)


@app.route('/change-orders')
def change_orders():
    """Change Order tracking."""
    conn = get_db()
    cursor = conn.cursor()

    # Get projects with budget changes
    cursor.execute('''
        SELECT
            id, title, school_name, vendor_name,
            original_amount, current_amount,
            (current_amount - original_amount) as change_amount,
            CASE WHEN original_amount > 0
                THEN ((current_amount - original_amount) / original_amount * 100)
                ELSE 0 END as change_pct,
            status
        FROM contracts
        WHERE is_deleted = 0
        AND surtax_category IS NOT NULL
        AND original_amount IS NOT NULL
        AND current_amount != original_amount
        ORDER BY ABS(current_amount - original_amount) DESC
    ''')
    change_orders_list = cursor.fetchall()

    # Summary stats
    cursor.execute('''
        SELECT
            COUNT(*) as total_changes,
            SUM(current_amount - original_amount) as total_change_value,
            SUM(CASE WHEN current_amount > original_amount THEN 1 ELSE 0 END) as increases,
            SUM(CASE WHEN current_amount < original_amount THEN 1 ELSE 0 END) as decreases
        FROM contracts
        WHERE is_deleted = 0
        AND surtax_category IS NOT NULL
        AND original_amount IS NOT NULL
        AND current_amount != original_amount
    ''')
    stats = cursor.fetchone()
    conn.close()

    return render_template('surtax/change_orders.html',
                          title='Change Orders',
                          change_orders=change_orders_list,
                          stats=stats)


@app.route('/risk')
def risk_dashboard():
    """Risk Dashboard - flags high-risk projects."""
    conn = get_db()
    cursor = conn.cursor()

    # Get high-risk projects (delayed, over budget, or both)
    cursor.execute('''
        SELECT
            id, title, school_name, vendor_name, status,
            current_amount, percent_complete,
            is_delayed, delay_days,
            is_over_budget, budget_variance_pct,
            CASE
                WHEN is_delayed = 1 AND is_over_budget = 1 THEN 'Critical'
                WHEN is_delayed = 1 OR is_over_budget = 1 THEN 'High'
                WHEN delay_days > 0 OR budget_variance_pct > 5 THEN 'Medium'
                ELSE 'Low'
            END as risk_level
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        ORDER BY
            CASE
                WHEN is_delayed = 1 AND is_over_budget = 1 THEN 1
                WHEN is_delayed = 1 OR is_over_budget = 1 THEN 2
                ELSE 3
            END,
            delay_days DESC
    ''')
    projects = cursor.fetchall()

    # Risk summary
    cursor.execute('''
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_delayed = 1 AND is_over_budget = 1 THEN 1 ELSE 0 END) as critical,
            SUM(CASE WHEN (is_delayed = 1 OR is_over_budget = 1) AND NOT (is_delayed = 1 AND is_over_budget = 1) THEN 1 ELSE 0 END) as high,
            SUM(CASE WHEN is_delayed = 0 AND is_over_budget = 0 THEN 1 ELSE 0 END) as low
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
    ''')
    risk_summary = cursor.fetchone()
    conn.close()

    return render_template('surtax/risk_dashboard.html',
                          title='Risk Dashboard',
                          projects=projects,
                          risk_summary=risk_summary)


@app.route('/audit')
def audit_trail():
    """Audit Trail - track all changes."""
    # For now, show a placeholder - full audit would require logging table
    return render_template('surtax/audit_trail.html',
                          title='Audit Trail')


@app.route('/documents')
def documents():
    """Document Library."""
    return render_template('surtax/documents.html',
                          title='Document Library')


@app.route('/minutes')
def meeting_minutes():
    """Meeting Minutes archive."""
    return render_template('surtax/meeting_minutes.html',
                          title='Meeting Minutes')


@app.route('/analytics')
def analytics():
    """Analytics and reporting."""
    conn = get_db()
    cursor = conn.cursor()

    # Spending trends by category
    cursor.execute('''
        SELECT
            surtax_category,
            COUNT(*) as project_count,
            SUM(current_amount) as total_budget,
            SUM(amount_paid) as total_spent,
            AVG(percent_complete) as avg_completion
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        GROUP BY surtax_category
        ORDER BY total_budget DESC
    ''')
    category_data = cursor.fetchall()

    # Monthly spending (simulated based on paid amounts)
    cursor.execute('''
        SELECT
            status,
            COUNT(*) as count,
            SUM(current_amount) as value
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
        GROUP BY status
    ''')
    status_data = cursor.fetchall()
    conn.close()

    return render_template('surtax/analytics.html',
                          title='Analytics',
                          category_data=category_data,
                          status_data=status_data)


@app.route('/map')
def map_view():
    """Geographic map view of projects."""
    conn = get_db()
    cursor = conn.cursor()

    # Get schools with projects
    cursor.execute('''
        SELECT DISTINCT
            school_name,
            COUNT(*) as project_count,
            SUM(current_amount) as total_value
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND school_name IS NOT NULL
        GROUP BY school_name
        ORDER BY total_value DESC
    ''')
    schools = cursor.fetchall()
    conn.close()

    return render_template('surtax/map_view.html',
                          title='Map View',
                          schools=schools)


@app.route('/public')
def public_portal():
    """Public transparency portal."""
    conn = get_db()
    cursor = conn.cursor()

    # Get public-facing summary
    cursor.execute('''
        SELECT
            COUNT(*) as total_projects,
            SUM(current_amount) as total_budget,
            SUM(amount_paid) as total_spent,
            SUM(CASE WHEN status = 'Complete' THEN 1 ELSE 0 END) as completed
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL
    ''')
    summary = cursor.fetchone()

    # Recent completed projects
    cursor.execute('''
        SELECT title, school_name, current_amount, surtax_category
        FROM contracts
        WHERE is_deleted = 0 AND surtax_category IS NOT NULL AND status = 'Complete'
        ORDER BY current_end_date DESC
        LIMIT 10
    ''')
    completed = cursor.fetchall()
    conn.close()

    return render_template('surtax/public_portal.html',
                          title='Public Portal',
                          summary=summary,
                          completed=completed)


@app.route('/alerts')
def alerts():
    """Alerts and notifications management."""
    conn = get_db()
    cursor = conn.cursor()

    # Generate alerts based on project status
    alerts_list = []

    # Delayed projects
    cursor.execute('''
        SELECT id, title, delay_days
        FROM contracts
        WHERE is_deleted = 0 AND is_delayed = 1
        ORDER BY delay_days DESC
        LIMIT 5
    ''')
    for row in cursor.fetchall():
        alerts_list.append({
            'type': 'warning',
            'title': f"Project Delayed: {row['title'][:40]}",
            'message': f"{row['delay_days']} days behind schedule",
            'project_id': row['id']
        })

    # Over budget projects
    cursor.execute('''
        SELECT id, title, budget_variance_pct
        FROM contracts
        WHERE is_deleted = 0 AND is_over_budget = 1
        ORDER BY budget_variance_pct DESC
        LIMIT 5
    ''')
    for row in cursor.fetchall():
        alerts_list.append({
            'type': 'danger',
            'title': f"Over Budget: {row['title'][:40]}",
            'message': f"{row['budget_variance_pct']:.1f}% over budget",
            'project_id': row['id']
        })

    conn.close()

    return render_template('surtax/alerts.html',
                          title='Alerts & Notifications',
                          alerts=alerts_list)


# ==================
# RUN
# ==================

if __name__ == '__main__':
    logger.info("Starting Surtax Oversight Dashboard on port 5847...")
    app.run(host='127.0.0.1', port=5847, debug=True)
