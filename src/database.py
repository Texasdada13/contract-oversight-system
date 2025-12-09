"""
Contract Oversight System - Database Module
SQLite database for contract, vendor, and spending management.
"""

import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)

# Database path
DB_PATH = Path(__file__).parent.parent / 'data' / 'contracts.db'


class DatabaseManager:
    """Manages SQLite database for contract oversight."""

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
        logger.info(f"Database initialized at {self.db_path}")

    def _get_connection(self):
        """Get database connection with row factory."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_database(self):
        """Initialize database tables."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Vendors table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vendors (
                vendor_id TEXT PRIMARY KEY,
                vendor_name TEXT NOT NULL,
                vendor_type TEXT,
                contact_name TEXT,
                contact_email TEXT,
                contact_phone TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                tax_id TEXT,
                certification_status TEXT,
                minority_owned INTEGER DEFAULT 0,
                woman_owned INTEGER DEFAULT 0,
                small_business INTEGER DEFAULT 0,
                local_business INTEGER DEFAULT 0,
                registration_date TEXT,
                status TEXT DEFAULT 'Active',
                performance_score REAL DEFAULT 50,
                total_contracts INTEGER DEFAULT 0,
                total_awarded REAL DEFAULT 0,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                is_deleted INTEGER DEFAULT 0
            )
        ''')

        # Contracts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contracts (
                contract_id TEXT PRIMARY KEY,
                contract_number TEXT,
                title TEXT NOT NULL,
                description TEXT,
                contract_type TEXT,
                department TEXT,
                fiscal_year TEXT,
                vendor_id TEXT,
                vendor_name TEXT,

                -- Financials
                original_amount REAL DEFAULT 0,
                current_amount REAL DEFAULT 0,
                total_paid REAL DEFAULT 0,
                remaining_balance REAL DEFAULT 0,

                -- Dates
                solicitation_date TEXT,
                award_date TEXT,
                start_date TEXT,
                original_end_date TEXT,
                current_end_date TEXT,
                actual_end_date TEXT,

                -- Status tracking
                status TEXT DEFAULT 'Draft',
                phase TEXT DEFAULT 'Planning',
                percent_complete REAL DEFAULT 0,

                -- Scoring
                performance_score REAL DEFAULT 50,
                cost_variance_score REAL DEFAULT 50,
                schedule_variance_score REAL DEFAULT 50,
                compliance_score REAL DEFAULT 50,
                overall_health_score REAL DEFAULT 50,
                risk_level TEXT DEFAULT 'Medium',

                -- Procurement info
                procurement_method TEXT,
                bid_count INTEGER DEFAULT 0,
                justification TEXT,

                -- Board/Council info
                board_approval_date TEXT,
                board_resolution TEXT,
                council_district TEXT,
                school_zone TEXT,

                -- Location
                project_location TEXT,
                latitude REAL,
                longitude REAL,

                -- Flags
                has_change_orders INTEGER DEFAULT 0,
                change_order_count INTEGER DEFAULT 0,
                total_change_order_amount REAL DEFAULT 0,
                is_emergency INTEGER DEFAULT 0,
                is_sole_source INTEGER DEFAULT 0,
                requires_insurance INTEGER DEFAULT 1,
                insurance_verified INTEGER DEFAULT 0,
                requires_bond INTEGER DEFAULT 0,
                bond_verified INTEGER DEFAULT 0,

                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                created_by TEXT,
                is_deleted INTEGER DEFAULT 0,

                FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
            )
        ''')

        # Change Orders table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS change_orders (
                change_order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id TEXT NOT NULL,
                change_order_number TEXT,
                description TEXT,
                reason TEXT,
                amount REAL DEFAULT 0,
                days_added INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                requested_date TEXT,
                approved_date TEXT,
                approved_by TEXT,
                board_approval_required INTEGER DEFAULT 0,
                board_approval_date TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (contract_id) REFERENCES contracts(contract_id)
            )
        ''')

        # Milestones/Deliverables table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS milestones (
                milestone_id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id TEXT NOT NULL,
                milestone_number INTEGER,
                title TEXT NOT NULL,
                description TEXT,
                planned_start_date TEXT,
                due_date TEXT,
                actual_start_date TEXT,
                completed_date TEXT,
                status TEXT DEFAULT 'Pending',
                percent_complete REAL DEFAULT 0,
                payment_amount REAL DEFAULT 0,
                payment_status TEXT DEFAULT 'Not Due',
                deliverable_type TEXT,
                responsible_party TEXT,
                blockers TEXT,
                verification_required INTEGER DEFAULT 1,
                verified_by TEXT,
                verified_date TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (contract_id) REFERENCES contracts(contract_id)
            )
        ''')

        # Payments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id TEXT NOT NULL,
                vendor_id TEXT,
                invoice_number TEXT,
                invoice_date TEXT,
                payment_date TEXT,
                amount REAL DEFAULT 0,
                payment_type TEXT,
                milestone_id INTEGER,
                status TEXT DEFAULT 'Pending',
                approved_by TEXT,
                approved_date TEXT,
                check_number TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (contract_id) REFERENCES contracts(contract_id),
                FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
                FOREIGN KEY (milestone_id) REFERENCES milestones(milestone_id)
            )
        ''')

        # Issues/Alerts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS issues (
                issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id TEXT,
                vendor_id TEXT,
                issue_type TEXT NOT NULL,
                severity TEXT DEFAULT 'Medium',
                title TEXT NOT NULL,
                description TEXT,
                status TEXT DEFAULT 'Open',
                reported_by TEXT,
                reported_date TEXT DEFAULT CURRENT_TIMESTAMP,
                assigned_to TEXT,
                due_date TEXT,
                resolved_date TEXT,
                resolution TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (contract_id) REFERENCES contracts(contract_id),
                FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
            )
        ''')

        # Audit/History table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                action TEXT NOT NULL,
                field_name TEXT,
                old_value TEXT,
                new_value TEXT,
                changed_by TEXT,
                changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                ip_address TEXT,
                notes TEXT
            )
        ''')

        # Comments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id TEXT,
                vendor_id TEXT,
                user_id TEXT,
                user_name TEXT,
                content TEXT NOT NULL,
                parent_id INTEGER,
                is_internal INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                is_deleted INTEGER DEFAULT 0,
                FOREIGN KEY (contract_id) REFERENCES contracts(contract_id),
                FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
                FOREIGN KEY (parent_id) REFERENCES comments(comment_id)
            )
        ''')

        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                email TEXT,
                display_name TEXT,
                role TEXT DEFAULT 'viewer',
                department TEXT,
                password_hash TEXT,
                is_active INTEGER DEFAULT 1,
                last_login TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Documents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                document_id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id TEXT,
                vendor_id TEXT,
                filename TEXT NOT NULL,
                document_type TEXT,
                description TEXT,
                file_path TEXT,
                file_size INTEGER,
                mime_type TEXT,
                uploaded_by TEXT,
                uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
                is_deleted INTEGER DEFAULT 0,
                FOREIGN KEY (contract_id) REFERENCES contracts(contract_id),
                FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
            )
        ''')

        # Notifications table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                title TEXT NOT NULL,
                message TEXT,
                notification_type TEXT,
                severity TEXT DEFAULT 'Info',
                related_contract_id TEXT,
                related_vendor_id TEXT,
                is_read INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                read_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (related_contract_id) REFERENCES contracts(contract_id),
                FOREIGN KEY (related_vendor_id) REFERENCES vendors(vendor_id)
            )
        ''')

        # Vendor Ratings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vendor_ratings (
                rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendor_id TEXT NOT NULL,
                contract_id TEXT,
                quality_rating INTEGER DEFAULT 3,
                timeliness_rating INTEGER DEFAULT 3,
                communication_rating INTEGER DEFAULT 3,
                value_rating INTEGER DEFAULT 3,
                overall_rating REAL,
                comments TEXT,
                rated_by TEXT,
                rated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
                FOREIGN KEY (contract_id) REFERENCES contracts(contract_id)
            )
        ''')

        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_contracts_vendor ON contracts(vendor_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_contracts_status ON contracts(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_contracts_department ON contracts(department)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_contract ON payments(contract_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_milestones_contract ON milestones(contract_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_change_orders_contract ON change_orders(contract_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_record ON audit_log(table_name, record_id)')

        conn.commit()
        conn.close()

    # ==================
    # CONTRACT OPERATIONS
    # ==================

    def get_all_contracts(self) -> pd.DataFrame:
        """Get all active contracts as DataFrame."""
        conn = self._get_connection()
        query = "SELECT * FROM contracts WHERE is_deleted = 0 ORDER BY updated_at DESC"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_contract(self, contract_id: str) -> Optional[Dict]:
        """Get a single contract by ID."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM contracts WHERE contract_id = ? AND is_deleted = 0", (contract_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def save_contract(self, data: Dict, changed_by: str = 'system') -> bool:
        """Save or update a contract."""
        conn = self._get_connection()
        cursor = conn.cursor()

        contract_id = data.get('contract_id')
        if not contract_id:
            return False

        # Check if exists
        cursor.execute("SELECT * FROM contracts WHERE contract_id = ?", (contract_id,))
        existing = cursor.fetchone()

        data['updated_at'] = datetime.now().isoformat()

        if existing:
            # Update - log changes
            existing_dict = dict(existing)
            for key, new_value in data.items():
                old_value = existing_dict.get(key)
                if old_value != new_value and key not in ['updated_at']:
                    cursor.execute('''
                        INSERT INTO audit_log (table_name, record_id, action, field_name, old_value, new_value, changed_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', ('contracts', contract_id, 'UPDATE', key, str(old_value), str(new_value), changed_by))

            # Build update query
            fields = [f"{k} = ?" for k in data.keys() if k != 'contract_id']
            values = [v for k, v in data.items() if k != 'contract_id']
            values.append(contract_id)

            cursor.execute(f"UPDATE contracts SET {', '.join(fields)} WHERE contract_id = ?", values)
        else:
            # Insert new
            data['created_at'] = datetime.now().isoformat()
            data['created_by'] = changed_by

            fields = list(data.keys())
            placeholders = ', '.join(['?' for _ in fields])
            cursor.execute(f"INSERT INTO contracts ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

            cursor.execute('''
                INSERT INTO audit_log (table_name, record_id, action, changed_by)
                VALUES (?, ?, ?, ?)
            ''', ('contracts', contract_id, 'CREATE', changed_by))

        conn.commit()
        conn.close()
        return True

    def delete_contract(self, contract_id: str, changed_by: str = 'system', hard_delete: bool = False) -> bool:
        """Delete a contract (soft delete by default)."""
        conn = self._get_connection()
        cursor = conn.cursor()

        if hard_delete:
            cursor.execute("DELETE FROM contracts WHERE contract_id = ?", (contract_id,))
        else:
            cursor.execute("UPDATE contracts SET is_deleted = 1, updated_at = ? WHERE contract_id = ?",
                          (datetime.now().isoformat(), contract_id))

        cursor.execute('''
            INSERT INTO audit_log (table_name, record_id, action, changed_by)
            VALUES (?, ?, ?, ?)
        ''', ('contracts', contract_id, 'DELETE', changed_by))

        conn.commit()
        conn.close()
        return True

    # ==================
    # VENDOR OPERATIONS
    # ==================

    def get_all_vendors(self) -> pd.DataFrame:
        """Get all active vendors."""
        conn = self._get_connection()
        df = pd.read_sql_query("SELECT * FROM vendors WHERE is_deleted = 0 ORDER BY vendor_name", conn)
        conn.close()
        return df

    def get_vendor(self, vendor_id: str) -> Optional[Dict]:
        """Get a single vendor."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vendors WHERE vendor_id = ? AND is_deleted = 0", (vendor_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def save_vendor(self, data: Dict, changed_by: str = 'system') -> bool:
        """Save or update a vendor."""
        conn = self._get_connection()
        cursor = conn.cursor()

        vendor_id = data.get('vendor_id')
        if not vendor_id:
            return False

        cursor.execute("SELECT * FROM vendors WHERE vendor_id = ?", (vendor_id,))
        existing = cursor.fetchone()

        data['updated_at'] = datetime.now().isoformat()

        if existing:
            fields = [f"{k} = ?" for k in data.keys() if k != 'vendor_id']
            values = [v for k, v in data.items() if k != 'vendor_id']
            values.append(vendor_id)
            cursor.execute(f"UPDATE vendors SET {', '.join(fields)} WHERE vendor_id = ?", values)
        else:
            data['created_at'] = datetime.now().isoformat()
            fields = list(data.keys())
            placeholders = ', '.join(['?' for _ in fields])
            cursor.execute(f"INSERT INTO vendors ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        conn.commit()
        conn.close()
        return True

    def get_vendor_contracts(self, vendor_id: str) -> List[Dict]:
        """Get all contracts for a vendor."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM contracts
            WHERE vendor_id = ? AND is_deleted = 0
            ORDER BY start_date DESC
        """, (vendor_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # ==================
    # CHANGE ORDERS
    # ==================

    def get_change_orders(self, contract_id: str) -> List[Dict]:
        """Get change orders for a contract."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM change_orders
            WHERE contract_id = ?
            ORDER BY requested_date DESC
        """, (contract_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_change_order(self, data: Dict) -> int:
        """Add a change order."""
        conn = self._get_connection()
        cursor = conn.cursor()

        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"INSERT INTO change_orders ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        change_order_id = cursor.lastrowid

        # Update contract totals
        contract_id = data.get('contract_id')
        if contract_id:
            cursor.execute("""
                UPDATE contracts SET
                    has_change_orders = 1,
                    change_order_count = change_order_count + 1,
                    total_change_order_amount = total_change_order_amount + ?,
                    current_amount = current_amount + ?
                WHERE contract_id = ?
            """, (data.get('amount', 0), data.get('amount', 0), contract_id))

        conn.commit()
        conn.close()
        return change_order_id

    # ==================
    # MILESTONES
    # ==================

    def get_milestones(self, contract_id: str) -> List[Dict]:
        """Get milestones for a contract."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM milestones
            WHERE contract_id = ?
            ORDER BY milestone_number, due_date
        """, (contract_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_milestone(self, data: Dict) -> int:
        """Add a milestone."""
        conn = self._get_connection()
        cursor = conn.cursor()

        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"INSERT INTO milestones ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        milestone_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return milestone_id

    def update_milestone(self, milestone_id: int, data: Dict) -> bool:
        """Update a milestone."""
        conn = self._get_connection()
        cursor = conn.cursor()

        data['updated_at'] = datetime.now().isoformat()
        fields = [f"{k} = ?" for k in data.keys()]
        values = list(data.values())
        values.append(milestone_id)

        cursor.execute(f"UPDATE milestones SET {', '.join(fields)} WHERE milestone_id = ?", values)
        conn.commit()
        conn.close()
        return True

    def delete_milestone(self, milestone_id: int) -> bool:
        """Delete a milestone."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM milestones WHERE milestone_id = ?", (milestone_id,))
        conn.commit()
        conn.close()
        return True

    def get_milestone_stats(self, contract_id: str) -> Dict:
        """Get milestone statistics for a contract."""
        conn = self._get_connection()
        cursor = conn.cursor()

        stats = {}

        # Total milestones
        cursor.execute("SELECT COUNT(*) FROM milestones WHERE contract_id = ?", (contract_id,))
        stats['total'] = cursor.fetchone()[0]

        # By status
        cursor.execute("""
            SELECT status, COUNT(*) FROM milestones
            WHERE contract_id = ?
            GROUP BY status
        """, (contract_id,))
        status_counts = {row[0]: row[1] for row in cursor.fetchall()}
        stats['completed'] = status_counts.get('Completed', 0)
        stats['in_progress'] = status_counts.get('In Progress', 0)
        stats['pending'] = status_counts.get('Pending', 0)
        stats['delayed'] = status_counts.get('Delayed', 0)

        # Overall progress
        if stats['total'] > 0:
            cursor.execute("""
                SELECT AVG(percent_complete) FROM milestones WHERE contract_id = ?
            """, (contract_id,))
            stats['avg_progress'] = cursor.fetchone()[0] or 0
        else:
            stats['avg_progress'] = 0

        # Overdue milestones
        cursor.execute("""
            SELECT COUNT(*) FROM milestones
            WHERE contract_id = ? AND due_date < date('now') AND status NOT IN ('Completed')
        """, (contract_id,))
        stats['overdue'] = cursor.fetchone()[0]

        # Next milestone due
        cursor.execute("""
            SELECT title, due_date FROM milestones
            WHERE contract_id = ? AND status NOT IN ('Completed') AND due_date >= date('now')
            ORDER BY due_date ASC LIMIT 1
        """, (contract_id,))
        next_milestone = cursor.fetchone()
        if next_milestone:
            stats['next_milestone'] = {'title': next_milestone[0], 'due_date': next_milestone[1]}
        else:
            stats['next_milestone'] = None

        conn.close()
        return stats

    def get_all_milestones_with_contracts(self, vendor_id: str = None) -> List[Dict]:
        """Get all milestones with contract info, optionally filtered by vendor."""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = """
            SELECT m.*, c.title as contract_title, c.vendor_id, c.vendor_name, c.status as contract_status
            FROM milestones m
            JOIN contracts c ON m.contract_id = c.contract_id
            WHERE c.is_deleted = 0
        """
        params = []

        if vendor_id:
            query += " AND c.vendor_id = ?"
            params.append(vendor_id)

        query += " ORDER BY m.due_date ASC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_aggregated_milestone_stats(self, vendor_prefix: str = None) -> Dict:
        """Get aggregated milestone statistics across multiple contracts, optionally filtered by vendor prefix."""
        conn = self._get_connection()
        cursor = conn.cursor()

        base_query = """
            SELECT m.*, c.title as contract_title, c.vendor_id, c.contract_id
            FROM milestones m
            JOIN contracts c ON m.contract_id = c.contract_id
            WHERE c.is_deleted = 0
        """
        params = []

        if vendor_prefix:
            base_query += " AND c.vendor_id LIKE ?"
            params.append(f"{vendor_prefix}%")

        cursor.execute(base_query, params)
        milestones = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if not milestones:
            return {
                'total': 0,
                'completed': 0,
                'in_progress': 0,
                'pending': 0,
                'delayed': 0,
                'overdue': 0,
                'avg_progress': 0,
                'upcoming': [],
                'overdue_list': [],
                'by_contract': {}
            }

        today = date.today().isoformat()
        total = len(milestones)
        completed = sum(1 for m in milestones if m.get('status') == 'Completed')
        in_progress = sum(1 for m in milestones if m.get('status') == 'In Progress')
        pending = sum(1 for m in milestones if m.get('status') == 'Pending')
        delayed = sum(1 for m in milestones if m.get('status') == 'Delayed')

        # Calculate overdue
        overdue = sum(1 for m in milestones
                     if m.get('due_date') and m.get('due_date') < today
                     and m.get('status') not in ('Completed',))

        # Average progress
        progress_values = [m.get('percent_complete', 0) or 0 for m in milestones]
        avg_progress = sum(progress_values) / len(progress_values) if progress_values else 0

        # Upcoming milestones (next 7 due within 30 days)
        upcoming = sorted(
            [m for m in milestones
             if m.get('due_date') and m.get('due_date') >= today and m.get('status') != 'Completed'],
            key=lambda x: x.get('due_date', '9999')
        )[:7]

        # Overdue milestones
        overdue_list = sorted(
            [m for m in milestones
             if m.get('due_date') and m.get('due_date') < today and m.get('status') != 'Completed'],
            key=lambda x: x.get('due_date', '0000')
        )

        # Stats by contract
        by_contract = {}
        for m in milestones:
            cid = m.get('contract_id')
            if cid not in by_contract:
                by_contract[cid] = {
                    'contract_title': m.get('contract_title'),
                    'total': 0,
                    'completed': 0,
                    'progress': []
                }
            by_contract[cid]['total'] += 1
            if m.get('status') == 'Completed':
                by_contract[cid]['completed'] += 1
            by_contract[cid]['progress'].append(m.get('percent_complete', 0) or 0)

        # Calculate average progress per contract
        for cid in by_contract:
            progress = by_contract[cid]['progress']
            by_contract[cid]['avg_progress'] = sum(progress) / len(progress) if progress else 0
            del by_contract[cid]['progress']

        return {
            'total': total,
            'completed': completed,
            'in_progress': in_progress,
            'pending': pending,
            'delayed': delayed,
            'overdue': overdue,
            'avg_progress': avg_progress,
            'upcoming': upcoming,
            'overdue_list': overdue_list,
            'by_contract': by_contract
        }

    # ==================
    # PAYMENTS
    # ==================

    def get_payments(self, contract_id: str) -> List[Dict]:
        """Get payments for a contract."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM payments
            WHERE contract_id = ?
            ORDER BY payment_date DESC
        """, (contract_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_payment(self, data: Dict) -> int:
        """Record a payment."""
        conn = self._get_connection()
        cursor = conn.cursor()

        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"INSERT INTO payments ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        payment_id = cursor.lastrowid

        # Update contract totals
        contract_id = data.get('contract_id')
        amount = data.get('amount', 0)
        if contract_id and data.get('status') == 'Paid':
            cursor.execute("""
                UPDATE contracts SET
                    total_paid = total_paid + ?,
                    remaining_balance = current_amount - (total_paid + ?)
                WHERE contract_id = ?
            """, (amount, amount, contract_id))

        conn.commit()
        conn.close()
        return payment_id

    # ==================
    # ISSUES/ALERTS
    # ==================

    def get_issues(self, contract_id: str = None, status: str = None) -> List[Dict]:
        """Get issues, optionally filtered."""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM issues WHERE 1=1"
        params = []

        if contract_id:
            query += " AND contract_id = ?"
            params.append(contract_id)
        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY CASE severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END, reported_date DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_issue(self, data: Dict) -> int:
        """Add an issue/alert."""
        conn = self._get_connection()
        cursor = conn.cursor()

        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"INSERT INTO issues ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        issue_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return issue_id

    # ==================
    # AUDIT & HISTORY
    # ==================

    def get_audit_log(self, table_name: str = None, record_id: str = None, limit: int = 100) -> List[Dict]:
        """Get audit log entries."""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM audit_log WHERE 1=1"
        params = []

        if table_name:
            query += " AND table_name = ?"
            params.append(table_name)
        if record_id:
            query += " AND record_id = ?"
            params.append(record_id)

        query += " ORDER BY changed_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_recent_changes(self, limit: int = 50) -> List[Dict]:
        """Get recent changes across all tables."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.*, c.title as contract_title
            FROM audit_log a
            LEFT JOIN contracts c ON a.record_id = c.contract_id AND a.table_name = 'contracts'
            ORDER BY a.changed_at DESC
            LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # ==================
    # COMMENTS
    # ==================

    def get_comments(self, contract_id: str) -> List[Dict]:
        """Get comments for a contract."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM comments
            WHERE contract_id = ? AND is_deleted = 0 AND parent_id IS NULL
            ORDER BY created_at DESC
        """, (contract_id,))
        comments = [dict(row) for row in cursor.fetchall()]

        # Get replies
        for comment in comments:
            cursor.execute("""
                SELECT * FROM comments
                WHERE parent_id = ? AND is_deleted = 0
                ORDER BY created_at ASC
            """, (comment['comment_id'],))
            comment['replies'] = [dict(row) for row in cursor.fetchall()]

        conn.close()
        return comments

    def add_comment(self, contract_id: str, content: str, user_id: str = None, user_name: str = None, parent_id: int = None) -> int:
        """Add a comment."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO comments (contract_id, content, user_id, user_name, parent_id)
            VALUES (?, ?, ?, ?, ?)
        """, (contract_id, content, user_id, user_name, parent_id))

        comment_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return comment_id

    # ==================
    # DOCUMENTS
    # ==================

    def get_documents(self, contract_id: str) -> List[Dict]:
        """Get documents for a contract."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM documents
            WHERE contract_id = ? AND is_deleted = 0
            ORDER BY uploaded_at DESC
        """, (contract_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_document(self, data: Dict) -> int:
        """Add a document record."""
        conn = self._get_connection()
        cursor = conn.cursor()

        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"INSERT INTO documents ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        doc_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return doc_id

    # ==================
    # NOTIFICATIONS
    # ==================

    def get_notifications(self, user_id: str = None, unread_only: bool = False) -> List[Dict]:
        """Get notifications."""
        conn = self._get_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM notifications WHERE 1=1"
        params = []

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        if unread_only:
            query += " AND is_read = 0"

        query += " ORDER BY created_at DESC LIMIT 50"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_notification(self, data: Dict) -> int:
        """Add a notification."""
        conn = self._get_connection()
        cursor = conn.cursor()

        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"INSERT INTO notifications ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        notification_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return notification_id

    def mark_notification_read(self, notification_id: int) -> bool:
        """Mark a notification as read."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE notifications SET is_read = 1, read_at = ? WHERE notification_id = ?
        """, (datetime.now().isoformat(), notification_id))
        conn.commit()
        conn.close()
        return True

    # ==================
    # VENDOR RATINGS
    # ==================

    def get_vendor_ratings(self, vendor_id: str) -> List[Dict]:
        """Get ratings for a vendor."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT r.*, c.title as contract_title
            FROM vendor_ratings r
            LEFT JOIN contracts c ON r.contract_id = c.contract_id
            WHERE r.vendor_id = ?
            ORDER BY r.rated_at DESC
        """, (vendor_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_vendor_rating(self, data: Dict) -> int:
        """Add a vendor rating."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Calculate overall rating
        ratings = [
            data.get('quality_rating', 3),
            data.get('timeliness_rating', 3),
            data.get('communication_rating', 3),
            data.get('value_rating', 3)
        ]
        data['overall_rating'] = sum(ratings) / len(ratings)

        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"INSERT INTO vendor_ratings ({', '.join(fields)}) VALUES ({placeholders})", list(data.values()))

        rating_id = cursor.lastrowid

        # Update vendor performance score
        cursor.execute("""
            SELECT AVG(overall_rating) FROM vendor_ratings WHERE vendor_id = ?
        """, (data['vendor_id'],))
        avg_rating = cursor.fetchone()[0]
        if avg_rating:
            # Convert 1-5 scale to 0-100 score
            performance_score = (avg_rating / 5) * 100
            cursor.execute("""
                UPDATE vendors SET performance_score = ? WHERE vendor_id = ?
            """, (performance_score, data['vendor_id']))

        conn.commit()
        conn.close()
        return rating_id

    def log_audit(self, table_name: str, record_id: str, action: str,
                  field_name: str = None, old_value: str = None,
                  new_value: str = None, changed_by: str = None, new_values: dict = None):
        """Log an audit entry."""
        conn = self._get_connection()
        cursor = conn.cursor()

        if new_values:
            # Log each field as a separate entry
            for key, value in new_values.items():
                cursor.execute("""
                    INSERT INTO audit_log (table_name, record_id, action, field_name, new_value, changed_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (table_name, record_id, action, key, str(value), changed_by))
        else:
            cursor.execute("""
                INSERT INTO audit_log (table_name, record_id, action, field_name, old_value, new_value, changed_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (table_name, record_id, action, field_name, old_value, new_value, changed_by))

        conn.commit()
        conn.close()

    # ==================
    # STATISTICS
    # ==================

    def get_statistics(self) -> Dict:
        """Get database statistics."""
        conn = self._get_connection()
        cursor = conn.cursor()

        stats = {}

        # Contract stats
        cursor.execute("SELECT COUNT(*) FROM contracts WHERE is_deleted = 0")
        stats['total_contracts'] = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(current_amount) FROM contracts WHERE is_deleted = 0")
        stats['total_contract_value'] = cursor.fetchone()[0] or 0

        cursor.execute("SELECT SUM(total_paid) FROM contracts WHERE is_deleted = 0")
        stats['total_paid'] = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(*) FROM contracts WHERE is_deleted = 0 AND status = 'Active'")
        stats['active_contracts'] = cursor.fetchone()[0]

        # Vendor stats
        cursor.execute("SELECT COUNT(*) FROM vendors WHERE is_deleted = 0")
        stats['total_vendors'] = cursor.fetchone()[0]

        # Issue stats
        cursor.execute("SELECT COUNT(*) FROM issues WHERE status = 'Open'")
        stats['open_issues'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM issues WHERE status = 'Open' AND severity IN ('Critical', 'High')")
        stats['critical_issues'] = cursor.fetchone()[0]

        # Change order stats
        cursor.execute("SELECT COUNT(*), SUM(amount) FROM change_orders")
        row = cursor.fetchone()
        stats['total_change_orders'] = row[0] or 0
        stats['total_change_order_value'] = row[1] or 0

        conn.close()
        return stats


# Global instance
_db_instance = None

def get_database() -> DatabaseManager:
    """Get or create database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseManager()
    return _db_instance
