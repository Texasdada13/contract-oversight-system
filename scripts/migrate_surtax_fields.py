"""
Migration script to add surtax oversight fields to the database.
This adds fields needed for the School Capital Outlay Surtax Oversight Dashboard.
"""

import sqlite3
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / 'data' / 'contracts.db'


def migrate_database():
    """Add surtax-related fields and tables to the database."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    logger.info("Starting surtax fields migration...")

    # ==================
    # ADD COLUMNS TO CONTRACTS TABLE
    # ==================

    new_columns = [
        # Surtax tracking
        ("surtax_funded", "INTEGER DEFAULT NULL"),  # NULL = unconfirmed, 1 = yes, 0 = no
        ("surtax_category", "TEXT"),  # New Construction, Renovation, Safety & Security, Technology, Portable Replacement, Debt Service
        ("surtax_subcategory", "TEXT"),  # HVAC, Roofing, Classroom Addition, etc.
        ("funding_source", "TEXT DEFAULT 'Unconfirmed'"),  # Surtax, Impact Fees, Borrowed Funds, State Allocation, Mixed, Unconfirmed

        # School-specific fields
        ("school_name", "TEXT"),  # Actual school name (e.g., "Belleview Elementary")
        ("school_id", "TEXT"),  # Reference to schools table

        # Promise tracking (for comparing to ballot commitments)
        ("promised_completion_date", "TEXT"),  # Date promised to voters (if known)
        ("ballot_priority", "INTEGER"),  # Priority ranking from official list (if known)
        ("on_official_list", "INTEGER DEFAULT NULL"),  # NULL = unknown, 1 = yes, 0 = no

        # Simple status flags for committee
        ("is_delayed", "INTEGER DEFAULT 0"),
        ("delay_reason", "TEXT"),
        ("delay_days", "INTEGER DEFAULT 0"),
        ("is_over_budget", "INTEGER DEFAULT 0"),
        ("budget_variance_amount", "REAL DEFAULT 0"),
        ("budget_variance_pct", "REAL DEFAULT 0"),

        # Committee watchlist
        ("is_watchlisted", "INTEGER DEFAULT 0"),
        ("watchlist_reason", "TEXT"),
        ("watchlist_added_date", "TEXT"),

        # Last reviewed by committee
        ("last_committee_review_date", "TEXT"),
        ("committee_notes", "TEXT"),
    ]

    for col_name, col_def in new_columns:
        try:
            cursor.execute(f"ALTER TABLE contracts ADD COLUMN {col_name} {col_def}")
            logger.info(f"  Added column: {col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                logger.info(f"  Column already exists: {col_name}")
            else:
                logger.error(f"  Error adding {col_name}: {e}")

    # ==================
    # CREATE SCHOOLS TABLE
    # ==================

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schools (
            school_id TEXT PRIMARY KEY,
            school_name TEXT NOT NULL,
            school_type TEXT,  -- Elementary, Middle, High, K-8, Alternative, etc.
            address TEXT,
            city TEXT DEFAULT 'Ocala',
            zip_code TEXT,
            zone TEXT,  -- Geographic zone within the county

            -- Facility info
            year_built INTEGER,
            building_age INTEGER,  -- Calculated or entered
            square_footage INTEGER,
            student_capacity INTEGER,
            current_enrollment INTEGER,
            portable_count INTEGER DEFAULT 0,

            -- Condition assessment
            facility_condition TEXT,  -- Good, Fair, Poor, Critical
            last_major_renovation_year INTEGER,
            deferred_maintenance_amount REAL DEFAULT 0,

            -- Surtax project summary (calculated)
            total_surtax_projects INTEGER DEFAULT 0,
            total_surtax_budget REAL DEFAULT 0,
            total_surtax_spent REAL DEFAULT 0,

            -- Metadata
            latitude REAL,
            longitude REAL,
            principal_name TEXT,
            phone TEXT,
            website TEXT,

            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            is_deleted INTEGER DEFAULT 0
        )
    ''')
    logger.info("Created schools table")

    # ==================
    # CREATE SURTAX CATEGORIES TABLE
    # ==================

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS surtax_categories (
            category_id TEXT PRIMARY KEY,
            category_name TEXT NOT NULL,
            description TEXT,
            ballot_language TEXT,  -- Exact wording from ballot
            display_order INTEGER DEFAULT 0,
            icon TEXT,  -- Icon name for UI
            color TEXT,  -- Color code for UI

            -- Budget tracking
            total_allocated REAL DEFAULT 0,
            total_spent REAL DEFAULT 0,
            project_count INTEGER DEFAULT 0,

            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    logger.info("Created surtax_categories table")

    # ==================
    # CREATE COMMITTEE MEETINGS TABLE
    # ==================

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS committee_meetings (
            meeting_id INTEGER PRIMARY KEY AUTOINCREMENT,
            meeting_date TEXT NOT NULL,
            meeting_type TEXT DEFAULT 'Regular',  -- Regular, Special, Emergency
            location TEXT,

            -- Attendance
            members_present TEXT,  -- JSON list of member names
            members_absent TEXT,
            quorum_met INTEGER DEFAULT 1,

            -- Content
            agenda_summary TEXT,
            minutes_summary TEXT,
            key_decisions TEXT,  -- JSON list
            concerns_raised TEXT,  -- JSON list
            action_items TEXT,  -- JSON list

            -- Documents
            agenda_document_id INTEGER,
            minutes_document_id INTEGER,
            presentation_document_id INTEGER,

            -- Status
            status TEXT DEFAULT 'Scheduled',  -- Scheduled, Completed, Cancelled

            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    logger.info("Created committee_meetings table")

    # ==================
    # CREATE CONCERNS TABLE (for auto-surfaced issues)
    # ==================

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS oversight_concerns (
            concern_id INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_id TEXT,
            school_id TEXT,
            vendor_id TEXT,

            concern_type TEXT NOT NULL,  -- Schedule Delay, Cost Overrun, Change Order Pattern, Vendor Pattern, etc.
            severity TEXT DEFAULT 'Medium',  -- Low, Medium, High, Critical

            title TEXT NOT NULL,
            description TEXT,

            -- Metrics that triggered the concern
            metric_name TEXT,
            metric_value REAL,
            threshold_value REAL,

            -- Suggested questions for committee
            suggested_questions TEXT,  -- JSON list

            -- Resolution
            status TEXT DEFAULT 'Open',  -- Open, Under Review, Addressed, Dismissed
            reviewed_by TEXT,
            reviewed_date TEXT,
            resolution_notes TEXT,

            -- Meeting where discussed
            meeting_id INTEGER,

            -- Auto-detection
            auto_detected INTEGER DEFAULT 1,
            detection_date TEXT DEFAULT CURRENT_TIMESTAMP,
            detection_rule TEXT,

            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (contract_id) REFERENCES contracts(contract_id),
            FOREIGN KEY (school_id) REFERENCES schools(school_id),
            FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
            FOREIGN KEY (meeting_id) REFERENCES committee_meetings(meeting_id)
        )
    ''')
    logger.info("Created oversight_concerns table")

    # ==================
    # CREATE INDEXES
    # ==================

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_contracts_surtax ON contracts(surtax_funded)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_surtax_category ON contracts(surtax_category)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_school ON contracts(school_id)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_school_name ON contracts(school_name)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_watchlist ON contracts(is_watchlisted)",
        "CREATE INDEX IF NOT EXISTS idx_schools_type ON schools(school_type)",
        "CREATE INDEX IF NOT EXISTS idx_concerns_status ON oversight_concerns(status)",
        "CREATE INDEX IF NOT EXISTS idx_concerns_severity ON oversight_concerns(severity)",
    ]

    for idx_sql in indexes:
        try:
            cursor.execute(idx_sql)
        except sqlite3.OperationalError as e:
            logger.warning(f"Index warning: {e}")

    logger.info("Created indexes")

    # ==================
    # INSERT DEFAULT SURTAX CATEGORIES
    # ==================

    default_categories = [
        {
            'category_id': 'new_construction',
            'category_name': 'New Construction',
            'description': 'Building new schools and facilities',
            'ballot_language': 'constructing...school facilities',
            'display_order': 1,
            'icon': 'building',
            'color': '#3B82F6'  # Blue
        },
        {
            'category_id': 'renovation',
            'category_name': 'Renovation & Modernization',
            'description': 'Upgrading and renovating existing facilities',
            'ballot_language': 'improving school facilities',
            'display_order': 2,
            'icon': 'wrench',
            'color': '#10B981'  # Green
        },
        {
            'category_id': 'safety_security',
            'category_name': 'Safety & Security',
            'description': 'Safety improvements, security systems, and related infrastructure',
            'ballot_language': 'improve safety and security',
            'display_order': 3,
            'icon': 'shield',
            'color': '#EF4444'  # Red
        },
        {
            'category_id': 'technology',
            'category_name': 'Technology',
            'description': 'Technology infrastructure and equipment',
            'ballot_language': 'expanding...school facilities',
            'display_order': 4,
            'icon': 'computer',
            'color': '#8B5CF6'  # Purple
        },
        {
            'category_id': 'capacity',
            'category_name': 'Capacity & Overcrowding',
            'description': 'Classroom additions and portable replacement to reduce overcrowding',
            'ballot_language': 'reduce classroom overcrowding',
            'display_order': 5,
            'icon': 'users',
            'color': '#F59E0B'  # Amber
        },
        {
            'category_id': 'maintenance',
            'category_name': 'Major Maintenance',
            'description': 'HVAC, roofing, electrical, plumbing, and other major systems',
            'ballot_language': 'improving school facilities',
            'display_order': 6,
            'icon': 'tools',
            'color': '#6366F1'  # Indigo
        },
        {
            'category_id': 'debt_service',
            'category_name': 'Debt Service',
            'description': 'Payments on borrowed funds for capital projects',
            'ballot_language': 'fund such activities',
            'display_order': 7,
            'icon': 'credit-card',
            'color': '#64748B'  # Slate
        },
        {
            'category_id': 'other',
            'category_name': 'Other Capital',
            'description': 'Other capital expenditures',
            'ballot_language': 'fund such activities',
            'display_order': 8,
            'icon': 'folder',
            'color': '#9CA3AF'  # Gray
        }
    ]

    for cat in default_categories:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO surtax_categories
                (category_id, category_name, description, ballot_language, display_order, icon, color)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (cat['category_id'], cat['category_name'], cat['description'],
                  cat['ballot_language'], cat['display_order'], cat['icon'], cat['color']))
        except sqlite3.IntegrityError:
            pass  # Already exists

    logger.info("Inserted default surtax categories")

    conn.commit()
    conn.close()

    logger.info("Migration completed successfully!")


if __name__ == '__main__':
    migrate_database()
