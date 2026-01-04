"""
Database migration script for executive dashboard enhancements.
Adds new fields for:
- Land cost separation
- Predictive analytics (forecast budget, expected completion)
- Risk probability scoring
- Dollar normalization (inflation adjustment)
- District equity analysis
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / 'contracts.db'

def migrate_database():
    """Add new columns to support executive dashboard enhancements."""
    print("Starting database migration for executive enhancements...")

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Get existing columns in contracts table
    cursor.execute("PRAGMA table_info(contracts)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    # Define new columns to add
    new_columns = {
        'land_cost': 'REAL DEFAULT 0',
        'construction_cost': 'REAL DEFAULT 0',
        'square_footage': 'REAL DEFAULT 0',
        'cost_per_sqft': 'REAL DEFAULT 0',
        'inflation_adjusted_cost': 'REAL DEFAULT 0',
        'base_year': 'INTEGER DEFAULT 2026',
        'forecast_budget_at_completion': 'REAL DEFAULT 0',
        'expected_completion_date': 'TEXT',
        'risk_probability_score': 'REAL DEFAULT 50',
        'risk_financial_impact': 'REAL DEFAULT 0',
        'risk_category': 'TEXT',
        'burn_rate_monthly': 'REAL DEFAULT 0',
        'student_count_impacted': 'INTEGER DEFAULT 0',
        'school_district_zone': 'TEXT',
        'project_category': 'TEXT',
        'is_capital_project': 'INTEGER DEFAULT 0',
        'historical_vendor_score': 'REAL DEFAULT 0',
        'vendor_20yr_performance': 'REAL DEFAULT 0'
    }

    # Add columns that don't exist
    added_count = 0
    for column_name, column_def in new_columns.items():
        if column_name not in existing_columns:
            try:
                sql = f"ALTER TABLE contracts ADD COLUMN {column_name} {column_def}"
                cursor.execute(sql)
                print(f"  [OK] Added column: {column_name}")
                added_count += 1
            except sqlite3.OperationalError as e:
                print(f"  [ERROR] Error adding {column_name}: {e}")
        else:
            print(f"  - Column already exists: {column_name}")

    # Create new tables for executive features

    # Project pipeline table (for capital planning)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS project_pipeline (
            pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT NOT NULL,
            fiscal_year TEXT NOT NULL,
            estimated_cost REAL DEFAULT 0,
            land_cost REAL DEFAULT 0,
            construction_cost REAL DEFAULT 0,
            proposed_start_date TEXT,
            proposed_end_date TEXT,
            funding_status TEXT DEFAULT 'Unfunded',
            priority_rank INTEGER,
            department TEXT,
            school_zone TEXT,
            student_count_impacted INTEGER DEFAULT 0,
            square_footage REAL DEFAULT 0,
            project_type TEXT,
            board_approval_status TEXT DEFAULT 'Pending',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("  [OK] Created/verified project_pipeline table")

    # Executive insights table (for AI-generated insights and summaries)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS executive_insights (
            insight_id INTEGER PRIMARY KEY AUTOINCREMENT,
            insight_date TEXT NOT NULL,
            insight_type TEXT NOT NULL,
            priority TEXT DEFAULT 'Medium',
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            recommendation TEXT,
            related_contract_id TEXT,
            related_vendor_id TEXT,
            metric_change REAL,
            auto_generated INTEGER DEFAULT 1,
            dismissed INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (related_contract_id) REFERENCES contracts(contract_id),
            FOREIGN KEY (related_vendor_id) REFERENCES vendors(vendor_id)
        )
    ''')
    print("  [OK] Created/verified executive_insights table")

    # Historical vendor performance table (20+ year tracking)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historical_vendor_performance (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            contracts_completed INTEGER DEFAULT 0,
            total_value REAL DEFAULT 0,
            avg_cost_variance REAL DEFAULT 0,
            avg_schedule_variance REAL DEFAULT 0,
            on_time_delivery_rate REAL DEFAULT 0,
            avg_performance_score REAL DEFAULT 0,
            change_order_rate REAL DEFAULT 0,
            issues_count INTEGER DEFAULT 0,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
            UNIQUE(vendor_id, year)
        )
    ''')
    print("  [OK] Created/verified historical_vendor_performance table")

    # Inflation index table (for dollar normalization)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inflation_index (
            year INTEGER PRIMARY KEY,
            construction_cost_index REAL NOT NULL,
            cpi_index REAL,
            base_year INTEGER DEFAULT 2026,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("  [OK] Created/verified inflation_index table")

    # Insert baseline inflation data (ENR Construction Cost Index approximation)
    # Base year 2026 = 100
    inflation_data = [
        (2000, 45.0, None),
        (2005, 52.0, None),
        (2010, 60.0, None),
        (2015, 72.0, None),
        (2020, 88.0, None),
        (2021, 91.5, None),
        (2022, 95.0, None),
        (2023, 97.5, None),
        (2024, 99.0, None),
        (2025, 99.5, None),
        (2026, 100.0, None),  # Base year
    ]

    for year, index, cpi in inflation_data:
        cursor.execute('''
            INSERT OR IGNORE INTO inflation_index (year, construction_cost_index, cpi_index, base_year)
            VALUES (?, ?, ?, 2026)
        ''', (year, index, cpi))

    print("  [OK] Inserted inflation index baseline data")

    conn.commit()
    conn.close()

    print(f"\n[OK] Migration completed! Added {added_count} new columns and 4 new tables.")
    print("Database is ready for executive dashboard enhancements.")

if __name__ == "__main__":
    migrate_database()
