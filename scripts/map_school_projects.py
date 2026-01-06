"""
Map existing school-related projects to surtax categories and populate schools table.
"""

import sqlite3
from pathlib import Path
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / 'data' / 'contracts.db'


# Known Marion County schools (from research)
KNOWN_SCHOOLS = [
    # Elementary Schools
    {'school_id': 'anthony_elem', 'school_name': 'Anthony Elementary', 'school_type': 'Elementary', 'zone': 'North'},
    {'school_id': 'belleview_elem', 'school_name': 'Belleview Elementary', 'school_type': 'Elementary', 'zone': 'South'},
    {'school_id': 'belleview_santos_elem', 'school_name': 'Belleview-Santos Elementary', 'school_type': 'Elementary', 'zone': 'South'},
    {'school_id': 'college_park_elem', 'school_name': 'College Park Elementary', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'dunnellon_elem', 'school_name': 'Dunnellon Elementary', 'school_type': 'Elementary', 'zone': 'Southwest'},
    {'school_id': 'emerald_shores_elem', 'school_name': 'Emerald Shores Elementary', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'fessenden_elem', 'school_name': 'Fessenden Elementary', 'school_type': 'Elementary', 'zone': 'North'},
    {'school_id': 'fort_king_elem', 'school_name': 'Fort King Middle', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'greenway_elem', 'school_name': 'Greenway Elementary', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'hammett_bowen_elem', 'school_name': 'Hammett Bowen Jr. Elementary', 'school_type': 'Elementary', 'zone': 'Southwest'},
    {'school_id': 'harbour_view_elem', 'school_name': 'Harbour View Elementary', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'horizon_academy', 'school_name': 'Horizon Academy at Marion Oaks', 'school_type': 'K-8', 'zone': 'Southwest'},
    {'school_id': 'legacy_elem', 'school_name': 'Legacy Elementary', 'school_type': 'Elementary', 'zone': 'Southwest'},
    {'school_id': 'maplewood_elem', 'school_name': 'Maplewood Elementary', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'marion_oaks_elem', 'school_name': 'Marion Oaks Elementary', 'school_type': 'Elementary', 'zone': 'Southwest'},
    {'school_id': 'ocala_springs_elem', 'school_name': 'Ocala Springs Elementary', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'reddick_collier_elem', 'school_name': 'Reddick-Collier Elementary', 'school_type': 'Elementary', 'zone': 'North'},
    {'school_id': 'ross_prairie_elem', 'school_name': 'Ross Prairie Elementary', 'school_type': 'Elementary', 'zone': 'Southwest', 'year_built': 2025},
    {'school_id': 'sparr_elem', 'school_name': 'Sparr Elementary', 'school_type': 'Elementary', 'zone': 'North'},
    {'school_id': 'sunrise_elem', 'school_name': 'Sunrise Elementary', 'school_type': 'Elementary', 'zone': 'Central'},
    {'school_id': 'winding_oaks_elem', 'school_name': 'Winding Oaks Elementary', 'school_type': 'Elementary', 'zone': 'Southwest', 'year_built': 2025},

    # Middle Schools
    {'school_id': 'belleview_middle', 'school_name': 'Belleview Middle', 'school_type': 'Middle', 'zone': 'South'},
    {'school_id': 'dunnellon_middle', 'school_name': 'Dunnellon Middle', 'school_type': 'Middle', 'zone': 'Southwest'},
    {'school_id': 'fort_king_middle', 'school_name': 'Fort King Middle', 'school_type': 'Middle', 'zone': 'Central'},
    {'school_id': 'howard_middle', 'school_name': 'Howard Middle', 'school_type': 'Middle', 'zone': 'Central'},
    {'school_id': 'lake_weir_middle', 'school_name': 'Lake Weir Middle', 'school_type': 'Middle', 'zone': 'South'},
    {'school_id': 'liberty_middle', 'school_name': 'Liberty Middle', 'school_type': 'Middle', 'zone': 'Southwest'},
    {'school_id': 'north_marion_middle', 'school_name': 'North Marion Middle', 'school_type': 'Middle', 'zone': 'North'},
    {'school_id': 'osceola_middle', 'school_name': 'Osceola Middle', 'school_type': 'Middle', 'zone': 'Central'},

    # High Schools
    {'school_id': 'belleview_high', 'school_name': 'Belleview High', 'school_type': 'High', 'zone': 'South'},
    {'school_id': 'dunnellon_high', 'school_name': 'Dunnellon High', 'school_type': 'High', 'zone': 'Southwest'},
    {'school_id': 'forest_high', 'school_name': 'Forest High', 'school_type': 'High', 'zone': 'Central'},
    {'school_id': 'lake_weir_high', 'school_name': 'Lake Weir High', 'school_type': 'High', 'zone': 'South'},
    {'school_id': 'north_marion_high', 'school_name': 'North Marion High', 'school_type': 'High', 'zone': 'North'},
    {'school_id': 'south_marion_high', 'school_name': 'South Marion High', 'school_type': 'High', 'zone': 'Southwest', 'year_built': 2026},
    {'school_id': 'vanguard_high', 'school_name': 'Vanguard High', 'school_type': 'High', 'zone': 'Central'},
    {'school_id': 'west_port_high', 'school_name': 'West Port High', 'school_type': 'High', 'zone': 'West'},
]


def categorize_project(title: str, description: str, contract_type: str, department: str) -> dict:
    """
    Determine surtax category and subcategory based on project details.
    Returns dict with category, subcategory, and school_name if identifiable.
    """
    title_lower = (title or '').lower()
    desc_lower = (description or '').lower()
    combined = f"{title_lower} {desc_lower}"

    result = {
        'surtax_category': None,
        'surtax_subcategory': None,
        'school_name': None,
        'surtax_funded': None,  # NULL = unconfirmed
        'funding_source': 'Unconfirmed'
    }

    # Try to identify school name
    for school in KNOWN_SCHOOLS:
        school_name_lower = school['school_name'].lower()
        # Check for school name in title
        if school_name_lower in title_lower or school_name_lower.replace(' ', '') in title_lower.replace(' ', ''):
            result['school_name'] = school['school_name']
            result['school_id'] = school['school_id']
            break
        # Also check common abbreviations
        words = school_name_lower.split()
        if len(words) >= 2:
            abbrev = words[0][:4] + ' ' + words[-1]  # e.g., "bell elem"
            if abbrev in title_lower:
                result['school_name'] = school['school_name']
                result['school_id'] = school['school_id']
                break

    # Determine category based on keywords
    if any(kw in combined for kw in ['new school', 'new high school', 'new elementary', 'new middle', 'new construction']):
        result['surtax_category'] = 'new_construction'
        result['surtax_subcategory'] = 'New School'
    elif any(kw in combined for kw in ['replace school', 'replacement']):
        result['surtax_category'] = 'new_construction'
        result['surtax_subcategory'] = 'School Replacement'
    elif any(kw in combined for kw in ['classroom addition', 'new wing', 'addition -', 'new 16 classroom', 'new 12 classroom']):
        result['surtax_category'] = 'capacity'
        result['surtax_subcategory'] = 'Classroom Addition'
    elif any(kw in combined for kw in ['cafeteria addition', 'new cafeteria']):
        result['surtax_category'] = 'capacity'
        result['surtax_subcategory'] = 'Cafeteria Addition'
    elif any(kw in combined for kw in ['gymnasium', 'new gym']):
        result['surtax_category'] = 'capacity'
        result['surtax_subcategory'] = 'Gymnasium'
    elif any(kw in combined for kw in ['hvac', 'air conditioning', 'heating', 'cooling']):
        result['surtax_category'] = 'maintenance'
        result['surtax_subcategory'] = 'HVAC'
    elif any(kw in combined for kw in ['roof', 'roofing']):
        result['surtax_category'] = 'maintenance'
        result['surtax_subcategory'] = 'Roofing'
    elif any(kw in combined for kw in ['electrical', 'wiring']):
        result['surtax_category'] = 'maintenance'
        result['surtax_subcategory'] = 'Electrical'
    elif any(kw in combined for kw in ['plumbing', 'water system']):
        result['surtax_category'] = 'maintenance'
        result['surtax_subcategory'] = 'Plumbing'
    elif any(kw in combined for kw in ['security', 'safety', 'fire alarm', 'surveillance', 'access control']):
        result['surtax_category'] = 'safety_security'
        result['surtax_subcategory'] = 'Security Systems'
    elif any(kw in combined for kw in ['technology', 'network', 'computer', 'it infrastructure']):
        result['surtax_category'] = 'technology'
        result['surtax_subcategory'] = 'IT Infrastructure'
    elif any(kw in combined for kw in ['portable', 'relocatable']):
        result['surtax_category'] = 'capacity'
        result['surtax_subcategory'] = 'Portable Replacement'
    elif any(kw in combined for kw in ['renovation', 'remodel', 'upgrade', 'modernization']):
        result['surtax_category'] = 'renovation'
        result['surtax_subcategory'] = 'General Renovation'
    elif any(kw in combined for kw in ['bus', 'transportation', 'vehicle']):
        result['surtax_category'] = 'other'
        result['surtax_subcategory'] = 'Transportation'
    elif any(kw in combined for kw in ['debt service', 'cop debt']):
        result['surtax_category'] = 'debt_service'
        result['surtax_subcategory'] = 'Debt Payment'
    elif 'school district' in department.lower() or 'school' in department.lower():
        # Generic school project
        result['surtax_category'] = 'other'
        result['surtax_subcategory'] = 'General'

    return result


def populate_schools():
    """Insert known schools into the schools table."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    for school in KNOWN_SCHOOLS:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO schools (school_id, school_name, school_type, zone, year_built)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                school['school_id'],
                school['school_name'],
                school['school_type'],
                school.get('zone'),
                school.get('year_built')
            ))
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    logger.info(f"Populated {len(KNOWN_SCHOOLS)} schools")


def map_projects():
    """Map existing school-related contracts to surtax categories."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Find school-related contracts
    cursor.execute('''
        SELECT contract_id, title, description, contract_type, department
        FROM contracts
        WHERE is_deleted = 0
        AND (
            department LIKE '%School%'
            OR department LIKE '%Facilities%'
            OR department LIKE '%Curriculum%'
            OR department LIKE '%Safety%'
            OR department LIKE '%Technology%'
            OR title LIKE '%School%'
            OR title LIKE '%Elementary%'
            OR title LIKE '%Middle%'
            OR title LIKE '%High School%'
            OR title LIKE '%HVAC%'
            OR title LIKE '%classroom%'
        )
    ''')

    contracts = cursor.fetchall()
    logger.info(f"Found {len(contracts)} school-related contracts to categorize")

    updated = 0
    for contract in contracts:
        categorization = categorize_project(
            contract['title'],
            contract['description'],
            contract['contract_type'],
            contract['department']
        )

        if categorization['surtax_category']:
            cursor.execute('''
                UPDATE contracts SET
                    surtax_category = ?,
                    surtax_subcategory = ?,
                    school_name = COALESCE(?, school_name),
                    school_id = COALESCE(?, school_id),
                    funding_source = ?
                WHERE contract_id = ?
            ''', (
                categorization['surtax_category'],
                categorization['surtax_subcategory'],
                categorization.get('school_name'),
                categorization.get('school_id'),
                categorization['funding_source'],
                contract['contract_id']
            ))
            updated += 1

            logger.info(f"  Categorized: {contract['title'][:50]} -> {categorization['surtax_category']}/{categorization['surtax_subcategory']}")

    conn.commit()
    conn.close()
    logger.info(f"Updated {updated} contracts with surtax categories")


def update_category_totals():
    """Update the totals in surtax_categories table based on contracts."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE surtax_categories SET
            total_allocated = (
                SELECT COALESCE(SUM(current_amount), 0)
                FROM contracts
                WHERE contracts.surtax_category = surtax_categories.category_id
                AND contracts.is_deleted = 0
            ),
            total_spent = (
                SELECT COALESCE(SUM(total_paid), 0)
                FROM contracts
                WHERE contracts.surtax_category = surtax_categories.category_id
                AND contracts.is_deleted = 0
            ),
            project_count = (
                SELECT COUNT(*)
                FROM contracts
                WHERE contracts.surtax_category = surtax_categories.category_id
                AND contracts.is_deleted = 0
            )
    ''')

    conn.commit()
    conn.close()
    logger.info("Updated category totals")


def update_school_totals():
    """Update project totals for each school."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE schools SET
            total_surtax_projects = (
                SELECT COUNT(*)
                FROM contracts
                WHERE contracts.school_id = schools.school_id
                AND contracts.is_deleted = 0
                AND contracts.surtax_category IS NOT NULL
            ),
            total_surtax_budget = (
                SELECT COALESCE(SUM(current_amount), 0)
                FROM contracts
                WHERE contracts.school_id = schools.school_id
                AND contracts.is_deleted = 0
                AND contracts.surtax_category IS NOT NULL
            ),
            total_surtax_spent = (
                SELECT COALESCE(SUM(total_paid), 0)
                FROM contracts
                WHERE contracts.school_id = schools.school_id
                AND contracts.is_deleted = 0
                AND contracts.surtax_category IS NOT NULL
            )
    ''')

    conn.commit()
    conn.close()
    logger.info("Updated school totals")


def calculate_delays_and_overruns():
    """Calculate delay and budget overrun flags for contracts."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Update is_over_budget flag
    cursor.execute('''
        UPDATE contracts SET
            is_over_budget = CASE
                WHEN original_amount > 0 AND current_amount > original_amount THEN 1
                ELSE 0
            END,
            budget_variance_amount = CASE
                WHEN original_amount > 0 THEN current_amount - original_amount
                ELSE 0
            END,
            budget_variance_pct = CASE
                WHEN original_amount > 0 THEN ((current_amount - original_amount) / original_amount) * 100
                ELSE 0
            END
        WHERE is_deleted = 0
    ''')

    # Update is_delayed flag (if current_end_date is later than original_end_date)
    cursor.execute('''
        UPDATE contracts SET
            is_delayed = CASE
                WHEN original_end_date IS NOT NULL
                AND current_end_date IS NOT NULL
                AND current_end_date > original_end_date THEN 1
                ELSE 0
            END,
            delay_days = CASE
                WHEN original_end_date IS NOT NULL
                AND current_end_date IS NOT NULL
                AND current_end_date > original_end_date
                THEN CAST(julianday(current_end_date) - julianday(original_end_date) AS INTEGER)
                ELSE 0
            END
        WHERE is_deleted = 0
    ''')

    conn.commit()
    conn.close()
    logger.info("Calculated delays and budget overruns")


def print_summary():
    """Print a summary of the categorized projects."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    print("\n" + "="*60)
    print("SURTAX PROJECT CATEGORIZATION SUMMARY")
    print("="*60)

    # By category
    cursor.execute('''
        SELECT
            COALESCE(surtax_category, 'Uncategorized') as category,
            COUNT(*) as count,
            SUM(current_amount) as total_value
        FROM contracts
        WHERE is_deleted = 0
        AND (
            department LIKE '%School%'
            OR department LIKE '%Facilities%'
            OR surtax_category IS NOT NULL
        )
        GROUP BY surtax_category
        ORDER BY total_value DESC
    ''')

    print("\nBy Category:")
    print("-"*60)
    for row in cursor.fetchall():
        cat = row[0] or 'Uncategorized'
        count = row[1]
        value = row[2] or 0
        print(f"  {cat:<25} {count:>5} projects  ${value:>15,.0f}")

    # By school
    cursor.execute('''
        SELECT
            COALESCE(school_name, 'No School Assigned') as school,
            COUNT(*) as count,
            SUM(current_amount) as total_value
        FROM contracts
        WHERE is_deleted = 0
        AND surtax_category IS NOT NULL
        GROUP BY school_name
        HAVING total_value > 0
        ORDER BY total_value DESC
        LIMIT 15
    ''')

    print("\nTop Schools by Project Value:")
    print("-"*60)
    for row in cursor.fetchall():
        school = row[0]
        count = row[1]
        value = row[2] or 0
        print(f"  {school:<35} {count:>3} projects  ${value:>12,.0f}")

    # Delayed projects
    cursor.execute('''
        SELECT COUNT(*) FROM contracts
        WHERE is_deleted = 0 AND is_delayed = 1 AND surtax_category IS NOT NULL
    ''')
    delayed = cursor.fetchone()[0]

    # Over budget projects
    cursor.execute('''
        SELECT COUNT(*) FROM contracts
        WHERE is_deleted = 0 AND is_over_budget = 1 AND surtax_category IS NOT NULL
    ''')
    over_budget = cursor.fetchone()[0]

    print("\nFlags:")
    print("-"*60)
    print(f"  Delayed projects:      {delayed}")
    print(f"  Over budget projects:  {over_budget}")

    conn.close()
    print("\n" + "="*60)


if __name__ == '__main__':
    logger.info("Starting school project mapping...")

    # Step 1: Populate schools table
    populate_schools()

    # Step 2: Categorize contracts
    map_projects()

    # Step 3: Update totals
    update_category_totals()
    update_school_totals()

    # Step 4: Calculate flags
    calculate_delays_and_overruns()

    # Step 5: Print summary
    print_summary()

    logger.info("Mapping completed!")
