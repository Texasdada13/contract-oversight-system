"""
Import Marion County School District capital projects data into the Contract Oversight System.

Data extracted from the 5-Year District Facilities Work Plan (2024-2025).
Source: Florida Department of Education, Office of Educational Facilities
"""

import sqlite3
from pathlib import Path
from datetime import datetime

# Paths
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'contracts.db'

# School District capital projects from the 5-Year Work Plan
# Extracted from pages 10-20 of the PDF
CAPITAL_PROJECTS = [
    # Capacity Projects (New Construction)
    {
        'project_name': 'New 16 classroom and cafeteria addition - Harbour View Elementary',
        'location': 'HARBOUR VIEW ELEMENTARY',
        'total_cost': 14000000,
        'year_2024': 0, 'year_2025': 0, 'year_2026': 0, 'year_2027': 14000000, 'year_2028': 0,
        'funded': False,
        'project_type': 'New Construction',
        'student_stations': 328,
    },
    {
        'project_name': 'New 16 classroom addition - Liberty Middle',
        'location': 'LIBERTY MIDDLE',
        'total_cost': 10230418,
        'year_2024': 10230418, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 384,
    },
    {
        'project_name': 'New 12 classroom addition - Ocala Springs Elementary',
        'location': 'OCALA SPRINGS ELEMENTARY',
        'total_cost': 10500000,
        'year_2024': 0, 'year_2025': 10500000, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': False,
        'project_type': 'New Construction',
        'student_stations': 248,
    },
    {
        'project_name': 'New Middle School "DD"',
        'location': 'LAKE WEIR MIDDLE',
        'total_cost': 64329977,
        'year_2024': 64329977, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 0,
    },
    {
        'project_name': 'New Southwest Elementary "W"',
        'location': 'Location not specified',
        'total_cost': 45233977,
        'year_2024': 45233977, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 860,
    },
    {
        'project_name': 'New Southwest Elementary "X"',
        'location': 'Location not specified',
        'total_cost': 47059418,
        'year_2024': 47059418, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 860,
    },
    {
        'project_name': 'New 16 classroom and cafeteria addition - Dunnellon Elementary',
        'location': 'DUNNELLON ELEMENTARY',
        'total_cost': 14000000,
        'year_2024': 0, 'year_2025': 0, 'year_2026': 0, 'year_2027': 14000000, 'year_2028': 0,
        'funded': False,
        'project_type': 'New Construction',
        'student_stations': 328,
    },
    {
        'project_name': 'New 16 classroom building and cafeteria - Horizon Academy',
        'location': 'HORIZON ACADEMY AT MARION OAKS',
        'total_cost': 7515014,
        'year_2024': 7515014, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 328,
    },
    {
        'project_name': 'New Auto/Diesel/Aviation Building - Marion Technical College',
        'location': 'MARION TECHNICAL COLLEGE',
        'total_cost': 5334674,
        'year_2024': 5334674, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 0,
    },
    {
        'project_name': 'New 16 Classroom Addition - Hammett Bowen Jr Elementary',
        'location': 'HAMMETT BOWEN JR. ELEMENTARY',
        'total_cost': 7814450,
        'year_2024': 7814450, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 328,
    },
    {
        'project_name': 'New SW High School "CCC"',
        'location': 'Location not specified',
        'total_cost': 135601456,
        'year_2024': 135601456, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 2011,
    },
    {
        'project_name': 'New 16 classroom and cafeteria - Marion Oaks Elementary',
        'location': 'MARION OAKS ELEMENTARY SCHOOL',
        'total_cost': 7814450,
        'year_2024': 7814450, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 328,
    },
    # Maintenance & Renovation Projects
    {
        'project_name': 'HVAC Upgrades - Belleview Middle Buildings 1-8',
        'location': 'BELLEVIEW MIDDLE',
        'total_cost': 7768101,
        'year_2024': 0, 'year_2025': 0, 'year_2026': 7768101, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades - Belleview Senior High',
        'location': 'BELLEVIEW SENIOR HIGH',
        'total_cost': 12031026,
        'year_2024': 0, 'year_2025': 0, 'year_2026': 0, 'year_2027': 3190620, 'year_2028': 8840406,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades - College Park Elementary',
        'location': 'COLLEGE PARK ELEMENTARY',
        'total_cost': 5960211,
        'year_2024': 0, 'year_2025': 760211, 'year_2026': 5200000, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades - Dunnellon Elementary',
        'location': 'DUNNELLON ELEMENTARY',
        'total_cost': 6245254,
        'year_2024': 6245254, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades - Emerald Shores Elementary',
        'location': 'EMERALD SHORES ELEMENTARY',
        'total_cost': 6300012,
        'year_2024': 100012, 'year_2025': 6200000, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades - Greenway Elementary',
        'location': 'GREENWAY ELEMENTARY',
        'total_cost': 6795282,
        'year_2024': 625000, 'year_2025': 6170282, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'Re-roof Buildings 3-7 and Covered Walkways - Lake Weir Senior High',
        'location': 'LAKE WEIR SENIOR HIGH',
        'total_cost': 2729600,
        'year_2024': 2729600, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'Roofing',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades Buildings 4, 6 - Lake Weir Senior High',
        'location': 'LAKE WEIR SENIOR HIGH',
        'total_cost': 3866157,
        'year_2024': 3866157, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades - Maplewood Elementary',
        'location': 'MAPLEWOOD ELEMENTARY',
        'total_cost': 7000000,
        'year_2024': 0, 'year_2025': 1000000, 'year_2026': 6000000, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Replacement - Marion Technical College',
        'location': 'MARION TECHNICAL COLLEGE',
        'total_cost': 3949717,
        'year_2024': 0, 'year_2025': 3949717, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Air Handler Upgrades - West Port Senior High',
        'location': 'WEST PORT SENIOR HIGH',
        'total_cost': 2880000,
        'year_2024': 2880000, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'New Gymnasium - Osceola Middle',
        'location': 'OSCEOLA MIDDLE',
        'total_cost': 9962570,
        'year_2024': 9962570, 'year_2025': 0, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 0,
    },
    {
        'project_name': 'HVAC Upgrades Buildings 1, 3, 5 - Osceola Middle',
        'location': 'OSCEOLA MIDDLE',
        'total_cost': 12800000,
        'year_2024': 3300000, 'year_2025': 3500000, 'year_2026': 6000000, 'year_2027': 0, 'year_2028': 0,
        'funded': True,
        'project_type': 'HVAC Upgrade',
        'student_stations': 0,
    },
    {
        'project_name': 'New Transportation Hub for Zone 4',
        'location': 'LAKE WEIR MIDDLE',
        'total_cost': 11887291,
        'year_2024': 0, 'year_2025': 0, 'year_2026': 1000000, 'year_2027': 10887291, 'year_2028': 0,
        'funded': True,
        'project_type': 'New Construction',
        'student_stations': 0,
    },
    # Major School Replacements (Not Funded)
    {
        'project_name': 'Replace School - Belleview Elementary',
        'location': 'BELLEVIEW ELEMENTARY',
        'total_cost': 45000000,
        'year_2024': 0, 'year_2025': 0, 'year_2026': 0, 'year_2027': 45000000, 'year_2028': 0,
        'funded': False,
        'project_type': 'School Replacement',
        'student_stations': 0,
    },
    {
        'project_name': 'Replace School - Dunnellon Middle',
        'location': 'DUNNELLON MIDDLE',
        'total_cost': 59865500,
        'year_2024': 0, 'year_2025': 0, 'year_2026': 59865500, 'year_2027': 0, 'year_2028': 0,
        'funded': False,
        'project_type': 'School Replacement',
        'student_stations': 0,
    },
    {
        'project_name': 'Replace School - Dunnellon Senior High',
        'location': 'DUNNELLON SENIOR HIGH',
        'total_cost': 160000000,
        'year_2024': 0, 'year_2025': 160000000, 'year_2026': 0, 'year_2027': 0, 'year_2028': 0,
        'funded': False,
        'project_type': 'School Replacement',
        'student_stations': 0,
    },
    # Maintenance Categories from page 2
    {
        'project_name': 'District-Wide HVAC Maintenance',
        'location': 'District-Wide',
        'total_cost': 325000,
        'year_2024': 65000, 'year_2025': 65000, 'year_2026': 65000, 'year_2027': 65000, 'year_2028': 65000,
        'funded': True,
        'project_type': 'HVAC Maintenance',
        'student_stations': 0,
    },
    {
        'project_name': 'District-Wide Flooring Replacement',
        'location': 'District-Wide',
        'total_cost': 1140384,
        'year_2024': 340384, 'year_2025': 200000, 'year_2026': 200000, 'year_2027': 200000, 'year_2028': 200000,
        'funded': True,
        'project_type': 'Flooring',
        'student_stations': 0,
    },
    {
        'project_name': 'District-Wide Safety to Life',
        'location': 'District-Wide',
        'total_cost': 756065,
        'year_2024': 156065, 'year_2025': 150000, 'year_2026': 150000, 'year_2027': 150000, 'year_2028': 150000,
        'funded': True,
        'project_type': 'Safety',
        'student_stations': 0,
    },
    {
        'project_name': 'District-Wide Parking Resurfacing',
        'location': 'District-Wide',
        'total_cost': 1131525,
        'year_2024': 331525, 'year_2025': 200000, 'year_2026': 200000, 'year_2027': 200000, 'year_2028': 200000,
        'funded': True,
        'project_type': 'Parking',
        'student_stations': 0,
    },
    {
        'project_name': 'District-Wide Painting',
        'location': 'District-Wide',
        'total_cost': 625000,
        'year_2024': 125000, 'year_2025': 125000, 'year_2026': 125000, 'year_2027': 125000, 'year_2028': 125000,
        'funded': True,
        'project_type': 'Painting',
        'student_stations': 0,
    },
    {
        'project_name': 'District-Wide School Bus Purchases',
        'location': 'District-Wide',
        'total_cost': 10004726,
        'year_2024': 2004726, 'year_2025': 2000000, 'year_2026': 2000000, 'year_2027': 2000000, 'year_2028': 2000000,
        'funded': True,
        'project_type': 'Vehicle Purchase',
        'student_stations': 0,
    },
    {
        'project_name': 'COP Debt Service',
        'location': 'District-Wide',
        'total_cost': 129263888,
        'year_2024': 26339792, 'year_2025': 25732783, 'year_2026': 25731533, 'year_2027': 25731015, 'year_2028': 25728765,
        'funded': True,
        'project_type': 'Debt Service',
        'student_stations': 0,
    },
]


def create_vendor():
    """Create the Marion County School District vendor."""
    return {
        'vendor_id': 'MCSD-FACILITIES',
        'vendor_name': 'Marion County School District',
        'vendor_type': 'Government/Education',
        'status': 'Active',
        'contact_name': 'Ivonne Bumbach',
        'contact_email': 'Ivonne.Bumbach@marion.k12.fl.us',
        'city': 'Ocala',
        'state': 'FL',
        'minority_owned': 0,
        'woman_owned': 0,
        'small_business': 0,
        'local_business': 1,
    }


def create_contracts_from_projects(projects):
    """Convert capital projects into contract records."""
    contracts = []
    current_year = datetime.now().year

    for i, proj in enumerate(projects, 1):
        # Determine status based on funding and year
        if proj['funded']:
            if proj['year_2024'] > 0:
                status = 'Active'
                percent = 50
            elif proj['year_2025'] > 0:
                status = 'Planning'
                percent = 10
            else:
                status = 'Planning'
                percent = 5
        else:
            status = 'Proposed'
            percent = 0

        # Calculate start year based on budget allocation
        start_year = 2024
        for year_offset, key in enumerate(['year_2024', 'year_2025', 'year_2026', 'year_2027', 'year_2028']):
            if proj[key] > 0:
                start_year = 2024 + year_offset
                break

        # Calculate end year
        end_year = start_year
        for year_offset, key in enumerate(['year_2024', 'year_2025', 'year_2026', 'year_2027', 'year_2028']):
            if proj[key] > 0:
                end_year = 2024 + year_offset

        contract_id = f"MCSD-{start_year}-{i:03d}"

        contracts.append({
            'contract_id': contract_id,
            'contract_number': f"MCSD-FY{start_year}-{proj['project_type'][:3].upper()}-{i:03d}",
            'title': proj['project_name'],
            'description': f"Marion County School District capital project: {proj['project_name']}. Location: {proj['location']}. Project Type: {proj['project_type']}. {'Funded' if proj['funded'] else 'Not Funded'}.",
            'vendor_id': 'MCSD-FACILITIES',
            'vendor_name': 'Marion County School District',
            'department': 'School District Facilities',
            'contract_type': 'Capital Project',
            'status': status,
            'original_amount': proj['total_cost'],
            'current_amount': proj['total_cost'],
            'total_paid': proj['total_cost'] * (percent / 100),
            'start_date': f"{start_year}-07-01",  # FL fiscal year for schools
            'end_date': f"{end_year + 1}-06-30",
            'current_end_date': f"{end_year + 1}-06-30",
            'percent_complete': percent,
            'procurement_method': 'Public Education Capital Outlay' if proj['funded'] else 'Pending Funding',
            'competitive_bids': 0,
            'fiscal_year': str(start_year),
        })

    return contracts


def import_to_database(contracts, vendor):
    """Import the processed data into SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Clear existing MCSD data
    cursor.execute("DELETE FROM contracts WHERE contract_id LIKE 'MCSD-%'")
    cursor.execute("DELETE FROM vendors WHERE vendor_id = 'MCSD-FACILITIES'")

    # Insert vendor
    cursor.execute("""
        INSERT OR REPLACE INTO vendors
        (vendor_id, vendor_name, vendor_type, status, contact_name, contact_email,
         city, state, minority_owned, woman_owned, small_business, local_business)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        vendor['vendor_id'], vendor['vendor_name'], vendor['vendor_type'],
        vendor['status'], vendor['contact_name'], vendor['contact_email'],
        vendor['city'], vendor['state'], vendor['minority_owned'], vendor['woman_owned'],
        vendor['small_business'], vendor['local_business']
    ))

    # Insert contracts
    for contract in contracts:
        cursor.execute("""
            INSERT OR REPLACE INTO contracts
            (contract_id, contract_number, title, description, vendor_id, vendor_name,
             department, contract_type, status, original_amount, current_amount, total_paid,
             start_date, original_end_date, current_end_date, percent_complete, procurement_method,
             bid_count, fiscal_year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            contract['contract_id'], contract['contract_number'], contract['title'],
            contract['description'], contract['vendor_id'], contract['vendor_name'],
            contract['department'], contract['contract_type'], contract['status'],
            contract['original_amount'], contract['current_amount'], contract['total_paid'],
            contract['start_date'], contract['end_date'], contract['current_end_date'],
            contract['percent_complete'], contract['procurement_method'],
            contract['competitive_bids'], contract.get('fiscal_year', '')
        ))

    conn.commit()
    print(f"Imported {len(contracts)} school district contracts and 1 vendor")

    # Print summary
    cursor.execute("SELECT COUNT(*) FROM contracts WHERE contract_id LIKE 'MCSD-%'")
    count = cursor.fetchone()[0]
    print(f"Total School District contracts in database: {count}")

    cursor.execute("""
        SELECT status, COUNT(*), SUM(current_amount)
        FROM contracts WHERE contract_id LIKE 'MCSD-%'
        GROUP BY status ORDER BY SUM(current_amount) DESC
    """)
    print("\nContracts by Status:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} contracts, ${row[2]:,.0f}")

    cursor.execute("""
        SELECT contract_type, COUNT(*), SUM(current_amount)
        FROM contracts WHERE contract_id LIKE 'MCSD-%'
        GROUP BY contract_type ORDER BY SUM(current_amount) DESC
    """)
    print("\nContracts by Type:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} projects, ${row[2]:,.0f}")

    conn.close()


def main():
    print("=" * 60)
    print("Marion County School District Capital Projects Import")
    print("=" * 60)
    print(f"\nSource: 5-Year District Facilities Work Plan (2024-2025)")
    print(f"Total projects to import: {len(CAPITAL_PROJECTS)}")

    # Calculate totals
    total_budget = sum(p['total_cost'] for p in CAPITAL_PROJECTS)
    funded_total = sum(p['total_cost'] for p in CAPITAL_PROJECTS if p['funded'])
    print(f"Total 5-Year Budget: ${total_budget:,.0f}")
    print(f"Funded Projects: ${funded_total:,.0f}")

    # Create contracts
    print("\nConverting to contract format...")
    vendor = create_vendor()
    contracts = create_contracts_from_projects(CAPITAL_PROJECTS)
    print(f"Created {len(contracts)} contracts")

    # Import to database
    print("\nImporting to database...")
    import_to_database(contracts, vendor)

    print("\n" + "=" * 60)
    print("Import complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
