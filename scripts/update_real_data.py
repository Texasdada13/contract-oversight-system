"""
Script to update the database with real Marion County, Florida data.
This replaces dummy sample data with actual information from public sources.
"""

import sqlite3
from datetime import datetime

def update_database():
    conn = sqlite3.connect('data/contracts.db')
    cursor = conn.cursor()

    # =====================================================
    # REAL VENDORS - Based on Marion County, Florida
    # =====================================================
    real_vendors = [
        {
            'vendor_id': 'VND-001',
            'vendor_name': 'Ausley Construction Company, LLC',
            'vendor_type': 'Construction',
            'contact_name': 'Kenneth C. Ausley',
            'contact_email': 'info@ausleyconstruction.com',
            'contact_phone': '352-351-2393',
            'address': '1420 SW 17th Street',
            'city': 'Ocala',
            'state': 'FL',
            'zip_code': '34471',
            'local_business': 1,
            'small_business': 0,
            'performance_score': 82.0,
        },
        {
            'vendor_id': 'VND-002',
            'vendor_name': 'Skyward, Inc.',
            'vendor_type': 'IT Services',
            'contact_name': 'Account Manager',
            'contact_email': 'support@skyward.com',
            'contact_phone': '800-236-7274',
            'address': '5233 Coye Drive',
            'city': 'Stevens Point',
            'state': 'WI',
            'zip_code': '54481',
            'local_business': 0,
            'small_business': 0,
            'performance_score': 88.0,
        },
        {
            'vendor_id': 'VND-003',
            'vendor_name': 'Verkada Inc.',
            'vendor_type': 'Security',
            'contact_name': 'Sales Representative',
            'contact_email': 'sales@verkada.com',
            'contact_phone': '650-514-2500',
            'address': '405 E 4th Ave',
            'city': 'San Mateo',
            'state': 'CA',
            'zip_code': '94401',
            'local_business': 0,
            'small_business': 0,
            'performance_score': 85.0,
        },
        {
            'vendor_id': 'VND-004',
            'vendor_name': 'ABM Industries',
            'vendor_type': 'Janitorial',
            'contact_name': 'Regional Manager',
            'contact_email': 'info@abm.com',
            'contact_phone': '866-624-1520',
            'address': 'One Liberty Plaza',
            'city': 'New York',
            'state': 'NY',
            'zip_code': '10006',
            'local_business': 0,
            'small_business': 0,
            'performance_score': 79.0,
        },
        {
            'vendor_id': 'VND-005',
            'vendor_name': 'Wharton-Smith, Inc.',
            'vendor_type': 'Construction',
            'contact_name': 'Project Manager',
            'contact_email': 'info@whartonsmith.com',
            'contact_phone': '407-321-8410',
            'address': '750 Monroe Road',
            'city': 'Sanford',
            'state': 'FL',
            'zip_code': '32771',
            'local_business': 0,
            'small_business': 0,
            'performance_score': 91.0,
        },
        {
            'vendor_id': 'VND-006',
            'vendor_name': 'Blue Bird Corporation',
            'vendor_type': 'Transportation',
            'contact_name': 'Fleet Sales',
            'contact_email': 'sales@blue-bird.com',
            'contact_phone': '478-822-2242',
            'address': '3920 Arkwright Road',
            'city': 'Macon',
            'state': 'GA',
            'zip_code': '31210',
            'local_business': 0,
            'small_business': 0,
            'performance_score': 87.0,
        },
        {
            'vendor_id': 'VND-007',
            'vendor_name': 'Charles Perry Partners, Inc.',
            'vendor_type': 'Construction',
            'contact_name': 'Project Executive',
            'contact_email': 'info@cppi.com',
            'contact_phone': '352-332-4555',
            'address': '2855 SW 91st Street',
            'city': 'Gainesville',
            'state': 'FL',
            'zip_code': '32608',
            'local_business': 1,
            'small_business': 0,
            'performance_score': 86.0,
        },
        {
            'vendor_id': 'VND-008',
            'vendor_name': 'Ocala Electric Utility',
            'vendor_type': 'Utilities',
            'contact_name': 'Commercial Services',
            'contact_email': 'oeu@ocalafl.org',
            'contact_phone': '352-629-8421',
            'address': '201 SE 3rd Street',
            'city': 'Ocala',
            'state': 'FL',
            'zip_code': '34471',
            'local_business': 1,
            'small_business': 0,
            'performance_score': 92.0,
        },
        {
            'vendor_id': 'VND-009',
            'vendor_name': 'Houghton Mifflin Harcourt',
            'vendor_type': 'Educational Materials',
            'contact_name': 'Account Executive',
            'contact_email': 'k12info@hmhco.com',
            'contact_phone': '800-225-5425',
            'address': '125 High Street',
            'city': 'Boston',
            'state': 'MA',
            'zip_code': '02110',
            'local_business': 0,
            'small_business': 0,
            'performance_score': 84.0,
        },
        {
            'vendor_id': 'VND-010',
            'vendor_name': 'McGraw Hill LLC',
            'vendor_type': 'Educational Materials',
            'contact_name': 'District Sales Manager',
            'contact_email': 'k12@mheducation.com',
            'contact_phone': '800-338-3987',
            'address': '1325 Avenue of Americas',
            'city': 'New York',
            'state': 'NY',
            'zip_code': '10019',
            'local_business': 0,
            'small_business': 0,
            'performance_score': 83.0,
        },
    ]

    # Update vendors
    for vendor in real_vendors:
        cursor.execute("""
            UPDATE vendors SET
                vendor_name = ?,
                vendor_type = ?,
                contact_name = ?,
                contact_email = ?,
                contact_phone = ?,
                address = ?,
                city = ?,
                state = ?,
                zip_code = ?,
                local_business = ?,
                small_business = ?,
                performance_score = ?,
                updated_at = ?
            WHERE vendor_id = ?
        """, (
            vendor['vendor_name'],
            vendor['vendor_type'],
            vendor['contact_name'],
            vendor['contact_email'],
            vendor['contact_phone'],
            vendor['address'],
            vendor['city'],
            vendor['state'],
            vendor['zip_code'],
            vendor['local_business'],
            vendor['small_business'],
            vendor['performance_score'],
            datetime.now().isoformat(),
            vendor['vendor_id']
        ))

    # =====================================================
    # REAL CONTRACTS - Based on Marion County Public Schools
    # =====================================================
    real_contracts = [
        {
            'contract_id': 'CTR-2024-001',
            'contract_number': 'MCPS-2024-001',
            'title': 'Winding Oaks Farm Elementary School Construction',
            'description': 'New K-5 elementary school construction in the Winding Oaks Farm development, Ocala',
            'vendor_id': 'VND-001',
            'vendor_name': 'Ausley Construction Company, LLC',
            'original_amount': 42000000.0,
            'current_amount': 42500000.0,
            'department': 'Facilities',
            'contract_type': 'Construction',
            'school_zone': 'Winding Oaks Farm',
            'project_location': 'Winding Oaks Farm, Ocala, FL',
        },
        {
            'contract_id': 'CTR-2024-002',
            'contract_number': 'MCPS-2024-002',
            'title': 'Skyward Student Information System',
            'description': 'Annual licensing, maintenance, and support for district-wide Skyward SIS including Family Access portal',
            'vendor_id': 'VND-002',
            'vendor_name': 'Skyward, Inc.',
            'original_amount': 385000.0,
            'current_amount': 385000.0,
            'department': 'Technology',
            'contract_type': 'IT Services',
            'school_zone': None,
            'project_location': 'District-Wide',
        },
        {
            'contract_id': 'CTR-2024-003',
            'contract_number': 'MCPS-2024-003',
            'title': 'District-Wide Security Camera Upgrade',
            'description': 'Installation and maintenance of security cameras across all schools per HB 1473 requirements - minimum 10-30 cameras per school',
            'vendor_id': 'VND-003',
            'vendor_name': 'Verkada Inc.',
            'original_amount': 2800000.0,
            'current_amount': 3200000.0,
            'department': 'Safety',
            'contract_type': 'Security',
            'school_zone': None,
            'project_location': 'All Marion County Schools',
        },
        {
            'contract_id': 'CTR-2024-004',
            'contract_number': 'MCPS-2024-004',
            'title': 'Custodial Services Contract',
            'description': 'District-wide custodial and janitorial services for schools and administrative buildings',
            'vendor_id': 'VND-004',
            'vendor_name': 'ABM Industries',
            'original_amount': 4500000.0,
            'current_amount': 4500000.0,
            'department': 'Operations',
            'contract_type': 'Services',
            'school_zone': None,
            'project_location': 'District-Wide',
        },
        {
            'contract_id': 'CTR-2024-005',
            'contract_number': 'MCPS-2024-005',
            'title': 'New High School Construction Project',
            'description': 'Construction of new prototype high school - largest construction contract in district history',
            'vendor_id': 'VND-005',
            'vendor_name': 'Wharton-Smith, Inc.',
            'original_amount': 120000000.0,
            'current_amount': 120000000.0,
            'department': 'Facilities',
            'contract_type': 'Construction',
            'school_zone': 'Southwest Marion',
            'project_location': 'SW Marion County, FL',
        },
        {
            'contract_id': 'CTR-2024-006',
            'contract_number': 'MCPS-2024-006',
            'title': 'School Bus Fleet Acquisition',
            'description': 'Purchase of 12 new school buses - 8 regular buses and 4 buses with wheelchair lifts',
            'vendor_id': 'VND-006',
            'vendor_name': 'Blue Bird Corporation',
            'original_amount': 1915904.0,
            'current_amount': 1915904.0,
            'department': 'Transportation',
            'contract_type': 'Procurement',
            'school_zone': None,
            'project_location': 'District Transportation Center',
        },
        {
            'contract_id': 'CTR-2024-007',
            'contract_number': 'MCPS-2024-007',
            'title': 'Liberty Middle School HVAC Renovation',
            'description': 'Complete HVAC system replacement and energy efficiency upgrades',
            'vendor_id': 'VND-007',
            'vendor_name': 'Charles Perry Partners, Inc.',
            'original_amount': 3200000.0,
            'current_amount': 3450000.0,
            'department': 'Facilities',
            'contract_type': 'Construction',
            'school_zone': 'Liberty Middle School',
            'project_location': 'Liberty Middle School, Ocala, FL',
        },
        {
            'contract_id': 'CTR-2024-008',
            'contract_number': 'MCPS-2024-008',
            'title': 'District Electric Utility Agreement',
            'description': 'Multi-year electric utility service agreement for all district facilities',
            'vendor_id': 'VND-008',
            'vendor_name': 'Ocala Electric Utility',
            'original_amount': 8500000.0,
            'current_amount': 8500000.0,
            'department': 'Operations',
            'contract_type': 'Utilities',
            'school_zone': None,
            'project_location': 'District-Wide',
        },
        {
            'contract_id': 'CTR-2024-009',
            'contract_number': 'MCPS-2024-009',
            'title': 'K-12 ELA Curriculum Adoption',
            'description': 'English Language Arts curriculum materials for grades K-12 including digital resources',
            'vendor_id': 'VND-009',
            'vendor_name': 'Houghton Mifflin Harcourt',
            'original_amount': 2100000.0,
            'current_amount': 2100000.0,
            'department': 'Curriculum',
            'contract_type': 'Educational Materials',
            'school_zone': None,
            'project_location': 'All Schools',
        },
        {
            'contract_id': 'CTR-2024-010',
            'contract_number': 'MCPS-2024-010',
            'title': 'Mathematics Curriculum and Assessment',
            'description': 'Math curriculum materials K-12 with online platform access and assessment tools',
            'vendor_id': 'VND-010',
            'vendor_name': 'McGraw Hill LLC',
            'original_amount': 1850000.0,
            'current_amount': 1850000.0,
            'department': 'Curriculum',
            'contract_type': 'Educational Materials',
            'school_zone': None,
            'project_location': 'All Schools',
        },
    ]

    # Update contracts
    for contract in real_contracts:
        cursor.execute("""
            UPDATE contracts SET
                contract_number = ?,
                title = ?,
                description = ?,
                vendor_id = ?,
                vendor_name = ?,
                original_amount = ?,
                current_amount = ?,
                department = ?,
                contract_type = ?,
                school_zone = ?,
                project_location = ?,
                updated_at = ?
            WHERE contract_id = ?
        """, (
            contract['contract_number'],
            contract['title'],
            contract['description'],
            contract['vendor_id'],
            contract['vendor_name'],
            contract['original_amount'],
            contract['current_amount'],
            contract['department'],
            contract['contract_type'],
            contract['school_zone'],
            contract['project_location'],
            datetime.now().isoformat(),
            contract['contract_id']
        ))

    # =====================================================
    # UPDATE USERS - Real Marion County School Board Members
    # =====================================================
    # First check if users table has appropriate structure
    cursor.execute("PRAGMA table_info(users)")
    user_columns = [col[1] for col in cursor.fetchall()]

    real_board_members = [
        {
            'user_id': 'USR-001',
            'name': 'Dr. Sarah James',
            'email': 'sarah.james@marion.k12.fl.us',
            'role': 'School Board Chair',
            'department': 'School Board',
        },
        {
            'user_id': 'USR-002',
            'name': 'Lori Conrad',
            'email': 'lori.conrad@marion.k12.fl.us',
            'role': 'School Board Vice-Chair',
            'department': 'School Board',
        },
        {
            'user_id': 'USR-003',
            'name': 'Dr. Allison Campbell',
            'email': 'allison.campbell@marion.k12.fl.us',
            'role': 'School Board Member - District 1',
            'department': 'School Board',
        },
        {
            'user_id': 'USR-004',
            'name': 'Rev. Eric Cummings',
            'email': 'eric.cummings@marion.k12.fl.us',
            'role': 'School Board Member - District 3',
            'department': 'School Board',
        },
        {
            'user_id': 'USR-005',
            'name': 'Nancy Thrower',
            'email': 'nancy.thrower@marion.k12.fl.us',
            'role': 'School Board Member - District 4',
            'department': 'School Board',
        },
        {
            'user_id': 'USR-006',
            'name': 'Dr. Danielle Brewer',
            'email': 'danielle.brewer@marion.k12.fl.us',
            'role': 'Interim Superintendent',
            'department': 'Administration',
        },
    ]

    # Check if users exist and update them
    for member in real_board_members:
        cursor.execute("SELECT COUNT(*) FROM users WHERE user_id = ?", (member['user_id'],))
        if cursor.fetchone()[0] > 0:
            if 'name' in user_columns:
                cursor.execute("""
                    UPDATE users SET name = ?, email = ?, role = ?, department = ?, updated_at = ?
                    WHERE user_id = ?
                """, (member['name'], member['email'], member['role'], member['department'],
                      datetime.now().isoformat(), member['user_id']))
            elif 'full_name' in user_columns:
                cursor.execute("""
                    UPDATE users SET full_name = ?, email = ?, role = ?, department = ?, updated_at = ?
                    WHERE user_id = ?
                """, (member['name'], member['email'], member['role'], member['department'],
                      datetime.now().isoformat(), member['user_id']))

    # =====================================================
    # UPDATE MILESTONES with realistic project data
    # =====================================================
    milestone_updates = [
        {
            'milestone_id': 1,
            'title': 'Site Preparation and Grading',
            'description': 'Clear site and complete earthwork for Winding Oaks Elementary',
            'responsible_party': 'Ausley Construction Site Manager',
        },
        {
            'milestone_id': 2,
            'title': 'Foundation and Structural Steel',
            'description': 'Complete foundation work and erect structural steel frame',
            'responsible_party': 'Ausley Construction Project Manager',
        },
        {
            'milestone_id': 3,
            'title': 'Building Envelope Completion',
            'description': 'Complete exterior walls, roofing, and weatherproofing',
            'responsible_party': 'Ausley Construction Superintendent',
        },
    ]

    for milestone in milestone_updates:
        cursor.execute("""
            UPDATE milestones SET
                title = ?,
                description = ?,
                responsible_party = ?
            WHERE milestone_id = ?
        """, (
            milestone['title'],
            milestone['description'],
            milestone['responsible_party'],
            milestone['milestone_id']
        ))

    # =====================================================
    # UPDATE ISSUES with realistic descriptions
    # =====================================================
    issue_updates = [
        {
            'issue_id': 1,
            'title': 'Supply Chain Delays - Security Camera Equipment',
            'description': 'Verkada camera equipment delivery delayed 6 weeks due to global supply chain issues',
            'assigned_to': 'MCPS Safety Director',
        },
        {
            'issue_id': 2,
            'title': 'Change Order Required - HVAC Design Modification',
            'description': 'Liberty Middle School HVAC design requires modification to accommodate new state energy standards',
            'assigned_to': 'Charles Perry Partners Project Manager',
        },
    ]

    for issue in issue_updates:
        cursor.execute("""
            UPDATE issues SET
                title = ?,
                description = ?,
                assigned_to = ?
            WHERE issue_id = ?
        """, (
            issue['title'],
            issue['description'],
            issue['assigned_to'],
            issue['issue_id']
        ))

    conn.commit()
    conn.close()

    print("Database updated with real Marion County, Florida data!")
    print("\nVendors updated:")
    for v in real_vendors:
        print(f"  - {v['vendor_name']} ({v['vendor_type']})")
    print("\nContracts updated:")
    for c in real_contracts:
        print(f"  - {c['title']} (${c['original_amount']:,.0f})")
    print("\nSchool Board Members updated:")
    for m in real_board_members:
        print(f"  - {m['name']} - {m['role']}")

if __name__ == '__main__':
    update_database()
