"""
Sample data generator for Contract Oversight System.
Creates realistic sample contracts, vendors, and related data.
"""

import random
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_database


def generate_sample_data():
    """Generate sample contracts, vendors, and related data."""
    db = get_database()

    # Sample vendors
    vendors = [
        {
            'vendor_id': 'VND-001',
            'vendor_name': 'ABC Construction LLC',
            'vendor_type': 'Construction',
            'contact_name': 'John Smith',
            'contact_email': 'jsmith@abcconstruction.com',
            'contact_phone': '555-0101',
            'city': 'Springfield',
            'state': 'OH',
            'minority_owned': 0,
            'woman_owned': 0,
            'small_business': 1,
            'local_business': 1,
            'status': 'Active',
            'performance_score': 78
        },
        {
            'vendor_id': 'VND-002',
            'vendor_name': 'TechSolutions Inc',
            'vendor_type': 'IT Services',
            'contact_name': 'Sarah Johnson',
            'contact_email': 'sjohnson@techsolutions.com',
            'contact_phone': '555-0102',
            'city': 'Columbus',
            'state': 'OH',
            'minority_owned': 1,
            'woman_owned': 1,
            'small_business': 1,
            'local_business': 0,
            'status': 'Active',
            'performance_score': 85
        },
        {
            'vendor_id': 'VND-003',
            'vendor_name': 'Guardian Security Services',
            'vendor_type': 'Security',
            'contact_name': 'Mike Brown',
            'contact_email': 'mbrown@guardiansec.com',
            'contact_phone': '555-0103',
            'city': 'Springfield',
            'state': 'OH',
            'minority_owned': 0,
            'woman_owned': 0,
            'small_business': 0,
            'local_business': 1,
            'status': 'Active',
            'performance_score': 72
        },
        {
            'vendor_id': 'VND-004',
            'vendor_name': 'Green Clean Environmental',
            'vendor_type': 'Janitorial',
            'contact_name': 'Lisa Garcia',
            'contact_email': 'lgarcia@greenclean.com',
            'contact_phone': '555-0104',
            'city': 'Dayton',
            'state': 'OH',
            'minority_owned': 1,
            'woman_owned': 1,
            'small_business': 1,
            'local_business': 0,
            'status': 'Active',
            'performance_score': 91
        },
        {
            'vendor_id': 'VND-005',
            'vendor_name': 'Apex Engineering Group',
            'vendor_type': 'Engineering',
            'contact_name': 'Robert Chen',
            'contact_email': 'rchen@apexeng.com',
            'contact_phone': '555-0105',
            'city': 'Cincinnati',
            'state': 'OH',
            'minority_owned': 1,
            'woman_owned': 0,
            'small_business': 0,
            'local_business': 0,
            'status': 'Active',
            'performance_score': 68
        },
        {
            'vendor_id': 'VND-006',
            'vendor_name': 'First Choice Bus Services',
            'vendor_type': 'Transportation',
            'contact_name': 'David Wilson',
            'contact_email': 'dwilson@firstchoicebus.com',
            'contact_phone': '555-0106',
            'city': 'Springfield',
            'state': 'OH',
            'minority_owned': 0,
            'woman_owned': 0,
            'small_business': 1,
            'local_business': 1,
            'status': 'Active',
            'performance_score': 75
        },
        {
            'vendor_id': 'VND-007',
            'vendor_name': 'Scholastic Supplies Co',
            'vendor_type': 'Supplies',
            'contact_name': 'Jennifer Adams',
            'contact_email': 'jadams@scholasticsupplies.com',
            'contact_phone': '555-0107',
            'city': 'Cleveland',
            'state': 'OH',
            'minority_owned': 0,
            'woman_owned': 1,
            'small_business': 1,
            'local_business': 0,
            'status': 'Active',
            'performance_score': 88
        },
        {
            'vendor_id': 'VND-008',
            'vendor_name': 'Metro HVAC Systems',
            'vendor_type': 'HVAC',
            'contact_name': 'Thomas Martinez',
            'contact_email': 'tmartinez@metrohvac.com',
            'contact_phone': '555-0108',
            'city': 'Springfield',
            'state': 'OH',
            'minority_owned': 1,
            'woman_owned': 0,
            'small_business': 1,
            'local_business': 1,
            'status': 'Active',
            'performance_score': 45  # Poor performer
        }
    ]

    # Save vendors
    for vendor in vendors:
        db.save_vendor(vendor, changed_by='sample_data')
    print(f"Created {len(vendors)} vendors")

    # Sample contracts
    contracts = [
        # School Board Contracts
        {
            'contract_id': 'CTR-2024-001',
            'contract_number': 'SB-2024-001',
            'title': 'Lincoln Elementary School Roof Replacement',
            'description': 'Complete tear-off and replacement of roof system including insulation',
            'contract_type': 'Construction',
            'department': 'Facilities',
            'fiscal_year': '2024',
            'vendor_id': 'VND-001',
            'vendor_name': 'ABC Construction LLC',
            'original_amount': 850000,
            'current_amount': 925000,  # Cost overrun
            'total_paid': 650000,
            'solicitation_date': '2023-10-01',
            'award_date': '2023-11-15',
            'start_date': '2024-01-15',
            'original_end_date': '2024-06-30',
            'current_end_date': '2024-08-15',  # Delayed
            'status': 'Active',
            'phase': 'Construction',
            'percent_complete': 70,
            'procurement_method': 'Competitive Bid',
            'bid_count': 5,
            'board_approval_date': '2023-11-10',
            'board_resolution': 'RES-2023-142',
            'school_zone': 'Lincoln Elementary',
            'has_change_orders': 1,
            'change_order_count': 2,
            'total_change_order_amount': 75000,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'requires_bond': 1,
            'bond_verified': 1,
            'notes': 'Unforeseen structural damage discovered during tear-off'
        },
        {
            'contract_id': 'CTR-2024-002',
            'contract_number': 'SB-2024-002',
            'title': 'District-Wide Student Information System',
            'description': 'Implementation of new SIS including data migration and training',
            'contract_type': 'IT Services',
            'department': 'Technology',
            'fiscal_year': '2024',
            'vendor_id': 'VND-002',
            'vendor_name': 'TechSolutions Inc',
            'original_amount': 450000,
            'current_amount': 450000,
            'total_paid': 225000,
            'solicitation_date': '2023-08-01',
            'award_date': '2023-09-20',
            'start_date': '2023-10-01',
            'original_end_date': '2024-08-01',
            'current_end_date': '2024-08-01',
            'status': 'Active',
            'phase': 'Implementation',
            'percent_complete': 55,
            'procurement_method': 'RFP',
            'bid_count': 3,
            'board_approval_date': '2023-09-15',
            'board_resolution': 'RES-2023-098',
            'has_change_orders': 0,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'notes': 'On track for summer go-live'
        },
        {
            'contract_id': 'CTR-2024-003',
            'contract_number': 'SB-2024-003',
            'title': 'School Security System Upgrade',
            'description': 'Installation of new access control and camera systems at 12 schools',
            'contract_type': 'Security',
            'department': 'Safety',
            'fiscal_year': '2024',
            'vendor_id': 'VND-003',
            'vendor_name': 'Guardian Security Services',
            'original_amount': 1200000,
            'current_amount': 1450000,  # Significant overrun
            'total_paid': 900000,
            'solicitation_date': '2023-06-01',
            'award_date': '2023-07-15',
            'start_date': '2023-08-01',
            'original_end_date': '2024-03-01',
            'current_end_date': '2024-06-01',  # Delayed
            'status': 'Active',
            'phase': 'Installation',
            'percent_complete': 65,
            'procurement_method': 'Competitive Bid',
            'bid_count': 4,
            'board_approval_date': '2023-07-10',
            'board_resolution': 'RES-2023-078',
            'has_change_orders': 1,
            'change_order_count': 3,
            'total_change_order_amount': 250000,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'requires_bond': 1,
            'bond_verified': 0,  # Issue!
            'notes': 'Supply chain delays and additional camera locations requested'
        },
        {
            'contract_id': 'CTR-2024-004',
            'contract_number': 'SB-2024-004',
            'title': 'Annual Janitorial Services',
            'description': 'Daily cleaning services for all district facilities',
            'contract_type': 'Services',
            'department': 'Operations',
            'fiscal_year': '2024',
            'vendor_id': 'VND-004',
            'vendor_name': 'Green Clean Environmental',
            'original_amount': 2400000,
            'current_amount': 2400000,
            'total_paid': 1600000,
            'solicitation_date': '2023-04-01',
            'award_date': '2023-05-15',
            'start_date': '2023-07-01',
            'original_end_date': '2024-06-30',
            'current_end_date': '2024-06-30',
            'status': 'Active',
            'phase': 'Ongoing',
            'percent_complete': 83,
            'procurement_method': 'Competitive Bid',
            'bid_count': 6,
            'board_approval_date': '2023-05-10',
            'board_resolution': 'RES-2023-056',
            'has_change_orders': 0,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'notes': 'Excellent performance, consider renewal'
        },
        {
            'contract_id': 'CTR-2024-005',
            'contract_number': 'SB-2024-005',
            'title': 'Jefferson Middle School HVAC Replacement',
            'description': 'Replace aging HVAC system with energy-efficient units',
            'contract_type': 'Construction',
            'department': 'Facilities',
            'fiscal_year': '2024',
            'vendor_id': 'VND-008',
            'vendor_name': 'Metro HVAC Systems',
            'original_amount': 680000,
            'current_amount': 890000,  # Major overrun
            'total_paid': 500000,
            'solicitation_date': '2023-09-01',
            'award_date': '2023-10-20',
            'start_date': '2023-11-01',
            'original_end_date': '2024-04-01',
            'current_end_date': '2024-07-15',  # Major delay
            'status': 'Active',
            'phase': 'Construction',
            'percent_complete': 45,
            'procurement_method': 'Competitive Bid',
            'bid_count': 3,
            'board_approval_date': '2023-10-15',
            'board_resolution': 'RES-2023-125',
            'has_change_orders': 1,
            'change_order_count': 4,
            'total_change_order_amount': 210000,
            'requires_insurance': 1,
            'insurance_verified': 0,  # Issue!
            'requires_bond': 1,
            'bond_verified': 1,
            'notes': 'PROBLEM CONTRACT - Multiple delays, vendor performance issues'
        },

        # City Council Contracts
        {
            'contract_id': 'CTR-2024-006',
            'contract_number': 'CC-2024-001',
            'title': 'Main Street Reconstruction Project',
            'description': 'Complete reconstruction of Main Street from 1st to 10th Avenue',
            'contract_type': 'Construction',
            'department': 'Public Works',
            'fiscal_year': '2024',
            'vendor_id': 'VND-001',
            'vendor_name': 'ABC Construction LLC',
            'original_amount': 3500000,
            'current_amount': 3750000,
            'total_paid': 2100000,
            'solicitation_date': '2023-07-01',
            'award_date': '2023-08-20',
            'start_date': '2023-09-15',
            'original_end_date': '2024-09-15',
            'current_end_date': '2024-10-30',
            'status': 'Active',
            'phase': 'Construction',
            'percent_complete': 60,
            'procurement_method': 'Competitive Bid',
            'bid_count': 7,
            'board_approval_date': '2023-08-15',
            'board_resolution': 'ORD-2023-089',
            'council_district': 'District 3',
            'project_location': 'Main Street',
            'latitude': 39.9242,
            'longitude': -83.8088,
            'has_change_orders': 1,
            'change_order_count': 2,
            'total_change_order_amount': 250000,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'requires_bond': 1,
            'bond_verified': 1,
            'notes': 'Underground utility conflicts caused delays'
        },
        {
            'contract_id': 'CTR-2024-007',
            'contract_number': 'CC-2024-002',
            'title': 'City Hall Network Infrastructure Upgrade',
            'description': 'Upgrade network switches, cabling, and wireless access points',
            'contract_type': 'IT Services',
            'department': 'IT',
            'fiscal_year': '2024',
            'vendor_id': 'VND-002',
            'vendor_name': 'TechSolutions Inc',
            'original_amount': 175000,
            'current_amount': 175000,
            'total_paid': 175000,
            'solicitation_date': '2023-11-01',
            'award_date': '2023-12-01',
            'start_date': '2024-01-02',
            'original_end_date': '2024-03-31',
            'current_end_date': '2024-03-31',
            'actual_end_date': '2024-03-28',
            'status': 'Completed',
            'phase': 'Closeout',
            'percent_complete': 100,
            'procurement_method': 'RFP',
            'bid_count': 4,
            'board_approval_date': '2023-11-28',
            'council_district': 'Citywide',
            'has_change_orders': 0,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'notes': 'Completed on time and under budget'
        },
        {
            'contract_id': 'CTR-2024-008',
            'contract_number': 'CC-2024-003',
            'title': 'Parks Department Equipment Purchase',
            'description': 'Purchase of mowers, utility vehicles, and maintenance equipment',
            'contract_type': 'Goods',
            'department': 'Parks',
            'fiscal_year': '2024',
            'vendor_id': 'VND-007',
            'vendor_name': 'Scholastic Supplies Co',
            'original_amount': 285000,
            'current_amount': 285000,
            'total_paid': 285000,
            'solicitation_date': '2024-01-15',
            'award_date': '2024-02-10',
            'start_date': '2024-02-15',
            'original_end_date': '2024-04-15',
            'current_end_date': '2024-04-15',
            'actual_end_date': '2024-04-10',
            'status': 'Completed',
            'phase': 'Closeout',
            'percent_complete': 100,
            'procurement_method': 'State Contract',
            'council_district': 'Citywide',
            'has_change_orders': 0,
            'requires_insurance': 0,
            'notes': 'All equipment delivered and inspected'
        },
        {
            'contract_id': 'CTR-2024-009',
            'contract_number': 'CC-2024-004',
            'title': 'Wastewater Treatment Plant Engineering Study',
            'description': 'Engineering analysis and design for plant capacity expansion',
            'contract_type': 'Professional Services',
            'department': 'Utilities',
            'fiscal_year': '2024',
            'vendor_id': 'VND-005',
            'vendor_name': 'Apex Engineering Group',
            'original_amount': 320000,
            'current_amount': 420000,  # Scope creep
            'total_paid': 280000,
            'solicitation_date': '2023-05-01',
            'award_date': '2023-06-15',
            'start_date': '2023-07-01',
            'original_end_date': '2024-01-31',
            'current_end_date': '2024-05-31',  # Extended
            'status': 'Active',
            'phase': 'Design',
            'percent_complete': 75,
            'procurement_method': 'RFQ',
            'bid_count': 5,
            'board_approval_date': '2023-06-12',
            'council_district': 'District 5',
            'has_change_orders': 1,
            'change_order_count': 2,
            'total_change_order_amount': 100000,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'notes': 'Additional environmental analysis required by EPA'
        },
        {
            'contract_id': 'CTR-2024-010',
            'contract_number': 'CC-2024-005',
            'title': 'Public Transit Bus Lease',
            'description': '3-year lease of 15 transit buses with maintenance',
            'contract_type': 'Lease',
            'department': 'Transit',
            'fiscal_year': '2024',
            'vendor_id': 'VND-006',
            'vendor_name': 'First Choice Bus Services',
            'original_amount': 1800000,
            'current_amount': 1800000,
            'total_paid': 600000,
            'solicitation_date': '2023-03-01',
            'award_date': '2023-04-20',
            'start_date': '2023-07-01',
            'original_end_date': '2026-06-30',
            'current_end_date': '2026-06-30',
            'status': 'Active',
            'phase': 'Ongoing',
            'percent_complete': 33,
            'procurement_method': 'Competitive Bid',
            'bid_count': 3,
            'board_approval_date': '2023-04-15',
            'council_district': 'Citywide',
            'has_change_orders': 0,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'notes': 'Year 1 of 3-year contract'
        },
        {
            'contract_id': 'CTR-2024-011',
            'contract_number': 'CC-2024-006',
            'title': 'Emergency Generator Installation - Fire Stations',
            'description': 'Install backup generators at 5 fire stations',
            'contract_type': 'Construction',
            'department': 'Fire',
            'fiscal_year': '2024',
            'vendor_id': 'VND-008',
            'vendor_name': 'Metro HVAC Systems',
            'original_amount': 425000,
            'current_amount': 525000,
            'total_paid': 200000,
            'solicitation_date': '2024-01-01',
            'award_date': '2024-02-01',
            'start_date': '2024-02-15',
            'original_end_date': '2024-06-15',
            'current_end_date': '2024-09-01',
            'status': 'Active',
            'phase': 'Construction',
            'percent_complete': 35,
            'procurement_method': 'Emergency',
            'bid_count': 1,
            'is_emergency': 1,
            'is_sole_source': 1,
            'justification': 'Emergency procurement due to grid reliability concerns',
            'council_district': 'Citywide',
            'has_change_orders': 1,
            'change_order_count': 2,
            'total_change_order_amount': 100000,
            'requires_insurance': 1,
            'insurance_verified': 0,
            'requires_bond': 1,
            'bond_verified': 0,
            'notes': 'PROBLEM CONTRACT - Emergency procurement, cost overruns, compliance issues'
        },
        {
            'contract_id': 'CTR-2024-012',
            'contract_number': 'CC-2024-007',
            'title': 'Annual Legal Services Retainer',
            'description': 'General legal counsel services for city operations',
            'contract_type': 'Professional Services',
            'department': 'Legal',
            'fiscal_year': '2024',
            'vendor_id': None,
            'vendor_name': 'Morrison & Associates Law Firm',
            'original_amount': 150000,
            'current_amount': 150000,
            'total_paid': 112500,
            'award_date': '2023-12-15',
            'start_date': '2024-01-01',
            'original_end_date': '2024-12-31',
            'current_end_date': '2024-12-31',
            'status': 'Active',
            'phase': 'Ongoing',
            'percent_complete': 75,
            'procurement_method': 'Professional Services',
            'council_district': 'Citywide',
            'has_change_orders': 0,
            'requires_insurance': 1,
            'insurance_verified': 1,
            'notes': 'Standard annual retainer'
        }
    ]

    # Save contracts
    for contract in contracts:
        db.save_contract(contract, changed_by='sample_data')
    print(f"Created {len(contracts)} contracts")

    # Add some milestones for the roof replacement project
    milestones = [
        {'contract_id': 'CTR-2024-001', 'milestone_number': 1, 'title': 'Demolition Complete', 'due_date': '2024-02-15', 'completed_date': '2024-02-20', 'status': 'Completed', 'payment_amount': 170000},
        {'contract_id': 'CTR-2024-001', 'milestone_number': 2, 'title': 'Decking Installed', 'due_date': '2024-03-30', 'completed_date': '2024-04-10', 'status': 'Completed', 'payment_amount': 255000},
        {'contract_id': 'CTR-2024-001', 'milestone_number': 3, 'title': 'Membrane Applied', 'due_date': '2024-05-15', 'status': 'In Progress', 'payment_amount': 255000},
        {'contract_id': 'CTR-2024-001', 'milestone_number': 4, 'title': 'Final Inspection', 'due_date': '2024-06-30', 'status': 'Pending', 'payment_amount': 170000},
    ]

    for m in milestones:
        db.add_milestone(m)
    print(f"Created {len(milestones)} milestones")

    # Add some change orders
    change_orders = [
        {
            'contract_id': 'CTR-2024-001',
            'change_order_number': 'CO-001',
            'description': 'Additional structural repairs discovered during tear-off',
            'reason': 'Unforeseen Conditions',
            'amount': 50000,
            'days_added': 15,
            'status': 'Approved',
            'requested_date': '2024-02-25',
            'approved_date': '2024-03-05',
            'approved_by': 'Facilities Director',
            'board_approval_required': 1,
            'board_approval_date': '2024-03-10'
        },
        {
            'contract_id': 'CTR-2024-001',
            'change_order_number': 'CO-002',
            'description': 'Upgrade to premium membrane per board request',
            'reason': 'Scope Change',
            'amount': 25000,
            'days_added': 0,
            'status': 'Approved',
            'requested_date': '2024-04-01',
            'approved_date': '2024-04-05',
            'approved_by': 'Facilities Director'
        },
        {
            'contract_id': 'CTR-2024-003',
            'change_order_number': 'CO-001',
            'description': 'Additional cameras requested for parking lots',
            'reason': 'Scope Change',
            'amount': 125000,
            'days_added': 30,
            'status': 'Approved',
            'requested_date': '2023-10-15',
            'approved_date': '2023-10-30',
            'approved_by': 'Safety Director',
            'board_approval_required': 1,
            'board_approval_date': '2023-11-01'
        }
    ]

    for co in change_orders:
        db.add_change_order(co)
    print(f"Created {len(change_orders)} change orders")

    # Add some issues
    issues = [
        {
            'contract_id': 'CTR-2024-005',
            'issue_type': 'Performance',
            'severity': 'Critical',
            'title': 'Vendor Missing Deadlines',
            'description': 'Metro HVAC has missed 3 consecutive milestone deadlines',
            'status': 'Open',
            'reported_by': 'Project Manager'
        },
        {
            'contract_id': 'CTR-2024-005',
            'issue_type': 'Compliance',
            'severity': 'High',
            'title': 'Insurance Certificate Expired',
            'description': 'Vendor insurance certificate expired, awaiting renewal',
            'status': 'Open',
            'reported_by': 'Risk Manager'
        },
        {
            'contract_id': 'CTR-2024-003',
            'issue_type': 'Compliance',
            'severity': 'High',
            'title': 'Bond Not Verified',
            'description': 'Performance bond documentation incomplete',
            'status': 'Open',
            'reported_by': 'Procurement'
        },
        {
            'contract_id': 'CTR-2024-011',
            'issue_type': 'Cost',
            'severity': 'High',
            'title': 'Budget Exceeded Without Approval',
            'description': 'Emergency contract costs exceeded original estimate by 23%',
            'status': 'Open',
            'reported_by': 'Finance Director'
        }
    ]

    for issue in issues:
        db.add_issue(issue)
    print(f"Created {len(issues)} issues")

    print("\nSample data generation complete!")


if __name__ == '__main__':
    generate_sample_data()
