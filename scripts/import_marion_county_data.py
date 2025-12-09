"""
Import Marion County, Florida expenditure and revenue data into the Contract Oversight System.

This script processes official Florida state data from:
https://edr.state.fl.us/Content/local-government/data/revenues-expenditures/cntyfiscal.cfm

Data includes fiscal years 2005-2024 expenditures and 2006-2024 revenues.
"""

import pandas as pd
import sqlite3
import os
from pathlib import Path
from datetime import datetime
import json

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data' / 'marion_county'
DB_PATH = BASE_DIR / 'data' / 'contracts.db'

# Department mapping from account codes to readable names
DEPARTMENT_MAPPING = {
    511: 'Legislative',
    512: 'Executive',
    513: 'Financial and Administrative',
    514: 'Legal Counsel',
    515: 'Comprehensive Planning',
    516: 'Information Systems',
    517: 'Agricultural Extension',
    518: 'Veterans Affairs',
    519: 'Other General Government',
    521: 'Law Enforcement',
    522: 'Fire Control',
    523: 'Detention/Corrections',
    524: 'Protective Inspections',
    525: 'Emergency & Disaster Relief',
    529: 'Other Public Safety',
    531: 'Highways & Streets',
    533: 'Sanitation/Solid Waste',
    534: 'Flood Control/Stormwater',
    536: 'Other Physical Environment',
    537: 'Water/Sewer Combined',
    538: 'Water Utility',
    539: 'Sewer/Wastewater',
    541: 'Transit',
    542: 'Airports',
    543: 'Port/Marina',
    549: 'Other Transportation',
    551: 'Health',
    552: 'Mental Health',
    553: 'Hospitals',
    554: 'Human Services',
    555: 'Public Assistance',
    559: 'Other Human Services',
    561: 'Libraries',
    562: 'Parks & Recreation',
    564: 'Cultural Services',
    566: 'Special Events',
    569: 'Other Culture/Recreation',
    572: 'Industry Development',
    579: 'Other Economic Environment',
}


def parse_expenditure_sheet(df, fiscal_year):
    """Parse a single year's expenditure sheet into structured data."""
    records = []

    # The data starts at row 3 (0-indexed), after headers
    # Column structure varies by year (15-17 columns):
    # 0: Category name or blank
    # 1: Account code (like 511, 512)
    # 2: Account name (like Legislative, Executive)
    # 3+: Various fund types (General, Special Revenue, etc.)
    # Second-to-last: Total Account
    # Last: Per Capita (in newer years)

    num_cols = len(df.columns)
    # Total is second-to-last column if there's a Per Capita column, otherwise last
    total_col_idx = num_cols - 2 if num_cols >= 16 else num_cols - 1

    current_category = None

    for idx, row in df.iterrows():
        if idx < 3:  # Skip header rows
            continue

        # Check if this is a category row (bold total row)
        first_col = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''
        account_code = row.iloc[1] if pd.notna(row.iloc[1]) else None
        account_name = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ''

        # If first column has text and no account code, it's a category
        if first_col and not account_code and not first_col.startswith('Local Fiscal'):
            current_category = first_col
            continue

        # If we have an account code, it's a line item
        if account_code and pd.notna(account_code):
            try:
                code = int(float(account_code))
            except:
                continue

            # Get the total amount (dynamically determined column)
            total = row.iloc[total_col_idx] if total_col_idx < len(row) and pd.notna(row.iloc[total_col_idx]) else 0
            try:
                total = float(total)
            except:
                total = 0

            # Get general fund amount (column 3)
            general_fund = row.iloc[3] if len(row) > 3 and pd.notna(row.iloc[3]) else 0
            try:
                general_fund = float(general_fund)
            except:
                general_fund = 0

            if total > 0:
                records.append({
                    'fiscal_year': fiscal_year,
                    'category': current_category,
                    'account_code': code,
                    'account_name': account_name,
                    'department': DEPARTMENT_MAPPING.get(code, account_name),
                    'general_fund': general_fund,
                    'total_amount': total,
                })

    return records


def create_contracts_from_expenditures(expenditure_records):
    """
    Create contract-like records from expenditure data.
    Each department's annual expenditure becomes a contract.
    """
    contracts = []
    vendors = {}

    # Group by fiscal year and department
    dept_totals = {}
    for rec in expenditure_records:
        key = (rec['fiscal_year'], rec['department'])
        if key not in dept_totals:
            dept_totals[key] = {
                'fiscal_year': rec['fiscal_year'],
                'department': rec['department'],
                'category': rec['category'],
                'total': 0,
                'account_codes': []
            }
        dept_totals[key]['total'] += rec['total_amount']
        dept_totals[key]['account_codes'].append(rec['account_code'])

    # Create a vendor for Marion County Government (internal)
    vendors['MARION-COUNTY-GOV'] = {
        'vendor_id': 'MARION-COUNTY-GOV',
        'vendor_name': 'Marion County Government',
        'vendor_type': 'Government',
        'status': 'Active',
        'contact_name': 'Finance Department',
        'contact_email': 'finance@marioncountyfl.org',
        'city': 'Ocala',
        'state': 'FL',
        'minority_owned': 0,
        'woman_owned': 0,
        'small_business': 0,
        'local_business': 1,
    }

    # Create contracts for recent years (last 5 years)
    recent_years = sorted(set(r['fiscal_year'] for r in expenditure_records))[-5:]

    for (year, dept), data in dept_totals.items():
        if year not in recent_years:
            continue
        if data['total'] < 100000:  # Skip small amounts
            continue

        contract_id = f"MC-{year}-{dept[:3].upper()}-{data['account_codes'][0]:03d}"

        # Determine status based on year
        current_year = datetime.now().year
        if year == current_year or year == current_year - 1:
            status = 'Active'
        else:
            status = 'Completed'

        contracts.append({
            'contract_id': contract_id,
            'contract_number': f"MC-FY{year}-{data['account_codes'][0]:03d}",
            'title': f"{dept} - FY{year} Budget",
            'description': f"Marion County {dept} department expenditures for fiscal year {year}. Category: {data['category']}",
            'vendor_id': 'MARION-COUNTY-GOV',
            'vendor_name': 'Marion County Government',
            'department': dept,
            'contract_type': 'Operating Budget',
            'status': status,
            'original_amount': data['total'],
            'current_amount': data['total'],
            'total_paid': data['total'] if status == 'Completed' else data['total'] * 0.75,
            'start_date': f"{year-1}-10-01",  # FL fiscal year starts Oct 1
            'end_date': f"{year}-09-30",
            'current_end_date': f"{year}-09-30",
            'percent_complete': 100 if status == 'Completed' else 75,
            'procurement_method': 'Budget Allocation',
            'competitive_bids': 0,
            'fiscal_year': str(year),
        })

    return contracts, vendors


def import_to_database(contracts, vendors):
    """Import the processed data into SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Clear existing Marion County data
    cursor.execute("DELETE FROM contracts WHERE contract_id LIKE 'MC-%'")
    cursor.execute("DELETE FROM vendors WHERE vendor_id = 'MARION-COUNTY-GOV'")

    # Insert vendors
    for vendor in vendors.values():
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
    print(f"Imported {len(contracts)} contracts and {len(vendors)} vendors")

    # Print summary
    cursor.execute("SELECT COUNT(*) FROM contracts WHERE contract_id LIKE 'MC-%'")
    count = cursor.fetchone()[0]
    print(f"Total Marion County contracts in database: {count}")

    cursor.execute("SELECT department, SUM(current_amount) FROM contracts WHERE contract_id LIKE 'MC-%' GROUP BY department ORDER BY SUM(current_amount) DESC LIMIT 10")
    print("\nTop 10 Departments by Spending:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: ${row[1]:,.0f}")

    conn.close()


def main():
    print("=" * 60)
    print("Marion County, Florida Data Import")
    print("=" * 60)

    # Check if files exist
    exp_file = DATA_DIR / 'marion_expenditures.xlsx'
    if not exp_file.exists():
        print(f"ERROR: Expenditure file not found: {exp_file}")
        print("Please download from: https://edr.state.fl.us/Content/local-government/data/revenues-expenditures/cntyfiscal.cfm")
        return

    print(f"\nReading expenditure data from: {exp_file}")

    # Read all sheets (years)
    xls = pd.ExcelFile(exp_file)
    all_records = []

    for sheet_name in xls.sheet_names:
        try:
            fiscal_year = int(sheet_name)
            print(f"  Processing FY{fiscal_year}...")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            records = parse_expenditure_sheet(df, fiscal_year)
            all_records.extend(records)
            print(f"    Found {len(records)} line items")
        except ValueError:
            print(f"  Skipping sheet: {sheet_name}")

    print(f"\nTotal expenditure records: {len(all_records)}")

    # Create contract records
    print("\nConverting to contract format...")
    contracts, vendors = create_contracts_from_expenditures(all_records)
    print(f"Created {len(contracts)} contracts")

    # Import to database
    print("\nImporting to database...")
    import_to_database(contracts, vendors)

    print("\n" + "=" * 60)
    print("Import complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
