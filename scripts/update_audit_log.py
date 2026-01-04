"""
Update audit log with realistic activity entries for Marion County contracts.
"""

import sqlite3
from datetime import datetime, timedelta
import random
import os

# Database path
db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'contracts.db')

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get contract IDs
cursor.execute('SELECT contract_id, title FROM contracts LIMIT 20')
contracts = cursor.fetchall()

# Clear old sample data entries
cursor.execute("DELETE FROM audit_log WHERE changed_by = 'sample_data'")

# Add realistic activity entries
activities = [
    ('UPDATE', 'status', 'Draft', 'Active', 'Contract approved by Board'),
    ('UPDATE', 'current_amount', '$450,000', '$485,000', 'Amendment approved - scope expansion'),
    ('CREATE', 'milestone', None, None, 'Quarterly review milestone scheduled'),
    ('UPDATE', 'payment', None, None, 'Payment voucher PV-2024-0892 processed'),
    ('UPDATE', 'status', 'Active', 'Completed', 'Final deliverables accepted'),
    ('CREATE', 'change_order', None, None, 'Change order CO-2024-015 submitted'),
    ('UPDATE', 'end_date', '2025-06-30', '2025-09-30', 'Contract extended per Board approval'),
    ('CREATE', 'issue', None, None, 'Performance concern flagged for review'),
    ('UPDATE', 'status', 'On Hold', 'Active', 'Hold released - funding confirmed'),
    ('UPDATE', 'vendor_assigned', None, None, 'Vendor reassignment completed'),
    ('CREATE', 'document', None, None, 'Insurance certificate uploaded'),
    ('UPDATE', 'compliance', None, None, 'Annual compliance review completed'),
]

# Marion County staff names
users = [
    'Maria Rodriguez, Procurement Manager',
    'John Thompson, Contract Administrator',
    'Sarah Chen, Finance Director',
    'Mike Williams, Public Works Director',
    'Lisa Johnson, School Board Liaison',
    'David Martinez, Budget Analyst',
    'System Automation'
]

now = datetime.now()
log_entries = []

for i in range(30):
    contract = random.choice(contracts)
    activity = random.choice(activities)
    user = random.choice(users)
    days_ago = random.randint(0, 45)
    hours_ago = random.randint(0, 23)
    timestamp = now - timedelta(days=days_ago, hours=hours_ago)

    log_entries.append((
        'contracts',
        contract[0],
        activity[0],
        activity[1],
        activity[2],
        activity[3],
        user,
        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        activity[4]
    ))

# Sort by timestamp descending before inserting
log_entries.sort(key=lambda x: x[7], reverse=True)

cursor.executemany('''
    INSERT INTO audit_log (table_name, record_id, action, field_name, old_value, new_value, changed_by, changed_at, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
''', log_entries)

conn.commit()
print(f'Added {len(log_entries)} audit log entries')

# Show sample of what was added
cursor.execute('SELECT * FROM audit_log ORDER BY changed_at DESC LIMIT 5')
for row in cursor.fetchall():
    print(f"  - {row[7]}: {row[6]} {row[2]}d {row[1]}")

conn.close()
