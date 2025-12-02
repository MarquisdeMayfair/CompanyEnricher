"""
Clean up email records that are actually image filenames
"""
import sqlite3

DB_PATH = 'companies.db'

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Count before cleanup
cursor.execute('''
    SELECT COUNT(*) FROM emails 
    WHERE email LIKE '%.png' 
       OR email LIKE '%.jpg' 
       OR email LIKE '%.jpeg' 
       OR email LIKE '%.gif' 
       OR email LIKE '%.svg'
       OR email LIKE '%.webp'
       OR email LIKE '%.ico'
       OR email LIKE '%.bmp'
       OR email LIKE '%.tiff'
''')
count_before = cursor.fetchone()[0]
print(f"Found {count_before} email records that are image filenames")

# Show some examples
cursor.execute('''
    SELECT email, company_number FROM emails 
    WHERE email LIKE '%.png' 
       OR email LIKE '%.jpg' 
       OR email LIKE '%.jpeg' 
       OR email LIKE '%.gif' 
       OR email LIKE '%.svg'
       OR email LIKE '%.webp'
       OR email LIKE '%.ico'
    LIMIT 10
''')
print("\nExamples of bad records:")
for row in cursor.fetchall():
    print(f"  {row[0]} (company: {row[1]})")

# Delete the bad records
print("\nDeleting image filename records...")
cursor.execute('''
    DELETE FROM emails 
    WHERE email LIKE '%.png' 
       OR email LIKE '%.jpg' 
       OR email LIKE '%.jpeg' 
       OR email LIKE '%.gif' 
       OR email LIKE '%.svg'
       OR email LIKE '%.webp'
       OR email LIKE '%.ico'
       OR email LIKE '%.bmp'
       OR email LIKE '%.tiff'
''')
deleted = cursor.rowcount
conn.commit()

print(f"✅ Deleted {deleted} bad email records")

# Also check for other invalid patterns
cursor.execute('''
    SELECT COUNT(*) FROM emails 
    WHERE email LIKE 'user@domain%'
       OR email LIKE 'email@domain%'
       OR email LIKE 'name@domain%'
       OR email LIKE 'your@email%'
       OR email LIKE '%@example.%'
''')
placeholder_count = cursor.fetchone()[0]
if placeholder_count > 0:
    print(f"\nFound {placeholder_count} placeholder email records (user@domain.com etc)")
    cursor.execute('''
        DELETE FROM emails 
        WHERE email LIKE 'user@domain%'
           OR email LIKE 'email@domain%'
           OR email LIKE 'name@domain%'
           OR email LIKE 'your@email%'
           OR email LIKE '%@example.%'
    ''')
    print(f"✅ Deleted {cursor.rowcount} placeholder email records")
    conn.commit()

# Final count
cursor.execute('SELECT COUNT(*) FROM emails')
total = cursor.fetchone()[0]
print(f"\nTotal valid emails remaining: {total}")

conn.close()
print("\n✅ Cleanup complete!")
