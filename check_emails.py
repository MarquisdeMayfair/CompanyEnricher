import sqlite3

conn = sqlite3.connect('companies.db')
cursor = conn.cursor()

output = []

# Find emails that look like image files
cursor.execute('''
    SELECT email, source, COUNT(*) as cnt 
    FROM emails 
    WHERE email LIKE '%.png' 
       OR email LIKE '%.jpg' 
       OR email LIKE '%.jpeg' 
       OR email LIKE '%.gif' 
       OR email LIKE '%.svg'
       OR email LIKE '%.webp'
    GROUP BY email
    ORDER BY cnt DESC
    LIMIT 30
''')
output.append('Sample of image-like emails:')
for row in cursor.fetchall():
    output.append(f'  {row[0]} (source: {row[1]}, count: {row[2]})')

# Get total count
cursor.execute('''
    SELECT COUNT(*) 
    FROM emails 
    WHERE email LIKE '%.png' 
       OR email LIKE '%.jpg' 
       OR email LIKE '%.jpeg' 
       OR email LIKE '%.gif' 
       OR email LIKE '%.svg'
       OR email LIKE '%.webp'
''')
total = cursor.fetchone()[0]
output.append(f'\nTotal image-like emails: {total}')

# Check the pattern - what common patterns are there?
cursor.execute('''
    SELECT 
        CASE 
            WHEN email LIKE '%@2x%' THEN '@2x pattern'
            WHEN email LIKE '%@3x%' THEN '@3x pattern'
            WHEN email LIKE '%@4x%' THEN '@4x pattern'
            WHEN email LIKE '%@1x%' THEN '@1x pattern'
            ELSE 'other'
        END as pattern,
        COUNT(*) as cnt
    FROM emails 
    WHERE email LIKE '%.png' 
       OR email LIKE '%.jpg' 
       OR email LIKE '%.jpeg' 
       OR email LIKE '%.gif' 
       OR email LIKE '%.svg'
       OR email LIKE '%.webp'
    GROUP BY pattern
''')
output.append('\nPatterns found:')
for row in cursor.fetchall():
    output.append(f'  {row[0]}: {row[1]}')

conn.close()

# Write to file
with open('email_check_result.txt', 'w') as f:
    f.write('\n'.join(output))

print('Results written to email_check_result.txt')
