import pyodbc

# Update info here for database. Autocommit will prevent updates from taking effect unintentionally.
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=server_address;'
                      'Database=database_name;'
                      'UID=user_name;'
                      'PWD=password@;'
                      'Trusted_Connection=No;'
                      'autocommit=False')

cursor = conn.cursor()
# Insert your initial query here
cursor.execute("insert SQL query here")
columns = [column[0] for column in cursor.description]

results = []

# Creating an array of dictionaries so we can access column names in our query
for row in cursor.fetchall():
    results.append(dict(zip(columns, row)))

unique = []
# Python sets require that each element be unique, so we can use it to pick out duplicates
seen = set()
duplicate = []

'''
Using Python set functionality to detect duplicate entries and put them into duplicate array
while retaining an original copy in the unique table.
Note that this will still leave a copy of the original entry in the unique table,
so we will have an original entry with no concatenated integer in the name, and only
subsequent duplicates will have them. This should be how the system would handle it by default.
'''
for num, name in enumerate(results, start=0):
    if results[num]['ColumnToCheckForDupes'] not in seen:
        unique.append(results[num])
        seen.add(results[num]['ColumnToCheckForDupes'])
    else:
        duplicate.append(results[num])

_size = len(duplicate)

# Add an adjusted key to our duplicate dictionary entries to track whether they have been iterated over
for i in range(_size):
    duplicate[i]['Adjusted'] = False

'''
Go through each duplicate entry, find those that share a name, and add an iterated integer to the
new username at an arbitrary index
'''
for i in range(_size):
    if not duplicate[i]['Adjusted']:
        count = 1
        nameToCheck = duplicate[i]['ColumnToCheckForDupes']

        # Use these points to determine arbitrary points of integer insertion based on contractor status
        if not duplicate[i]['condition']:
            insertPoint = duplicate[i]['ColumnToCheckForDupes'].find('@')
        else:
            insertPoint = duplicate[i]['ColumnToCheckForDupes'].find('.contractor')

        adjustedName = duplicate[i]['ColumnToCheckForDupes'][:insertPoint] + str(count) + duplicate[i]['ColumnToCheckForDupes'][insertPoint:]
        duplicate[i]['ColumnToCheckForDupes'] = adjustedName
        duplicate[i]['Adjusted'] = True
        k = i + 1
        for j in range(k, _size):
            if duplicate[j]['ColumnToCheckForDupes'] == nameToCheck and not duplicate[j]['Adjusted']:
                count = count + 1
                adjustedName = duplicate[j]['ColumnToCheckForDupes'][:insertPoint] + str(count) + duplicate[j]['ColumnToCheckForDupes'][insertPoint:]
                duplicate[j]['ColumnToCheckForDupes'] = adjustedName
                duplicate[j]['Adjusted'] = True

# Run updates to change our usernames. Comment out the cnxn.commit() ONLY when you want it to commit!
dupChangeCount = 0
for i in range(len(duplicate)):
    cursor.execute("UPDATE usr SET ORIG_COL = ? where id = ?", duplicate[i]['ColumnToCheckForDupes'], duplicate[i]['id'])
    dupChangeCount = dupChangeCount + 1
    # cnxn.commit()

print('Final Count changed: ' + str(dupChangeCount))

uneditedCount = 0
for i in range(len(unique)):
    cursor.execute("UPDATE usr SET ORIG_COL = ? where id = ?", duplicate[i]['ColumnToCheckForDupes'], duplicate[i]['id'])
    uneditedCount = uneditedCount + 1
    # cnxn.commit()

print('Unedited print count changed: ' + str(uneditedCount))
