# How Many Calls are made per run?
NumCalls = 1000

# How Many Warm-up Calls are made per run?
NumWarmup = 100

# How many documents are in the database initially? 
NumDocuments = 100*1000

# How many runs per benchmark? 
NumRuns = 20 

EntryPrefix = "entry"
NewEntryPrefix = "newentry"

assert EntryPrefix != ""
assert NewEntryPrefix != ""
assert EntryPrefix != NewEntryPrefix
assert NumDocuments > 0
assert NumCalls+NumWarmup < NumDocuments
