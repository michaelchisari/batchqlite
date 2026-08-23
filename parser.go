package batchqlite

var illegalKeywords = []string{
	"BEGIN",
	"COMMIT",
	"END",
	"ROLLBACK",
	"SAVEPOINT",
	"RELEASE",
	"ATTACH",
	"DETACH",
	"PRAGMA",
	"RETURNING",
	"VACUUM",
	"REINDEX",
	"ANALYZE",
}

// lexicalParser returns the SQL reserved words found in query and reports
// whether query contains multiple statements.
func lexicalParser(query string) (words []string, multiple bool) {
	return []string{}, false
}
