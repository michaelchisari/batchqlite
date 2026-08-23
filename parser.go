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
func lexicalParser(q string) (words map[string]struct{}, multiple bool) {

	words = make(map[string]struct{})

	for i := 0; i < len(q); {
		// whitespace
		if isSpace(q[i]) {
			i++
			continue
		}

		// line comment --
		if i+1 < len(q) && q[i] == '-' && q[i+1] == '-' {
			i = skipLineComment(q, i)
			continue
		}

		// block comment /**/
		if i+1 < len(q) && q[i] == '/' && q[i+1] == '*' {
			i = skipBlockComment(q, i)
			continue
		}

		// quoted string
		if q[i] == '\'' || q[i] == '"' || q[i] == '`' {
			i = skipQuoted(q, i, q[i])
			continue
		}

		// bracket
		if q[i] == '[' {
			i = skipBracket(q, i)
			continue
		}

		// statement separator
		if q[i] == ';' {
			i++

			for i < len(q) && isSpace(q[i]) {
				i++
			}

			if i < len(q) {
				multiple = true
			}

			continue
		}

		// keyword
		if isIdentStart(q[i]) {
			start := i
			i++

			for i < len(q) && isIdentChar(q[i]) {
				i++
			}

			w := q[start:i]
			n := make([]byte, len(w))

			// lowercase
			for j := 0; j < len(w); j++ {
				c := w[j]

				if c >= 'a' && c <= 'z' {
					n[j] = c - 32
				} else {
					n[j] = c
				}
			}

			words[w] = struct{}{}
			continue
		}
		i++
	}

	return words, multiple
}

func skipLineComment(s string, i int) int {
	i += 2
	for i < len(s) && s[i] != '\n' {
		i++
	}

	return i
}

func skipBlockComment(s string, i int) int {
	i += 2
	for i+1 < len(s) {
		if s[i] == '*' && s[i+1] == '/' {
			return i + 2
		}
		i++
	}

	return len(s)
}

func skipQuoted(s string, i int, quote byte) int {
	i++

	for i < len(s) {
		if s[i] != quote {
			i++
			continue
		}

		// escaped quote
		if i+1 < len(s) && s[i+1] == quote {
			i += 2
			continue
		}

		return i + 1
	}

	return len(s)
}

func skipBracket(s string, i int) int {
	i++

	for i < len(s) {
		if s[i] == ']' {
			return i + 1
		}
		i++
	}

	return len(s)
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f'
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}
