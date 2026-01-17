package parser

// Token types
type tokenType int

const (
	tokenEOF tokenType = iota
	tokenIdent
	tokenNumber
	tokenString
	tokenOperator
	tokenLParen
	tokenRParen
	tokenComma
	tokenSemicolon
	tokenStar
	tokenDot
	tokenParameter

	// Bitwise operator tokens
	tokenAmpersand   // & (bitwise AND)
	tokenPipe        // | (bitwise OR) - distinct from || (string concatenation)
	tokenCaret       // ^ (bitwise XOR)
	tokenTilde       // ~ (bitwise NOT)
	tokenShiftLeft   // << (left shift)
	tokenShiftRight  // >> (right shift)

	// Array literal tokens
	tokenLBracket // [ (left square bracket)
	tokenRBracket // ] (right square bracket)
)

type token struct {
	typ   tokenType
	value string
	pos   int
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z')
}

func isOperatorChar(ch byte) bool {
	return ch == '+' || ch == '-' || ch == '/' || ch == '%' ||
		ch == '<' || ch == '>' ||
		ch == '=' ||
		ch == '!' ||
		ch == '|' ||
		ch == ':' ||
		ch == '&' || // bitwise AND
		ch == '^' || // bitwise XOR
		ch == '~' // bitwise NOT
}
