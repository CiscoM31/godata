package godata

import (
	"fmt"
	"strings"
	"testing"
)

func TestTokenTypes(t *testing.T) {
	if expressionTokenLast.String() != "expressionTokenLast" {
		t.Errorf("Unexpected String() value: %v", expressionTokenLast)
	}
}

func TestExpressionDateTime(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	tokens := map[string]ExpressionTokenType{
		"2011-08-29T21:58Z":             ExpressionTokenDateTime,
		"2011-08-29T21:58:33Z":          ExpressionTokenDateTime,
		"2011-08-29T21:58:33.123Z":      ExpressionTokenDateTime,
		"2011-08-29T21:58+11:23":        ExpressionTokenDateTime,
		"2011-08-29T21:58:33+11:23":     ExpressionTokenDateTime,
		"2011-08-29T21:58:33.123+11:23": ExpressionTokenDateTime,
		"2011-08-29T21:58:33-11:23":     ExpressionTokenDateTime,
		"2011-08-29":                    ExpressionTokenDate,
		"21:58:33":                      ExpressionTokenTime,
	}
	for tokenValue, tokenType := range tokens {
		// Previously, the unit test had no space character after 'gt'
		// E.g. 'CreateTime gt2011-08-29T21:58Z' was considered valid.
		// However the ABNF notation for ODATA logical operators is:
		//   gtExpr = RWS "gt" RWS commonExpr
		//   RWS = 1*( SP / HTAB / "%20" / "%09" )  ; "required" whitespace
		//
		// See http://docs.oasis-open.org/odata/odata/v4.01/csprd03/abnf/odata-abnf-construction-rules.txt
		input := "CreateTime gt " + tokenValue
		expect := []*Token{
			{Value: "CreateTime", Type: ExpressionTokenLiteral},
			{Value: "gt", Type: ExpressionTokenLogical},
			{Value: tokenValue, Type: tokenType},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Errorf("Failed to tokenize input %s. Error: %v", input, err)
		}

		result, err := CompareTokens(expect, output)
		if !result {
			var a []string
			for _, t := range output {
				a = append(a, t.Value)
			}

			t.Errorf("Unexpected tokens for input '%s'. Tokens: %s Error: %v", input, strings.Join(a, ", "), err)
		}
	}
}

func TestValidBooleanExpressionSyntax(t *testing.T) {
	queries := []string{
		"substring(CompanyName,1,2) eq 'lf'", // substring with 3 arguments.
		// Bolean values
		"true",
		"false",
		"(true)",
		"((true))",
		"((true)) or false",
		"not true",
		"not false",
		"not (not true)",
		//"not not true", // TODO: I think this should work. 'not not true' is true
		// String functions
		"contains(CompanyName,'freds')",
		"endswith(CompanyName,'Futterkiste')",
		"startswith(CompanyName,'Alfr')",
		"length(CompanyName) eq 19",
		"indexof(CompanyName,'lfreds') eq 1",
		"substring(CompanyName,1) eq 'lfreds Futterkiste'", // substring() with 2 arguments.
		"'lfreds Futterkiste' eq substring(CompanyName,1)", // Same as above, but order of operands is reversed.
		"substring(CompanyName,1,2) eq 'lf'",               // substring() with 3 arguments.
		"'lf' eq substring(CompanyName,1,2) ",              // Same as above, but order of operands is reversed.
		"substringof('Alfreds', CompanyName) eq true",
		"tolower(CompanyName) eq 'alfreds futterkiste'",
		"toupper(CompanyName) eq 'ALFREDS FUTTERKISTE'",
		"trim(CompanyName) eq 'Alfreds Futterkiste'",
		"concat(concat(City,', '), Country) eq 'Berlin, Germany'",
		// GUID
		"GuidValue eq 01234567-89ab-cdef-0123-456789abcdef", // TODO According to ODATA ABNF notation, GUID values do not have quotes.
		// Date and Time functions
		"StartDate eq 2012-12-03",
		"DateTimeOffsetValue eq 2012-12-03T07:16:23Z",
		// duration      = [ "duration" ] SQUOTE durationValue SQUOTE
		// "DurationValue eq duration'P12DT23H59M59.999999999999S'", // TODO See ODATA ABNF notation
		"TimeOfDayValue eq 07:59:59.999",
		"year(BirthDate) eq 0",
		"month(BirthDate) eq 12",
		"day(StartTime) eq 8",
		"hour(StartTime) eq 1",
		"hour    (StartTime) eq 12",     // function followed by space characters
		"hour    ( StartTime   ) eq 15", // function followed by space characters
		"minute(StartTime) eq 0",
		"totaloffsetminutes(StartTime) eq 0",
		"second(StartTime) eq 0",
		"fractionalseconds(StartTime) lt 0.123456", // The fractionalseconds function returns the fractional seconds component of the
		// DateTimeOffset or TimeOfDay parameter value as a non-negative decimal value less than 1.
		"date(StartTime) ne date(EndTime)",
		"totaloffsetminutes(StartTime) eq 60",
		"StartTime eq mindatetime()",
		// "totalseconds(EndTime sub StartTime) lt duration'PT23H59'", // TODO The totalseconds function returns the duration of the value in total seconds, including fractional seconds.
		"EndTime eq maxdatetime()",
		"time(StartTime) le StartOfDay",
		"time('2015-10-14T23:30:00.104+02:00') lt now()",
		"time(2015-10-14T23:30:00.104+02:00) lt now()",
		// Math functions
		"round(Freight) eq 32",
		"floor(Freight) eq 32",
		"ceiling(Freight) eq 33",
		"Rating mod 5 eq 0",
		"Price div 2 eq 3",
		// Type functions
		"isof(ShipCountry,Edm.String)",
		"isof(NorthwindModel.BigOrder)",
		"cast(ShipCountry,Edm.String)",
		// Parameter aliases
		// See http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part1-protocol/odata-v4.0-errata03-os-part1-protocol-complete.html#_Toc453752288
		"Region eq @p1", // Aliases start with @
		// Geo functions
		"geo.distance(CurrentPosition,TargetPosition)",
		"geo.length(DirectRoute)",
		"geo.intersects(Position,TargetArea)",
		"GEO.INTERSECTS(Position,TargetArea)", // functions are case insensitive in ODATA 4.0.1
		// Logical operators
		"'Milk' eq 'Milk'",  // Compare two literals
		"'Water' ne 'Milk'", // Compare two literals
		"Name eq 'Milk'",
		"Name EQ 'Milk'", // operators are case insensitive in ODATA 4.0.1
		"Name ne 'Milk'",
		"Name NE 'Milk'",
		"Name gt 'Milk'",
		"Name ge 'Milk'",
		"Name lt 'Milk'",
		"Name le 'Milk'",
		"Name eq Name", // parameter equals to itself
		"Name eq 'Milk' and Price lt 2.55",
		"not endswith(Name,'ilk')",
		"Name eq 'Milk' or Price lt 2.55",
		"City eq 'Dallas' or City eq 'Houston'",
		// Nested properties
		"Product/Name eq 'Milk'",
		"Region/Product/Name eq 'Milk'",
		"Country/Region/Product/Name eq 'Milk'",
		//"style has Sales.Pattern'Yellow'", // TODO
		// Arithmetic operators
		"Price add 2.45 eq 5.00",
		"2.46 add Price eq 5.00",
		"Price add (2.47) eq 5.00",
		"(Price add (2.48)) eq 5.00",
		"Price ADD 2.49 eq 5.00", // 4.01 Services MUST support case-insensitive operator names.
		"Price sub 0.55 eq 2.00",
		"Price SUB 0.56 EQ 2.00", // 4.01 Services MUST support case-insensitive operator names.
		"Price mul 2.0 eq 5.10",
		"Price div 2.55 eq 1",
		"Rating div 2 eq 2",
		"Rating mod 5 eq 0",
		// Grouping
		"(4 add 5) mod (4 sub 1) eq 0",
		"not (City eq 'Dallas') or Name in ('a', 'b', 'c') and not (State eq 'California')",
		// Nested functions
		"length(trim(CompanyName)) eq length(CompanyName)",
		"concat(concat(City, ', '), Country) eq 'Berlin, Germany'",
		// Various parenthesis combinations
		"City eq 'Dallas'",
		"City eq ('Dallas')",
		"'Dallas' eq City",
		"not (City eq 'Dallas')",
		"City in ('Dallas')",
		"(City in ('Dallas'))",
		"(City in ('Dallas', 'Houston'))",
		"not (City in ('Dallas'))",
		"not (City in ('Dallas', 'Houston'))",
		"not (((City eq 'Dallas')))",
		"not(S1 eq 'foo')",
		// Lambda operators
		"Tags/any()",                    // The any operator without an argument returns true if the collection is not empty
		"Tags/any(tag:tag eq 'London')", // 'Tags' is array of strings
		"Tags/any(tag:tag eq 'London' or tag eq 'Berlin')",          // 'Tags' is array of strings
		"Tags/any(var:var/Key eq 'Site' and var/Value eq 'London')", // 'Tags' is array of {"Key": "abc", "Value": "def"}
		"Tags/ANY(var:var/Key eq 'Site' AND var/Value eq 'London')",
		"Tags/any(var:var/Key eq 'Site' and var/Value eq 'London') and not (City in ('Dallas'))",
		"Tags/all(var:var/Key eq 'Site' and var/Value eq 'London')",
		"Price/any(t:not (12345 eq t))",
		// A long query.
		"Tags/any(var:var/Key eq 'Site' and var/Value eq 'London') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'Berlin') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'Paris') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'New York City') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'San Francisco')",
	}
	p := NewExpressionParser()
	p.ExpectBoolExpr = true
	for _, input := range queries {
		q, err := p.ParseExpressionString(input)
		if err != nil {
			t.Errorf("Error parsing query '%s'. Error: %v", input, err)
		} else {
			if q.Tree == nil {
				t.Errorf("Error parsing query '%s'. Tree is nil", input)
			}
			if q.Tree.Token == nil {
				t.Errorf("Error parsing query '%s'. Root token is nil", input)
			}
			if q.Tree.Token.Type == ExpressionTokenLiteral {
				t.Errorf("Error parsing query '%s'. Unexpected root token type: %+v", input, q.Tree.Token)
			}
		}
		//printTree(q.Tree)
	}
}

// The URLs below are not valid ODATA syntax, the parser should return an error.
func TestInvalidExpressionSyntax(t *testing.T) {
	queries := []string{
		"()", // It's not a boolean expression
		"(TRUE)",
		"(City)",
		"(",
		"((((",
		")",
		"12345",                                // Number 12345 is not a boolean expression
		"0",                                    // Number 0 is not a boolean expression
		"'123'",                                // String '123' is not a boolean expression
		"TRUE",                                 // Should be 'true' lowercase
		"FALSE",                                // Should be 'false' lowercase
		"yes",                                  // yes is not a boolean expression
		"no",                                   // yes is not a boolean expression
		"",                                     // Empty string.
		"eq",                                   // Just a single logical operator
		"and",                                  // Just a single logical operator
		"add",                                  // Just a single arithmetic operator
		"add ",                                 // Just a single arithmetic operator
		"add 2",                                // Missing operands
		"add 2 3",                              // Missing operands
		"City",                                 // Just a single literal
		"City City City City",                  // Sequence of literals
		"City eq",                              // Missing operand
		"City eq (",                            // Wrong operand
		"City eq )",                            // Wrong operand
		"City equals 'Dallas'",                 // Unknown operator that starts with the same letters as a known operator
		"City near 'Dallas'",                   // Unknown operator that starts with the same letters as a known operator
		"City isNot 'Dallas'",                  // Unknown operator
		"not [City eq 'Dallas']",               // Wrong delimiter
		"not (City eq )",                       // Missing operand
		"not ((City eq 'Dallas'",               // Missing closing parenthesis
		"not (City eq 'Dallas'",                // Missing closing parenthesis
		"not (City eq 'Dallas'))",              // Extraneous closing parenthesis
		"not City eq 'Dallas')",                // Missing open parenthesis
		"City eq 'Dallas' orCity eq 'Houston'", // missing space between or and City
		// TODO: the query below should fail.
		//"Tags/any(var:var/Key eq 'Site') orTags/any(var:var/Key eq 'Site')",
		"not (City eq 'Dallas') and Name eq 'Houston')",
		"Tags/all()",                   // The all operator cannot be used without an argument expression.
		"LastName contains 'Smith'",    // Previously the godata library was not returning an error.
		"contains",                     // Function with missing parenthesis and arguments
		"contains()",                   // Function with missing arguments
		"contains LastName, 'Smith'",   // Missing parenthesis
		"contains(LastName)",           // Insufficent number of function arguments
		"contains(LastName, 'Smith'))", // Extraneous closing parenthesis
		"contains(LastName, 'Smith'",   // Missing closing parenthesis
		"contains LastName, 'Smith')",  // Missing open parenthesis
		"City eq 'Dallas' 'Houston'",   // extraneous string value
		//"contains(Name, 'a', 'b', 'c', 'd')", // Too many function arguments
	}
	p := NewExpressionParser()
	p.ExpectBoolExpr = true
	for _, input := range queries {
		q, err := p.ParseExpressionString(input)
		if err == nil {
			// The parser has incorrectly determined the syntax is valid.
			t.Errorf("The expression '%s' is not valid ODATA syntax. The ODATA parser should return an error. Tree:\n%v", input, q.Tree)
		}
	}
}

func BenchmarkExpressionTokenizer(b *testing.B) {
	t := NewExpressionTokenizer()
	for i := 0; i < b.N; i++ {
		input := "Name eq 'Milk' and Price lt 2.55"
		if _, err := t.Tokenize(input); err != nil {
			b.Fatalf("Failed to tokenize expression: %v", err)
		}
	}
}

func tokenArrayToString(list []*Token) string {
	var sb []string
	for _, t := range list {
		sb = append(sb, fmt.Sprintf("%s[%d]", t.Value, t.Type))
	}
	return strings.Join(sb, ", ")
}

// Check if two slices of tokens are the same.
func CompareTokens(expected, actual []*Token) (bool, error) {
	if len(expected) != len(actual) {
		return false, fmt.Errorf("Infix tokens unexpected lengths. Expected %d, Got len=%d. Tokens=%v",
			len(expected), len(actual), tokenArrayToString(actual))
	}
	for i := range expected {
		if expected[i].Type != actual[i].Type {
			return false, fmt.Errorf("Infix token types at index %d. Expected %v, Got %v. Value: %v",
				i, expected[i].Type, actual[i].Type, expected[i].Value)
		}
		if expected[i].Value != actual[i].Value {
			return false, fmt.Errorf("Infix token values at index %d. Expected %v, Got %v",
				i, expected[i].Value, actual[i].Value)
		}
	}
	return true, nil
}

func CompareQueue(expect []*Token, b *tokenQueue) (bool, error) {
	bl := func() int {
		if b.Empty() {
			return 0
		}
		l := 1
		for node := b.Head; node != b.Tail; node = node.Next {
			l++
		}
		return l
	}()
	if len(expect) != bl {
		return false, fmt.Errorf("Postfix queue unexpected length. Got len=%d, expected %d. queue=%v",
			bl, len(expect), b)
	}
	node := b.Head
	for i := range expect {
		if expect[i].Type != node.Token.Type {
			return false, fmt.Errorf("Postfix token types at index %d. Got: %v, expected: %v. Expected value: %v",
				i, node.Token.Type, expect[i].Type, expect[i].Value)
		}
		if expect[i].Value != node.Token.Value {
			return false, fmt.Errorf("Postfix token values at index %d. Got: %v, expected: %v",
				i, node.Token.Value, expect[i].Value)
		}
		node = node.Next
	}
	return true, nil
}

func printTokens(tokens []*Token) {
	s := make([]string, len(tokens))
	for i := range tokens {
		s[i] = tokens[i].Value
	}
	fmt.Printf("TOKENS: %s\n", strings.Join(s, " "))
}

// CompareTree compares a tree representing a ODATA filter with the expected results.
// The expected values are a slice of nodes in breadth-first traversal.
func CompareTree(node *ParseNode, expect []expectedParseNode, pos *int, level int) error {
	if *pos >= len(expect) {
		return fmt.Errorf("Unexpected token at pos %d. Got %s, expected no value",
			*pos, node.Token.Value)
	}
	if node.Token.Value != expect[*pos].Value {
		return fmt.Errorf("Unexpected token at pos %d. Got %s -> %d, expected: %s -> %d",
			*pos, node.Token.Value, level, expect[*pos].Value, expect[*pos].Depth)
	}
	if node.Token.Type != expect[*pos].Type {
		return fmt.Errorf("Unexpected token type at pos %d. Got %v -> %d, expected: %v -> %d",
			*pos, node.Token.Type, level, expect[*pos].Type, expect[*pos].Depth)
	}
	if level != expect[*pos].Depth {
		return fmt.Errorf("Unexpected level at pos %d. Got %s -> %d, expected: %s -> %d",
			*pos, node.Token.Value, level, expect[*pos].Value, expect[*pos].Depth)
	}
	for _, v := range node.Children {
		*pos++
		if err := CompareTree(v, expect, pos, level+1); err != nil {
			return err
		}
	}
	if level == 0 && *pos+1 != len(expect) {
		return fmt.Errorf("Expected number of tokens: %d, got %d", len(expect), *pos+1)
	}
	return nil
}

func TestExpressions(t *testing.T) {

	p := NewExpressionParser()
	for _, testCase := range testCases {
		t.Logf("Expression: %s", testCase.expression)
		tokens, err := GlobalExpressionTokenizer.Tokenize(testCase.expression)
		if err != nil {
			t.Errorf("Failed to tokenize expression '%s'. Error: %v", testCase.expression, err)
			continue
		}
		if testCase.infixTokens != nil {
			if result, err := CompareTokens(testCase.infixTokens, tokens); !result {
				t.Errorf("Unexpected tokens: %v", err)
				continue
			}
		}
		output, err := p.InfixToPostfix(tokens)
		if err != nil {
			t.Errorf("Failed to convert expression to postfix notation: %v", err)
			continue
		}
		if testCase.postfixTokens != nil {
			if result, err := CompareQueue(testCase.postfixTokens, output); !result {
				t.Errorf("Unexpected postfix tokens: %v", err)
				continue
			}
		}
		tree, err := p.PostfixToTree(output)
		if err != nil {
			t.Errorf("Failed to parse expression '%s'. Error: %v", testCase.expression, err)
			continue
		}
		pos := 0
		err = CompareTree(tree, testCase.tree, &pos, 0)
		if err != nil {
			t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
		}
	}

}
