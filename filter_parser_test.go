package godata

import (
	"fmt"
	"strings"
	"testing"
)

func TestFilterDateTime(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	tokens := map[string]TokenType{
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

func TestFilterAnyArrayOfObjects(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "Tags/any(d:d/Key eq 'Site' and d/Value lt 10)"
	expect := []*Token{
		{Value: "Tags", Type: ExpressionTokenLiteral},
		{Value: "/", Type: ExpressionTokenLambdaNav},
		{Value: "any", Type: ExpressionTokenLambda},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "d", Type: ExpressionTokenLiteral},
		{Value: ",", Type: ExpressionTokenColon}, // ':' is replaced by ',' which is the function argument separator.
		{Value: "d", Type: ExpressionTokenLiteral},
		{Value: "/", Type: ExpressionTokenNav},
		{Value: "Key", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "'Site'", Type: ExpressionTokenString},
		{Value: "and", Type: ExpressionTokenLogical},
		{Value: "d", Type: ExpressionTokenLiteral},
		{Value: "/", Type: ExpressionTokenNav},
		{Value: "Value", Type: ExpressionTokenLiteral},
		{Value: "lt", Type: ExpressionTokenLogical},
		{Value: "10", Type: ExpressionTokenInteger},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}

	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}

func TestFilterAnyArrayOfPrimitiveTypes(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "Tags/any(d:d eq 'Site')"
	{
		expect := []*Token{
			{Value: "Tags", Type: ExpressionTokenLiteral},
			{Value: "/", Type: ExpressionTokenLambdaNav},
			{Value: "any", Type: ExpressionTokenLambda},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "d", Type: ExpressionTokenLiteral},
			{Value: ",", Type: ExpressionTokenColon},
			{Value: "d", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "'Site'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}

		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %v", input, err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "d", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "d", Depth: 3, Type: ExpressionTokenLiteral},
		{Value: "'Site'", Depth: 3, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, q.Tree)
	}
}

// geographyPolygon   = geographyPrefix SQUOTE fullPolygonLiteral SQUOTE
// geographyPrefix = "geography"
// fullPolygonLiteral = sridLiteral polygonLiteral
// sridLiteral      = "SRID" EQ 1*5DIGIT SEMI
// polygonLiteral     = "Polygon" polygonData
// polygonData        = OPEN ringLiteral *( COMMA ringLiteral ) CLOSE
// positionLiteral  = doubleValue SP doubleValue  ; longitude, then latitude
/*
func TestFilterGeographyPolygon(t *testing.T) {
	input := "geo.intersects(location, geography'SRID=0;Polygon(-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581)')"
	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %s", input, err.Error())
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value:"geo.intersects", Depth:0, Type: 0},
		{Value:"location", Depth:1, Type: 0},
		{Value:"geography'SRID=0;Polygon(-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581)'", Depth:1, Type: 0},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		fmt.Printf("Got tree:\n%v\n", q.Tree.String())
		t.Errorf("Tree representation does not match expected value. error: %s", err.Error())
	}
}
*/

// TestFilterAnyGeography matches documents where any of the geo coordinates in the locations field is within the given polygon.
/*
func TestFilterAnyGeography(t *testing.T) {
	input := "locations/any(loc: geo.intersects(loc, geography'Polygon((-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581))'))"
	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %s", input, err.Error())
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value:"/", Depth:0, Type: 0},
		{Value:"Tags", Depth:1, Type: 0},
		{Value:"any", Depth:1, Type: 0},
		{Value:"d", Depth:2, Type: 0},
		{Value:"or", Depth:2, Type: 0},
		{Value:"or", Depth:3, Type: 0},
		{Value:"or", Depth:4, Type: 0},
		{Value:"eq", Depth:5, Type: 0},
		{Value:"d", Depth:6, Type: 0},
		{Value:"'Site'", Depth:6, Type: 0},
		{Value:"eq", Depth:5, Type: 0},
		{Value:"'Environment'", Depth:6, Type: 0},
		{Value:"/", Depth:6, Type: 0},
		{Value:"d", Depth:7, Type: 0},
		{Value:"Key", Depth:7, Type: 0},
		{Value:"eq", Depth:4, Type: 0},
		{Value:"/", Depth:5, Type: 0},
		{Value:"/", Depth:6, Type: 0},
		{Value:"d", Depth:7, Type: 0},
		{Value:"d", Depth:7, Type: 0},
		{Value:"d", Depth:6, Type: 0},
		{Value:"123456", Depth:5, Type: 0},
		{Value:"eq", Depth:3, Type: 0},
		{Value:"concat", Depth:4, Type: 0},
		{Value:"/", Depth:5, Type: 0},
		{Value:"d", Depth:6, Type: 0},
		{Value:"FirstName", Depth:6, Type: 0},
		{Value:"/", Depth:5, Type: 0},
		{Value:"d", Depth:6, Type: 0},
		{Value:"LastName", Depth:6, Type: 0},
		{Value:"/", Depth:4, Type: 0},
		{Value:"$it", Depth:5, Type: 0},
		{Value:"FullName", Depth:5, Type: 0},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		fmt.Printf("Got tree:\n%v\n", q.Tree.String())
		t.Errorf("Tree representation does not match expected value. error: %s", err.Error())
	}
}
*/

func TestFilterAnyMixedQuery(t *testing.T) {
	/*
		{
			"Tags": [
				"Site",
				{ "Key": "Environment" },
				{ "d" : { "d": 123456 }},
				{ "FirstName" : "Bob", "LastName": "Smith"}
			],
			"FullName": "BobSmith"
		}
	*/
	// The argument of a lambda operator is a case-sensitive lambda variable name followed by a colon (:) and a Boolean expression that
	// uses the lambda variable name to refer to properties of members of the collection identified by the navigation path.
	// If the name chosen for the lambda variable matches a property name of the current resource referenced by the resource path, the lambda variable takes precedence.
	// Clients can prefix properties of the current resource referenced by the resource path with $it.
	// Other path expressions in the Boolean expression neither prefixed with the lambda variable nor $it are evaluated in the scope of
	// the collection instances at the origin of the navigation path prepended to the lambda operator.
	input := "Tags/any(d:d eq 'Site' or 'Environment' eq d/Key or d/d/d eq 123456 or concat(d/FirstName, d/LastName) eq $it/FullName)"
	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %v", input, err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "d", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "or", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "or", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "or", Depth: 4, Type: ExpressionTokenLogical},
		{Value: "eq", Depth: 5, Type: ExpressionTokenLogical},
		{Value: "d", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "'Site'", Depth: 6, Type: ExpressionTokenString},
		{Value: "eq", Depth: 5, Type: ExpressionTokenLogical},
		{Value: "'Environment'", Depth: 6, Type: ExpressionTokenString},
		{Value: "/", Depth: 6, Type: ExpressionTokenNav},
		{Value: "d", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "Key", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "eq", Depth: 4, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 5, Type: ExpressionTokenNav},
		{Value: "/", Depth: 6, Type: ExpressionTokenNav},
		{Value: "d", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "d", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "d", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "123456", Depth: 5, Type: ExpressionTokenInteger},
		{Value: "eq", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "concat", Depth: 4, Type: ExpressionTokenFunc},
		{Value: "/", Depth: 5, Type: ExpressionTokenNav},
		{Value: "d", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "FirstName", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "/", Depth: 5, Type: ExpressionTokenNav},
		{Value: "d", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "LastName", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "/", Depth: 4, Type: ExpressionTokenNav},
		{Value: "$it", Depth: 5, Type: ExpressionTokenIt},
		{Value: "FullName", Depth: 5, Type: ExpressionTokenLiteral},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, q.Tree)
	}
}

func TestFilterGuid(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "GuidValue eq 01234567-89ab-cdef-0123-456789abcdef"

	expect := []*Token{
		{Value: "GuidValue", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "01234567-89ab-cdef-0123-456789abcdef", Type: ExpressionTokenGuid},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}

func TestFilterDurationWithType(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "Task eq duration'P12DT23H59M59.999999999999S'"

	expect := []*Token{
		{Value: "Task", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		// Note the duration token is extracted.
		{Value: "P12DT23H59M59.999999999999S", Type: ExpressionTokenDuration},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, output)
	if !result {
		printTokens(output)
		t.Error(err)
	}
}

func TestFilterDurationWithoutType(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "Task eq 'P12DT23H59M59.999999999999S'"

	expect := []*Token{
		{Value: "Task", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "P12DT23H59M59.999999999999S", Type: ExpressionTokenDuration},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, output)
	if !result {
		printTokens(output)
		t.Error(err)
	}
}

func TestFilterAnyWithNoArgs(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "Tags/any()"
	{
		expect := []*Token{
			{Value: "Tags", Type: ExpressionTokenLiteral},
			{Value: "/", Type: ExpressionTokenLambdaNav},
			{Value: "any", Type: ExpressionTokenLambda},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: ")", Type: ExpressionTokenCloseParen},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}

		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %v", input, err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, q.Tree)
	}
}
func TestFilterDivby(t *testing.T) {
	{
		tokenizer := NewExpressionTokenizer()
		input := "Price div 2 gt 3.5"
		expect := []*Token{
			{Value: "Price", Type: ExpressionTokenLiteral},
			{Value: "div", Type: ExpressionTokenOp},
			{Value: "2", Type: ExpressionTokenInteger},
			{Value: "gt", Type: ExpressionTokenLogical},
			{Value: "3.5", Type: ExpressionTokenFloat},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}

		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	{
		tokenizer := NewExpressionTokenizer()
		input := "Price divby 2 gt 3.5"
		expect := []*Token{
			{Value: "Price", Type: ExpressionTokenLiteral},
			{Value: "divby", Type: ExpressionTokenOp},
			{Value: "2", Type: ExpressionTokenInteger},
			{Value: "gt", Type: ExpressionTokenLogical},
			{Value: "3.5", Type: ExpressionTokenFloat},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}

		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
}

func TestFilterNotBooleanProperty(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "not Enabled"
	{
		expect := []*Token{
			{Value: "not", Type: ExpressionTokenLogical},
			{Value: "Enabled", Type: ExpressionTokenLiteral},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}
		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %v", input, err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "Enabled", Depth: 1, Type: ExpressionTokenLiteral},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, q.Tree)
	}

}

func TestFilterEmptyStringToken(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "City eq ''"
	expect := []*Token{
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "''", Type: ExpressionTokenString},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}

// Note: according to ODATA ABNF notation, there must be a space between not and open parenthesis.
// http://docs.oasis-open.org/odata/odata/v4.01/csprd03/abnf/odata-abnf-construction-rules.txt
func TestFilterNotWithNoSpace(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "not(City eq 'Seattle')"
	{
		expect := []*Token{
			{Value: "not", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}
		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}

	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %v", input, err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
		{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, q.Tree)
	}
}

// TestFilterInOperator tests the "IN" operator with a comma-separated list of values.
func TestFilterInOperator(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "City in ( 'Seattle', 'Atlanta', 'Paris' )"

	expect := []*Token{
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "in", Type: ExpressionTokenLogical},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "'Seattle'", Type: ExpressionTokenString},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'Atlanta'", Type: ExpressionTokenString},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'Paris'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	tokens, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, tokens)
	if !result {
		t.Error(err)
	}
	var postfix *tokenQueue
	postfix, err = GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
	}
	expect = []*Token{
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "'Seattle'", Type: ExpressionTokenString},
		{Value: "'Atlanta'", Type: ExpressionTokenString},
		{Value: "'Paris'", Type: ExpressionTokenString},
		{Value: "3", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},
		{Value: "in", Type: ExpressionTokenLogical},
	}
	result, err = CompareQueue(expect, postfix)
	if !result {
		t.Error(err)
	}

	tree, err := GlobalFilterParser.PostfixToTree(postfix)
	if err != nil {
		t.Error(err)
	}

	var treeExpect []expectedParseNode = []expectedParseNode{
		{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
		{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
		{Value: "'Atlanta'", Depth: 2, Type: ExpressionTokenString},
		{Value: "'Paris'", Depth: 2, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, treeExpect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

// TestFilterInOperatorSingleValue tests the "IN" operator with a list containing a single value.
func TestFilterInOperatorSingleValue(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "City in ( 'Seattle' )"

	expect := []*Token{
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "in", Type: ExpressionTokenLogical},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "'Seattle'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	tokens, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, tokens)
	if !result {
		t.Error(err)
	}
	var postfix *tokenQueue
	postfix, err = GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
	}
	expect = []*Token{
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "'Seattle'", Type: ExpressionTokenString},
		{Value: "1", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},
		{Value: "in", Type: ExpressionTokenLogical},
	}
	result, err = CompareQueue(expect, postfix)
	if !result {
		t.Error(err)
	}

	tree, err := GlobalFilterParser.PostfixToTree(postfix)
	if err != nil {
		t.Error(err)
	}

	var treeExpect []expectedParseNode = []expectedParseNode{
		{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
		{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, treeExpect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

// TestFilterInOperatorEmptyList tests the "IN" operator with a list containing no value.
func TestFilterInOperatorEmptyList(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "City in ( )"

	expect := []*Token{
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "in", Type: ExpressionTokenLogical},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	tokens, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, tokens)
	if !result {
		t.Error(err)
	}
	var postfix *tokenQueue
	postfix, err = GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
	}
	expect = []*Token{
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "0", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},
		{Value: "in", Type: ExpressionTokenLogical},
	}
	result, err = CompareQueue(expect, postfix)
	if !result {
		t.Error(err)
	}

	tree, err := GlobalFilterParser.PostfixToTree(postfix)
	if err != nil {
		t.Error(err)
	}

	var treeExpect []expectedParseNode = []expectedParseNode{
		{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
	}
	pos := 0
	err = CompareTree(tree, treeExpect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

// TestFilterInOperatorBothSides tests the "IN" operator.
// Use a listExpr on both sides of the IN operator.
//   listExpr  = OPEN BWS commonExpr BWS *( COMMA BWS commonExpr BWS ) CLOSE
// Validate if a list is within another list.
func TestFilterInOperatorBothSides(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "(1, 2) in ( ('ab', 'cd'), (1, 2), ('abc', 'def') )"

	expect := []*Token{
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "1", Type: ExpressionTokenInteger},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "2", Type: ExpressionTokenInteger},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: "in", Type: ExpressionTokenLogical},
		{Value: "(", Type: ExpressionTokenOpenParen},

		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "'ab'", Type: ExpressionTokenString},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'cd'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: ",", Type: ExpressionTokenComma},

		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "1", Type: ExpressionTokenInteger},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "2", Type: ExpressionTokenInteger},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: ",", Type: ExpressionTokenComma},

		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "'abc'", Type: ExpressionTokenString},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'def'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	tokens, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}
	result, err := CompareTokens(expect, tokens)
	if !result {
		t.Error(err)
	}
	var postfix *tokenQueue
	postfix, err = GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
	}
	expect = []*Token{
		{Value: "1", Type: ExpressionTokenInteger},
		{Value: "2", Type: ExpressionTokenInteger},
		{Value: "2", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},

		{Value: "'ab'", Type: ExpressionTokenString},
		{Value: "'cd'", Type: ExpressionTokenString},
		{Value: "2", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},

		{Value: "1", Type: ExpressionTokenInteger},
		{Value: "2", Type: ExpressionTokenInteger},
		{Value: "2", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},

		{Value: "'abc'", Type: ExpressionTokenString},
		{Value: "'def'", Type: ExpressionTokenString},
		{Value: "2", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},

		{Value: "3", Type: TokenTypeArgCount},
		{Value: TokenListExpr, Type: TokenTypeListExpr},

		{Value: "in", Type: ExpressionTokenLogical},
	}
	result, err = CompareQueue(expect, postfix)
	if !result {
		fmt.Printf("postfix notation: %s\n", postfix.String())
		t.Error(err)
	}

	tree, err := GlobalFilterParser.PostfixToTree(postfix)
	if err != nil {
		t.Error(err)
	}

	var treeExpect []expectedParseNode = []expectedParseNode{
		{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
		{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
		{Value: "1", Depth: 2, Type: ExpressionTokenInteger},
		{Value: "2", Depth: 2, Type: ExpressionTokenInteger},
		//  ('ab', 'cd'), (1, 2), ('abc', 'def')
		{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
		{Value: TokenListExpr, Depth: 2, Type: TokenTypeListExpr},
		{Value: "'ab'", Depth: 3, Type: ExpressionTokenString},
		{Value: "'cd'", Depth: 3, Type: ExpressionTokenString},
		{Value: TokenListExpr, Depth: 2, Type: TokenTypeListExpr},
		{Value: "1", Depth: 3, Type: ExpressionTokenInteger},
		{Value: "2", Depth: 3, Type: ExpressionTokenInteger},
		{Value: TokenListExpr, Depth: 2, Type: TokenTypeListExpr},
		{Value: "'abc'", Depth: 3, Type: ExpressionTokenString},
		{Value: "'def'", Depth: 3, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, treeExpect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

// TestFilterInOperatorWithFunc tests the "IN" operator with a comma-separated list
// of values, one of which is a function call which itself has a comma-separated list of values.
func TestFilterInOperatorWithFunc(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	// 'Atlanta' is enclosed in a unecessary parenExpr to validate the expression is properly unwrapped.
	input := "City in ( 'Seattle', concat('San', 'Francisco'), ('Atlanta') )"

	{
		expect := []*Token{
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "in", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "concat", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'San'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "'Francisco'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Atlanta'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: ")", Type: ExpressionTokenCloseParen},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}
		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	q, err := ParseFilterString(input)
	if err != nil {
		t.Fatalf("Error parsing filter: %v", err)
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
		{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
		{Value: "concat", Depth: 2, Type: ExpressionTokenFunc},
		{Value: "'San'", Depth: 3, Type: ExpressionTokenString},
		{Value: "'Francisco'", Depth: 3, Type: ExpressionTokenString},
		{Value: "'Atlanta'", Depth: 2, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, q.Tree)
	}
}

func TestFilterNotInListExpr(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "not ( City in ( 'Seattle', 'Atlanta' ) )"

	{
		expect := []*Token{
			{Value: "not", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "in", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "'Atlanta'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: ")", Type: ExpressionTokenCloseParen},
		}
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}
		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	{
		tokens, err := GlobalExpressionTokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
			return
		}
		output, err := GlobalFilterParser.InfixToPostfix(tokens)
		if err != nil {
			t.Error(err)
			return
		}

		tree, err := GlobalFilterParser.PostfixToTree(output)
		if err != nil {
			t.Error(err)
			return
		}
		var expect []expectedParseNode = []expectedParseNode{
			{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "in", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: TokenListExpr, Depth: 2, Type: TokenTypeListExpr},
			{Value: "'Seattle'", Depth: 3, Type: ExpressionTokenString},
			{Value: "'Atlanta'", Depth: 3, Type: ExpressionTokenString},
		}
		pos := 0
		err = CompareTree(tree, expect, &pos, 0)
		if err != nil {
			t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
		}

	}
}

func TestFilterAll(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "Tags/all(d:d/Key eq 'Site')"
	expect := []*Token{
		{Value: "Tags", Type: ExpressionTokenLiteral},
		{Value: "/", Type: ExpressionTokenLambdaNav},
		{Value: "all", Type: ExpressionTokenLambda},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "d", Type: ExpressionTokenLiteral},
		{Value: ",", Type: ExpressionTokenColon},
		{Value: "d", Type: ExpressionTokenLiteral},
		{Value: "/", Type: ExpressionTokenNav},
		{Value: "Key", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "'Site'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}

	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}

func TestExpressionTokenizer(t *testing.T) {

	tokenizer := NewExpressionTokenizer()
	input := "Name eq 'Milk' and Price lt 2.55"
	expect := []*Token{
		{Value: "Name", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "'Milk'", Type: ExpressionTokenString},
		{Value: "and", Type: ExpressionTokenLogical},
		{Value: "Price", Type: ExpressionTokenLiteral},
		{Value: "lt", Type: ExpressionTokenLogical},
		{Value: "2.55", Type: ExpressionTokenFloat},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}

	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}

func TestFilterFunction(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	// The syntax for ODATA functions follows the inline parameter syntax. The function name must be followed
	// by an opening parenthesis, followed by a comma-separated list of parameters, followed by a closing parenthesis.
	// For example:
	// GET serviceRoot/Airports?$filter=contains(Location/Address, 'San Francisco')
	input := "contains(LastName, 'Smith') and FirstName eq 'John' and City eq 'Houston'"
	expect := []*Token{
		{Value: "contains", Type: ExpressionTokenFunc},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "LastName", Type: ExpressionTokenLiteral},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'Smith'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: "and", Type: ExpressionTokenLogical},
		{Value: "FirstName", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "'John'", Type: ExpressionTokenString},
		{Value: "and", Type: ExpressionTokenLogical},
		{Value: "City", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "'Houston'", Type: ExpressionTokenString},
	}
	{
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}
		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	{
		tokens, err := GlobalExpressionTokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
			return
		}
		output, err := GlobalFilterParser.InfixToPostfix(tokens)
		if err != nil {
			t.Error(err)
			return
		}
		tree, err := GlobalFilterParser.PostfixToTree(output)
		if err != nil {
			t.Error(err)
			return
		}
		if tree.Token.Value != "and" {
			t.Errorf("Root is '%v', not 'and'", tree.Token.Value)
		}
		if len(tree.Children) != 2 {
			t.Errorf("Unexpected number of operators. Expected 2, got %d", len(tree.Children))
		}
		if tree.Children[0].Token.Value != "and" {
			t.Errorf("First child is '%v', not 'and'", tree.Children[0].Token.Value)
		}
		if len(tree.Children[0].Children) != 2 {
			t.Errorf("Unexpected number of operators. Expected 2, got %d", len(tree.Children))
		}
		if tree.Children[0].Children[0].Token.Value != "contains" {
			t.Errorf("First child is '%v', not 'contains'", tree.Children[0].Children[0].Token.Value)
		}
		if tree.Children[1].Token.Value != "eq" {
			t.Errorf("First child is '%v', not 'eq'", tree.Children[1].Token.Value)
		}
	}
}

func TestFilterNestedFunction(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	// Test ODATA syntax with nested function calls
	input := "contains(LastName, toupper('Smith')) or FirstName eq 'John'"
	expect := []*Token{
		{Value: "contains", Type: ExpressionTokenFunc},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "LastName", Type: ExpressionTokenLiteral},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "toupper", Type: ExpressionTokenFunc},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "'Smith'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: "or", Type: ExpressionTokenLogical},
		{Value: "FirstName", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "'John'", Type: ExpressionTokenString},
	}
	{
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}
		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	{
		tokens, err := GlobalExpressionTokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
			return
		}
		output, err := GlobalFilterParser.InfixToPostfix(tokens)
		if err != nil {
			t.Error(err)
			return
		}
		tree, err := GlobalFilterParser.PostfixToTree(output)
		if err != nil {
			t.Error(err)
			return
		}
		if tree.Token.Value != "or" {
			t.Errorf("Root is '%v', not 'or'", tree.Token.Value)
		}
		if len(tree.Children) != 2 {
			t.Errorf("Unexpected number of operators. Expected 2, got %d", len(tree.Children))
		}
		if tree.Children[0].Token.Value != "contains" {
			t.Errorf("First child is '%v', not 'contains'", tree.Children[0].Token.Value)
		}
		if len(tree.Children[0].Children) != 2 {
			t.Errorf("Unexpected number of nested children. Expected 2, got %d", len(tree.Children[0].Children))
		}
		if tree.Children[0].Children[1].Token.Value != "toupper" {
			t.Errorf("First child is '%v', not 'toupper'", tree.Children[0].Children[1].Token.Value)
		}
		if tree.Children[1].Token.Value != "eq" {
			t.Errorf("First child is '%v', not 'eq'", tree.Children[1].Token.Value)
		}
	}
}

func TestValidFilterSyntax(t *testing.T) {
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
	for _, input := range queries {
		q, err := ParseFilterString(input)
		if err != nil {
			t.Errorf("Error parsing query %s. Error: %v", input, err)
			return
		} else if q.Tree == nil {
			t.Errorf("Error parsing query %s. Tree is nil", input)
		}
		if q.Tree.Token == nil {
			t.Errorf("Error parsing query %s. Root token is nil", input)
		}
		if q.Tree.Token.Type == ExpressionTokenLiteral {
			t.Errorf("Error parsing query %s. Unexpected root token type: %+v", input, q.Tree.Token)
		}
		//printTree(q.Tree)
	}
}

// The URLs below are not valid ODATA syntax, the parser should return an error.
func TestInvalidFilterSyntax(t *testing.T) {
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
		"City",                                 // Just a single literal
		"City City",                            // Sequence of two literals
		"City City City City",                  // Sequence of literals
		"eq eq",                                // Two consecutive operators
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
	for _, input := range queries {
		q, err := ParseFilterString(input)
		if err == nil {
			// The parser has incorrectly determined the syntax is valid.
			t.Errorf("The query '$filter=%s' is not valid ODATA syntax. The ODATA parser should return an error. Tree:\n%v", input, q.Tree)
			return
		}
	}
}

// See http://docs.oasis-open.org/odata/odata/v4.01/csprd02/part1-protocol/odata-v4.01-csprd02-part1-protocol.html#_Toc486263411
// Test 'in', which is the 'Is a member of' operator.
func TestFilterIn(t *testing.T) {
	tokenizer := NewExpressionTokenizer()
	input := "contains(LastName, 'Smith') and Site in ('London', 'Paris', 'San Francisco', 'Dallas') and FirstName eq 'John'"
	expect := []*Token{
		{Value: "contains", Type: ExpressionTokenFunc},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "LastName", Type: ExpressionTokenLiteral},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'Smith'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: "and", Type: ExpressionTokenLogical},
		{Value: "Site", Type: ExpressionTokenLiteral},
		{Value: "in", Type: ExpressionTokenLogical},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "'London'", Type: ExpressionTokenString},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'Paris'", Type: ExpressionTokenString},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'San Francisco'", Type: ExpressionTokenString},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'Dallas'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
		{Value: "and", Type: ExpressionTokenLogical},
		{Value: "FirstName", Type: ExpressionTokenLiteral},
		{Value: "eq", Type: ExpressionTokenLogical},
		{Value: "'John'", Type: ExpressionTokenString},
	}
	{
		output, err := tokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
		}
		result, err := CompareTokens(expect, output)
		if !result {
			t.Error(err)
		}
	}
	{
		tokens, err := GlobalExpressionTokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
			return
		}
		output, err := GlobalFilterParser.InfixToPostfix(tokens)
		if err != nil {
			t.Error(err)
			return
		}
		tree, err := GlobalFilterParser.PostfixToTree(output)
		if err != nil {
			t.Error(err)
			return
		}

		/*
		  The expected tree is:
		  and        6
		   and        6
		     contains   8
		       LastName   20
		       'Smith'    15
		     in         6
		       Site       20
		       (          0
		         'London'   15
		         'Paris'    15
		         'San Francisco' 15
		         'Dallas'   15
		   eq         6
		     FirstName  20
		     'John'     15

		*/
		if tree.Token.Value != "and" {
			t.Errorf("Root is '%v', not 'and'", tree.Token.Value)
		}
		if len(tree.Children) != 2 {
			t.Errorf("Unexpected number of operators. Expected 2, got %d", len(tree.Children))
		}
		if tree.Children[0].Token.Value != "and" {
			t.Errorf("First child is '%v', not 'and'", tree.Children[0].Token.Value)
		}
		if len(tree.Children[0].Children) != 2 {
			t.Errorf("Unexpected number of operators. Expected 2, got %d", len(tree.Children))
		}
		if tree.Children[0].Children[0].Token.Value != "contains" {
			t.Errorf("First child is '%v', not 'contains'", tree.Children[0].Children[0].Token.Value)
		}
		if tree.Children[0].Children[1].Token.Value != "in" {
			t.Errorf("First child is '%v', not 'in'", tree.Children[0].Children[1].Token.Value)
		}
		if len(tree.Children[0].Children[1].Children) != 2 {
			t.Errorf("Unexpected number of operands for the 'in' operator. Expected 2, got %d",
				len(tree.Children[0].Children[1].Children))
		}
		if tree.Children[0].Children[1].Children[0].Token.Value != "Site" {
			t.Errorf("Unexpected operand for the 'in' operator. Expected 'Site', got %s",
				tree.Children[0].Children[1].Children[0].Token.Value)
		}
		if tree.Children[0].Children[1].Children[1].Token.Value != TokenListExpr {
			t.Errorf("Unexpected operand for the 'in' operator. Expected 'list', got %s",
				tree.Children[0].Children[1].Children[1].Token.Value)
		}
		if len(tree.Children[0].Children[1].Children[1].Children) != 4 {
			t.Errorf("Unexpected number of operands for the 'in' operator. Expected 4, got %d",
				len(tree.Children[0].Children[1].Children[1].Token.Value))
		}
		if tree.Children[1].Token.Value != "eq" {
			t.Errorf("First child is '%v', not 'eq'", tree.Children[1].Token.Value)
		}
	}
}

func TestExpressionTokenizerFunc(t *testing.T) {

	tokenizer := NewExpressionTokenizer()
	input := "not endswith(Name,'ilk')"
	expect := []*Token{
		{Value: "not", Type: ExpressionTokenLogical},
		{Value: "endswith", Type: ExpressionTokenFunc},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "Name", Type: ExpressionTokenLiteral},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "'ilk'", Type: ExpressionTokenString},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}

	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}

func BenchmarkFilterTokenizer(b *testing.B) {
	t := NewExpressionTokenizer()
	for i := 0; i < b.N; i++ {
		input := "Name eq 'Milk' and Price lt 2.55"
		if _, err := t.Tokenize(input); err != nil {
			b.Fatalf("Failed to tokenize filter: %v", err)
		}
	}
}

func TestFilterParserTree(t *testing.T) {

	input := "not (A eq B)"

	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)

	if err != nil {
		t.Error(err)
		return
	}

	tree, err := GlobalFilterParser.PostfixToTree(output)

	if err != nil {
		t.Error(err)
		return
	}

	if tree.Token.Value != "not" {
		t.Error("Root is '" + tree.Token.Value + "' not 'not'")
	}
	if tree.Children[0].Token.Value != "eq" {
		t.Error("First child is '" + tree.Children[1].Token.Value + "' not 'eq'")
	}

}

func TestFilterNestedPath(t *testing.T) {
	input := "Address/City eq 'Redmond'"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}

	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}

	var expect []expectedParseNode = []expectedParseNode{
		{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 1, Type: ExpressionTokenNav},
		{Value: "Address", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "'Redmond'", Depth: 1, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterMultipleNestedPath(t *testing.T) {
	input := "Product/Address/City eq 'Redmond'"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}

	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}

	var expect []expectedParseNode = []expectedParseNode{
		{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 1, Type: ExpressionTokenNav},
		{Value: "/", Depth: 2, Type: ExpressionTokenNav},
		{Value: "Product", Depth: 3, Type: ExpressionTokenLiteral},
		{Value: "Address", Depth: 3, Type: ExpressionTokenLiteral},
		{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "'Redmond'", Depth: 1, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterSubstringFunction(t *testing.T) {
	// substring can take 2 or 3 arguments.
	{
		input := "substring(CompanyName,1) eq 'Foo'"
		tokens, err := GlobalExpressionTokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
			return
		}
		output, err := GlobalFilterParser.InfixToPostfix(tokens)
		if err != nil {
			t.Error(err)
			return
		}
		tree, err := GlobalFilterParser.PostfixToTree(output)
		if err != nil {
			t.Error(err)
			return
		}
		var expect []expectedParseNode = []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "substring", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "CompanyName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "1", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "'Foo'", Depth: 1, Type: ExpressionTokenString},
		}
		pos := 0
		err = CompareTree(tree, expect, &pos, 0)
		if err != nil {
			t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
		}
	}
	{
		input := "substring(CompanyName,1,2) eq 'lf'"
		tokens, err := GlobalExpressionTokenizer.Tokenize(input)
		if err != nil {
			t.Error(err)
			return
		}
		output, err := GlobalFilterParser.InfixToPostfix(tokens)
		if err != nil {
			t.Error(err)
			return
		}
		tree, err := GlobalFilterParser.PostfixToTree(output)
		if err != nil {
			t.Error(err)
			return
		}
		var expect []expectedParseNode = []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "substring", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "CompanyName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "1", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "2", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "'lf'", Depth: 1, Type: ExpressionTokenString},
		}
		pos := 0
		err = CompareTree(tree, expect, &pos, 0)
		if err != nil {
			t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
		}
	}
}

func TestFilterSubstringofFunction(t *testing.T) {
	// Previously, the parser was incorrectly interpreting the 'substringof' function as the 'sub' operator.
	input := "substringof('Alfreds', CompanyName) eq true"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	{
		expect := []*Token{
			{Value: "substringof", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Alfreds'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "CompanyName", Type: ExpressionTokenLiteral},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "true", Type: ExpressionTokenBoolean},
		}
		result, err := CompareTokens(expect, tokens)
		if !result {
			t.Error(err)
		}
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}
	{
		expect := []*Token{
			{Value: "'Alfreds'", Type: ExpressionTokenString},
			{Value: "CompanyName", Type: ExpressionTokenLiteral},
			{Value: "2", Type: TokenTypeArgCount}, // The number of function arguments.
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "substringof", Type: ExpressionTokenFunc},
			{Value: "true", Type: ExpressionTokenBoolean},
			{Value: "eq", Type: ExpressionTokenLogical},
		}
		result, err := CompareQueue(expect, output)
		if !result {
			t.Error(err)
		}
	}
	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "substringof", Depth: 1, Type: ExpressionTokenFunc},
		{Value: "'Alfreds'", Depth: 2, Type: ExpressionTokenString},
		{Value: "CompanyName", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "true", Depth: 1, Type: ExpressionTokenBoolean},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

// TestSubstringNestedFunction tests the substring function with a nested call
// to substring, with the use of 2-argument and 3-argument substring.
func TestFilterSubstringNestedFunction(t *testing.T) {
	// Previously, the parser was incorrectly interpreting the 'substringof' function as the 'sub' operator.
	input := "substring(substring('Francisco', 1), 3, 2) eq 'ci'"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	{
		expect := []*Token{
			{Value: "substring", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "substring", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Francisco'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "1", Type: ExpressionTokenInteger},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "3", Type: ExpressionTokenInteger},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "2", Type: ExpressionTokenInteger},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "'ci'", Type: ExpressionTokenString},
		}
		result, err := CompareTokens(expect, tokens)
		if !result {
			t.Error(err)
		}
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}
	{
		expect := []*Token{
			{Value: "'Francisco'", Type: ExpressionTokenString},
			{Value: "1", Type: ExpressionTokenInteger},
			{Value: "2", Type: TokenTypeArgCount}, // The number of function arguments.
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "substring", Type: ExpressionTokenFunc},
			{Value: "3", Type: ExpressionTokenInteger},
			{Value: "2", Type: ExpressionTokenInteger},
			{Value: "3", Type: TokenTypeArgCount}, // The number of function arguments.
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "substring", Type: ExpressionTokenFunc},
			{Value: "'ci'", Type: ExpressionTokenString},
			{Value: "eq", Type: ExpressionTokenLogical},
		}
		result, err := CompareQueue(expect, output)
		if !result {
			t.Error(err)
		}
	}
	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
		{Value: "substring", Depth: 1, Type: ExpressionTokenFunc},
		{Value: "substring", Depth: 2, Type: ExpressionTokenFunc},
		{Value: "'Francisco'", Depth: 3, Type: ExpressionTokenString},
		{Value: "1", Depth: 3, Type: ExpressionTokenInteger},
		{Value: "3", Depth: 2, Type: ExpressionTokenInteger},
		{Value: "2", Depth: 2, Type: ExpressionTokenInteger},
		{Value: "'ci'", Depth: 1, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}
func TestFilterGeoFunctions(t *testing.T) {
	// Previously, the parser was incorrectly interpreting the 'geo.xxx' functions as the 'ge' operator.
	input := "geo.distance(CurrentPosition,TargetPosition)"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}
	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "geo.distance", Depth: 0, Type: ExpressionTokenFunc},
		{Value: "CurrentPosition", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "TargetPosition", Depth: 1, Type: ExpressionTokenLiteral},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterLambdaAny(t *testing.T) {
	input := "Tags/any(var:var/Key eq 'Site')"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}
	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}

	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "var", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 3, Type: ExpressionTokenNav},
		{Value: "var", Depth: 4, Type: ExpressionTokenLiteral},
		{Value: "Key", Depth: 4, Type: ExpressionTokenLiteral},
		{Value: "'Site'", Depth: 3, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterLambdaAnyNot(t *testing.T) {
	input := "Price/any(t:not (12345 eq t ))"

	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}
	{
		expect := []*Token{
			{Value: "Price", Type: ExpressionTokenLiteral},
			{Value: "t", Type: ExpressionTokenLiteral},
			{Value: "12345", Type: ExpressionTokenInteger},
			{Value: "t", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "not", Type: ExpressionTokenLogical},
			{Value: "2", Type: TokenTypeArgCount},
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "any", Type: ExpressionTokenLambda},
			{Value: "/", Type: ExpressionTokenLambdaNav},
		}
		var result bool
		result, err = CompareQueue(expect, output)
		if !result {
			t.Error(err)
		}
	}
	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Price", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "t", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "not", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "eq", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "12345", Depth: 4, Type: ExpressionTokenInteger},
		{Value: "t", Depth: 4, Type: ExpressionTokenLiteral},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterLambdaAnyAnd(t *testing.T) {
	input := "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London')"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}
	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}

	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "var", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "and", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "eq", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 4, Type: ExpressionTokenNav},
		{Value: "var", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "Key", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "'Site'", Depth: 4, Type: ExpressionTokenString},
		{Value: "eq", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 4, Type: ExpressionTokenNav},
		{Value: "var", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "Value", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "'London'", Depth: 4, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterLambdaNestedAny(t *testing.T) {
	input := "Enabled/any(t:t/Value eq Config/any(c:c/AdminState eq 'TRUE'))"
	q, err := ParseFilterString(input)
	if err != nil {
		t.Errorf("Error parsing query %s. Error: %v", input, err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Enabled", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "t", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 3, Type: ExpressionTokenNav},
		{Value: "t", Depth: 4, Type: ExpressionTokenLiteral},
		{Value: "Value", Depth: 4, Type: ExpressionTokenLiteral},
		{Value: "/", Depth: 3, Type: ExpressionTokenLambdaNav},
		{Value: "Config", Depth: 4, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 4, Type: ExpressionTokenLambda},
		{Value: "c", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "eq", Depth: 5, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 6, Type: ExpressionTokenNav},
		{Value: "c", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "AdminState", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "'TRUE'", Depth: 6, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(q.Tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, q.Tree)
	}
}

// TestLambdaAnyNested validates the any() lambda function with multiple nested properties.
func TestFilterLambdaAnyNestedProperties(t *testing.T) {
	input := "Config/any(var:var/Config/Priority eq 123)"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}
	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}

	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Config", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "var", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 3, Type: ExpressionTokenNav},
		{Value: "/", Depth: 4, Type: ExpressionTokenNav},
		{Value: "var", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "Config", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "Priority", Depth: 4, Type: ExpressionTokenLiteral},
		{Value: "123", Depth: 3, Type: ExpressionTokenInteger},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterLambda2(t *testing.T) {
	input := "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London' or Price gt 1.0)"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}

	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "var", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "or", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "and", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "eq", Depth: 4, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 5, Type: ExpressionTokenNav},
		{Value: "var", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "Key", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "'Site'", Depth: 5, Type: ExpressionTokenString},
		{Value: "eq", Depth: 4, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 5, Type: ExpressionTokenNav},
		{Value: "var", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "Value", Depth: 6, Type: ExpressionTokenLiteral},
		{Value: "'London'", Depth: 5, Type: ExpressionTokenString},
		{Value: "gt", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "Price", Depth: 4, Type: ExpressionTokenLiteral},
		{Value: "1.0", Depth: 4, Type: ExpressionTokenFloat},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestFilterLambda3(t *testing.T) {
	input := "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London' or Price gt 1.0 or contains(var/Value, 'Smith'))"
	tokens, err := GlobalExpressionTokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
		return
	}
	output, err := GlobalFilterParser.InfixToPostfix(tokens)
	if err != nil {
		t.Error(err)
		return
	}

	tree, err := GlobalFilterParser.PostfixToTree(output)
	if err != nil {
		t.Error(err)
		return
	}
	var expect []expectedParseNode = []expectedParseNode{
		{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
		{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
		{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		{Value: "var", Depth: 2, Type: ExpressionTokenLiteral},
		{Value: "or", Depth: 2, Type: ExpressionTokenLogical},
		{Value: "or", Depth: 3, Type: ExpressionTokenLogical},
		{Value: "and", Depth: 4, Type: ExpressionTokenLogical},
		{Value: "eq", Depth: 5, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 6, Type: ExpressionTokenNav},
		{Value: "var", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "Key", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "'Site'", Depth: 6, Type: ExpressionTokenString},
		{Value: "eq", Depth: 5, Type: ExpressionTokenLogical},
		{Value: "/", Depth: 6, Type: ExpressionTokenNav},
		{Value: "var", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "Value", Depth: 7, Type: ExpressionTokenLiteral},
		{Value: "'London'", Depth: 6, Type: ExpressionTokenString},
		{Value: "gt", Depth: 4, Type: ExpressionTokenLogical},
		{Value: "Price", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "1.0", Depth: 5, Type: ExpressionTokenFloat},
		{Value: "contains", Depth: 3, Type: ExpressionTokenFunc},
		{Value: "/", Depth: 4, Type: ExpressionTokenNav},
		{Value: "var", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "Value", Depth: 5, Type: ExpressionTokenLiteral},
		{Value: "'Smith'", Depth: 4, Type: ExpressionTokenString},
	}
	pos := 0
	err = CompareTree(tree, expect, &pos, 0)
	if err != nil {
		t.Errorf("Tree representation does not match expected value. error: %v. Tree:\n%v", err, tree)
	}
}

func TestExpressionTokenizerExists(t *testing.T) {

	tokenizer := NewExpressionTokenizer()
	input := "exists(Name,false)"
	expect := []*Token{
		{Value: "exists", Type: ExpressionTokenFunc},
		{Value: "(", Type: ExpressionTokenOpenParen},
		{Value: "Name", Type: ExpressionTokenLiteral},
		{Value: ",", Type: ExpressionTokenComma},
		{Value: "false", Type: ExpressionTokenBoolean},
		{Value: ")", Type: ExpressionTokenCloseParen},
	}
	output, err := tokenizer.Tokenize(input)
	if err != nil {
		t.Error(err)
	}

	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}
