package godata

type expectedParseNode struct {
	Value       string    // The expected token value.
	Type        TokenType // The expected token type.
	Depth       int       // The expected tree depth.
	BooleanExpr bool      // True if this is expected to be a boolean expression.
}

var testCases = []struct {
	expression    string
	infixTokens   []*Token            // The expected tokens.
	postfixTokens []*Token            // The expected infix tokens.
	tree          []expectedParseNode // The expected tree.
}{
	{
		expression: "fractionalseconds(StartTime) lt 0.123456",
		tree: []expectedParseNode{
			{Value: "lt", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "fractionalseconds", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "StartTime", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "0.123456", Depth: 1, Type: ExpressionTokenFloat},
		},
	},
	{
		// Test precedence. 'and' has higher precedence compared to 'or'.
		expression: "a or b and c", // same as a or (b and c)
		tree: []expectedParseNode{
			{Value: "or", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "a", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "and", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "b", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "c", Depth: 2, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Same expression as above, with explicit parenthesis. The result should be the same
		expression: "a or (b and c)",
		tree: []expectedParseNode{
			{Value: "or", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "a", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "and", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "b", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "c", Depth: 2, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Validate precedence between 'and', 'or'.
		expression: "a and b or c",
		tree: []expectedParseNode{
			{Value: "or", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "and", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "a", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "b", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "c", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Validate precedence between assignment and 'or'.
		expression: "a=b or c",
		tree: []expectedParseNode{
			{Value: "=", Depth: 0, Type: ExpressionTokenAssignement},
			{Value: "a", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "or", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "b", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "c", Depth: 2, Type: ExpressionTokenLiteral},
		},
	},
	{
		expression: "Address/City eq 'Redmond'",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "/", Depth: 1, Type: ExpressionTokenNav},
			{Value: "Address", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'Redmond'", Depth: 1, Type: ExpressionTokenString},
		},
	},
	{
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
		expression: "Tags/any(d:d eq 'Site' or 'Environment' eq d/Key or d/d/d eq 123456 or concat(d/FirstName, d/LastName) eq $it/FullName)",
		tree: []expectedParseNode{
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
		},
	},
	{
		// matches documents where any of the geo coordinates in the locations field is within the given polygon.
		expression: "locations/any(loc: geo.intersects(loc, geography'SRID=0;Polygon((-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581))'))",
		infixTokens: []*Token{
			{Value: "locations", Type: ExpressionTokenLiteral},
			{Value: "/", Type: ExpressionTokenLambdaNav},
			{Value: "any", Type: ExpressionTokenLambda},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "loc", Type: ExpressionTokenLiteral},
			{Value: ",", Type: ExpressionTokenColon}, // TODO: this should be a colon (?)
			{Value: "geo.intersects", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "loc", Type: ExpressionTokenLiteral},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "geography'SRID=0;Polygon((-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581))'", Type: ExpressionTokenGeographyPolygon},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},

		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
			{Value: "locations", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
			{Value: "loc", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "geo.intersects", Depth: 2, Type: ExpressionTokenFunc},
			{Value: "loc", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "geography'SRID=0;Polygon((-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581))'", Depth: 3, Type: ExpressionTokenGeographyPolygon},
		},
	},
	{
		// geographyPolygon   = geographyPrefix SQUOTE fullPolygonLiteral SQUOTE
		// geographyPrefix = "geography"
		// fullPolygonLiteral = sridLiteral polygonLiteral
		// sridLiteral      = "SRID" EQ 1*5DIGIT SEMI
		// polygonLiteral     = "Polygon" polygonData
		// polygonData        = OPEN ringLiteral *( COMMA ringLiteral ) CLOSE
		// positionLiteral  = doubleValue SP doubleValue  ; longitude, then latitude
		expression: "geo.intersects(location, geometry'SRID=123;Polygon((-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581))')",
		tree: []expectedParseNode{
			{Value: "geo.intersects", Depth: 0, Type: ExpressionTokenFunc},
			{Value: "location", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "geometry'SRID=123;Polygon((-122.031577 47.578581, -122.031577 47.678581, -122.131577 47.678581, -122.031577 47.578581))'", Depth: 1, Type: ExpressionTokenGeometryPolygon},
		},
	},
	{
		expression: "Tags/any(d:d/Key eq 'Site' and d/Value lt 10)",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
			{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
			{Value: "d", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "and", Depth: 2, Type: ExpressionTokenLogical},
			{Value: "eq", Depth: 3, Type: ExpressionTokenLogical},
			{Value: "/", Depth: 4, Type: ExpressionTokenNav},
			{Value: "d", Depth: 5, Type: ExpressionTokenLiteral},
			{Value: "Key", Depth: 5, Type: ExpressionTokenLiteral},
			{Value: "'Site'", Depth: 4, Type: ExpressionTokenString},
			{Value: "lt", Depth: 3, Type: ExpressionTokenLogical},
			{Value: "/", Depth: 4, Type: ExpressionTokenNav},
			{Value: "d", Depth: 5, Type: ExpressionTokenLiteral},
			{Value: "Value", Depth: 5, Type: ExpressionTokenLiteral},
			{Value: "10", Depth: 4, Type: ExpressionTokenInteger},
		},
	},
	{
		expression: "City eq ''",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "''", Depth: 1, Type: ExpressionTokenString},
		},
	},
	{
		// TestExpressionInOperator tests the "IN" operator with a comma-separated list of values.
		expression: "City in ( 'Seattle', 'Atlanta', 'Paris' )",
		infixTokens: []*Token{
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "in", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "'Atlanta'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "'Paris'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		postfixTokens: []*Token{
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: "'Atlanta'", Type: ExpressionTokenString},
			{Value: "'Paris'", Type: ExpressionTokenString},
			{Value: "3", Type: TokenTypeArgCount},
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "in", Type: ExpressionTokenLogical},
		},
		tree: []expectedParseNode{
			{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
			{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
			{Value: "'Atlanta'", Depth: 2, Type: ExpressionTokenString},
			{Value: "'Paris'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		// TestExpressionInOperatorSingleValue tests the "IN" operator with a list containing a single value.
		expression: "City in ( 'Seattle' )",
		infixTokens: []*Token{
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "in", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		postfixTokens: []*Token{
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: "1", Type: TokenTypeArgCount},
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "in", Type: ExpressionTokenLogical},
		},
		tree: []expectedParseNode{
			{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
			{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		// TestExpressionInOperatorEmptyList tests the "IN" operator with a list containing no value.
		expression: "City in ( )",
		infixTokens: []*Token{
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "in", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		postfixTokens: []*Token{
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "0", Type: TokenTypeArgCount},
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "in", Type: ExpressionTokenLogical},
		},
		tree: []expectedParseNode{
			{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
		},
	},
	{
		// Note: according to ODATA ABNF notation, there must be a space between not and open parenthesis.
		// http://docs.oasis-open.org/odata/odata/v4.01/csprd03/abnf/odata-abnf-construction-rules.txt
		expression: "not(City eq 'Seattle')",
		infixTokens: []*Token{
			{Value: "not", Type: ExpressionTokenLogical},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "City", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "'Seattle'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		tree: []expectedParseNode{
			{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		// Not in list
		expression: "not ( City in ( 'Seattle', 'Atlanta' ) )",
		tree: []expectedParseNode{
			{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "in", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: TokenListExpr, Depth: 2, Type: TokenTypeListExpr},
			{Value: "'Seattle'", Depth: 3, Type: ExpressionTokenString},
			{Value: "'Atlanta'", Depth: 3, Type: ExpressionTokenString},
		},
	},
	{
		// tests the "IN" operator with a comma-separated list
		// of values, one of which is a function call which itself has a comma-separated list of values.
		// 'Atlanta' is enclosed in a unecessary parenExpr to validate the expression is properly unwrapped.
		expression: "City in ( 'Seattle', concat('San', 'Francisco'), ('Atlanta') )",
		infixTokens: []*Token{
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
		},
		tree: []expectedParseNode{
			{Value: "in", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: TokenListExpr, Depth: 1, Type: TokenTypeListExpr},
			{Value: "'Seattle'", Depth: 2, Type: ExpressionTokenString},
			{Value: "concat", Depth: 2, Type: ExpressionTokenFunc},
			{Value: "'San'", Depth: 3, Type: ExpressionTokenString},
			{Value: "'Francisco'", Depth: 3, Type: ExpressionTokenString},
			{Value: "'Atlanta'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		expression: "Tags/all(d:d/Key eq 'Site')",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
			{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "all", Depth: 1, Type: ExpressionTokenLambda},
			{Value: "d", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
			{Value: "/", Depth: 3, Type: ExpressionTokenNav},
			{Value: "d", Depth: 4, Type: ExpressionTokenLiteral},
			{Value: "Key", Depth: 4, Type: ExpressionTokenLiteral},
			{Value: "'Site'", Depth: 3, Type: ExpressionTokenString},
		},
	},
	{
		// substring can take 2 or 3 arguments.
		expression: "substring(CompanyName,1) eq 'Foo'",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "substring", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "CompanyName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "1", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "'Foo'", Depth: 1, Type: ExpressionTokenString},
		},
	},
	{
		expression: "substring(CompanyName,1,2) eq 'lf'",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "substring", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "CompanyName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "1", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "2", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "'lf'", Depth: 1, Type: ExpressionTokenString},
		},
	},
	{
		// Previously, the parser was incorrectly interpreting the 'geo.xxx' functions as the 'ge' operator.
		expression: "geo.distance(CurrentPosition,TargetPosition)",
		tree: []expectedParseNode{
			{Value: "geo.distance", Depth: 0, Type: ExpressionTokenFunc},
			{Value: "CurrentPosition", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "TargetPosition", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		expression: "Tags/any(var:var/Key eq 'Site')",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
			{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
			{Value: "var", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
			{Value: "/", Depth: 3, Type: ExpressionTokenNav},
			{Value: "var", Depth: 4, Type: ExpressionTokenLiteral},
			{Value: "Key", Depth: 4, Type: ExpressionTokenLiteral},
			{Value: "'Site'", Depth: 3, Type: ExpressionTokenString},
		},
	},
	{
		expression: "Price/any(t:not (12345 eq t ))",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
			{Value: "Price", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
			{Value: "t", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "not", Depth: 2, Type: ExpressionTokenLogical},
			{Value: "eq", Depth: 3, Type: ExpressionTokenLogical},
			{Value: "12345", Depth: 4, Type: ExpressionTokenInteger},
			{Value: "t", Depth: 4, Type: ExpressionTokenLiteral},
		},
	},
	{
		expression: "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London')",
		tree: []expectedParseNode{
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
		},
	},
	{
		expression: "Enabled/any(t:t/Value eq Config/any(c:c/AdminState eq 'TRUE'))",
		tree: []expectedParseNode{
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
		},
	},
	{
		// Validate the any() lambda function with multiple nested properties.
		expression: "Config/any(var:var/Config/Priority eq 123)",
		tree: []expectedParseNode{
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
		},
	},
	{
		expression: "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London' or Price gt 1.0)",
		tree: []expectedParseNode{
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
		},
	},
	{
		expression: "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London' or Price gt 1.0 or contains(var/Value, 'Smith'))",
		tree: []expectedParseNode{
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
		},
	},
	{
		expression: "Product/Address/City eq 'Redmond'",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "/", Depth: 1, Type: ExpressionTokenNav},
			{Value: "/", Depth: 2, Type: ExpressionTokenNav},
			{Value: "Product", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "Address", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'Redmond'", Depth: 1, Type: ExpressionTokenString},
		},
	},
	{
		// TestSubstringNestedFunction tests the substring function with a nested call
		// to substring, with the use of 2-argument and 3-argument substring.
		// Previously, the parser was incorrectly interpreting the 'substringof' function as the 'sub' operator.

		expression: "substring(substring('Francisco', 1), 3, 2) eq 'ci'",
		infixTokens: []*Token{
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
		},
		postfixTokens: []*Token{
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
		},
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "substring", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "substring", Depth: 2, Type: ExpressionTokenFunc},
			{Value: "'Francisco'", Depth: 3, Type: ExpressionTokenString},
			{Value: "1", Depth: 3, Type: ExpressionTokenInteger},
			{Value: "3", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "2", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "'ci'", Depth: 1, Type: ExpressionTokenString},
		},
	},
	{
		// Previously, the parser was incorrectly interpreting the 'substringof' function as the 'sub' operator.
		expression: "substringof('Alfreds', CompanyName) eq true",
		infixTokens: []*Token{
			{Value: "substringof", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "'Alfreds'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "CompanyName", Type: ExpressionTokenLiteral},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "true", Type: ExpressionTokenBoolean},
		},
		postfixTokens: []*Token{
			{Value: "'Alfreds'", Type: ExpressionTokenString},
			{Value: "CompanyName", Type: ExpressionTokenLiteral},
			{Value: "2", Type: TokenTypeArgCount}, // The number of function arguments.
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "substringof", Type: ExpressionTokenFunc},
			{Value: "true", Type: ExpressionTokenBoolean},
			{Value: "eq", Type: ExpressionTokenLogical},
		},
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "substringof", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "'Alfreds'", Depth: 2, Type: ExpressionTokenString},
			{Value: "CompanyName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "true", Depth: 1, Type: ExpressionTokenBoolean},
		},
	},
	{
		expression: "exists(Name,false)",
		infixTokens: []*Token{
			{Value: "exists", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "Name", Type: ExpressionTokenLiteral},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "false", Type: ExpressionTokenBoolean},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		tree: []expectedParseNode{
			{Value: "exists", Depth: 0, Type: ExpressionTokenFunc},
			{Value: "Name", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "false", Depth: 1, Type: ExpressionTokenBoolean},
		},
	},
	{
		expression: "not (A eq B)",
		tree: []expectedParseNode{
			{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "A", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "B", Depth: 2, Type: ExpressionTokenLiteral},
		},
	},
	{
		expression: "not endswith(Name,'ilk')",
		infixTokens: []*Token{
			{Value: "not", Type: ExpressionTokenLogical},
			{Value: "endswith", Type: ExpressionTokenFunc},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "Name", Type: ExpressionTokenLiteral},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "'ilk'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		tree: []expectedParseNode{
			{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "endswith", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "Name", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'ilk'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		// See http://docs.oasis-open.org/odata/odata/v4.01/csprd02/part1-protocol/odata-v4.01-csprd02-part1-protocol.html#_Toc486263411
		// Test 'in', which is the 'Is a member of' operator.
		expression: "contains(LastName, 'Smith') and Site in ('London', 'Paris', 'San Francisco', 'Dallas') and FirstName eq 'John'",
		infixTokens: []*Token{
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
		},
		tree: []expectedParseNode{
			{Value: "and", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "and", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "contains", Depth: 2, Type: ExpressionTokenFunc},
			{Value: "LastName", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "'Smith'", Depth: 3, Type: ExpressionTokenString},
			{Value: "in", Depth: 2, Type: ExpressionTokenLogical},
			{Value: "Site", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: TokenListExpr, Depth: 3, Type: TokenTypeListExpr},
			{Value: "'London'", Depth: 4, Type: ExpressionTokenString},
			{Value: "'Paris'", Depth: 4, Type: ExpressionTokenString},
			{Value: "'San Francisco'", Depth: 4, Type: ExpressionTokenString},
			{Value: "'Dallas'", Depth: 4, Type: ExpressionTokenString},
			{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "FirstName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'John'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		expression: "Tags/any(d:d eq 'Site')",
		infixTokens: []*Token{
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
		},
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
			{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
			{Value: "d", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
			{Value: "d", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "'Site'", Depth: 3, Type: ExpressionTokenString},
		},
	},
	{
		expression: "GuidValue eq 01234567-89ab-cdef-0123-456789abcdef",
		infixTokens: []*Token{
			{Value: "GuidValue", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "01234567-89ab-cdef-0123-456789abcdef", Type: ExpressionTokenGuid},
		},
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "GuidValue", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "01234567-89ab-cdef-0123-456789abcdef", Depth: 1, Type: ExpressionTokenGuid},
		},
	},
	{
		expression: "Task eq duration'P12DT23H59M59.999999999999S'",
		infixTokens: []*Token{
			{Value: "Task", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			// Note the duration token is extracted.
			{Value: "P12DT23H59M59.999999999999S", Type: ExpressionTokenDuration},
		},
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "Task", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "P12DT23H59M59.999999999999S", Depth: 1, Type: ExpressionTokenDuration},
		},
	},
	{
		expression: "Task eq 'P12DT23H59M59.999999999999S'",
		infixTokens: []*Token{
			{Value: "Task", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "P12DT23H59M59.999999999999S", Type: ExpressionTokenDuration},
		},
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "Task", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "P12DT23H59M59.999999999999S", Depth: 1, Type: ExpressionTokenDuration},
		},
	},
	{
		expression: "Tags/any()",
		infixTokens: []*Token{
			{Value: "Tags", Type: ExpressionTokenLiteral},
			{Value: "/", Type: ExpressionTokenLambdaNav},
			{Value: "any", Type: ExpressionTokenLambda},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenLambdaNav},
			{Value: "Tags", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "any", Depth: 1, Type: ExpressionTokenLambda},
		},
	},
	{
		expression: "Price div 2 gt 3.5",
		infixTokens: []*Token{
			{Value: "Price", Type: ExpressionTokenLiteral},
			{Value: "div", Type: ExpressionTokenOp},
			{Value: "2", Type: ExpressionTokenInteger},
			{Value: "gt", Type: ExpressionTokenLogical},
			{Value: "3.5", Type: ExpressionTokenFloat},
		},
		tree: []expectedParseNode{
			{Value: "gt", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "div", Depth: 1, Type: ExpressionTokenOp},
			{Value: "Price", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "2", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "3.5", Depth: 1, Type: ExpressionTokenFloat},
		},
	},
	{
		expression: "Price divby 2 gt 3.5",
		infixTokens: []*Token{
			{Value: "Price", Type: ExpressionTokenLiteral},
			{Value: "divby", Type: ExpressionTokenOp},
			{Value: "2", Type: ExpressionTokenInteger},
			{Value: "gt", Type: ExpressionTokenLogical},
			{Value: "3.5", Type: ExpressionTokenFloat},
		},
		tree: []expectedParseNode{
			{Value: "gt", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "divby", Depth: 1, Type: ExpressionTokenOp},
			{Value: "Price", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "2", Depth: 2, Type: ExpressionTokenInteger},
			{Value: "3.5", Depth: 1, Type: ExpressionTokenFloat},
		},
	},
	{
		expression: "not Enabled",
		infixTokens: []*Token{
			{Value: "not", Type: ExpressionTokenLogical},
			{Value: "Enabled", Type: ExpressionTokenLiteral},
		},
		tree: []expectedParseNode{
			{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "Enabled", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// TestExpressionInOperatorBothSides tests the "IN" operator.
		// Use a listExpr on both sides of the IN operator.
		//   listExpr  = OPEN BWS commonExpr BWS *( COMMA BWS commonExpr BWS ) CLOSE
		// Validate if a list is within another list.
		expression: "(1, 2) in ( ('ab', 'cd'), (1, 2), ('abcdefghijk', 'def') )",
		infixTokens: []*Token{
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
			{Value: "'abcdefghijk'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "'def'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: ")", Type: ExpressionTokenCloseParen},
		},
		postfixTokens: []*Token{
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

			{Value: "'abcdefghijk'", Type: ExpressionTokenString},
			{Value: "'def'", Type: ExpressionTokenString},
			{Value: "2", Type: TokenTypeArgCount},
			{Value: TokenListExpr, Type: TokenTypeListExpr},

			{Value: "3", Type: TokenTypeArgCount},
			{Value: TokenListExpr, Type: TokenTypeListExpr},

			{Value: "in", Type: ExpressionTokenLogical},
		},
		tree: []expectedParseNode{
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
			{Value: "'abcdefghijk'", Depth: 3, Type: ExpressionTokenString},
			{Value: "'def'", Depth: 3, Type: ExpressionTokenString},
		},
	},
	{
		// TestExpressionInOperatorBothSides tests the "IN" operator.
		// Use a listExpr on both sides of the IN operator.
		//   listExpr  = OPEN BWS commonExpr BWS *( COMMA BWS commonExpr BWS ) CLOSE
		// Validate if a list is within another list.
		expression: "Name eq 'Milk' and (1, 2) in ( ('ab', 'cd'), (1, 2), ('abc', 'def') )",
		tree: []expectedParseNode{
			{Value: "and", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "Name", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'Milk'", Depth: 2, Type: ExpressionTokenString},

			{Value: "in", Depth: 1, Type: ExpressionTokenLogical},
			{Value: TokenListExpr, Depth: 2, Type: TokenTypeListExpr},
			{Value: "1", Depth: 3, Type: ExpressionTokenInteger},
			{Value: "2", Depth: 3, Type: ExpressionTokenInteger},
			//  ('ab', 'cd'), (1, 2), ('abc', 'def')
			{Value: TokenListExpr, Depth: 2, Type: TokenTypeListExpr},
			{Value: TokenListExpr, Depth: 3, Type: TokenTypeListExpr},
			{Value: "'ab'", Depth: 4, Type: ExpressionTokenString},
			{Value: "'cd'", Depth: 4, Type: ExpressionTokenString},
			{Value: TokenListExpr, Depth: 3, Type: TokenTypeListExpr},
			{Value: "1", Depth: 4, Type: ExpressionTokenInteger},
			{Value: "2", Depth: 4, Type: ExpressionTokenInteger},
			{Value: TokenListExpr, Depth: 3, Type: TokenTypeListExpr},
			{Value: "'abc'", Depth: 4, Type: ExpressionTokenString},
			{Value: "'def'", Depth: 4, Type: ExpressionTokenString},
		},
	},
	{
		expression: "Name eq 'Milk' and Price lt 2.55",
		infixTokens: []*Token{
			{Value: "Name", Type: ExpressionTokenLiteral},
			{Value: "eq", Type: ExpressionTokenLogical},
			{Value: "'Milk'", Type: ExpressionTokenString},
			{Value: "and", Type: ExpressionTokenLogical},
			{Value: "Price", Type: ExpressionTokenLiteral},
			{Value: "lt", Type: ExpressionTokenLogical},
			{Value: "2.55", Type: ExpressionTokenFloat},
		},
		tree: []expectedParseNode{
			{Value: "and", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "Name", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'Milk'", Depth: 2, Type: ExpressionTokenString},
			{Value: "lt", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "Price", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "2.55", Depth: 2, Type: ExpressionTokenFloat},
		},
	},
	{
		// The syntax for ODATA functions follows the inline parameter syntax. The function name must be followed
		// by an opening parenthesis, followed by a comma-separated list of parameters, followed by a closing parenthesis.
		// For example:
		// GET serviceRoot/Airports?$filter=contains(Location/Address, 'San Francisco')
		expression: "contains(LastName, 'Smith') and FirstName eq 'John' and City eq 'Houston'",
		infixTokens: []*Token{
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
		},
		tree: []expectedParseNode{
			{Value: "and", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "and", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "contains", Depth: 2, Type: ExpressionTokenFunc},
			{Value: "LastName", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "'Smith'", Depth: 3, Type: ExpressionTokenString},
			{Value: "eq", Depth: 2, Type: ExpressionTokenLogical},
			{Value: "FirstName", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "'John'", Depth: 3, Type: ExpressionTokenString},
			{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "City", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'Houston'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		// Test ODATA syntax with nested function calls
		expression: "contains(LastName, toupper('Smith')) or FirstName eq 'John'",
		infixTokens: []*Token{
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
		},
		tree: []expectedParseNode{
			{Value: "or", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "contains", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "LastName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "toupper", Depth: 2, Type: ExpressionTokenFunc},
			{Value: "'Smith'", Depth: 3, Type: ExpressionTokenString},
			{Value: "eq", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "FirstName", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'John'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		expression: "LastName eq null",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "LastName", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "null", Depth: 1, Type: ExpressionTokenNull},
		},
	},
	{
		expression: "Enabled eq true",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "Enabled", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "true", Depth: 1, Type: ExpressionTokenBoolean},
		},
	},
	{
		// A property navigation path without arguments (see next fixture).
		expression: "Products/Value",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenNav},
			{Value: "Products", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "Value", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// A property navigation path without arguments (see next fixture).
		expression: "Products/Value eq 2",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "/", Depth: 1, Type: ExpressionTokenNav},
			{Value: "Products", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "Value", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "2", Depth: 1, Type: ExpressionTokenInteger},
		},
	},
	{
		// Common Schema Definition Language (CSDL) JSON Representation, Section 14.4.1 specifies the syntax of path expressions.
		// See Example 65 in section 14.4.1.1
		expression: "Products(sku='abc123',vendor='globex')/Value",
		infixTokens: []*Token{
			{Value: "Products", Type: ExpressionTokenLiteral},
			{Value: "(", Type: ExpressionTokenOpenParen},
			{Value: "sku", Type: ExpressionTokenLiteral},
			{Value: "=", Type: ExpressionTokenAssignement},
			{Value: "'abc123'", Type: ExpressionTokenString},
			{Value: ",", Type: ExpressionTokenComma},
			{Value: "vendor", Type: ExpressionTokenLiteral},
			{Value: "=", Type: ExpressionTokenAssignement},
			{Value: "'globex'", Type: ExpressionTokenString},
			{Value: ")", Type: ExpressionTokenCloseParen},
			{Value: "/", Type: ExpressionTokenNav},
			{Value: "Value", Type: ExpressionTokenLiteral},
		},
		postfixTokens: []*Token{
			{Value: "sku", Type: ExpressionTokenLiteral},
			{Value: "'abc123'", Type: ExpressionTokenString},
			{Value: "=", Type: ExpressionTokenAssignement},
			{Value: "vendor", Type: ExpressionTokenLiteral},
			{Value: "'globex'", Type: ExpressionTokenString},
			{Value: "=", Type: ExpressionTokenAssignement},
			{Value: "2", Type: TokenTypeArgCount}, // The argument count
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "Products", Type: ExpressionTokenLiteral},
			{Value: "Value", Type: ExpressionTokenLiteral},
			{Value: "/", Type: ExpressionTokenNav},
		},
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenNav},
			{Value: "Products", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "=", Depth: 2, Type: ExpressionTokenAssignement},
			{Value: "sku", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "'abc123'", Depth: 3, Type: ExpressionTokenString},
			{Value: "=", Depth: 2, Type: ExpressionTokenAssignement},
			{Value: "vendor", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "'globex'", Depth: 3, Type: ExpressionTokenString},
			{Value: "Value", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Navigation within a collection with a single argument.
		expression: "Products(sku='abc123')/Value",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenNav},
			{Value: "Products", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "=", Depth: 2, Type: ExpressionTokenAssignement},
			{Value: "sku", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "'abc123'", Depth: 3, Type: ExpressionTokenString},
			{Value: "Value", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Navigation within nested collections.
		expression: "Products(sku='abc123')/Components(id='abc')/Name",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenNav},
			{Value: "/", Depth: 1, Type: ExpressionTokenNav},
			{Value: "Products", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "=", Depth: 3, Type: ExpressionTokenAssignement},
			{Value: "sku", Depth: 4, Type: ExpressionTokenLiteral},
			{Value: "'abc123'", Depth: 4, Type: ExpressionTokenString},
			{Value: "Components", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "=", Depth: 3, Type: ExpressionTokenAssignement},
			{Value: "id", Depth: 4, Type: ExpressionTokenLiteral},
			{Value: "'abc'", Depth: 4, Type: ExpressionTokenString},
			{Value: "Name", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Navigation within property collection and sub-expression.
		expression: "Products(sku=concat('abc', '123'))/Name",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenNav},
			{Value: "Products", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "=", Depth: 2, Type: ExpressionTokenAssignement},
			{Value: "sku", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "concat", Depth: 3, Type: ExpressionTokenFunc},
			{Value: "'abc'", Depth: 4, Type: ExpressionTokenString},
			{Value: "'123'", Depth: 4, Type: ExpressionTokenString},
			{Value: "Name", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Navigation within a collection with a single argument.
		// TODO: should we allow this?
		expression: "Products('abc123')/Value",
		tree: []expectedParseNode{
			{Value: "/", Depth: 0, Type: ExpressionTokenNav},
			{Value: "Products", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "'abc123'", Depth: 2, Type: ExpressionTokenString},
			{Value: "Value", Depth: 1, Type: ExpressionTokenLiteral},
		},
	},
	{
		// Navigation within a collection with a single argument, no nested path.
		expression: "Products(sku='abc123')",
		postfixTokens: []*Token{
			{Value: "sku", Type: ExpressionTokenLiteral},
			{Value: "'abc123'", Type: ExpressionTokenString},
			{Value: "=", Type: ExpressionTokenAssignement},
			{Value: "1", Type: TokenTypeArgCount}, // The argument count
			{Value: TokenListExpr, Type: TokenTypeListExpr},
			{Value: "Products", Type: ExpressionTokenLiteral},
		},
		tree: []expectedParseNode{
			{Value: "Products", Depth: 0, Type: ExpressionTokenLiteral},
			{Value: "=", Depth: 1, Type: ExpressionTokenAssignement},
			{Value: "sku", Depth: 2, Type: ExpressionTokenLiteral},
			{Value: "'abc123'", Depth: 2, Type: ExpressionTokenString},
		},
	},
	{
		expression: "not not true",
		tree: []expectedParseNode{
			{Value: "not", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "not", Depth: 1, Type: ExpressionTokenLogical},
			{Value: "true", Depth: 2, Type: ExpressionTokenBoolean},
		},
	},
	{
		// duration      = [ "duration" ] SQUOTE durationValue SQUOTE
		expression: "TaskDuration eq duration'P12DT23H59M59.999999999999S'",
		tree: []expectedParseNode{
			{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "TaskDuration", Depth: 1, Type: ExpressionTokenLiteral},
			{Value: "P12DT23H59M59.999999999999S", Depth: 1, Type: ExpressionTokenDuration},
		},
	},
	{
		expression: "totalseconds(EndTime sub StartTime) lt duration'PT23H59M'",
		tree: []expectedParseNode{
			{Value: "lt", Depth: 0, Type: ExpressionTokenLogical},
			{Value: "totalseconds", Depth: 1, Type: ExpressionTokenFunc},
			{Value: "sub", Depth: 2, Type: ExpressionTokenOp},
			{Value: "EndTime", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "StartTime", Depth: 3, Type: ExpressionTokenLiteral},
			{Value: "PT23H59M", Depth: 1, Type: ExpressionTokenDuration},
		},
	},
}
