package godata

import (
	"fmt"
	"net/url"
	"regexp"
	"testing"
)

func TestUrlParser(t *testing.T) {
	testUrl := "Employees(1)/Sales.Manager?$expand=DirectReports%28$select%3DFirstName%2CLastName%3B$levels%3D4%29"
	parsedUrl, err := url.Parse(testUrl)

	if err != nil {
		t.Error(err)
		return
	}

	request, err := ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)

	if err != nil {
		t.Error(err)
		return
	}

	if request.FirstSegment.Name != "Employees" {
		t.Error("First segment is '" + request.FirstSegment.Name + "' not Employees")
		return
	}
	if request.FirstSegment.Identifier.Get() != "1" {
		t.Error("Employee identifier not found")
		return
	}
	if request.FirstSegment.Next.Name != "Sales.Manager" {
		t.Error("Second segment is not Sales.Manager")
		return
	}
}

func TestUrlParserStrictValidation(t *testing.T) {
	testUrl := "Employees(1)/Sales.Manager?$expand=DirectReports%28$select%3DFirstName%2CLastName%3B$levels%3D4%29"
	parsedUrl, err := url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err != nil {
		t.Error(err)
		return
	}

	testUrl = "Employees(1)/Sales.Manager?$filter=FirstName eq 'Bob'"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err != nil {
		t.Error(err)
		return
	}

	// Wrong filter with an extraneous single quote
	testUrl = "Employees(1)/Sales.Manager?$filter=FirstName eq' 'Bob'"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err == nil {
		t.Errorf("Parser should have returned invalid filter error: %s", testUrl)
		return
	}

	// Valid query with two parameters:
	// $filter=FirstName eq 'Bob'
	// at=Version eq '123'
	testUrl = "Employees(1)/Sales.Manager?$filter=FirstName eq 'Bob'&at=Version eq '123'"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err != nil {
		t.Error(err)
		return
	}

	// Invalid query:
	// $filter=FirstName eq' 'Bob' has extraneous single quote.
	// at=Version eq '123'         is valid
	testUrl = "Employees(1)/Sales.Manager?$filter=FirstName eq' 'Bob'&at=Version eq '123'"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err == nil {
		t.Errorf("Parser should have returned invalid filter error: %s", testUrl)
		return
	}

	testUrl = "Employees(1)/Sales.Manager?$select=3DFirstName"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err != nil {
		t.Error(err)
		return
	}

	testUrl = "Employees(1)/Sales.Manager?$filter=Name in ('Bob','Alice')&$select=Name,Address%3B$expand=Address($select=City)"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false /*strict*/)
	if err != nil {
		t.Errorf("Unexpected parsing error: %v", err)
		return
	}

	// A $select option cannot be wrapped with parenthesis. This is not legal ODATA.

	/*
		 queryOptions = queryOption *( "&" queryOption )
		 queryOption  = systemQueryOption
				/ aliasAndValue
				/ nameAndValue
				/ customQueryOption
		 systemQueryOption = compute
				/ deltatoken
				/ expand
				/ filter
				/ format
				/ id
				/ inlinecount
				/ orderby
				/ schemaversion
				/ search
				/ select
				/ skip
				/ skiptoken
				/ top
				/ index
		  select = ( "$select" / "select" ) EQ selectItem *( COMMA selectItem )
	*/
	testUrl = "Employees(1)/Sales.Manager?$filter=Name in ('Bob','Alice')&($select=Name,Address%3B$expand=Address($select=City))"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false /*strict*/)
	if err == nil {
		t.Errorf("Parser should have raised error")
		return
	}

	// Duplicate keyword: '$select' is present twice.
	testUrl = "Employees(1)/Sales.Manager?$select=3DFirstName&$select=3DFirstName"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	// In lenient mode, do not return an error when there is a duplicate keyword.
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), true /*lenient*/)
	if err != nil {
		t.Error(err)
		return
	}
	// In strict mode, return an error when there is a duplicate keyword.
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false /*strict*/)
	if err == nil {
		t.Error("Parser should have returned duplicate keyword error")
		return
	}

	// Unsupported keywords
	testUrl = "Employees(1)/Sales.Manager?orderby=FirstName"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), true /*lenient*/)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false /*strict*/)
	if err == nil {
		t.Error("Parser should have returned unsupported keyword error")
		return
	}

	testUrl = "Employees(1)/Sales.Manager?$select=LastName&$expand=Address"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err != nil {
		t.Error(err)
		return
	}

	testUrl = "Employees(1)/Sales.Manager?$select=FirstName,LastName&$expand=Address"
	parsedUrl, err = url.Parse(testUrl)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = ParseRequest(parsedUrl.Path, parsedUrl.Query(), false)
	if err != nil {
		t.Error(err)
		return
	}

}

// TestUnescapeStringTokens tests string encoding rules specified in the ODATA ABNF:
// http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_URLSyntax
func TestUnescapeStringTokens(t *testing.T) {

	testCases := []struct {
		url string // The test URL
		// Set to nil if no error is expected.
		// If error is expected, it is used to match err.Error()
		errRegex *regexp.Regexp

		expectedFilterTree []expectedParseNode
		expectedOrderBy    []OrderByItem
	}{
		{
			// Unescaped single quotes.
			// This is not a valid filter because:
			// 1. there are two consecutive literal values, 'ab' and 'c,
			// 2. 'c is not terminated with a quote.
			url:      "/Books?$filter=Description eq 'ab'c'",
			errRegex: regexp.MustCompile("Token ''' is invalid"),
		},
		{
			// Simple string with special characters.
			url:      "/Books?$filter=Description eq 'abc'",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'abc'", Depth: 1, Type: ExpressionTokenString},
			},
		},
		{
			// Two consecutive single quotes.
			// One of the URL syntax rules for ODATA is that single quotes within string
			// literals are represented as two consecutive single quotes.
			// This is done to make the input strings in the ABNF test cases more readable.
			url:      "/Books?$filter=Description eq 'ab''c'",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				// Note below two consecutive single-quotes are the encoding of one single quote,
				// so after the tokenization it is unescaped to one single quote.
				{Value: "'ab'c'", Depth: 1, Type: ExpressionTokenString},
			},
		},
		{
			// Test single quotes escaped as %27.
			url:      "/Books?$filter=Description eq 'O%27%27Neil'",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				// Percent-encoded character %27 must be decoded to single quote.
				{Value: "'O'Neil'", Depth: 1, Type: ExpressionTokenString},
			},
		},
		{
			// Test single quotes escaped as %27.
			// This time all single quotes are percent-encoded, including the outer single-quotes.
			url:      "/Books?$filter=Description eq %27O%27%27Neil%27",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				// Percent-encoded character %27 must be decoded to single quote.
				{Value: "'O'Neil'", Depth: 1, Type: ExpressionTokenString},
			},
		},
		{
			// According to RFC 1738, URLs should not include UTF-8 characters,
			// but the string tokens are parsed anyway.
			url:      "/Books?$filter=Description eq '♺⛺⛵⚡'",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				// Percent-encoded character %27 must be decoded to single quote.
				{Value: "'♺⛺⛵⚡'", Depth: 1, Type: ExpressionTokenString},
			},
		},
		{
			// Strings with percent encoding
			url:      "/Books?$filter=Description eq '%34%35%36'",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'456'", Depth: 1, Type: ExpressionTokenString},
			},
		},
		{
			url:      "/Books?$filter=Description eq 'abc'&$orderby=Title",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'abc'", Depth: 1, Type: ExpressionTokenString},
			},
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Title"}, Order: "asc"},
			},
		},
		{
			url:      "/Books?$filter=Description eq 'abc'&$orderby=Title asc",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'abc'", Depth: 1, Type: ExpressionTokenString},
			},
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Title"}, Order: "asc"},
			},
		},
		{
			url:      "/Books?$filter=Description eq 'abc'&$orderby=Title desc",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'abc'", Depth: 1, Type: ExpressionTokenString},
			},
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Title"}, Order: "desc"},
			},
		},
		{
			url:      "/Books?$filter=Description eq 'abc'&$orderby=Author asc,Title desc",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'abc'", Depth: 1, Type: ExpressionTokenString},
			},
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Author"}, Order: "asc"},
				{Field: &Token{Value: "Title"}, Order: "desc"},
			},
		},
		{
			url:      "/Books?$filter=Description eq 'abc'&$orderby=Author    asc,Title     DESC",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'abc'", Depth: 1, Type: ExpressionTokenString},
			},
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Author"}, Order: "asc"},
				{Field: &Token{Value: "Title"}, Order: "desc"},
			},
		},
		{
			url:                "/Products?$orderby=Asc",
			errRegex:           nil,
			expectedFilterTree: nil,
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Asc"}, Order: "asc"},
			},
		},
		{
			url:                "/Products?$orderby=Asc Asc",
			errRegex:           nil,
			expectedFilterTree: nil,
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Asc"}, Order: "asc"},
			},
		},
		{
			url:                "/Products?$orderby=Desc Asc",
			errRegex:           nil,
			expectedFilterTree: nil,
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Desc"}, Order: "asc"},
			},
		},
		{
			url:                "/Products?$orderby=Asc Desc",
			errRegex:           nil,
			expectedFilterTree: nil,
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "Asc"}, Order: "desc"},
			},
		},
		{
			url:                "/Products?$orderby=ProductDesc",
			errRegex:           nil,
			expectedFilterTree: nil,
			expectedOrderBy: []OrderByItem{
				{Field: &Token{Value: "ProductDesc"}, Order: "asc"},
			},
		},

		/*
			TODO: this is not supported yet.
			{
				// return all Categories ordered by the number of Products within each category.
				url:                "Categories?$orderby=Products/$count",
				errRegex:           nil,
				expectedFilterTree: nil,
				expectedOrderBy: []OrderByItem{
					{
						Field: &Token{Value: "Products/$count"},
						Order: "asc",
					},
				},
			},
		*/
		{
			url:      "/Product?$filter=Description eq 'abc'&$orderby=part_x0020_number asc",
			errRegex: nil,
			expectedFilterTree: []expectedParseNode{
				{Value: "eq", Depth: 0, Type: ExpressionTokenLogical},
				{Value: "Description", Depth: 1, Type: ExpressionTokenLiteral},
				{Value: "'abc'", Depth: 1, Type: ExpressionTokenString},
			},
			expectedOrderBy: []OrderByItem{
				{
					Field: &Token{Value: "part number"},
					Order: "asc",
				},
			},
		},
		{
			url:                "/Product?$orderby=Tags(Key='Environment')/Value desc",
			errRegex:           nil,
			expectedFilterTree: nil,
			expectedOrderBy: []OrderByItem{
				{
					Field: &Token{Value: "Tags(Key='Environment')/Value"},
					Order: "desc",
				},
			},
		},
		{
			url:                "/Product?$orderby=Tags(Key='Sku Number')/Value",
			errRegex:           nil,
			expectedFilterTree: nil,
			expectedOrderBy: []OrderByItem{
				{
					Field: &Token{Value: "Tags(Key='Sku Number')/Value"},
					Order: "asc",
				},
			},
		},
		{
			// Disallow $orderby=+Name
			// Query string uses %2B which is the escape for +. The + character is itself a url escape for space, see https://www.w3schools.com/tags/ref_urlencode.asp.
			url:                "/Product?$orderby=%2BName",
			errRegex:           regexp.MustCompile(`.*Token '\+Name' is invalid.*`),
			expectedFilterTree: nil,
			expectedOrderBy:    nil,
		},
		{
			url:                "/Product?$orderby=-Name",
			errRegex:           regexp.MustCompile(".*Token '-Name' is invalid.*"),
			expectedFilterTree: nil,
			expectedOrderBy:    nil,
		},
	}
	for _, testCase := range testCases {
		parsedUrl, err := url.Parse(testCase.url)
		if err != nil {
			t.Errorf("Test case '%s' failed: %v", testCase.url, err)
			continue
		}
		t.Logf("Running test case %s", testCase.url)
		var request *GoDataRequest
		urlQuery := parsedUrl.Query()
		request, err = ParseRequest(parsedUrl.Path, urlQuery, false /*strict*/)
		if testCase.errRegex == nil && err != nil {
			t.Errorf("Test case '%s' failed: %v", testCase.url, err)
			continue
		} else if testCase.errRegex != nil && err == nil {
			t.Errorf("Test case '%s' failed. Expected error but obtained nil error", testCase.url)
			continue
		} else if err != nil && !testCase.errRegex.MatchString(err.Error()) {
			t.Errorf("Test case '%s' failed. Obtained error [%v] does not match expected regex [%v]",
				testCase.url, err, testCase.errRegex)
			continue
		}
		if err == nil {
			filter := request.Query.Filter
			if filter == nil {
				if testCase.expectedFilterTree != nil {
					t.Errorf("Test case '%s' failed. Parsed filter is nil", testCase.url)
				}
			} else {
				pos := 0
				err = CompareTree(filter.Tree, testCase.expectedFilterTree, &pos, 0)
				if err != nil {
					t.Errorf("Tree representation does not match expected value. error: %s", err.Error())
				}
			}

			err = compareOrderBy(request.Query.OrderBy, testCase.expectedOrderBy)
			if err != nil {
				t.Errorf("orderby does not match expected value. error: %s", err.Error())
			}
		}
	}
}

func compareOrderBy(obtained *GoDataOrderByQuery, expected []OrderByItem) error {
	if len(expected) == 0 && (obtained == nil || obtained.OrderByItems == nil) {
		return nil
	}
	if len(expected) > 0 && (obtained == nil || obtained.OrderByItems == nil) {
		return fmt.Errorf("Unexpected number of $orderby fields. Got nil, expected %d",
			len(expected))
	}
	if len(obtained.OrderByItems) != len(expected) {
		return fmt.Errorf("Unexpected number of $orderby fields. Got %d, expected %d",
			len(obtained.OrderByItems), len(expected))
	}
	for i, v := range expected {
		if v.Field.Value != obtained.OrderByItems[i].Field.Value {
			return fmt.Errorf("Unexpected $orderby field at index %d. Got '%s', expected '%s'",
				i, obtained.OrderByItems[i].Field.Value, v.Field.Value)
		}
		if v.Order != obtained.OrderByItems[i].Order {
			return fmt.Errorf("Unexpected $orderby at index %d. Got '%s', expected '%s'",
				i, obtained.OrderByItems[i].Order, v.Order)
		}
	}
	return nil
}
