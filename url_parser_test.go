package godata

import (
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

// TestUrlTokenization tests URL parsing
// String encoding rules specified in the ODATA ABNF:
// http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_URLSyntax
func TestUrlTokenization(t *testing.T) {

	testCases := []struct {
		url string // The test URL
		// Set to nil if no error is expected.
		// If error is expected, it is used to match err.Error()
		errRegex *regexp.Regexp

		expectedTree []expectedParseNode
	}{
		{
			// Unescaped single quotes.
			// This is not a valid filter because:
			// 1. there are two consecutive literal values, 'ab' and 'c,
			// 2. 'c is not terminated with a quote.
			url:      "/Books?$filter=Description eq 'ab'c'",
			errRegex: regexp.MustCompile("No matching token for '"),
		},
		{
			// Simple string with special characters.
			url:      "/Books?$filter=Description eq 'abc'",
			errRegex: nil,
			expectedTree: []expectedParseNode{
				{"eq", 0},
				{"Description", 1},
				{"'abc'", 1},
			},
		},
		{
			// Two consecutive single quotes.
			// One of the URL syntax rules for ODATA is that single quotes within string
			// literals are represented as two consecutive single quotes.
			// This is done to make the input strings in the ABNF test cases more readable.
			url:      "/Books?$filter=Description eq 'ab''c'",
			errRegex: nil,
			expectedTree: []expectedParseNode{
				{"eq", 0},
				{"Description", 1},
				// Note below two consecutive single-quotes are the encoding of one single quote,
				// so after the tokenization it is unescaped to one single quote.
				{"'ab'c'", 1},
			},
		},
		{
			// Test single quotes escaped as %27.
			url:      "/Books?$filter=Description eq 'O%27%27Neil'",
			errRegex: nil,
			expectedTree: []expectedParseNode{
				{"eq", 0},
				{"Description", 1},
				// Percent-encoded character %27 must be decoded to single quote.
				{"'O'Neil'", 1},
			},
		},
		{
			// Test single quotes escaped as %27.
			// This time all single quotes are percent-encoded, including the outer single-quotes.
			url:      "/Books?$filter=Description eq %27O%27%27Neil%27",
			errRegex: nil,
			expectedTree: []expectedParseNode{
				{"eq", 0},
				{"Description", 1},
				// Percent-encoded character %27 must be decoded to single quote.
				{"'O'Neil'", 1},
			},
		},
		{
			// According to RFC 1738, URLs should not include UTF-8 characters,
			// but the string tokens are parsed anyway.
			url:      "/Books?$filter=Description eq '♺⛺⛵⚡'",
			errRegex: nil,
			expectedTree: []expectedParseNode{
				{"eq", 0},
				{"Description", 1},
				// Percent-encoded character %27 must be decoded to single quote.
				{"'♺⛺⛵⚡'", 1},
			},
		},
		{
			// Strings with percent encoding
			url:      "/Books?$filter=Description eq '%34%35%36'",
			errRegex: nil,
			expectedTree: []expectedParseNode{
				{"eq", 0},
				{"Description", 1},
				{"'456'", 1},
			},
		},
		{
			url:      "/Books?$filter=Description eq 'abc'&$order=Title",
			errRegex: nil,
			expectedTree: []expectedParseNode{
				{"eq", 0},
				{"Description", 1},
				{"'456'", 1},
			},
		},
	}
	for _, testCase := range testCases {
		parsedUrl, err := url.Parse(testCase.url)
		if err != nil {
			t.Errorf("Test case '%s' failed: %v", testCase.url, err)
			continue
		}
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
			t.Errorf("Test case '%s' failed. Obtained error %v does not match expected regex %v",
				testCase.url, err, testCase.errRegex)
			continue
		}
		if err == nil {
			filter := request.Query.Filter
			if filter == nil {
				t.Errorf("Test case '%s' failed. Parsed filter is nil", testCase.url)
				continue
			}
			pos := 0
			err = CompareTree(filter.Tree, testCase.expectedTree, &pos, 0)
			if err != nil {
				t.Errorf("Tree representation does not match expected value. error: %s", err.Error())
			}
		}
	}
}
