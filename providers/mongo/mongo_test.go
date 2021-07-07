package mongo

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/CiscoM31/godata"
)

func TestFilters(t *testing.T) {
	testCases := []struct {
		filter   string
		mgoQuery string
	}{
		{filter: "substring(CompanyName,1,2) eq 'lf'"}, // substring with 3 arguments.
		// Bolean values
		{filter: "true"},
		{filter: "false"},
		{filter: "(true)"},
		{filter: "((true))"},
		{filter: "((true)) or false"},
		{filter: "not true"},
		{filter: "not false"},
		{filter: "not (not true)"},
		//"not not true", // TODO: I think this should work. 'not not true' is true
		// String functions
		{filter: "contains(CompanyName,'freds')"},
		{filter: "endswith(CompanyName,'Futterkiste')"},
		{filter: "startswith(CompanyName,'Alfr')"},
		{filter: "length(CompanyName) eq 19"},
		{filter: "indexof(CompanyName,'lfreds') eq 1"},
		{filter: "substring(CompanyName,1) eq 'lfreds Futterkiste'"}, // substring() with 2 arguments.
		{filter: "'lfreds Futterkiste' eq substring(CompanyName,1)"}, // Same as above, but order of operands is reversed.
		{filter: "substring(CompanyName,1,2) eq 'lf'"},               // substring() with 3 arguments.
		{filter: "'lf' eq substring(CompanyName,1,2) "},              // Same as above, but order of operands is reversed.
		{filter: "substringof('Alfreds', CompanyName) eq true"},
		{filter: "tolower(CompanyName) eq 'alfreds futterkiste'"},
		{filter: "toupper(CompanyName) eq 'ALFREDS FUTTERKISTE'"},
		{filter: "trim(CompanyName) eq 'Alfreds Futterkiste'"},
		{filter: "concat(concat(City,', '), Country) eq 'Berlin, Germany'"},
		// GUID
		{filter: "GuidValue eq 01234567-89ab-cdef-0123-456789abcdef"}, // TODO According to ODATA ABNF notation, GUID values do not have quotes.
		// Date and Time functions
		{filter: "StartDate eq 2012-12-03"},
		{filter: "DateTimeOffsetValue eq 2012-12-03T07:16:23Z"},
		// duration      = [ "duration" ] SQUOTE durationValue SQUOTE
		// "DurationValue eq duration'P12DT23H59M59.999999999999S'", // TODO See ODATA ABNF notation
		{filter: "TimeOfDayValue eq 07:59:59.999"},
		{filter: "year(BirthDate) eq 0"},
		{filter: "month(BirthDate) eq 12"},
		{filter: "day(StartTime) eq 8"},
		{filter: "hour(StartTime) eq 1"},
		{filter: "hour    (StartTime) eq 12"},     // function followed by space characters
		{filter: "hour    ( StartTime   ) eq 15"}, // function followed by space characters
		{filter: "minute(StartTime) eq 0"},
		{filter: "totaloffsetminutes(StartTime) eq 0"},
		{filter: "second(StartTime) eq 0"},
		{filter: "fractionalsecond(StartTime) lt 0.123456"}, // The fractionalseconds function returns the fractional seconds component of the
		// DateTimeOffset or TimeOfDay parameter value as a non-negative decimal value less than 1.
		{filter: "date(StartTime) ne date(EndTime)"},
		{filter: "totaloffsetminutes(StartTime) eq 60"},
		{filter: "StartTime eq mindatetime()"},
		// "totalseconds(EndTime sub StartTime) lt duration'PT23H59'", // TODO The totalseconds function returns the duration of the value in total seconds, including fractional seconds.
		{filter: "EndTime eq maxdatetime()"},
		{filter: "time(StartTime) le StartOfDay"},
		{filter: "time('2015-10-14T23:30:00.104+02:00') lt now()"},
		{filter: "time(2015-10-14T23:30:00.104+02:00) lt now()"},
		// Math functions
		{filter: "round(Freight) eq 32"},
		{filter: "floor(Freight) eq 32"},
		{filter: "ceiling(Freight) eq 33"},
		{filter: "Rating mod 5 eq 0"},
		{filter: "Price div 2 eq 3"},
		// Type functions
		{filter: "isof(ShipCountry,Edm.String)"},
		{filter: "isof(NorthwindModel.BigOrder)"},
		{filter: "cast(ShipCountry,Edm.String)"},
		// Parameter aliases
		// See http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part1-protocol/odata-v4.0-errata03-os-part1-protocol-complete.html#_Toc453752288
		{filter: "Region eq @p1"}, // Aliases start with @
		// Geo functions
		{filter: "geo.distance(CurrentPosition,TargetPosition)"},
		{filter: "geo.length(DirectRoute)"},
		{filter: "geo.intersects(Position,TargetArea)"},
		{filter: "GEO.INTERSECTS(Position,TargetArea)"}, // functions are case insensitive in ODATA 4.0.1
		// Logical operators
		{filter: "'Milk' eq 'Milk'"},  // Compare two literals
		{filter: "'Water' ne 'Milk'"}, // Compare two literals
		{filter: "Name eq 'Milk'"},
		{filter: "Name EQ 'Milk'"}, // operators are case insensitive in ODATA 4.0.1
		{filter: "Name ne 'Milk'"},
		{filter: "Name NE 'Milk'"},
		{filter: "Name gt 'Milk'"},
		{filter: "Name ge 'Milk'"},
		{filter: "Name lt 'Milk'"},
		{filter: "Name le 'Milk'"},
		{filter: "Name eq Name"}, // parameter equals to itself
		{filter: "Name eq 'Milk' and Price lt 2.55"},
		{filter: "not endswith(Name,'ilk')"},
		{filter: "Name eq 'Milk' or Price lt 2.55"},
		{filter: "City eq 'Dallas' or City eq 'Houston'"},
		// Nested properties
		{filter: "Product/Name eq 'Milk'"},
		{filter: "Region/Product/Name eq 'Milk'"},
		{filter: "Country/Region/Product/Name eq 'Milk'"},
		//"style has Sales.Pattern'Yellow'", // TODO
		// Arithmetic operators
		{filter: "Price add 2.45 eq 5.00"},
		{filter: "2.46 add Price eq 5.00"},
		{filter: "Price add (2.47) eq 5.00"},
		{filter: "(Price add (2.48)) eq 5.00"},
		{filter: "Price ADD 2.49 eq 5.00"}, // 4.01 Services MUST support case-insensitive operator names.
		{filter: "Price sub 0.55 eq 2.00"},
		{filter: "Price SUB 0.56 EQ 2.00"}, // 4.01 Services MUST support case-insensitive operator names.
		{filter: "Price mul 2.0 eq 5.10"},
		{filter: "Price mul Quantity gt 300.0"},   // Arithmetic operator with two fields
		{filter: "(Price mul Quantity) gt 300.0"}, // Arithmetic operator with two fields
		{filter: "Price div 2.55 eq 1"},
		{filter: "Rating div 2 eq 2"},
		{filter: "Rating mod 5 eq 0"},
		// Grouping
		{filter: "(4 add 5) mod (4 sub 1) eq 0"},
		{filter: "not (City eq 'Dallas') or Name in ('a', 'b', 'c') and not (State eq 'California')"},
		// Nested functions
		{filter: "length(trim(CompanyName)) eq length(CompanyName)"},
		{filter: "concat(concat(City, ', '), Country) eq 'Berlin, Germany'"},
		// Various parenthesis combinations
		{filter: "City eq 'Dallas'"},
		{filter: "City eq ('Dallas')"},
		{filter: "'Dallas' eq City"},
		{filter: "not (City eq 'Dallas')"},
		{filter: "City in ('Dallas')"},
		{filter: "(City in ('Dallas'))"},
		{filter: "(City in ('Dallas', 'Houston'))"},
		{filter: "not (City in ('Dallas'))"},
		{filter: "not (City in ('Dallas', 'Houston'))"},
		{filter: "not (((City eq 'Dallas')))"},
		{filter: "not(S1 eq 'foo')"},
		// Lambda operators
		{filter: "Tags/any()"},                                                // The any operator without an argument returns true if the collection is not empty
		{filter: "Tags/any(tag:tag eq 'London')"},                             // 'Tags' is array of strings
		{filter: "Tags/any(tag:tag eq 'London' or tag eq 'Berlin')"},          // 'Tags' is array of strings
		{filter: "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London')"}, // 'Tags' is array of {"Key": "abc", "Value": "def"}
		{filter: "Tags/ANY(var:var/Key eq 'Site' AND var/Value eq 'London')"},
		{filter: "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London') and not (City in ('Dallas'))"},
		{filter: "Tags/all(var:var/Key eq 'Site' and var/Value eq 'London')"},
		{filter: "Price/any(t:not (12345 eq t))"},
		// A long query.
		{filter: "Tags/any(var:var/Key eq 'Site' and var/Value eq 'London') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'Berlin') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'Paris') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'New York City') or " +
			"Tags/any(var:var/Key eq 'Site' and var/Value eq 'San Francisco')"},
	}
	p := MongoGoDataProvider{}

	for _, testCase := range testCases {
		_, err := godata.ParseFilterString(testCase.filter)
		if err != nil {
			t.Errorf("Error parsing ODATA filter %s. Error: %v", testCase, err)
			return
		}
		u, err := url.Parse(fmt.Sprintf("/api/v1/PurchaseOrder?$filter=%s",
			url.QueryEscape(testCase.filter)))
		if err != nil {
			t.Errorf("Error parsing URL %s. Error: %v", u, err)
			return
		}
		var q *godata.GoDataRequest
		q, err = godata.ParseRequest(u.Path, u.Query(), false)
		if err != nil {
			t.Errorf("Error parsing ODATA query %s. Error: %v", u, err)
			return
		}
		var query MongoQuery
		query, err = p.BuildQuery(q)
		if err != nil {
			t.Errorf("Error building Mongo query %s. Error: %v", testCase, err)
			return
		}
		_, err = query.GetQuery()
		if err != nil {
			t.Errorf("Error unmarshaling Mongo query %s. Mongo: %s. Error: %v",
				testCase, query.StringQuery, err)
			return
		}
		fmt.Printf("query: %s => %s\n", testCase, query.StringQuery)
	}
}
