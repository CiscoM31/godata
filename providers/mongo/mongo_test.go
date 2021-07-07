package mongo

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/CiscoM31/godata"
)

func TestFilters(t *testing.T) {
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
		"fractionalsecond(StartTime) lt 0.123456", // The fractionalseconds function returns the fractional seconds component of the
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
		"Price mul Quantity gt 300.0",   // Arithmetic operator with two fields
		"(Price mul Quantity) gt 300.0", // Arithmetic operator with two fields
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
	p := MongoGoDataProvider{}

	for _, input := range queries {
		_, err := godata.ParseFilterString(input)
		if err != nil {
			t.Errorf("Error parsing ODATA filter %s. Error: %s", input, err.Error())
			return
		}
		u, err := url.Parse(fmt.Sprintf("/api/v1/PurchaseOrder?$filter=%s",
			url.QueryEscape(input)))
		if err != nil {
			t.Errorf("Error parsing URL %s. Error: %s", u, err.Error())
			return
		}
		var q *godata.GoDataRequest
		q, err = godata.ParseRequest(u.Path, u.Query(), false)
		if err != nil {
			t.Errorf("Error parsing ODATA query %s. Error: %s", u, err.Error())
			return
		}
		var query string
		query, err = p.BuildQuery(q)
		if err != nil {
			t.Errorf("Error building Mongo query %s. Error: %s", input, err.Error())
			return
		}
		fmt.Printf("query: %s => %s\n", input, query)
	}
}
