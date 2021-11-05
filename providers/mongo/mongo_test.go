package mongo

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/CiscoM31/godata"
)

var testCases = []struct {
	query string
}{
	{
		query: "$filter=(Name eq 'Bob') or (city eq 'London')",
	},
	{
		query: "$filter=Price gt 1000.00",
	},
	{
		query: "$filter=Price ge 1000",
	},
	{
		query: "$filter=Price lt 1000",
	},
	{
		query: "$filter=Price le 1000",
	},
	{
		query: "$filter=Name in ('Bob', 'Alice')",
	},
	{
		query: "$filter=Name in ('Bob', 'Alice')&$orderby=Name asc&$top=10&$skip=50",
	},
}

func TestFilters(t *testing.T) {

	p := MongoGoDataProvider{}

	for _, testCase := range testCases {
		u, err := url.Parse(fmt.Sprintf("/api/v1/PurchaseOrder?%s", testCase.query))
		if err != nil {
			t.Errorf("Error parsing URL %s. Error: %v", u, err)
			continue
		}
		var q *godata.GoDataRequest
		q, err = godata.ParseRequest(u.Path, u.Query(), false)
		if err != nil {
			t.Errorf("Error parsing ODATA query %s. Error: %v", u, err)
			continue
		}
		var query MongoQuery
		query, err = p.BuildQuery(q)
		if err != nil {
			t.Errorf("Error building Mongo query %s. Error: %v", testCase, err)
			continue
		}
		/*
			_, err = query.GetQuery()
			if err != nil {
				t.Errorf("Error unmarshaling Mongo query %s. Mongo: %s. Error: %v",
					testCase, query.query, err)
				return
			}
		*/
		t.Logf("query: %s => %+v\n", testCase, query.query)
	}
}
