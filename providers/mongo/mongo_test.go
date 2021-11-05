package mongo

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/CiscoM31/godata"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
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

	os.Setenv("MONGODB_URI", "mongodb://localhost:27017")
	clientType := mtest.Default // mtest.Mock
	mt := mtest.New(t, mtest.NewOptions().ClientType(clientType))
	defer mt.Close()

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
		t.Logf("query: %s => %+v\n", testCase, query.query)
		// Create a new T instance for a sub-test and runs the given callback.
		// Create a new collection using the given name which is available to the callback through
		// the T.Coll variable and is dropped after the callback returns.
		mt.Run(fmt.Sprintf("Query: %s", testCase.query), func(mt *mtest.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := mt.Coll.InsertOne(ctx, bson.D{})
			if err != nil {
				mt.Fatalf("Error building Mongo query %s. Error: %v", testCase, err)
			}
			_, err = mt.Coll.Find(ctx, query)
			if err != nil {
				mt.Errorf("Error building Mongo query %s. Error: %v", testCase, err)
			}
		})
	}
}
