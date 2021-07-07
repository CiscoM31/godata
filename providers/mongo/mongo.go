package mongo

import (
	"fmt"
	"strings"

	"github.com/CiscoM31/godata"
)

var mongoNodeMap = map[string]string{
	// comparison operators
	"eq": "{ %s : { $eq: %s } }",
	"ne": "{ %s : { $ne: %s } }",
	"gt": "{ %s: { $gt: %s } }",
	"ge": "{ %s: { $gte: %s } }",
	"lt": "{ %s: { $lt: %s } }",
	"le": "{ %s: { $lte: %s } }",
	"in": "{ %s: { $in: [ %s ] } }",
	// logical operators
	"and": "{ $and: [ %s, %s ] }",
	"or":  "{ $or: [ %s, %s ] }",
	"not": "{ $not: { %s } }",
	// string functions
	"substring":   "{ $substrCP: [ %s, %s, %s ] }",
	"substringof": "{ $substrCP: [ %s, %s, %s ] }",
	"contains":    "{ %s: { $regex: /%s/ } }",
	"startswith":  "{ %s: { $regex: /^%s/ } }",
	"endswith":    "{ %s: { $regex: /%s$/ } }",
	"length":      "{ $strLenCP: %s }",
	"indexof":     "{ %indexOfCP: [ %s, %s ] }",
	"trim":        "{ $trim: { input: %s} }",
	"concat":      "{ $concat: { %s, %s } }",
	"tolower":     "",
	"toupper":     "",
	// date-time functions
	"date":               "",
	"time":               "",
	"year":               "",
	"month":              "",
	"day":                "",
	"hour":               "",
	"minute":             "",
	"second":             "",
	"fractionalsecond":   "",
	"totaloffsetminutes": "",
	"mindatetime":        "",
	"maxdatetime":        "",
	"now":                "",
	// arithmetic functions
	"round":   "",
	"floor":   "",
	"ceiling": "",
	"mod":     "",
	"div":     "",
	"add":     "",
	"mul":     "",
	"sub":     "",
	//
	"isof": "",
	"cast": "",
	// geo functions
	"geo.distance":   "",
	"geo.length":     "",
	"geo.intersects": "",
	// Navigation
	"/": "",
	// Lambda operators
	"any": "",
	"all": "",
}

type MongoGoDataProvider struct{}

// Build a where clause that can be appended to a Mongo query, and also return
// the values to send to a prepared statement.
func (p *MongoGoDataProvider) BuildQuery(r *godata.GoDataRequest) (string, error) {
	// Builds the WHERE clause recursively using DFS
	return recursiveBuildWhere(r.Query.Filter.Tree)
}

func recursiveBuildWhere(n *godata.ParseNode) (string, error) {
	switch n.Token.Type {
	case godata.FilterTokenLiteral,
		godata.FilterTokenFloat,
		godata.FilterTokenInteger,
		godata.FilterTokenBoolean:
		return n.Token.Value, nil
	case godata.FilterTokenString:
		return n.Token.Value[1 : len(n.Token.Value)-1], nil
	case godata.FilterTokenGuid:
		return n.Token.Value, nil
	case godata.FilterTokenDate,
		godata.FilterTokenTime,
		godata.FilterTokenDateTime:
		return n.Token.Value, nil
	case godata.TokenTypeListExpr:
		var result strings.Builder
		result.WriteString("[")
		// build each child first using DFS
		for i, child := range n.Children {
			q, err := recursiveBuildWhere(child)
			if err != nil {
				return "", err
			}
			result.WriteString(q)
			if i < len(n.Children)-1 {
				result.WriteString(", ")
			}
		}
		return result.String(), nil
	case godata.FilterTokenLogical,
		godata.FilterTokenOp,
		godata.FilterTokenFunc,
		godata.FilterTokenLambda,
		godata.FilterTokenNav:
		if v, ok := mongoNodeMap[n.Token.Value]; ok {
			var children []string
			// build each child first using DFS
			for _, child := range n.Children {
				q, err := recursiveBuildWhere(child)
				if err != nil {
					return "", err
				}
				children = append(children, q)
			}
			// merge together the children and the current node
			result := fmt.Sprintf(v, children)
			return result, nil
		} else {
			return "", godata.NotImplementedError(n.Token.Value + " is not implemented.")
		}
	default:
		return "", godata.NotImplementedError(fmt.Sprintf("%d type is not implemented.", n.Token.Type))
	}
}
