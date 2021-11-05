package mongo

import (
	"fmt"

	"github.com/CiscoM31/godata/v2"
	"go.mongodb.org/mongo-driver/bson"
)

type compile func(args ...interface{}) (interface{}, error)

func isPrimitiveType(token *godata.Token) bool {
	switch token.Type {
	case godata.ExpressionTokenBoolean,
		godata.ExpressionTokenDate,
		godata.ExpressionTokenDateTime,
		godata.ExpressionTokenTime,
		godata.ExpressionTokenDuration,
		godata.ExpressionTokenFloat,
		godata.ExpressionTokenGuid,
		godata.ExpressionTokenInteger,
		godata.ExpressionTokenString:
		return true
	default:
		return false
	}
}

func compileNot(operator string, args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, godata.BadRequestError(
			fmt.Sprintf("'%s' operator must have one arguments, got %d", operator, len(args)))
	}
	return nil, godata.BadRequestError(fmt.Sprintf("Unsupported operator: %v", operator))
}

func compileOperator(operator string, args ...interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, godata.BadRequestError(
			fmt.Sprintf("'%s' operator must have two arguments, got %d", operator, len(args)))
	}
	lhs, ok := args[0].(*godata.ParseNode)
	if !ok {
		return nil, godata.BadRequestError(fmt.Sprintf("Unsupported LHS: %v", args[0]))
	}
	if lhs.Token.Type != godata.ExpressionTokenLiteral {
		return nil, godata.BadRequestError(fmt.Sprintf("Unsupported LHS: %v", args[0]))
	}
	rhs, ok := args[1].(*godata.ParseNode)
	if !ok {
		return nil, godata.BadRequestError(fmt.Sprintf("Unsupported RHS: %v", args[1]))
	}
	switch {
	case isPrimitiveType(rhs.Token):
		return bson.M{
			lhs.Token.Value: bson.E{Key: operator, Value: rhs.Token.Value},
		}, nil
	case rhs.Token.Type == godata.TokenTypeListExpr:
		var children bson.A
		for _, child := range rhs.Children {
			c, err := buildQuery(child)
			if err != nil {
				return nil, err
			}
			children = append(children, c)
		}
		return bson.M{
			lhs.Token.Value: bson.E{Key: operator, Value: children},
		}, nil
	}
	return nil, godata.BadRequestError(fmt.Sprintf("Unsupported RHS: %v", args[1]))
}

// Support $and, $or.
func compileLogical(operator string, args ...interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, godata.BadRequestError(
			fmt.Sprintf("'%s' operator must have two arguments, got %d", operator, len(args)))
	}
	var children bson.A
	for _, child := range args {
		switch v := child.(type) {
		case *godata.ParseNode:
			switch v.Token.Type {
			case godata.ExpressionTokenBoolean:
				children = append(children, v.Token.Value)
			default:
				return nil, godata.BadRequestError(
					fmt.Sprintf("Invalid argument for '%s' operator: %s", operator, v.Token.Value))
			}
		default:
			children = append(children, v)
		}
	}
	return bson.D{{Key: operator, Value: children}}, nil
}

var compilerMap map[string]compile

func init() {
	compilerMap = map[string]compile{
		// comparison operators
		"eq":  func(args ...interface{}) (interface{}, error) { return compileOperator("$eq", args...) },
		"ne":  func(args ...interface{}) (interface{}, error) { return compileOperator("$ne", args...) },
		"gt":  func(args ...interface{}) (interface{}, error) { return compileOperator("$gt", args...) },
		"ge":  func(args ...interface{}) (interface{}, error) { return compileOperator("$gte", args...) },
		"lt":  func(args ...interface{}) (interface{}, error) { return compileOperator("$lt", args...) },
		"le":  func(args ...interface{}) (interface{}, error) { return compileOperator("$lte", args...) },
		"in":  func(args ...interface{}) (interface{}, error) { return compileOperator("$in", args...) },
		"not": func(args ...interface{}) (interface{}, error) { return compileNot("$not", args...) },

		"and": func(args ...interface{}) (interface{}, error) { return compileLogical("$and", args...) },
		"or":  func(args ...interface{}) (interface{}, error) { return compileLogical("$or", args...) },
		/*
			"in": "{ %s: { $in: [ %s ] } }",
			// string functions
			"substring":   "$substrCP: [ %s, %s, %s ]",
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
		*/
	}

}

type MongoGoDataProvider struct{}

type MongoQuery struct {
	query interface{} // The MongoDB query.
}

// Build a where clause that can be appended to a Mongo query, and also return
// the values to send to a prepared statement.
func (p *MongoGoDataProvider) BuildQuery(r *godata.GoDataRequest) (MongoQuery, error) {
	// Builds the MongoDB find query, recursively using DFS
	q, err := buildQuery(r.Query.Filter.Tree)
	if err != nil {
		return MongoQuery{}, err
	}
	return MongoQuery{query: q}, err
}

/*
func (p *MongoQuery) GetQuery() (bson.M, error) {
	var ret bson.M
	err := json.Unmarshal([]byte(p.query), &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
*/

func buildQuery(n *godata.ParseNode) (interface{}, error) {
	switch n.Token.Type {
	case godata.ExpressionTokenLiteral,
		godata.ExpressionTokenFloat,
		godata.ExpressionTokenInteger,
		godata.ExpressionTokenBoolean,
		godata.TokenTypeListExpr,
		godata.ExpressionTokenString,
		godata.ExpressionTokenDate,
		godata.ExpressionTokenTime,
		godata.ExpressionTokenDateTime,
		godata.ExpressionTokenGuid:
		return n, nil
		//`"` + n.Token.Value[1:len(n.Token.Value)-1] + `"`
		/*
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
		*/
	case godata.ExpressionTokenLogical,
		godata.ExpressionTokenOp,
		godata.ExpressionTokenFunc,
		godata.ExpressionTokenLambda,
		godata.ExpressionTokenNav:
		if v, ok := compilerMap[n.Token.Value]; ok {
			var children []interface{}
			// build each child first using DFS
			for _, child := range n.Children {
				q, err := buildQuery(child)
				if err != nil {
					return nil, err
				}
				children = append(children, q)
			}
			return v(children...)
		} else {
			return nil, godata.NotImplementedError(n.Token.Value + " is not implemented.")
		}
	default:
		return nil, godata.NotImplementedError(fmt.Sprintf("%d type is not implemented.", n.Token.Type))
	}
}
