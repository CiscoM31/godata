package godata

import (
	"context"
	"testing"
)

func TestSearchQuery(t *testing.T) {
	ctx := context.Background()
	tokenizer := SearchTokenizer()
	input := "mountain OR (\"red bikes\" AND avocados)"

	expect := []*Token{
		{Value: "mountain", Type: SearchTokenLiteral},
		{Value: "OR", Type: SearchTokenOp},
		{Value: "(", Type: SearchTokenOpenParen},
		{Value: "\"red bikes\"", Type: SearchTokenLiteral},
		{Value: "AND", Type: SearchTokenOp},
		{Value: "avocados", Type: SearchTokenLiteral},
		{Value: ")", Type: SearchTokenCloseParen},
	}
	output, err := tokenizer.Tokenize(ctx, input)
	if err != nil {
		t.Error(err)
	}

	result, err := CompareTokens(expect, output)
	if !result {
		t.Error(err)
	}
}
