package godata

import (
	"context"
	"strconv"
	"testing"
)

func TestPEMDAS(t *testing.T) {
	ctx := context.Background()
	parser := EmptyParser()
	parser.DefineFunction("sin", []int{1})
	parser.DefineFunction("max", []int{2})
	parser.DefineOperator("^", 2, OpAssociationRight, 5)
	parser.DefineOperator("*", 2, OpAssociationLeft, 5)
	parser.DefineOperator("/", 2, OpAssociationLeft, 5)
	parser.DefineOperator("+", 2, OpAssociationLeft, 4)
	parser.DefineOperator("-", 2, OpAssociationLeft, 4)

	// 3 + 4 * 2 / ( 1 - 5 ) ^ 2 ^ 3
	tokens := []*Token{
		{Value: "3"},
		{Value: "+"},
		{Value: "4"},
		{Value: "*"},
		{Value: "2"},
		{Value: "/"},
		{Value: "("},
		{Value: "1"},
		{Value: "-"},
		{Value: "5"},
		{Value: ")"},
		{Value: "^"},
		{Value: "2"},
		{Value: "^"},
		{Value: "3"},
	}

	// 3 4 2 * 1 5 - 2 3 ^ ^ / +
	expected := []string{"3", "4", "2", "*", "1", "5", "-", "2", "3", "^", "^",
		"/", "+"}

	result, err := parser.InfixToPostfix(ctx, tokens)

	if err != nil {
		t.Error(err)
		return
	}

	for i, v := range expected {
		if result.Empty() {
			t.Error("Output is not the expected length.")
			return
		}

		token := result.Dequeue()
		if v != token.Value {
			t.Error("Expected " + v + " at index " + strconv.Itoa(i) + " got " +
				token.Value)
		}
	}
}

func BenchmarkPEMDAS(b *testing.B) {
	ctx := context.Background()
	parser := EmptyParser()
	parser.DefineFunction("sin", []int{1})
	parser.DefineFunction("max", []int{2})
	parser.DefineOperator("^", 2, OpAssociationRight, 5)
	parser.DefineOperator("*", 2, OpAssociationLeft, 5)
	parser.DefineOperator("/", 2, OpAssociationLeft, 5)
	parser.DefineOperator("+", 2, OpAssociationLeft, 4)
	parser.DefineOperator("-", 2, OpAssociationLeft, 4)

	// 3 + 4 * 2 / ( 1 - 5 ) ^ 2 ^ 3
	tokens := []*Token{
		{Value: "3"},
		{Value: "+"},
		{Value: "4"},
		{Value: "*"},
		{Value: "2"},
		{Value: "/"},
		{Value: "("},
		{Value: "1"},
		{Value: "-"},
		{Value: "5"},
		{Value: ")"},
		{Value: "^"},
		{Value: "2"},
		{Value: "^"},
		{Value: "3"},
	}

	for i := 0; i < b.N; i++ {
		if _, err := parser.InfixToPostfix(ctx, tokens); err != nil {
			b.Fatalf("Failed to transform tokens from infix to postfix: %v", err)
		}
	}
}

func TestBoolean(t *testing.T) {
	ctx := context.Background()
	parser := EmptyParser()
	parser.DefineOperator("NOT", 1, OpAssociationNone, 3)
	parser.DefineOperator("AND", 2, OpAssociationLeft, 2)
	parser.DefineOperator("OR", 2, OpAssociationLeft, 1)

	// (A OR NOT B) AND C OR B
	tokens := []*Token{
		{Value: "("},
		{Value: "A"},
		{Value: "OR"},
		{Value: "NOT"},
		{Value: "B"},
		{Value: ")"},
		{Value: "AND"},
		{Value: "C"},
		{Value: "OR"},
		{Value: "B"},
	}

	// A B NOT OR C AND B OR
	expected := []string{"A", "B", "NOT", "OR", "C", "AND", "B", "OR"}
	result, err := parser.InfixToPostfix(ctx, tokens)

	if err != nil {
		t.Error(err)
		return
	}

	for i, v := range expected {
		if result.Empty() {
			t.Error("Output is not the expected length.")
			return
		}

		token := result.Dequeue()
		if v != token.Value {
			t.Error("Expected " + v + " at index " + strconv.Itoa(i) + " got " +
				token.Value)
		}
	}
}

func TestFunc(t *testing.T) {
	ctx := context.Background()
	parser := EmptyParser()
	parser.DefineFunction("sin", []int{1})
	parser.DefineFunction("max", []int{2})
	parser.DefineFunction("volume", []int{3})
	parser.DefineOperator("^", 2, OpAssociationRight, 5)
	parser.DefineOperator("*", 2, OpAssociationLeft, 5)
	parser.DefineOperator("/", 2, OpAssociationLeft, 5)
	parser.DefineOperator("+", 2, OpAssociationLeft, 4)
	parser.DefineOperator("-", 2, OpAssociationLeft, 4)

	// max(sin(5*pi)+3, sin(5)+volume(3,2,4)/[]int{3})
	tokens := []*Token{
		{Value: "max"},
		{Value: "("},
		{Value: "sin"},
		{Value: "("},
		{Value: "5"},
		{Value: "*"},
		{Value: "pi"},
		{Value: ")"},
		{Value: "+"},
		{Value: "3"},
		{Value: ","},
		{Value: "sin"},
		{Value: "("},
		{Value: "5"},
		{Value: ")"},
		{Value: "+"},
		{Value: "volume"},
		{Value: "("},
		{Value: "3"},
		{Value: ","},
		{Value: "2"},
		{Value: ","},
		{Value: "4"},
		{Value: ")"},
		{Value: "/"},
		{Value: "2"},
		{Value: ")"},
	}

	// 5 pi * sin 3 + 5 sin 3 2 4 volume 2 / + max
	expected := []string{
		"5", "pi", "*", "1" /* arg count */, "list", /* list expression */
		"sin", "3", "+", "5", "1" /* arg count */, "list", /* list expression */
		"sin", "3", "2", "4", "3" /* arg count */, "list", /* list expression */
		"volume", "2",
		"/", "+",
		"2" /* arg count */, "list" /* list expression */, "max"}
	result, err := parser.InfixToPostfix(ctx, tokens)

	if err != nil {
		t.Error(err)
		return
	}

	for i, v := range expected {
		if result.Empty() {
			t.Error("Output is not the expected length.")
			return
		}

		token := result.Dequeue()
		if v != token.Value {
			t.Errorf("Expected %v at index %d got %v", v, i, token.Value)
		}
	}
}

func TestTree(t *testing.T) {
	ctx := context.Background()
	parser := EmptyParser()
	parser.DefineFunction("sin", []int{1})
	parser.DefineFunction("max", []int{2})
	parser.DefineOperator("^", 2, OpAssociationRight, 5)
	parser.DefineOperator("*", 2, OpAssociationLeft, 5)
	parser.DefineOperator("/", 2, OpAssociationLeft, 5)
	parser.DefineOperator("+", 2, OpAssociationLeft, 4)
	parser.DefineOperator("-", 2, OpAssociationLeft, 4)

	// sin ( max ( 2, 3 ) / 3 * 3.1415 )
	tokens := []*Token{
		{Value: "sin"},
		{Value: "("},
		{Value: "max"},
		{Value: "("},
		{Value: "2"},
		{Value: ","},
		{Value: "3"},
		{Value: ")"},
		{Value: "/"},
		{Value: "3"},
		{Value: "*"},
		{Value: "pi"},
		{Value: ")"},
	}

	// 2 3 max 3 / 3.1415 * sin
	result, err := parser.InfixToPostfix(ctx, tokens)
	if err != nil {
		t.Fatal(err)
	}

	root, err := parser.PostfixToTree(ctx, result)
	if err != nil {
		t.Fatalf("Error parsing query: %v", err)
	}
	if root.Token.Value != "sin" {
		t.Error("Root node is not sin")
	}
	if root.Children[0].Token.Value != "*" {
		t.Error("Level 2 node is not *")
	}
	if root.Children[0].Children[1].Token.Value != "pi" {
		t.Error("Level 3 right node is not pi", root.Children[0].Children[1].Token.Value)
	}
	if root.Children[0].Children[0].Token.Value != "/" {
		t.Error("Level 3 left node is not /", root.Children[0].Children[0].Token.Value)
	}
	if root.Children[0].Children[0].Children[1].Token.Value != "3" {
		t.Error("Level 4 right node is not 3", root.Children[0].Children[0].Children[1].Token.Value)
	}
	if root.Children[0].Children[0].Children[0].Token.Value != "max" {
		t.Error("Level 4 left node is not max", root.Children[0].Children[0].Children[0].Token.Value)
	}
	if root.Children[0].Children[0].Children[0].Children[0].Token.Value != "2" {
		t.Error("Level 5 ieft node is not 2", root.Children[0].Children[0].Children[0].Children[0].Token.Value)
	}
	if root.Children[0].Children[0].Children[0].Children[1].Token.Value != "3" {
		t.Error("Level 5 right node is not 3", root.Children[0].Children[0].Children[0].Children[1].Token.Value)
	}
}

func BenchmarkBuildTree(b *testing.B) {
	ctx := context.Background()
	parser := EmptyParser()
	parser.DefineFunction("sin", []int{1})
	parser.DefineFunction("max", []int{2})
	parser.DefineOperator("^", 2, OpAssociationRight, 5)
	parser.DefineOperator("*", 2, OpAssociationLeft, 5)
	parser.DefineOperator("/", 2, OpAssociationLeft, 5)
	parser.DefineOperator("+", 2, OpAssociationLeft, 4)
	parser.DefineOperator("-", 2, OpAssociationLeft, 4)

	// sin ( max ( 2, 3 ) / 3 * 3.1415 )
	tokens := []*Token{
		{Value: "sin"},
		{Value: "("},
		{Value: "max"},
		{Value: "("},
		{Value: "2"},
		{Value: ","},
		{Value: "3"},
		{Value: ")"},
		{Value: "/"},
		{Value: "3"},
		{Value: "*"},
		{Value: "pi"},
		{Value: ")"},
	}

	// 2 3 max 3 / 3.1415 * sin
	for i := 0; i < b.N; i++ {
		result, _ := parser.InfixToPostfix(ctx, tokens)
		if _, err := parser.PostfixToTree(ctx, result); err != nil {
			b.Fatalf("Failed to transform tokens: %v", err)
		}
	}
}
