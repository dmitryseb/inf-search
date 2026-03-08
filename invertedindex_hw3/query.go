package invertedindex

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/RoaringBitmap/roaring/v2"
)

func (idx *InvertedIndex) Search(query string) ([]int, error) {
	tokens := lexQuery(query)
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	values := make([]*roaring.Bitmap, 0, len(tokens))
	ops := make([]string, 0, len(tokens))
	expectValue := true

	for _, token := range tokens {
		switch token {
		case "(":
			if !expectValue {
				return nil, fmt.Errorf("unexpected token %q", token)
			}
			ops = append(ops, token)
		case ")":
			if expectValue {
				return nil, fmt.Errorf("unexpected token %q", token)
			}
			foundOpen := false
			for len(ops) > 0 {
				top := popString(&ops)
				if top == "(" {
					foundOpen = true
					break
				}
				if err := applyOperator(top, &values); err != nil {
					return nil, err
				}
			}
			if !foundOpen {
				return nil, fmt.Errorf("missing opening parenthesis")
			}
			expectValue = false
		case "AND", "OR":
			if expectValue {
				return nil, fmt.Errorf("unexpected token %q", token)
			}
			for len(ops) > 0 && ops[len(ops)-1] != "(" && precedence(ops[len(ops)-1]) >= precedence(token) {
				if err := applyOperator(popString(&ops), &values); err != nil {
					return nil, err
				}
			}
			ops = append(ops, token)
			expectValue = true
		default:
			if !expectValue {
				return nil, fmt.Errorf("unexpected token %q", token)
			}
			values = append(values, idx.termBitmap(token))
			expectValue = false
		}
	}

	if expectValue {
		return nil, fmt.Errorf("unexpected end of query")
	}

	for len(ops) > 0 {
		top := popString(&ops)
		if top == "(" {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		if err := applyOperator(top, &values); err != nil {
			return nil, err
		}
	}

	if len(values) != 1 {
		return nil, fmt.Errorf("invalid query")
	}
	return bitmapToIntSlice(values[0]), nil
}

func (idx *InvertedIndex) SearchPrefix(prefix string) ([]int, error) {
	prefix = normalizePattern(prefix)
	if prefix == "" {
		return nil, fmt.Errorf("empty prefix")
	}

	bm := roaring.New()
	for term := range idx.terms {
		if strings.HasPrefix(term, prefix) {
			bm.Or(idx.loadPosting(term))
		}
	}
	return bitmapToIntSlice(bm), nil
}

func (idx *InvertedIndex) SearchWildcard(pattern string) ([]int, error) {
	pattern = normalizePattern(pattern)
	if pattern == "" {
		return nil, fmt.Errorf("empty wildcard")
	}
	if !strings.Contains(pattern, "*") {
		return bitmapToIntSlice(idx.loadPosting(pattern)), nil
	}

	candidates := idx.kgramCandidates(pattern)
	if len(candidates) == 0 {
		return nil, nil
	}

	bm := roaring.New()
	for _, term := range candidates {
		if wildcardMatch(pattern, term) {
			bm.Or(idx.loadPosting(term))
		}
	}
	return bitmapToIntSlice(bm), nil
}

func (idx *InvertedIndex) termBitmap(term string) *roaring.Bitmap {
	normalized := idx.normalizeWord(term)
	if normalized == "" {
		return roaring.New()
	}
	return idx.loadPosting(normalized)
}

func applyOperator(op string, values *[]*roaring.Bitmap) error {
	if len(*values) < 2 {
		return fmt.Errorf("unexpected operator %q", op)
	}

	right := popBitmap(values)
	left := popBitmap(values)

	switch op {
	case "AND":
		left.And(right)
	case "OR":
		left.Or(right)
	default:
		return fmt.Errorf("unknown operator %q", op)
	}

	*values = append(*values, left)
	return nil
}

func precedence(op string) int {
	if op == "AND" {
		return 2
	}
	if op == "OR" {
		return 1
	}
	return 0
}

func popString(stack *[]string) string {
	s := *stack
	last := s[len(s)-1]
	*stack = s[:len(s)-1]
	return last
}

func popBitmap(stack *[]*roaring.Bitmap) *roaring.Bitmap {
	s := *stack
	last := s[len(s)-1]
	*stack = s[:len(s)-1]
	return last
}

func bitmapToIntSlice(bm *roaring.Bitmap) []int {
	if bm == nil || bm.IsEmpty() {
		return nil
	}
	out := make([]int, 0, bm.GetCardinality())
	it := bm.Iterator()
	for it.HasNext() {
		out = append(out, int(it.Next()))
	}
	return out
}

func lexQuery(query string) []string {
	var tokens []string
	var cur strings.Builder

	flushWord := func() {
		if cur.Len() == 0 {
			return
		}
		word := cur.String()
		upper := strings.ToUpper(word)
		if upper == "AND" || upper == "OR" {
			tokens = append(tokens, upper)
		} else {
			tokens = append(tokens, word)
		}
		cur.Reset()
	}

	for _, r := range query {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			cur.WriteRune(r)
		case unicode.IsSpace(r):
			flushWord()
		case r == '(' || r == ')':
			flushWord()
			tokens = append(tokens, string(r))
		default:
			flushWord()
		}
	}
	flushWord()
	return tokens
}

func normalizePattern(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '*' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (idx *InvertedIndex) kgramCandidates(pattern string) []string {
	required := patternKgrams(pattern, idx.k)
	var current map[string]struct{}

	for _, gram := range required {
		terms := idx.kgrams[gram]
		if len(terms) == 0 {
			return nil
		}
		if current == nil {
			current = cloneTermSet(terms)
			continue
		}
		for t := range current {
			if _, ok := terms[t]; !ok {
				delete(current, t)
			}
		}
		if len(current) == 0 {
			return nil
		}
	}

	if current == nil {
		current = cloneTermSet(idx.terms)
	}
	out := make([]string, 0, len(current))
	for term := range current {
		out = append(out, term)
	}
	return out
}

func patternKgrams(pattern string, k int) []string {
	if k <= 0 {
		return nil
	}
	padded := "$" + pattern + "$"
	out := make([]string, 0, len(padded))
	seen := make(map[string]struct{})
	for i := 0; i <= len(padded)-k; i++ {
		gram := padded[i : i+k]
		if strings.ContainsRune(gram, '*') {
			continue
		}
		if _, ok := seen[gram]; ok {
			continue
		}
		seen[gram] = struct{}{}
		out = append(out, gram)
	}
	return out
}

func cloneTermSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func wildcardMatch(pattern, term string) bool {
	expr := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, ".*") + "$"
	re := regexp.MustCompile(expr)
	return re.MatchString(term)
}
