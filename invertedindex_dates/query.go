package invertedindex_dates

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/RoaringBitmap/roaring/v2"
)

func (idx *InvertedIndex) Search(query string) ([]int, error) {
	tokens, err := lexQuery(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	values := make([]*roaring.Bitmap, 0, len(tokens))
	ops := make([]string, 0, len(tokens))
	expectValue := true

	for _, tok := range tokens {
		switch {
		case tok == "(":
			if !expectValue {
				return nil, fmt.Errorf("unexpected token %q", tok)
			}
			ops = append(ops, tok)
		case tok == ")":
			if expectValue {
				return nil, fmt.Errorf("unexpected token %q", tok)
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
		case tok == "AND" || tok == "OR":
			if expectValue {
				return nil, fmt.Errorf("unexpected token %q", tok)
			}
			for len(ops) > 0 && ops[len(ops)-1] != "(" && precedence(ops[len(ops)-1]) >= precedence(tok) {
				if err := applyOperator(popString(&ops), &values); err != nil {
					return nil, err
				}
			}
			ops = append(ops, tok)
			expectValue = true
		case strings.HasPrefix(tok, "__RANGE__:"):
			if !expectValue {
				return nil, fmt.Errorf("unexpected token %q", tok)
			}
			bm, err := idx.bitmapFromRangeToken(tok)
			if err != nil {
				return nil, err
			}
			values = append(values, bm)
			expectValue = false
		default:
			if !expectValue {
				return nil, fmt.Errorf("unexpected token %q", tok)
			}
			values = append(values, idx.termBitmap(tok))
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

func (idx *InvertedIndex) bitmapFromRangeToken(tok string) (*roaring.Bitmap, error) {
	rest := strings.TrimPrefix(tok, "__RANGE__:")
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("internal range token")
	}
	from, err := time.ParseInLocation("2006-01-02", parts[1], time.UTC)
	if err != nil {
		return nil, err
	}
	to, err := time.ParseInLocation("2006-01-02", parts[2], time.UTC)
	if err != nil {
		return nil, err
	}
	switch parts[0] {
	case "D", "A":
		return idx.bitmapAppearedInRange(from, to), nil
	case "V":
		return idx.bitmapValidInRange(from, to), nil
	default:
		return nil, fmt.Errorf("internal range kind")
	}
}

func (idx *InvertedIndex) SearchDateInRange(from, to time.Time) []int {
	return bitmapToIntSlice(idx.bitmapAppearedInRange(from, to))
}

func (idx *InvertedIndex) SearchValidInRange(from, to time.Time) []int {
	return bitmapToIntSlice(idx.bitmapValidInRange(from, to))
}

func (idx *InvertedIndex) SearchAppearedInRange(from, to time.Time) []int {
	return bitmapToIntSlice(idx.bitmapAppearedInRange(from, to))
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

func lexQuery(query string) ([]string, error) {
	var tokens []string
	var cur strings.Builder
	s := query

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

	rangeToken := func(kind rangeKind, from, to time.Time) string {
		kch := byte('D')
		switch kind {
		case rangeValid:
			kch = 'V'
		case rangeAppeared:
			kch = 'A'
		}
		return fmt.Sprintf("__RANGE__:%c:%s:%s", kch, from.Format("2006-01-02"), to.Format("2006-01-02"))
	}

	for i := 0; i < len(s); {
		r := rune(s[i])
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			cur.WriteByte(s[i])
			i++
		case unicode.IsSpace(r):
			flushWord()
			i++
		case s[i] == '(' || s[i] == ')':
			flushWord()
			tokens = append(tokens, string(s[i]))
			i++
		case s[i] == '[':
			var kind rangeKind
			if cur.Len() == 0 {
				kind = rangeDateAttr
			} else {
				switch strings.ToUpper(cur.String()) {
				case "DATE":
					kind = rangeDateAttr
				case "VALID":
					kind = rangeValid
				case "APPEARED":
					kind = rangeAppeared
				default:
					return nil, fmt.Errorf("unexpected [ after %q", cur.String())
				}
				cur.Reset()
			}
			from, to, end, err := parseDateRangeTail(s, i+1)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, rangeToken(kind, from, to))
			i = end
		default:
			flushWord()
			return nil, fmt.Errorf("unexpected character %q at %d", r, i)
		}
	}
	flushWord()
	return tokens, nil
}
