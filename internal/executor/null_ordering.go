package executor

func compareOrderByValues(a, b any, nullsFirst *bool, desc bool, collationName string) (int, bool) {
	aIsNil := a == nil
	bIsNil := b == nil
	if aIsNil || bIsNil {
		if aIsNil && bIsNil {
			return 0, true
		}
		nf := desc
		if nullsFirst != nil {
			nf = *nullsFirst
		}
		if aIsNil {
			if nf {
				return -1, true
			}
			return 1, true
		}
		if nf {
			return 1, true
		}
		return -1, true
	}
	return compareWithCollation(a, b, collationName), false
}
