package functions

// registerDirectAliases registers function aliases with identical behavior.
// This calls category-specific registration methods to keep files small.
func (r *FunctionAliasRegistry) registerDirectAliases() {
	r.registerDateTimeFuncs()
	r.registerStringFuncs()
	r.registerMathFuncs()
	r.registerAggregateFuncs()
	r.registerWindowFuncs()
	r.registerConditionalFuncs()
	r.registerJSONFuncs()
	r.registerMiscFuncs()
}
