package nabu

var emptyQuery = new(EmptyQuery)

type EmptyQuery struct{}

func (q *EmptyQuery) NoCache() Query {
	return q
}
func (q *EmptyQuery) Where(index string, condition Condition) Query {
	return q
}
func (q *EmptyQuery) Desc() Query {
	return q
}
func (q *EmptyQuery) Limit(limit int) Query {
	return q
}
func (q *EmptyQuery) Offset(offset int) Query {
	return q
}
func (q *EmptyQuery) IncludeTotal() Query {
	return q
}
func (q *EmptyQuery) Execute() Result {
	return EmptyResult
}
