package valuable

// Valuable structs can be converted into an array to be persisted
type Valuable interface {
	Values() []interface{}
}
