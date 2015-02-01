package catena

//go:generate listgen -package catena -list-type pointList -value-type Point -cmp "return int(a.Timestamp-b.Timestamp)" -out pointslist.go
//go:generate listgen -package catena -list-type sourcelist -value-type *memorySource -cmp "if a.name < b.name {return -1}; if a.name > b.name {return 1}; return 0;" -out memorysourcelist.go
//go:generate listgen -package catena -list-type metriclist -value-type *memoryMetric -cmp "if a.name < b.name {return -1}; if a.name > b.name {return 1}; return 0;" -out memorymetriclist.go
//go:generate gofmt -w pointslist.go
//go:generate gofmt -w memorysourcelist.go
//go:generate gofmt -w memorymetriclist.go
