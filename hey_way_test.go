package hey

func assert(value any, prepare string) {
	switch v := value.(type) {
	case *SQL:
		if v.Prepare != prepare {
			panic("assert failed:\ngot:" + v.Prepare + "\nexpect:" + prepare)
		}
	case Maker:
		script := v.ToSQL()
		if script.Prepare != prepare {
			panic("assert failed:\ngot:" + script.Prepare + "\nexpect:" + prepare)
		}
	default:
		panic("unexpected data")
	}
}

var way *Way

func init() {
	opts := make([]Option, 0, 1<<3)
	if false {
		opts = append(opts, WithConfig(nil))
		opts = append(opts, WithDatabase(nil))
		opts = append(opts, WithTrack(nil))
		opts = append(opts, WithReader(nil))
	}
	way = NewWay(opts...)
}
