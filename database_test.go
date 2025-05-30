package hey

import (
	"fmt"
	"testing"
)

func TestNewWay(t *testing.T) {
	way := NewWay(nil)
	way.cfg.Manual = Mysql()
	a := way.Get("user1")
	b := way.Get("user2")
	c := way.Get("user3")
	ta := way.TA()
	tb := way.TB()
	tc := way.TC()
	get := way.Get().Subquery(a, ta.Alias()).Join(func(join QueryJoin) {
		join.LeftJoin(nil, join.NewSubquery(b, tb.Alias()), join.OnEqual("id1", "id2"))
		join.LeftJoin(nil, join.NewSubquery(c, tc.Alias()), join.On(join.OnEqual("id1", "id2"), join.OnEqual("name1", "name2")))
	})
	fmt.Println(get.Cmd())
}
