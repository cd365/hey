package main

import (
	"database/sql"

	"github.com/cd365/hey"
)

type User struct {
	Id        int64   `json:"-" db:"id"`
	Username  string  `json:"username" db:"username"`
	Nickname  string  `json:"nickname" db:"nickname"`
	Company   *string `json:"company" db:"company"`
	Age       int     `json:"age" db:"age"`
	Money     float64 `json:"money" db:"money"`
	CreatedAt int64   `json:"created_at" db:"created_at"`
	UpdatedAt int64   `json:"updated_at" db:"updated_at"`
	DeletedAt int64   `json:"-" db:"deleted_at"`
}

var db *sql.DB

var way = hey.NewWay(db, hey.WithPrepare(hey.DefaultPgsql.Prepare))

var table = "account"

var timestamp = way.Now().Unix()

func Insert() {
	add := way.Add(table)

	// insert a piece of data

	// scene 1, specify fields and field corresponding values
	_, _ = add.FieldValue("name", "Jack").
		FieldValue("email", "jack@gmail.com").
		DefaultFieldValue("created_at", timestamp).
		Add()

	// scene 2, through structure
	create := &User{
		Username:  "Alice",
		CreatedAt: timestamp,
		UpdatedAt: timestamp,
	}
	_ = add.Except("id").Create(create).
		Returning(func(row *sql.Row) error {
			return row.Scan(&create.Id)
		}, "id")

	// scene 2-1, through map
	createMap := map[string]interface{}{
		"username":   "Alice",
		"created_at": timestamp,
		"updated_at": timestamp,
	}
	var createMapId int64
	_ = add.Except("id").Create(createMap).
		Returning(func(row *sql.Row) error {
			return row.Scan(&createMapId)
		}, "id")

	create.Nickname = "harbor"
	create.Age = 18
	create.Money = 100.05

	// scene 3, insert or update data
	add.Except("id").Create(create).OnConflict(
		hey.DefaultPgsql.InsertOnConflict(
			[]string{"username"},
			[]string{"nickname", "age", "money"},
		),
	)
	// only insert or update a row data
	// _ = add.Returning(func(row *sql.Row) error {
	// 	return row.Scan(&create.Id, &create.Username)
	// }, "id", "username")
	_, _ = add.Add()

	// insert data in batches

	// scene 1, set fields and corresponding field value lists for multiple pieces of data respectively
	addFields := []string{
		"name",
		"email",
	}
	addValues := [][]interface{}{
		{
			"Alice",
			"alice@gmail.com",
		},
		{
			"Tom",
			"tom@gmail.com",
		},
	}
	_, _ = add.FieldsValues(addFields, addValues).Add()

	// scene 2, through structure
	_, _ = add.Except("id").Create([]*User{
		&User{Username: "Alice"},
		&User{Username: "Jerry"},
	}).Add()

	// scene 3, by querying the result set
	fields := []string{"username", "nickname", "company", "age", "money", "created_at", "updated_at", "deleted_at"}
	_, _ = add.Fields(fields).ValuesSubQueryGet(way.Get("target_table").Column(fields...).Where(nil).Asc("id").Limit(1000)).Add()
}

func Delete() {
	del := way.Del(table)
	_, _ = del.Where(way.F().In("id", 1, 2, 3)).Del()
}

func Update() {
	mod := way.Mod(table)
	_, _ = mod.Where(way.F().In("id", 1, 2, 3)).
		Incr("times", 1).
		Set("name", "Jerry").
		DefaultSet("updated_at", way.Now().Unix()).
		Mod()
}

func Query() {
	// select count
	get := way.Get(table)
	get.Where(way.F().IsNotNull("id")).Count()

	result := make([]*User, 0)
	query := get.Column("username", "company").
		Where(way.F().Greater("id", 0)).
		Desc("id").
		Limit(10).
		Offset(0)

	// query rows 1, by reflect
	_ = query.Get(&result)

	// query rows 2, by custom
	_ = query.Query(func(rows *sql.Rows) (err error) {
		for rows.Next() {
			company := ""
			tmp := &User{}
			tmp.Company = &company
			if err = rows.Scan(&tmp.Username, &tmp.Company); err != nil {
				return
			}
			result = append(result, tmp)
		}
		return nil
	})
}

func Transaction() {
	way.TxTry(func(tx *hey.Way) (err error) {
		// todo...
		tx.Get(table)
		// todo...
		tx.Add(table).Add()
		tx.Del(table).Del()
		tx.Mod(table).Mod()
		// todo...
		return
	}, 1)
}

func main() {

	Query()

	Delete()

	Insert()

	Update()

	Transaction()

}
