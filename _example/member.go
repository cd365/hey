package _example

// Member | member | member table
type Member struct {
	Id        int64   `json:"id" db:"id"`                 // id
	MemberId  int64   `json:"member_id" db:"member_id"`   // member-id
	Username  string  `json:"username" db:"username"`     // username
	Password  string  `json:"password" db:"password"`     // password
	Avatar    string  `json:"avatar" db:"avatar"`         // avatar
	Nickname  string  `json:"nickname" db:"nickname"`     // nickname
	Mobile    string  `json:"mobile" db:"mobile"`         // phone number
	Email     string  `json:"email" db:"email"`           // email
	Balance   float64 `json:"balance" db:"balance"`       // balance
	Category  int     `json:"category" db:"category"`     // category
	City      string  `json:"city" db:"city"`             // city
	Gender    int     `json:"gender" db:"gender"`         // gender {0: unknown, 1: male, 2: female}
	State     int     `json:"state" db:"state"`           // status {0:default,1:normal,2:abnormal,3:freeze,4:title}
	StateDesc string  `json:"state_desc" db:"state_desc"` // status describe
	AddAt     int64   `json:"add_at" db:"add_at"`         // insert timestamp
	ModAt     int64   `json:"mod_at" db:"mod_at"`         // update timestamp
	DelAt     int64   `json:"del_at" db:"del_at"`         // delete timestamp
}

func (s *Member) COPY() *Member {
	tmp := *s
	return &tmp
}

func (s *Member) COMPARE(c *Member) map[string]interface{} {
	tmp := make(map[string]interface{})
	if s.MemberId != c.MemberId {
		tmp["member_id"] = c.MemberId
	}
	if s.Username != c.Username {
		tmp["username"] = c.Username
	}
	if s.Password != c.Password {
		tmp["password"] = c.Password
	}
	if s.Avatar != c.Avatar {
		tmp["avatar"] = c.Avatar
	}
	if s.Nickname != c.Nickname {
		tmp["nickname"] = c.Nickname
	}
	if s.Mobile != c.Mobile {
		tmp["mobile"] = c.Mobile
	}
	if s.Email != c.Email {
		tmp["email"] = c.Email
	}
	if s.Balance != c.Balance {
		tmp["balance"] = c.Balance
	}
	if s.Category != c.Category {
		tmp["category"] = c.Category
	}
	if s.City != c.City {
		tmp["city"] = c.City
	}
	if s.Gender != c.Gender {
		tmp["gender"] = c.Gender
	}
	if s.State != c.State {
		tmp["state"] = c.State
	}
	if s.StateDesc != c.StateDesc {
		tmp["state_desc"] = c.StateDesc
	}
	if s.ModAt != c.ModAt {
		tmp["mod_at"] = c.ModAt
	}
	if s.DelAt != c.DelAt {
		tmp["del_at"] = c.DelAt
	}
	if len(tmp) == 0 {
		return nil
	}
	return tmp
}

type HeyMember struct {
	Id         string // id
	MemberId   string // member-id
	Username   string // username
	Password   string // password
	Avatar     string // avatar
	Nickname   string // nickname
	Mobile     string // phone number
	Email      string // email
	Balance    string // balance
	Category   string // category
	City       string // city
	Gender     string // gender {0: unknown, 1: male, 2: female}
	State      string // status {0:default,1:normal,2:abnormal,3:freeze,4:title}
	StateDesc  string // status describe
	AddAt      string // insert timestamp
	ModAt      string // update timestamp
	DelAt      string // delete timestamp
	fieldMap   map[string]*struct{}
	fieldSlice []string
}

func (s *HeyMember) Table() string {
	return "member" // member table
}

func (s *HeyMember) Comment() string {
	return "member table"
}

func (s *HeyMember) Field(except ...string) []string {
	excepted := make(map[string]*struct{})
	for _, v := range except {
		excepted[v] = &struct{}{}
	}
	result := make([]string, 0, len(s.fieldSlice))
	for _, v := range s.fieldSlice {
		if _, ok := excepted[v]; ok {
			continue
		}
		result = append(result, v)
	}
	return result
}

func (s *HeyMember) FieldMap() map[string]*struct{} {
	result := make(map[string]*struct{}, len(s.fieldMap))
	for k, v := range s.fieldMap {
		result[k] = v
	}
	return result
}

func (s *HeyMember) FieldExist(field string) bool {
	_, exist := s.fieldMap[field]
	return exist
}

func NewMember() *HeyMember {
	s := &HeyMember{
		Id:        "id",         // id
		MemberId:  "member_id",  // member-id
		Username:  "username",   // username
		Password:  "password",   // password
		Avatar:    "avatar",     // avatar
		Nickname:  "nickname",   // nickname
		Mobile:    "mobile",     // phone number
		Email:     "email",      // email
		Balance:   "balance",    // balance
		Category:  "category",   // category
		City:      "city",       // city
		Gender:    "gender",     // gender {0: unknown, 1: male, 2: female}
		State:     "state",      // status {0:default,1:normal,2:abnormal,3:freeze,4:title}
		StateDesc: "state_desc", // status describe
		AddAt:     "add_at",     // insert timestamp
		ModAt:     "mod_at",     // update timestamp
		DelAt:     "del_at",     // delete timestamp
	}
	s.fieldMap = map[string]*struct{}{
		s.Id:        {}, // id
		s.MemberId:  {}, // member-id
		s.Username:  {}, // username
		s.Password:  {}, // password
		s.Avatar:    {}, // avatar
		s.Nickname:  {}, // nickname
		s.Mobile:    {}, // phone number
		s.Email:     {}, // email
		s.Balance:   {}, // balance
		s.Category:  {}, // category
		s.City:      {}, // city
		s.Gender:    {}, // gender {0: unknown, 1: male, 2: female}
		s.State:     {}, // status {0:default,1:normal,2:abnormal,3:freeze,4:title}
		s.StateDesc: {}, // status describe
		s.AddAt:     {}, // insert timestamp
		s.ModAt:     {}, // update timestamp
		s.DelAt:     {}, // delete timestamp
	}
	s.fieldSlice = []string{
		s.Id,        // id
		s.MemberId,  // member-id
		s.Username,  // username
		s.Password,  // password
		s.Avatar,    // avatar
		s.Nickname,  // nickname
		s.Mobile,    // phone number
		s.Email,     // email
		s.Balance,   // balance
		s.Category,  // category
		s.City,      // city
		s.Gender,    // gender {0: unknown, 1: male, 2: female}
		s.State,     // status {0:default,1:normal,2:abnormal,3:freeze,4:title}
		s.StateDesc, // status describe
		s.AddAt,     // insert timestamp
		s.ModAt,     // update timestamp
		s.DelAt,     // delete timestamp
	}
	return s
}

type AddMember struct {
	MemberId  int64   `json:"member_id" db:"member_id" validate:"omitempty"`                // member-id
	Username  string  `json:"username" db:"username" validate:"omitempty,min=0,max=32"`     // username
	Password  string  `json:"password" db:"password" validate:"omitempty,min=0,max=64"`     // password
	Avatar    string  `json:"avatar" db:"avatar" validate:"omitempty,min=0,max=255"`        // avatar
	Nickname  string  `json:"nickname" db:"nickname" validate:"omitempty,min=0,max=32"`     // nickname
	Mobile    string  `json:"mobile" db:"mobile" validate:"omitempty,min=0,max=32"`         // phone number
	Email     string  `json:"email" db:"email" validate:"omitempty,min=0,max=128"`          // email
	Balance   float64 `json:"balance" db:"balance" validate:"omitempty"`                    // balance
	Category  int     `json:"category" db:"category" validate:"omitempty"`                  // category
	City      string  `json:"city" db:"city" validate:"omitempty,min=0,max=128"`            // city
	Gender    int     `json:"gender" db:"gender" validate:"omitempty"`                      // gender {0: unknown, 1: male, 2: female}
	State     int     `json:"state" db:"state" validate:"omitempty"`                        // status {0:default,1:normal,2:abnormal,3:freeze,4:title}
	StateDesc string  `json:"state_desc" db:"state_desc" validate:"omitempty,min=0,max=32"` // status describe
}

type ModMember struct {
	Id        int64    `json:"id" db:"-" validate:"omitempty"`                               // id
	MemberId  *int64   `json:"member_id" db:"member_id" validate:"omitempty"`                // member-id
	Username  *string  `json:"username" db:"username" validate:"omitempty,min=0,max=32"`     // username
	Password  *string  `json:"password" db:"password" validate:"omitempty,min=0,max=64"`     // password
	Avatar    *string  `json:"avatar" db:"avatar" validate:"omitempty,min=0,max=255"`        // avatar
	Nickname  *string  `json:"nickname" db:"nickname" validate:"omitempty,min=0,max=32"`     // nickname
	Mobile    *string  `json:"mobile" db:"mobile" validate:"omitempty,min=0,max=32"`         // phone number
	Email     *string  `json:"email" db:"email" validate:"omitempty,min=0,max=128"`          // email
	Balance   *float64 `json:"balance" db:"balance" validate:"omitempty"`                    // balance
	Category  *int     `json:"category" db:"category" validate:"omitempty"`                  // category
	City      *string  `json:"city" db:"city" validate:"omitempty,min=0,max=128"`            // city
	Gender    *int     `json:"gender" db:"gender" validate:"omitempty"`                      // gender {0: unknown, 1: male, 2: female}
	State     *int     `json:"state" db:"state" validate:"omitempty"`                        // status {0:default,1:normal,2:abnormal,3:freeze,4:title}
	StateDesc *string  `json:"state_desc" db:"state_desc" validate:"omitempty,min=0,max=32"` // status describe
}
