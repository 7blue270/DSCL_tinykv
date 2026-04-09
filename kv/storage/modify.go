package storage

// Modify is a single modification to TinyKV's underlying storage.
// 这个代码定义了tinykv存储层写入时使用的“修改描述”数据结构：把一次写操作（Put 或 Delete）抽象成统一的 Modify 类型，方便批量提交。
type Modify struct {
	Data interface{}
}

type Put struct {
	Key   []byte
	Value []byte
	Cf    string
}

type Delete struct {
	Key []byte
	Cf  string
}

func (m *Modify) Key() []byte {
	switch m.Data.(type) {
	case Put:
		return m.Data.(Put).Key
	case Delete:
		return m.Data.(Delete).Key
	}
	return nil
}

func (m *Modify) Value() []byte {
	//只有Put操作才有Value，Delete没有，所以只处理Put的情况
	if putData, ok := m.Data.(Put); ok {
		return putData.Value
	}

	return nil
}

func (m *Modify) Cf() string {
	switch m.Data.(type) {
	case Put:
		return m.Data.(Put).Cf
	case Delete:
		return m.Data.(Delete).Cf
	}
	return ""
}
