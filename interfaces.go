package db_bolt

type BoltModel interface {
	PrimaryKey() string
	Bucket() []byte
}

type DatabaseBackend interface {
	Initialize(databaseFile string, verbose bool, blotModels ...BoltModel) error
	ConnectionEstablish(callback func(connection DatabaseBackendConnection) error) error
}

type DatabaseBackendConnection interface {
	CreateDatabase(model BoltModel) error
	FindAll(dataSlice interface{}) error
	FindById(id string, data BoltModel) error
	Persist(data BoltModel) error
	SaveOrUpdate(data BoltModel) error
	Delete(data BoltModel) error
}
