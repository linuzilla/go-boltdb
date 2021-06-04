package dbBolt

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/linuzilla/go-reflex"
	"time"
)

type BoltDbBackend struct {
	databaseFile string
}

type boltConnection struct {
	db      *bolt.DB
	backend *BoltDbBackend
}

var _ DatabaseBackendConnection = (*boltConnection)(nil)
var _ DatabaseBackend = (*BoltDbBackend)(nil)

var BoltBackend BoltDbBackend

///////////////////////////////////////////////////

func (backend *BoltDbBackend) Initialize(databaseFileName string, verbose bool, blotModels ...BoltModel) error {
	if verbose {
		fmt.Println("Initialize database: " + databaseFileName)
	}

	backend.databaseFile = databaseFileName

	return backend.ConnectionEstablish(func(connection DatabaseBackendConnection) (err error) {
		for _, blotModel := range blotModels {
			err = connection.CreateDatabase(blotModel)

			if err != nil {
				break
			}
		}

		return
	})
}

func (backend *BoltDbBackend) ConnectionEstablish(callback func(connection DatabaseBackendConnection) error) error {
	if db, err := bolt.Open(backend.databaseFile, 0600, &bolt.Options{Timeout: 1 * time.Second}); err != nil {
		return err
	} else {
		defer db.Close()
		return callback(&boltConnection{db: db, backend: backend})
	}
}

///////////////////////////////////////////////////

func (conn *boltConnection) CreateDatabase(model BoltModel) error {
	return conn.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(model.Bucket()); err != nil {
			return fmt.Errorf("create bucket: %v", err)
		}
		return nil
	})
}

func (conn *boltConnection) FindAll(dataSlice interface{}) error {
	r := reflex.New(dataSlice)
	entry := r.Instance().(BoltModel)

	return conn.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(entry.Bucket()).ForEach(func(k, v []byte) error {
			instance := r.NewInstance()
			err := json.Unmarshal(v, instance)
			if err == nil {
				r.Append(instance)
			}
			return err
		})
	})
}

func (conn *boltConnection) FindById(id string, data BoltModel) error {
	return conn.db.View(func(tx *bolt.Tx) error {
		bytes := tx.Bucket(data.Bucket()).Get([]byte(id))

		if len(bytes) == 0 {
			return fmt.Errorf("%s: not found", id)
		} else {
			return json.Unmarshal(bytes, data)
		}
	})
}

func (conn *boltConnection) Persist(data BoltModel) error {
	return conn.SaveOrUpdate(data)
}

func (conn *boltConnection) SaveOrUpdate(data BoltModel) error {
	return conn.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(data.Bucket())
		if jsonBlob, err := json.Marshal(data); err != nil {
			return err
		} else {
			return bucket.Put([]byte(data.PrimaryKey()), jsonBlob)
		}
	})
}

func (conn *boltConnection) Delete(data BoltModel) error {
	return conn.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(data.Bucket()).Delete([]byte(data.PrimaryKey()))
	})
}
