package kv

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/grafana/sobek"	
	"go.k6.io/k6/js/modules"
)

type (
	// KV is the global module instance that will create Client
	// instances for each VU.
	KV struct{}

	// ModuleInstance represents an instance of the JS module.
	ModuleInstance struct {
		vu modules.VU
		*Client
	}
)

// Ensure the interfaces are implemented correctly
var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &KV{}
)

type Client struct {
	vu  modules.VU
	db 	*badger.DB
}

var clients map[string]*Client

func init() {
	clients = make(map[string]*Client)
	modules.Register("k6/x/kv", new(KV))	
}

// New returns a pointer to a new KV instance
func New() *KV {
	return &KV{}
}

// NewModuleInstance implements the modules.Module interface and returns
// a new instance for each VU.
func (*KV) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{vu: vu, Client: &Client{vu: vu}}
}

// Exports implements the modules.Instance interface and returns
// the exports of the JS module.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{Named: map[string]interface{}{
		"Client": mi.NewClient,
	}}
}

// NewClient is the JS constructor for the Client
// Arguments :
// 	1 arg :  kv_name
//	2 args : kv_name, filename
// If filename="" then memory=true
func (mi *ModuleInstance) NewClient(call sobek.ConstructorCall) *sobek.Object {	
	rt := mi.vu.Runtime()

	var name string = ""
	var kvName string = ""
	var memory bool = false

	if len(call.Arguments) == 1 {
		kvName = call.Arguments[0].String()	
		//fmt.Printf("call.Arguments=%i, kvname=%s\n", call.Arguments, kvName)
		memory=true	
	}

	if len(call.Arguments) == 2 {
		kvName = call.Arguments[0].String()
		name = call.Arguments[1].String()	
		if name=="" {
			memory=true
		}
		//fmt.Printf("call.Arguments=%i, kvname=%s, filename=%s, memory=%t\n", call.Arguments, kvName, name, memory)	
	}
	
	if kvName==""{
		kvName="default"
	}

	if client, exists:=clients[kvName];!exists {
		var db *badger.DB
		if memory {
			db, _ = badger.Open(badger.DefaultOptions("").WithLoggingLevel(badger.ERROR).WithInMemory(true))
			//fmt.Printf("Create badger in memory\n")	
		} else {
			db, _ = badger.Open(badger.DefaultOptions(name).WithLoggingLevel(badger.ERROR))
			//fmt.Printf("Create badger in disk\n")	
		}
		client = &Client{vu:mi.vu,db: db}
		clients[kvName]=client

		return rt.ToValue(client).ToObject(rt)	
	} else {
		return rt.ToValue(client).ToObject(rt)
	}
}

// Set the given key with the given value.
func (c *Client) Set(key string, value string) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})
	return err
}

// Set the given key with the given value with TTL in second
func (c *Client) SetWithTTLInSecond(key string, value string, ttl int) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), []byte(value)).WithTTL((time.Duration(ttl) * time.Second))
		err := txn.SetEntry(e)
		return err
	})
	return err
}

// Get returns the value for the given key.
func (c *Client) Get(key string) (string, error) {
	var valCopy []byte
	_ = c.db.View(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte(key))
		if item != nil {
			valCopy, _ = item.ValueCopy(nil)
		}
		return nil
	})
	if len(valCopy) > 0 {
		return string(valCopy), nil
	}
	return "", fmt.Errorf("error in get value with key %s", key)
}

// Pop returns the value for the given key and remove it
func (c *Client) Pop(key string) (string, error) {
	var valCopy []byte
	_ = c.db.Update(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte(key))
		if item != nil {
			valCopy, _ = item.ValueCopy(nil)
			_ = txn.Delete([]byte(key))
		}
		return nil;
	})
	if len(valCopy) > 0 {
		return string(valCopy), nil
	}
	return "", fmt.Errorf("error in get value with key %s", key)
}


func (c *Client) PopFirst() (string, error) {
	var key []byte
	_ = c.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Rewind(); it.Valid(); it.Next() {
		  item := it.Item()
		  key := item.Key()
		  _ = txn.Delete([]byte(key))
		  fmt.Printf("First() - key=%s\n", key)	
		  break
		}
		return nil
	  })

	fmt.Printf("First() - check len(key) > 0 key=%s\n", string(key))	
	//fmt.Printf("First() - len key=%i\n", len(key))	

	if len(key) > 0  {
		fmt.Printf("First() - len(key) > 0 key=%s\n", key)	
		return string(key), nil
	}
	return "", fmt.Errorf("First() - no data")
}

// Display the keys - values
func (c *Client) Show() error {
	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
		  item := it.Item()
		  k := item.Key()
		  err := item.Value(func(v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		  })
		  if err != nil {
			return err
		  }
		}
		return nil
	  })
	return err
}


// ViewPrefix return all the key value pairs where the key starts with some prefix.
func (c *Client) ViewPrefix(prefix string) map[string]string {
	m := make(map[string]string)
	c.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				m[string(k)] = string(v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return m
}

// Delete the given key
func (c *Client) Delete(key string) error {
	err := c.db.Update(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte(key))
		if item != nil {
			err := txn.Delete([]byte(key))
			return err
		}
		return nil
	})
	return err
}
