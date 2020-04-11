package db

import (
	"fmt"
	"github.com/callingsid/shopping_utils/logger"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

const (
	hosts    = "127.0.0.1:27017"
	//hosts = "mongod-0:27017"
	database = "shopping"
	username = ""
	password = ""
)

var (
	Client mongoInterface = &mongoClient{}
)

type mongoInterface interface {
	setClient(session *mgo.Session)
	Get( string, bson.ObjectId) (interface{}, error)
	Create( string, interface{}) error
}

type mongoClient struct {
	client *mgo.Session
}


func init() {
	info := &mgo.DialInfo{
		Addrs:    []string{hosts},
		Timeout:  60 * time.Second,
		Database: database,
		Username: username,
		Password: password,
	}
	client, err := mgo.DialWithInfo(info)
	if err != nil {
		panic(err)
	} else {
		logger.Info("mongodb connection created successfully")
	}
	Client.setClient(client)
}

func (c *mongoClient) setClient(client *mgo.Session)  {
	c.client = client
}

func (c *mongoClient) Create(collection string, data interface{}) error {
	logger.Info(fmt.Sprintf("saving to MongoDB and collection is  %s\n", collection))
	col := c.client.DB(database).C(collection)

	//Insert job into MongoDB
	logger.Info(fmt.Sprintf("the entry is %v", data))
	//data = fmt.Sprintf("the entry is %v", data)
	//i := bson.NewObjectId()
	//errMongo := col.Insert(bson.M{"_id": i},data)
	errMongo := col.Insert(data)
	if errMongo != nil {
		logger.Error("Mongo error ", errMongo)
		return errMongo
	}
	logger.Info(fmt.Sprintf("Saved to MongoDB : %s", collection))
	return nil
}

func (c *mongoClient) Get(collection string, id bson.ObjectId) (interface{}, error) {
	var data interface{}
	logger.Info(fmt.Sprintf("id is   %s\n", id))
	col := c.client.DB(database).C(collection)
	err := col.Find(bson.M{"id": id}).One(&data)

	if err != nil {
		logger.Info(fmt.Sprintf("The database entry with id %s not found", id))
		logger.Error("database error ", err)
		return nil, err
	}

	logger.Info(fmt.Sprintf("The data returned from db is %s", data))
	return data, nil
}





