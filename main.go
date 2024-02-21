package main

import (
	"context"
	"log"
	"medium-article-dynamodb/database"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatal(err)
	}
	svc := dynamodb.NewFromConfig(cfg)
	table, err := database.NewVehicleTrackerTable(svc)
	if err != nil {
		log.Fatal(err)
	}
	err = table.CreateIfNotExist(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

}
