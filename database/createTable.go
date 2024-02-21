package database

import (
	"context"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type AtributesDefinitions []types.AttributeDefinition

type TableProvisionedThroughput struct {
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

func (tpt *TableProvisionedThroughput) Generate() *types.ProvisionedThroughput {
	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(tpt.ReadCapacityUnits),
		WriteCapacityUnits: aws.Int64(tpt.WriteCapacityUnits),
	}

}

func NewDefaultAtributesDefinitions() AtributesDefinitions {
	return AtributesDefinitions{{
		AttributeName: aws.String("LicensePlate"),
		AttributeType: types.ScalarAttributeTypeS,
	}, {
		AttributeName: aws.String("TypeVehicle"),
		AttributeType: types.ScalarAttributeTypeS,
	}}
}

type KeySchema []types.KeySchemaElement

func NewDefaultKeySchema() KeySchema {
	return KeySchema{{
		AttributeName: aws.String("LicensePlate"),
		KeyType:       types.KeyTypeHash,
	}, {
		AttributeName: aws.String("TypeVehicle"),
		KeyType:       types.KeyTypeRange,
	}}
}

type VehicleTrackerTable struct {
	Client                *dynamodb.Client
	TableName             string
	AtributesDefinitions  AtributesDefinitions
	KeySchema             KeySchema
	ProvisionedThroughput *TableProvisionedThroughput
	BillingMode           types.BillingMode
}

type TableOptions func(*VehicleTrackerTable) error

func OptionTableName(name string) TableOptions {
	return func(t *VehicleTrackerTable) error {
		t.TableName = name
		return nil
	}
}

func OptionAtributesDefinitions(atributesDefinitions AtributesDefinitions) TableOptions {
	return func(t *VehicleTrackerTable) error {
		t.AtributesDefinitions = atributesDefinitions
		return nil
	}
}

func OptionKeySchema(keySchema KeySchema) TableOptions {
	return func(t *VehicleTrackerTable) error {
		t.KeySchema = keySchema
		return nil
	}
}

func OptionProvisionedThroughput(readCapacityUnits int64, writeCapacityUnits int64) TableOptions {
	return func(t *VehicleTrackerTable) error {
		t.ProvisionedThroughput = &TableProvisionedThroughput{ReadCapacityUnits: readCapacityUnits, WriteCapacityUnits: writeCapacityUnits}
		return nil
	}
}

func OptionBillingMode(billingMode types.BillingMode) TableOptions {
	return func(t *VehicleTrackerTable) error {
		t.BillingMode = billingMode
		return nil
	}
}
func NewVehicleTrackerTable(client *dynamodb.Client, opts ...TableOptions) (*VehicleTrackerTable, error) {
	provisionedThroughput := &TableProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 5}
	vtt := &VehicleTrackerTable{
		Client:                client,
		TableName:             "VehicleTracker",
		AtributesDefinitions:  NewDefaultAtributesDefinitions(),
		KeySchema:             NewDefaultKeySchema(),
		ProvisionedThroughput: provisionedThroughput,
		BillingMode:           types.BillingModeProvisioned,
	}

	for _, opt := range opts {
		if err := opt(vtt); err != nil {
			return nil, err
		}
	}
	return vtt, nil
}
func (vt *VehicleTrackerTable) Exist(ctx context.Context) (bool, error) {
	tableFind, err := vt.Client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return false, err
	}
	if slices.Contains(tableFind.TableNames, vt.TableName) {
		return true, nil
	}
	return false, nil

}
func (vt *VehicleTrackerTable) Create(ctx context.Context) error {
	_, err := vt.Client.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions:  vt.AtributesDefinitions,
		KeySchema:             vt.KeySchema,
		TableName:             aws.String(vt.TableName),
		ProvisionedThroughput: vt.ProvisionedThroughput.Generate(),
		BillingMode:           vt.BillingMode,
	})
	if err != nil {
		return err
	} else {
		waiter := dynamodb.NewTableExistsWaiter(vt.Client)
		err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(vt.TableName)}, 5*time.Minute)
		if err != nil {
			return err
		}

	}
	return nil
}

func (vt *VehicleTrackerTable) CreateIfNotExist(ctx context.Context) error {
	exist, err := vt.Exist(ctx)
	if err != nil {
		return err
	}
	if exist {
		return nil
	} else {
		err = vt.Create(ctx)
		if err != nil {
			return err
		}
		return nil
	}

}
