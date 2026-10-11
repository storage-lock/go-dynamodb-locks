package dynamodb_locks

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodb_storage "github.com/storage-lock/go-dynamodb-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_lock_factory "github.com/storage-lock/go-storage-lock-factory"
)

// dynamodbClientConnectionManager 包装已有的 dynamodb.Client
type dynamodbClientConnectionManager struct {
	client *dynamodb.Client
}

func (m *dynamodbClientConnectionManager) Name() string {
	return "dynamodb-client-connection-manager"
}

func (m *dynamodbClientConnectionManager) Take(ctx context.Context) (*dynamodb.Client, error) {
	return m.client, nil
}

func (m *dynamodbClientConnectionManager) Return(ctx context.Context, connection *dynamodb.Client) error {
	return nil
}

func (m *dynamodbClientConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

var dynamodbClientBeanFactory = storage_lock_factory.NewStorageLockFactoryBeanFactory[*dynamodb.Client, *dynamodb.Client]()

// newDynamoDBStorage 构造存储，包级变量以便单测注入假存储/构造失败。
// 默认透传真实 dynamodb_storage.NewDynamoDBStorage。
// 本地无 DynamoDB 真后端时，单测注入内存假存储走通成功路径。
var newDynamoDBStorage = func(ctx context.Context, options *dynamodb_storage.DynamoDBStorageOptions) (storage.Storage, error) {
	return dynamodb_storage.NewDynamoDBStorage(ctx, options)
}

// NewDynamoDBLockByClient 从已有 DynamoDB Client 创建分布式锁
func NewDynamoDBLockByClient(ctx context.Context, client *dynamodb.Client, tableName string, lockId string) (*storage_lock.StorageLock, error) {
	factory, err := GetDynamoDBLockFactoryByClient(ctx, client, tableName)
	if err != nil {
		return nil, err
	}
	return factory.CreateLock(lockId)
}

// GetDynamoDBLockFactoryByClient 获取基于 DynamoDB Client 的锁工厂
func GetDynamoDBLockFactoryByClient(ctx context.Context, client *dynamodb.Client, tableName string) (*storage_lock_factory.StorageLockFactory[*dynamodb.Client], error) {
	return dynamodbClientBeanFactory.GetOrInit(ctx, client, func(ctx context.Context) (*storage_lock_factory.StorageLockFactory[*dynamodb.Client], error) {
		cm := dynamodb_storage.NewDynamoDBConnectionManagerFromClient(client)
		options := dynamodb_storage.NewDynamoDBStorageOptions().
			SetConnectionManager(cm).
			SetTableName(tableName).
			SetTimeProvider(&dynamodb_storage.LocalTimeProvider{})
		storage, err := newDynamoDBStorage(ctx, options)
		if err != nil {
			return nil, err
		}
		return storage_lock_factory.NewStorageLockFactory[*dynamodb.Client](storage, nil), nil
	})
}
