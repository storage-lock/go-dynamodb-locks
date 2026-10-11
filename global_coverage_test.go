package dynamodb_locks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodb_storage "github.com/storage-lock/go-dynamodb-storage"
	"github.com/storage-lock/go-storage"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

// fakeStorage 内存假存储：满足 CAS+可靠时间，走通锁创建与加解锁，不依赖真实 DynamoDB。
type fakeStorage struct {
	data map[string]string
	vers map[string]storage.Version
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{data: map[string]string{}, vers: map[string]storage.Version{}}
}

func (f *fakeStorage) GetName() string { return "fake-dynamodb-storage" }
func (f *fakeStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime, storage.CapabilityAtomicDelete}
}
func (f *fakeStorage) Init(ctx context.Context) error  { return nil }
func (f *fakeStorage) Close(ctx context.Context) error { return nil }
func (f *fakeStorage) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}
func (f *fakeStorage) Get(ctx context.Context, lockId string) (string, error) {
	v, ok := f.data[lockId]
	if !ok {
		return "", storage_lock.ErrLockNotFound
	}
	return v, nil
}
func (f *fakeStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, info *storage.LockInformation) error {
	if _, ok := f.data[lockId]; ok {
		return storage_lock.ErrLockAlreadyExists
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = version
	return nil
}
func (f *fakeStorage) UpdateWithVersion(ctx context.Context, lockId string, expected, newVersion storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrVersionMiss
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = newVersion
	return nil
}
func (f *fakeStorage) DeleteWithVersion(ctx context.Context, lockId string, expected storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrLockNotFound
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	delete(f.data, lockId)
	delete(f.vers, lockId)
	return nil
}
func (f *fakeStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	infos := make([]*storage.LockInformation, 0, len(f.data))
	for _, s := range f.data {
		info, err := storage.LockInformationFromJsonString(s)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return go_iterator.FromSlice(infos), nil
}

func testDynamoDBClient() *dynamodb.Client {
	return dynamodb.New(dynamodb.Options{Region: "us-east-1"})
}

func withFakeDynamoDBStorage() func() {
	old := newDynamoDBStorage
	newDynamoDBStorage = func(ctx context.Context, options *dynamodb_storage.DynamoDBStorageOptions) (storage.Storage, error) {
		return newFakeStorage(), nil
	}
	return func() { newDynamoDBStorage = old }
}

func TestDynamoDBClientConnectionManager(t *testing.T) {
	client := testDynamoDBClient()
	m := &dynamodbClientConnectionManager{client: client}
	assert.Equal(t, "dynamodb-client-connection-manager", m.Name())

	ctx := context.Background()
	got, err := m.Take(ctx)
	assert.Nil(t, err)
	assert.Equal(t, client, got)
	assert.Nil(t, m.Return(ctx, got))
	assert.Nil(t, m.Shutdown(ctx))
}

func TestGetDynamoDBLockFactoryByClient(t *testing.T) {
	restore := withFakeDynamoDBStorage()
	defer restore()

	ctx := context.Background()
	client := testDynamoDBClient()

	factory, err := GetDynamoDBLockFactoryByClient(ctx, client, "test-table")
	assert.Nil(t, err)
	assert.NotNil(t, factory)

	// 缓存命中
	factory2, err := GetDynamoDBLockFactoryByClient(ctx, client, "test-table")
	assert.Nil(t, err)
	assert.Equal(t, factory, factory2)

	// 端到端加解锁
	lock, err := NewDynamoDBLockByClient(ctx, client, "test-table", "test-dynamodb-lock")
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, lock.Lock(ctx, "owner-1"))
	assert.Nil(t, lock.UnLock(ctx, "owner-1"))
}

func TestGetDynamoDBLockFactoryByClient_StorageError(t *testing.T) {
	old := newDynamoDBStorage
	newDynamoDBStorage = func(ctx context.Context, options *dynamodb_storage.DynamoDBStorageOptions) (storage.Storage, error) {
		return nil, errors.New("boom")
	}
	defer func() { newDynamoDBStorage = old }()

	ctx := context.Background()
	// 独立 client 避免命中成功用例的缓存
	errClient := dynamodb.New(dynamodb.Options{Region: "us-west-2"})

	_, err := GetDynamoDBLockFactoryByClient(ctx, errClient, "test-table")
	assert.NotNil(t, err)
	_, err = NewDynamoDBLockByClient(ctx, errClient, "test-table", "x")
	assert.NotNil(t, err)
}

func TestNewDynamoDBStorage_DefaultPassthrough(t *testing.T) {
	// 直调默认透传体：空 options 触发 Check 快速失败，不联网。
	ctx := context.Background()
	_, err := newDynamoDBStorage(ctx, &dynamodb_storage.DynamoDBStorageOptions{})
	assert.NotNil(t, err)
}
