package dynamodb_locks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobalDynamoDBLockFactory_InitiallyInitialized(t *testing.T) {
	assert.NotNil(t, dynamodbClientBeanFactory)
}
