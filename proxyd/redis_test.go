package proxyd

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestApplyPoolConfig_CustomValues(t *testing.T) {
	cfg := RedisConfig{
		PoolSize:          50,
		MinIdleConns:      10,
		MaxIdleConns:      30,
		ReadTimeoutMS:     2000,
		WriteTimeoutMS:    1500,
		DialTimeoutMS:     3000,
		PoolTimeoutMS:     4000,
		MaxRetries:        5,
		MinRetryBackoffMS: 16,
		MaxRetryBackoffMS: 1024,
	}

	opts := &redis.Options{}
	applyPoolConfig(opts, cfg)

	require.Equal(t, 50, opts.PoolSize)
	require.Equal(t, 10, opts.MinIdleConns)
	require.Equal(t, 30, opts.MaxIdleConns)
	require.Equal(t, 2000*time.Millisecond, opts.ReadTimeout)
	require.Equal(t, 1500*time.Millisecond, opts.WriteTimeout)
	require.Equal(t, 3000*time.Millisecond, opts.DialTimeout)
	require.Equal(t, 4000*time.Millisecond, opts.PoolTimeout)
	require.Equal(t, 5, opts.MaxRetries)
	require.Equal(t, 16*time.Millisecond, opts.MinRetryBackoff)
	require.Equal(t, 1024*time.Millisecond, opts.MaxRetryBackoff)
}

func TestApplyPoolConfig_Defaults(t *testing.T) {
	cfg := RedisConfig{}

	opts := &redis.Options{}
	applyPoolConfig(opts, cfg)

	require.Equal(t, defaultPoolSize, opts.PoolSize)
	require.Equal(t, defaultMinIdleConns, opts.MinIdleConns)
	require.Equal(t, 0, opts.MaxIdleConns)
	require.Equal(t, defaultReadTimeoutMS*time.Millisecond, opts.ReadTimeout)
	require.Equal(t, defaultWriteTimeoutMS*time.Millisecond, opts.WriteTimeout)
	require.Equal(t, defaultDialTimeoutMS*time.Millisecond, opts.DialTimeout)
	require.Equal(t, time.Duration(0), opts.PoolTimeout)
	require.Equal(t, defaultMaxRetries, opts.MaxRetries)
	require.Equal(t, defaultMinRetryBackoff*time.Millisecond, opts.MinRetryBackoff)
	require.Equal(t, defaultMaxRetryBackoff*time.Millisecond, opts.MaxRetryBackoff)
}

func TestApplyPoolConfig_DisableRetryBackoff(t *testing.T) {
	cfg := RedisConfig{
		MinRetryBackoffMS: -1,
		MaxRetryBackoffMS: -1,
	}

	opts := &redis.Options{}
	applyPoolConfig(opts, cfg)

	require.Equal(t, time.Duration(-1), opts.MinRetryBackoff)
	require.Equal(t, time.Duration(-1), opts.MaxRetryBackoff)
}

func TestApplyClusterPoolConfig_CustomValues(t *testing.T) {
	cfg := RedisConfig{
		PoolSize:          50,
		MinIdleConns:      10,
		MaxIdleConns:      30,
		ReadTimeoutMS:     2000,
		WriteTimeoutMS:    1500,
		DialTimeoutMS:     3000,
		PoolTimeoutMS:     4000,
		MaxRetries:        5,
		MinRetryBackoffMS: 16,
		MaxRetryBackoffMS: 1024,
	}

	opts := &redis.ClusterOptions{}
	applyClusterPoolConfig(opts, cfg)

	require.Equal(t, 50, opts.PoolSize)
	require.Equal(t, 10, opts.MinIdleConns)
	require.Equal(t, 30, opts.MaxIdleConns)
	require.Equal(t, 2000*time.Millisecond, opts.ReadTimeout)
	require.Equal(t, 1500*time.Millisecond, opts.WriteTimeout)
	require.Equal(t, 3000*time.Millisecond, opts.DialTimeout)
	require.Equal(t, 4000*time.Millisecond, opts.PoolTimeout)
	require.Equal(t, 5, opts.MaxRetries)
	require.Equal(t, 16*time.Millisecond, opts.MinRetryBackoff)
	require.Equal(t, 1024*time.Millisecond, opts.MaxRetryBackoff)
}

func TestApplyClusterPoolConfig_Defaults(t *testing.T) {
	cfg := RedisConfig{}

	opts := &redis.ClusterOptions{}
	applyClusterPoolConfig(opts, cfg)

	require.Equal(t, defaultPoolSize, opts.PoolSize)
	require.Equal(t, defaultMinIdleConns, opts.MinIdleConns)
	require.Equal(t, 0, opts.MaxIdleConns)
	require.Equal(t, defaultReadTimeoutMS*time.Millisecond, opts.ReadTimeout)
	require.Equal(t, defaultWriteTimeoutMS*time.Millisecond, opts.WriteTimeout)
	require.Equal(t, defaultDialTimeoutMS*time.Millisecond, opts.DialTimeout)
	require.Equal(t, time.Duration(0), opts.PoolTimeout)
	require.Equal(t, defaultMaxRetries, opts.MaxRetries)
	require.Equal(t, defaultMinRetryBackoff*time.Millisecond, opts.MinRetryBackoff)
	require.Equal(t, defaultMaxRetryBackoff*time.Millisecond, opts.MaxRetryBackoff)
}
