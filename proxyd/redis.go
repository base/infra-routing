package proxyd

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/redis/go-redis/v9"
)

const (
	defaultPoolSize        = 100
	defaultMinIdleConns    = 20
	defaultReadTimeoutMS   = 3000
	defaultWriteTimeoutMS  = 3000
	defaultDialTimeoutMS   = 5000
	defaultMaxRetries      = 3
	defaultMinRetryBackoff = 8
	defaultMaxRetryBackoff = 512
)

func NewRedisClient(url string, enable_cluster bool) (redis.UniversalClient, error) {
	return NewRedisClientWithConfig(url, enable_cluster, RedisConfig{})
}

func NewRedisClientWithConfig(url string, enable_cluster bool, cfg RedisConfig) (redis.UniversalClient, error) {
	if enable_cluster {
		log.Info("Using cluster redis client.")
		opts, err := redis.ParseClusterURL(url)
		if err != nil {
			return nil, err
		}
		applyClusterPoolConfig(opts, cfg)
		logPoolConfig(cfg)
		return redis.NewClusterClient(opts), nil
	} else {
		log.Info("Using default redis client.")
		opts, err := redis.ParseURL(url)
		if err != nil {
			return nil, err
		}
		applyPoolConfig(opts, cfg)
		logPoolConfig(cfg)
		return redis.NewClient(opts), nil
	}
}

func applyPoolConfig(opts *redis.Options, cfg RedisConfig) {
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	} else {
		opts.PoolSize = defaultPoolSize
	}

	if cfg.MinIdleConns > 0 {
		opts.MinIdleConns = cfg.MinIdleConns
	} else {
		opts.MinIdleConns = defaultMinIdleConns
	}

	if cfg.MaxIdleConns > 0 {
		opts.MaxIdleConns = cfg.MaxIdleConns
	}

	if cfg.ReadTimeoutMS > 0 {
		opts.ReadTimeout = time.Duration(cfg.ReadTimeoutMS) * time.Millisecond
	} else {
		opts.ReadTimeout = defaultReadTimeoutMS * time.Millisecond
	}

	if cfg.WriteTimeoutMS > 0 {
		opts.WriteTimeout = time.Duration(cfg.WriteTimeoutMS) * time.Millisecond
	} else {
		opts.WriteTimeout = defaultWriteTimeoutMS * time.Millisecond
	}

	if cfg.DialTimeoutMS > 0 {
		opts.DialTimeout = time.Duration(cfg.DialTimeoutMS) * time.Millisecond
	} else {
		opts.DialTimeout = defaultDialTimeoutMS * time.Millisecond
	}

	if cfg.PoolTimeoutMS > 0 {
		opts.PoolTimeout = time.Duration(cfg.PoolTimeoutMS) * time.Millisecond
	}

	if cfg.MaxRetries != 0 {
		opts.MaxRetries = cfg.MaxRetries
	} else {
		opts.MaxRetries = defaultMaxRetries
	}

	if cfg.MinRetryBackoffMS > 0 {
		opts.MinRetryBackoff = time.Duration(cfg.MinRetryBackoffMS) * time.Millisecond
	} else if cfg.MinRetryBackoffMS < 0 {
		opts.MinRetryBackoff = -1
	} else {
		opts.MinRetryBackoff = defaultMinRetryBackoff * time.Millisecond
	}

	if cfg.MaxRetryBackoffMS > 0 {
		opts.MaxRetryBackoff = time.Duration(cfg.MaxRetryBackoffMS) * time.Millisecond
	} else if cfg.MaxRetryBackoffMS < 0 {
		opts.MaxRetryBackoff = -1
	} else {
		opts.MaxRetryBackoff = defaultMaxRetryBackoff * time.Millisecond
	}
}

func applyClusterPoolConfig(opts *redis.ClusterOptions, cfg RedisConfig) {
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	} else {
		opts.PoolSize = defaultPoolSize
	}

	if cfg.MinIdleConns > 0 {
		opts.MinIdleConns = cfg.MinIdleConns
	} else {
		opts.MinIdleConns = defaultMinIdleConns
	}

	if cfg.MaxIdleConns > 0 {
		opts.MaxIdleConns = cfg.MaxIdleConns
	}

	if cfg.ReadTimeoutMS > 0 {
		opts.ReadTimeout = time.Duration(cfg.ReadTimeoutMS) * time.Millisecond
	} else {
		opts.ReadTimeout = defaultReadTimeoutMS * time.Millisecond
	}

	if cfg.WriteTimeoutMS > 0 {
		opts.WriteTimeout = time.Duration(cfg.WriteTimeoutMS) * time.Millisecond
	} else {
		opts.WriteTimeout = defaultWriteTimeoutMS * time.Millisecond
	}

	if cfg.DialTimeoutMS > 0 {
		opts.DialTimeout = time.Duration(cfg.DialTimeoutMS) * time.Millisecond
	} else {
		opts.DialTimeout = defaultDialTimeoutMS * time.Millisecond
	}

	if cfg.PoolTimeoutMS > 0 {
		opts.PoolTimeout = time.Duration(cfg.PoolTimeoutMS) * time.Millisecond
	}

	if cfg.MaxRetries != 0 {
		opts.MaxRetries = cfg.MaxRetries
	} else {
		opts.MaxRetries = defaultMaxRetries
	}

	if cfg.MinRetryBackoffMS > 0 {
		opts.MinRetryBackoff = time.Duration(cfg.MinRetryBackoffMS) * time.Millisecond
	} else if cfg.MinRetryBackoffMS < 0 {
		opts.MinRetryBackoff = -1
	} else {
		opts.MinRetryBackoff = defaultMinRetryBackoff * time.Millisecond
	}

	if cfg.MaxRetryBackoffMS > 0 {
		opts.MaxRetryBackoff = time.Duration(cfg.MaxRetryBackoffMS) * time.Millisecond
	} else if cfg.MaxRetryBackoffMS < 0 {
		opts.MaxRetryBackoff = -1
	} else {
		opts.MaxRetryBackoff = defaultMaxRetryBackoff * time.Millisecond
	}
}

func logPoolConfig(cfg RedisConfig) {
	poolSize := cfg.PoolSize
	if poolSize == 0 {
		poolSize = defaultPoolSize
	}
	minIdleConns := cfg.MinIdleConns
	if minIdleConns == 0 {
		minIdleConns = defaultMinIdleConns
	}
	readTimeout := cfg.ReadTimeoutMS
	if readTimeout == 0 {
		readTimeout = defaultReadTimeoutMS
	}
	writeTimeout := cfg.WriteTimeoutMS
	if writeTimeout == 0 {
		writeTimeout = defaultWriteTimeoutMS
	}

	log.Info("Redis connection pool configured",
		"pool_size", poolSize,
		"min_idle_conns", minIdleConns,
		"max_idle_conns", cfg.MaxIdleConns,
		"read_timeout_ms", readTimeout,
		"write_timeout_ms", writeTimeout,
		"max_retries", cfg.MaxRetries,
	)
}

func CheckRedisConnection(client redis.UniversalClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return wrapErr(err, "error connecting to redis")
	}

	return nil
}
