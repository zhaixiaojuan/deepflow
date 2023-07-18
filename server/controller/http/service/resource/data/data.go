/*
 * Copyright (c) 2023 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package data

import (
	"context"
	"fmt"
	"time"

	"github.com/baidubce/bce-sdk-go/util/log"
	"github.com/go-redis/redis/v9"
	"github.com/google/wire"
	"github.com/op/go-logging"

	"github.com/deepflowio/deepflow/server/controller/config"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewRedisCmd)

type Data struct {
	// db       *gorm.DB
	redisCli *Redis
}

type Redis struct {
	ResourceAPI       redis.UniversalClient
	DimensionResource redis.UniversalClient
}

func NewRedisCmd(conf *config.ControllerConfig) *Redis {
	log := logging.MustGetLogger("config.redis")
	cfg := conf.RedisCfg
	client := &Redis{
		ResourceAPI:       createUniversalRedisClient(cfg, cfg.ResourceAPIDatabase),
		DimensionResource: createUniversalRedisClient(cfg, cfg.DimensionResourceDatabase),
	}

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*time.Duration(cfg.TimeOut))
	defer cancelFunc()
	err := client.ResourceAPI.Ping(timeout).Err()
	if err != nil {
		log.Fatalf("redis(resource api) connect error: %v", err)
	}
	timeout, cancelFunc = context.WithTimeout(context.Background(), time.Second*time.Duration(cfg.TimeOut))
	defer cancelFunc()
	if err = client.DimensionResource.Ping(timeout).Err(); err != nil {
		log.Fatalf("redis(dimension resource) connect error: %v", err)
	}

	return client
}

func NewData(redis *Redis) (*Data, error) {
	d := &Data{
		// db:       db,
		redisCli: redis,
	}
	return d, nil
}

func generateAddrs(cfg config.RedisConfig) []string {
	var addrs []string
	for i := range cfg.Host {
		addrs = append(addrs, fmt.Sprintf("%s:%d", cfg.Host[i], cfg.Port))
	}
	return addrs
}

func generateSimpleAddr(cfg config.RedisConfig) string {
	return generateAddrs(cfg)[0]
}

func createSimpleClient(cfg config.RedisConfig, database int) redis.UniversalClient {
	addr := generateSimpleAddr(cfg)
	log.Infof("redis addr: %v", addr)
	return redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    cfg.Password,
		DB:          database,
		DialTimeout: time.Duration(cfg.TimeOut) * time.Second,
	})
}

func generateClusterAddrs(cfg config.RedisConfig) []string {
	return generateAddrs(cfg)
}

func createClusterClient(cfg config.RedisConfig) redis.UniversalClient {
	addrs := generateClusterAddrs(cfg)
	log.Infof("redis addrs: %v", addrs)
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       addrs,
		Password:    cfg.Password,
		DialTimeout: time.Duration(cfg.TimeOut) * time.Second,
	})
}

func createUniversalRedisClient(cfg config.RedisConfig, database int) redis.UniversalClient {
	if cfg.ClusterEnabled {
		return createClusterClient(cfg)
	} else {
		return createSimpleClient(cfg, database)
	}
}
