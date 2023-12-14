package config

import (
	"github.com/spf13/viper"
	"log"
	"path/filepath"
	"runtime"
	"strings"
)

type Config struct {
	AppConfig AppConfig   `mapstructure:"app"`
	Redis     RedisConfig `mapstructure:"redis"`
}

type AppConfig struct {
	AppName string `env:"APP_NAME" envDefault:"event-streaming-redis-demo"`
	Version string `env:"APP_VERSION" envDefault:"v1"`
}

type RedisConfig struct {
	Host     string `env:"REDIS_HOST" envDefault:"localhost" mapstructure:"host"`
	Port     int    `env:"REDIS_PORT" envDefault:"6379" mapstructure:"port"`
	Password string `env:"REDIS_PASSWORD" envDefault:"" mapstructure:"password"`
	DB       int    `env:"REDIS_DB" envDefault:"0" mapstructure:"db"`
}

func LoadConfig() Config {
	// root directory
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)
	log.Println(basepath)
	viper.AddConfigPath(basepath)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.SetEnvKeyReplacer(strings.NewReplacer(`.`, `_`))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("Config file not found")
			// Config file not found; ignore error if desired
		} else {
			// Config file was found but another error was produced
		}
	}

	var config Config
	err := viper.Unmarshal(&config)
	if err != nil {
		panic(err)
	}
	return config
}
