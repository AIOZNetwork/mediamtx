package database

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

func MustConnectToDatabase(config *conf.Conf) *gorm.DB {
	if config == nil {
		panic("config is nil")
	}

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=Asia/Shanghai",
		config.PostgresHost,
		config.PostgresUser,
		config.PostgresPassword,
		config.PostgresDBName,
		config.PostgresPort,
	)

	//logger
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             1000 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,  // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries:      false, // Don't include params in the SQL log
			Colorful:                  true,  // Enable color
		},
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})

	if err != nil {
		panic(fmt.Sprintf("failed to connect to database: %v", err))
	}

	sqlDB, err := db.DB()
	if err != nil {
		panic(fmt.Sprintf("failed to get database instance: %v", err))
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	sqlDB.SetConnMaxIdleTime(2 * time.Minute)

	if err := sqlDB.Ping(); err != nil {
		panic(fmt.Sprintf("failed to ping database: %v", err))
	}

	fmt.Println("Connected Successfully to the database.")
	DB = db
	return db
}

func MustInitLiveStreamStatisticsDatabase() {
	err := DB.AutoMigrate(&models.LiveStreamStatistic{})
	if err != nil {
		panic(fmt.Sprintf("failed to migrate LiveStreamStatistic: %v", err))
	}
}

func MustInitLiveStreamMulticastDatabase() {
	err := DB.AutoMigrate(&models.LiveStreamMulticast{})
	if err != nil {
		panic(fmt.Sprintf("failed to migrate LiveStreamMulticast: %v", err))
	}
}
