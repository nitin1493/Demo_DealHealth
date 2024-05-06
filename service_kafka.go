package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

const (
	host       = "localhost"
	port       = 5432
	user       = "readwrite"
	password   = "s2DmTV1rfPx795azJuGK4cN6ey83o0vS"
	dbname     = "dspi"
	kafkaURL   = "localhost:9092"
	topic      = "prediction_topic"
	httpServer = ":8080"
)

type Prediction struct {
	Prediction            string  `json:"prediction"`
	PredictionProbability float64 `json:"predicted_probability"`
}

func main() {
	// Connect to PostgreSQL database
	db := connectToDB()
	defer db.Close()

	// Create a new Kafka producer
	producer := initializeKafkaProducer()
	defer producer.Close()

	// Create a new router
	router := gin.Default()

	// Define a route to handle prediction requests
	router.GET("/predict", func(c *gin.Context) {
		// Get query parameters from the request
		bento := c.Query("bento")
		oID := c.Query("o_id")
		opportunityID := c.Query("opportunity_id")

		// Query the database for prediction and prediction probability
		prediction, err := getPredictionFromDB(db, bento, oID, opportunityID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Publish the prediction to Kafka topic
		err = publishPredictionToKafka(producer, prediction)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, prediction)
	})

	// Run the HTTP server
	router.Run(httpServer)
}

// connectToDB establishes a connection to the PostgreSQL database.
func connectToDB() *sql.DB {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// initializeKafkaProducer initializes a Kafka producer.
func initializeKafkaProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{kafkaURL}, config)
	if err != nil {
		log.Fatal("Error creating Kafka producer:", err)
	}
	return producer
}

// getPredictionFromDB retrieves prediction data from the PostgreSQL database.
func getPredictionFromDB(db *sql.DB, bento, oID, opportunityID string) (Prediction, error) {
	var prediction Prediction
	row := db.QueryRow("SELECT prediction, predicted_probability FROM dealhealth_new_schema_nitin.deal_health_output WHERE bento=$1 AND o_id=$2 AND opportunity_id=$3", bento, oID, opportunityID)
	err := row.Scan(&prediction.Prediction, &prediction.PredictionProbability)
	if err != nil {
		if err == sql.ErrNoRows {
			return prediction, fmt.Errorf("No prediction found for the given parameters")
		}
		return prediction, err
	}
	return prediction, nil
}

// publishPredictionToKafka publishes prediction data to a Kafka topic.
func publishPredictionToKafka(producer sarama.SyncProducer, prediction Prediction) error {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(fmt.Sprintf(`{"prediction": "%s", "predicted_probability": %f}`, prediction.Prediction, prediction.PredictionProbability)),
	}
	_, _, err := producer.SendMessage(message)
	if err != nil {
		return err
	}
	return nil
}
