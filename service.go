package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "readwrite"
	password = "s2DmTV1rfPx795azJuGK4cN6ey83o0vS"
	dbname   = "dspi"
)

type Prediction struct {
	Prediction            string  `json:"prediction"`
	PredictionProbability float64 `json:"predicted_probability"`
}

func main() {
	// Create connection string
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// Connect to PostgreSQL database
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Create a new router
	router := gin.Default()

	// Define a route to handle prediction requests
	router.GET("/predict", func(c *gin.Context) {
		// Get query parameters from the request
		bento := c.Query("bento")
		oID := c.Query("o_id")
		opportunityID := c.Query("opportunity_id")

		log.Printf("Parameters: bento=%s, oID=%s, opportunityID=%s\n", bento, oID, opportunityID)

		// Query the database for prediction and prediction probability
		var prediction Prediction
		row := db.QueryRow("SELECT prediction, predicted_probability FROM dealhealth_new_schema_nitin.deal_health_output WHERE bento=$1 AND o_id=$2 AND opportunity_id=$3", bento, oID, opportunityID)
		err := row.Scan(&prediction.Prediction, &prediction.PredictionProbability)
		if err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{"error": "Prediction not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Return the prediction as JSON
		c.JSON(http.StatusOK, prediction)
	})
	// Start the server
	router.Run(":8080")
}
