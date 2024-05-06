package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

const (
	host       = "localhost"
	port       = 5432
	user       = "readwrite"
	password   = "s2DmTV1rfPx795azJuGK4cN6ey83o0vS"
	dbname     = "dspi"
	schemaName = "dealhealth_new_schema_nitin"
	tableName  = "dealhealth_op_v5"
)

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

	// Start the scheduler
	c := cron.New()

	_, err = c.AddFunc("@every 2m", func() {
		log.Println("Scheduler triggered")

		// Drop table if exists
		_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE", schemaName, tableName))
		if err != nil {
			log.Printf("Error dropping table: %v", err)
			return
		}

		// Get data from S3 bucket
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("us-west-2"), // Update with your AWS region
		})
		if err != nil {
			log.Printf("Error creating AWS session: %v", err)
			return
		}

		svc := s3.New(sess)

		bucketName := "annotation-databricks-access-granted"
		objectKey := "ShortTerm/Users/nitin/dealhealth_op.csv"

		input := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		}

		result, err := svc.GetObject(input)
		if err != nil {
			log.Printf("Error getting object from S3: %v", err)
			return
		}

		defer result.Body.Close()

		// Read the CSV data from S3
		csvReader := csv.NewReader(result.Body)
		records, err := csvReader.ReadAll()
		if err != nil {
			log.Printf("Error reading CSV data from S3: %v", err)
			return
		}

		if len(records) < 1 {
			log.Println("No records found in CSV")
			return
		}

		// Get the first row to get the column names
		columns := records[0]

		// Create table
		query := fmt.Sprintf("CREATE TABLE %s.%s (%s)", schemaName, tableName, strings.Join(columns, " TEXT,")+" TEXT")
		_, err = db.Exec(query)
		if err != nil {
			log.Printf("Error creating table: %v", err)
			return
		}
		log.Println("Table created successfully")

		// Start a transaction
		tx, err := db.Begin()
		if err != nil {
			log.Printf("Error starting transaction: %v", err)
			return
		}
		defer tx.Rollback()

		// Prepare the SQL statement for bulk insert
		stmt, err := tx.Prepare(pq.CopyInSchema(schemaName, tableName, columns...))
		if err != nil {
			log.Printf("Error preparing SQL statement: %v", err)
			return
		}
		defer stmt.Close()

		// Iterate over CSV records and insert into database
		for _, record := range records[1:] {
			// Convert the slice of strings to a slice of interfaces
			var recordInterface []interface{}
			for _, value := range record {
				recordInterface = append(recordInterface, value)
			}

			_, err = stmt.Exec(recordInterface...)
			if err != nil {
				log.Printf("Error inserting data: %v", err)
				return
			}
		}

		// Execute the bulk insert
		_, err = stmt.Exec()
		if err != nil {
			log.Printf("Error executing bulk insert: %v", err)
			return
		}

		// Commit the transaction
		err = tx.Commit()
		if err != nil {
			log.Printf("Error committing transaction: %v", err)
			return
		}

		log.Println("Data ingested successfully")
	})

	c.Start()

	log.Println("Starting the scheduler")

	// Run until interrupted
	select {}
}
