package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"

	_ "github.com/lib/pq" // Import PostgreSQL driver package

	pb "grpc_dhs/proto"

	"google.golang.org/grpc"
)

const (
	host       = "localhost"
	port       = 5432
	user       = "readwrite"
	password   = "s2DmTV1rfPx795azJuGK4cN6ey83o0vS"
	dbname     = "dspi"
	grpcServer = ":50051"
)

type server struct {
	pb.UnimplementedPredictionServiceServer
}

type Prediction struct {
	Prediction            string  `json:"prediction"`
	PredictionProbability float64 `json:"predicted_probability"`
}

func (s *server) Predict(ctx context.Context, req *pb.PredictionRequest) (*pb.PredictionResponse, error) {
	log.Println("Received prediction request.") // Add this line
	bento := req.Bento
	oID := req.OId
	opportunityID := req.OpportunityId

	// Connect to PostgreSQL database
	db := connectToDB()
	defer db.Close()

	// Query the database for prediction and prediction probability
	prediction, err := getPredictionFromDB(db, bento, oID, opportunityID)
	if err != nil {
		return nil, err
	}

	resp := &pb.PredictionResponse{
		Prediction:            prediction.Prediction,
		PredictionProbability: prediction.PredictionProbability,
	}

	return resp, nil
}

func main() {
	// Start the gRPC server
	listen, err := net.Listen("tcp", grpcServer)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPredictionServiceServer(s, &server{})
	log.Println("gRPC server started on port", grpcServer)
	if err := s.Serve(listen); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
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
