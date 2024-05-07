package main

import (
	"context"
	"log"
	"time"

	pb "grpc_dhs/proto" // Update with your package name

	"google.golang.org/grpc"
)

const (
	address = "localhost:50051" // Update with your gRPC server address
)

func main() {
	log.Println("Checking if server is running...")
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	log.Println("Server is running.")
	log.Printf("Client connecting to: %s", address)
	c := pb.NewPredictionServiceClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	log.Println("Client connected successfully")
	log.Println("Client sending prediction request")
	r, err := c.Predict(ctx, &pb.PredictionRequest{Bento: "app1a", OId: "b054925b-574a-4e9d-b03d-f4d318905938", OpportunityId: "2812"})
	if err != nil {
		log.Fatalf("Could not predict: %v", err)
	}
	log.Printf("Prediction: %s, Probability: %f", r.GetPrediction(), r.GetPredictionProbability())
}
