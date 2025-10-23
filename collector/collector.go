package main

import (
	"context"
	"fmt"
	pb "grpc-sandbox/protobuf/collector"
	"log/slog"
	"net"
	"os"

	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"

	"google.golang.org/grpc"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type collector struct {
	mongoClient *mongo.Client
	pb.UnimplementedCollectorServer
}

func (c *collector) LogEvent(ctx context.Context, event *pb.Event) (*wrapperspb.BoolValue, error) {
	slog.Info("Logging event start", "event", event)
	res, err := c.mongoClient.Database("betelgeuse").Collection("events").InsertOne(ctx, event)
	if err != nil {
		slog.Error("failed to log event", "error", err)
		return &wrapperspb.BoolValue{Value: false}, err
	}
	slog.Info("event logged", "event", res)
	return &wrapperspb.BoolValue{Value: true}, nil
}

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		slog.Error("failed to load environment variables", "error", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		slog.Error("failed to connect to mongo", "error", err)
		return
	}
	defer client.Disconnect(ctx)
	collector := &collector{
		mongoClient: client,
	}
	port := os.Getenv("COLLECTOR_PORT")
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		slog.Error("failed to listen on port", "error", err.Error(), "port", port)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterCollectorServer(grpcServer, collector)
	slog.Info("collector server started", "port", port)
	grpcServer.Serve(lis)
}
