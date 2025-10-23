package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	pb "grpc-sandbox/protobuf/collector"

	"google.golang.org/grpc/credentials/insecure"
	structpb "google.golang.org/protobuf/types/known/structpb"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

type auth struct {
	collectorClient pb.CollectorClient
}

func (a *auth) LogEvent(ctx context.Context, event *pb.Event) {
	_, err := a.collectorClient.LogEvent(ctx, event)
	if err != nil {
		slog.Error("failed to log event", "error", err)
	}
}

func NewAuthService(collectorClient pb.CollectorClient) *auth {
	return &auth{
		collectorClient: collectorClient,
	}
}

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		slog.Error("failed to load environment variables", "error", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	collectorAddr := os.Getenv("COLLECTOR_URL")
	conn, err := grpc.NewClient(collectorAddr, opts...)
	if err != nil {
		slog.Error("failed to connect", "serverAddr", collectorAddr, "error", err)
		return
	}
	defer conn.Close()
	client := pb.NewCollectorClient(conn)
	auth := NewAuthService(client)
	event := &pb.Event{
		EventId:   "authentication",
		EventName: "sign_up_successful",
		EventMetadata: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"user_id": {
					Kind: &structpb.Value_StringValue{
						StringValue: "jzxcopasdpw234",
					},
				},
				"user_name": {
					Kind: &structpb.Value_StringValue{
						StringValue: "emil muradov",
					},
				},
				"user_email": {
					Kind: &structpb.Value_StringValue{
						StringValue: "emil-dev@gmail.com",
					},
				},
				"user_birthday": {
					Kind: &structpb.Value_StringValue{
						StringValue: "1990-01-01",
					},
				},
				"provider": {
					Kind: &structpb.Value_StringValue{
						StringValue: "email",
					},
				},
			},
		},
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	auth.LogEvent(ctx, event)
}
