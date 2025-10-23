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

type purchase struct {
	collectorClient pb.CollectorClient
}

func (p *purchase) LogEvent(ctx context.Context, event *pb.Event) {
	_, err := p.collectorClient.LogEvent(ctx, event)
	if err != nil {
		slog.Error("failed to log event", "error", err)
	}
}

func NewPurchaseService(collectorClient pb.CollectorClient) *purchase {
	return &purchase{
		collectorClient: collectorClient,
	}
}

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		slog.Error("failed to load environment variables", "error", err)
	}
	collectorAddr := os.Getenv("COLLECTOR_URL")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(collectorAddr, opts...)
	if err != nil {
		slog.Error("failed to connect", "serverAddr", collectorAddr, "error", err)
		return
	}
	defer conn.Close()
	client := pb.NewCollectorClient(conn)
	purchase := NewPurchaseService(client)
	event := &pb.Event{
		EventId:   "purchase",
		EventName: "purchase_successful",
		EventMetadata: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"product_id": {
					Kind: &structpb.Value_StringValue{
						StringValue: "com.annual.premium.trial",
					},
				},
				"quantity": {
					Kind: &structpb.Value_NumberValue{
						NumberValue: 1,
					},
				},
				"price": {
					Kind: &structpb.Value_NumberValue{
						NumberValue: 69.99,
					},
				},
				"currency_code": {
					Kind: &structpb.Value_StringValue{
						StringValue: "USD",
					},
				},
				"provider": {
					Kind: &structpb.Value_StringValue{
						StringValue: "App Store",
					},
				},
			},
		},
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	purchase.LogEvent(ctx, event)
}
