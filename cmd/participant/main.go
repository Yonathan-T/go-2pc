package main

import (
	"Two-Phase-Commit/participants"
	"Two-Phase-Commit/proto"
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

type Server struct {
	proto.UnimplementedParticipantServiceServer
	node *participants.Node
}

func (s *Server) Prepare(ctx context.Context, req *proto.PrepareRequest) (*proto.PrepareResponse, error) {
	tx := protocol.Transaction{
		ID:      req.TxId,
		Payload: req.Payload,
		Data:    req.Data,
	}
	vote, err := s.node.Prepare(tx)
	if err != nil || vote != protocol.VOTE_YES {
		return &proto.PrepareResponse{
			Vote:  proto.Vote_VOTE_NO,
			Error: fmt.Sprintf("%v", err),
		}, nil
	}
	return &proto.PrepareResponse{
		Vote: proto.Vote_VOTE_YES,
	}, nil
}

func (s *Server) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	err := s.node.Commit(req.TxId)
	if err != nil {
		return &proto.CommitResponse{
			Ok:    false,
			Error: fmt.Sprintf("%v", err),
		}, nil
	}
	return &proto.CommitResponse{
		Ok: true,
	}, nil
}

func (s *Server) Abort(ctx context.Context, req *proto.AbortRequest) (*proto.AbortResponse, error) {
	err := s.node.Abort(req.TxId)
	if err != nil {
		return &proto.AbortResponse{
			Ok:    false,
			Error: fmt.Sprintf("%v", err),
		}, nil
	}
	return &proto.AbortResponse{
		Ok: true,
	}, nil
}

func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	val, ok := s.node.GetData(req.Key)
	return &proto.GetResponse{
		Value: val,
		Found: ok,
	}, nil
}

func (s *Server) GetAll(ctx context.Context, req *proto.GetAllRequest) (*proto.GetAllResponse, error) {
	data := s.node.GetAllData()
	return &proto.GetAllResponse{
		Data: data,
	}, nil
}

func main() {
	id := flag.String("id", "P1", "Participant node ID")
	port := flag.Int("port", 50051, "Port to listen on")
	walPath := flag.String("wal", "logs/participant_1.wal", "Path to WAL file")
	flag.Parse()

	w, err := wal.NewWAL(*walPath)
	if err != nil {
		log.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	node := participants.NewNode(*id, w)
	_ = node.Recover()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen on port %d: %v", *port, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterParticipantServiceServer(grpcServer, &Server{node: node})
	fmt.Printf("[%s] Participant gRPC server listening on port %d...\n", *id, *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
