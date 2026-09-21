package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	pb "grpc-leilao-go/proto"

	"google.golang.org/grpc"
)

type LeilaoServer struct {
	pb.UnimplementedLeilaoServiceServer
	mu             sync.Mutex
	maiorValor     float64
	maiorLanceador string
	clientes       map[pb.LeilaoService_ParticiparLeilaoServer]bool
}

func newServer() *LeilaoServer {
	return &LeilaoServer{
		maiorValor:     0,
		maiorLanceador: "Nenhum",
		clientes:       make(map[pb.LeilaoService_ParticiparLeilaoServer]bool),
	}
}

func (s *LeilaoServer) ParticiparLeilao(stream pb.LeilaoService_ParticiparLeilaoServer) error {
	s.mu.Lock()
	s.clientes[stream] = true
	maiorVal := s.maiorValor
	maiorLanc := s.maiorLanceador
	s.mu.Unlock()

	log.Println("⚡ Novo cliente se conectou!")

	stream.Send(&pb.AtualizacaoLeilao{
		MaiorLanceador: maiorLanc,
		MaiorValor:     maiorVal,
		Mensagem:       "Bem-vindo ao Leilão!",
	})

	defer func() {
		s.mu.Lock()
		delete(s.clientes, stream)
		s.mu.Unlock()
		log.Println("🔴 Cliente desconectado.")
	}()

	for {
		lance, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("[LANCE RECEBIDO] %s ofertou R$ %.2f\n", lance.ClienteId, lance.Valor)

		s.mu.Lock()
		if lance.Valor > s.maiorValor {
			s.maiorValor = lance.Valor
			s.maiorLanceador = lance.ClienteId
			log.Printf("🏆 NOVO MAIOR LANCE: R$ %.2f por %s\n", s.maiorValor, s.maiorLanceador)

			notificacao := &pb.AtualizacaoLeilao{
				MaiorLanceador: s.maiorLanceador,
				MaiorValor:     s.maiorValor,
				Mensagem:       fmt.Sprintf("Novo maior lance de %s!", s.maiorLanceador),
			}

			for clienteStream := range s.clientes {
				_ = clienteStream.Send(notificacao)
			}
		} else {
			stream.Send(&pb.AtualizacaoLeilao{
				MaiorLanceador: s.maiorLanceador,
				MaiorValor:     s.maiorValor,
				Mensagem:       fmt.Sprintf("Lance recusado. O lance atual é R$ %.2f.", s.maiorValor),
			})
		}
		s.mu.Unlock()
	}
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Falha ao escutar porta: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterLeilaoServiceServer(grpcServer, newServer())

	fmt.Println("🚀 Servidor gRPC (Go) rodando na porta :50051...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Falha ao iniciar servidor gRPC: %v", err)
	}
}