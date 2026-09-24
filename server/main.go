package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	pb "grpc-leilao-go/proto"

	"google.golang.org/grpc"
)

type LeilaoServer struct {
	pb.UnimplementedLeilaoServiceServer
	mu             sync.Mutex
	maiorValor     float64
	maiorLanceador string
	clientes       map[pb.LeilaoService_ParticiparLeilaoServer]bool
	timer     *time.Timer
	encerrado bool
}

func newServer() *LeilaoServer {
	s := &LeilaoServer{
		maiorValor:     0,
		maiorLanceador: "Nenhum",
		clientes:       make(map[pb.LeilaoService_ParticiparLeilaoServer]bool),
	}

	s.resetarTimer(150 * time.Second)
	return s
}

func (s *LeilaoServer) resetarTimer(duracao time.Duration) {
	if s.timer != nil {
		s.timer.Stop()
	}

	s.timer = time.AfterFunc(duracao, func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.encerrado = true
		log.Printf("LEILÃO ENCERRADO! Vencedor: %s com R$ %.2f\n", s.maiorLanceador, s.maiorValor)

		notificacaoFim := &pb.AtualizacaoLeilao{
			MaiorLanceador: s.maiorLanceador,
			MaiorValor:     s.maiorValor,
			Mensagem:       fmt.Sprintf("LEILÃO ENCERRADO! Vencedor: %s!", s.maiorLanceador),
		}

		for clienteStream := range s.clientes {
			_ = clienteStream.Send(notificacaoFim)
		}
	})
}

func (s *LeilaoServer) ParticiparLeilao(stream pb.LeilaoService_ParticiparLeilaoServer) error {
	s.mu.Lock()
	s.clientes[stream] = true
	maiorVal := s.maiorValor
	maiorLanc := s.maiorLanceador
	leilaoEncerrado := s.encerrado
	s.mu.Unlock()

	log.Println("⚡ Novo cliente se conectou!")

	msgBoasVindas := "Bem-vindo ao Leilão!"
	if leilaoEncerrado {
		msgBoasVindas = "O leilão já se encontra encerrado."
	}

	stream.Send(&pb.AtualizacaoLeilao{
		MaiorLanceador: maiorLanc,
		MaiorValor:     maiorVal,
		Mensagem:       msgBoasVindas,
	})

	defer func() {
		s.mu.Lock()
		delete(s.clientes, stream)
		s.mu.Unlock()
		log.Println("Cliente desconectado.")
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
		if s.encerrado {
			stream.Send(&pb.AtualizacaoLeilao{
				MaiorLanceador: s.maiorLanceador,
				MaiorValor:     s.maiorValor,
				Mensagem:       "O leilão já foi encerrado! Lances não são mais aceitos.",
			})
			s.mu.Unlock()
			continue
		}

		if lance.Valor > s.maiorValor {
			s.maiorValor = lance.Valor
			s.maiorLanceador = lance.ClienteId
			log.Printf("NOVO MAIOR LANCE: R$ %.2f por %s\n", s.maiorValor, s.maiorLanceador)

			s.resetarTimer(30 * time.Second)

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