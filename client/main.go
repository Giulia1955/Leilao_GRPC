package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	pb "grpc-leilao-go/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := "localhost:50051"
	clienteID := fmt.Sprintf("Cliente_Go_%d", os.Getpid())

	if len(os.Args) > 1 {
		serverAddr = os.Args[1]
	}
	if len(os.Args) > 2 {
		clienteID = os.Args[2]
	}

	fmt.Printf("Conectando em %s como [%s]...\n", serverAddr, clienteID)

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Erro ao conectar: %v", err)
	}
	defer conn.Close()

	client := pb.NewLeilaoServiceClient(conn)
	stream, err := client.ParticiparLeilao(context.Background())
	if err != nil {
		log.Fatalf("Erro ao abrir stream: %v", err)
	}

	go func() {
		for {
			atualizacao, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("Servidor fechou a conexão.")
				return
			}
			if err != nil {
				log.Printf("Erro ao receber dados: %v\n", err)
				return
			}

			fmt.Println("\n--- [ATUALIZAÇÃO DO LEILÃO] ---")
			fmt.Printf("Mensagem: %s\n", atualizacao.Mensagem)
			fmt.Printf("Maior Lance: R$ %.2f (por: %s)\n", atualizacao.MaiorValor, atualizacao.MaiorLanceador)
			fmt.Println("-------------------------------")
			fmt.Print("Digite o valor do seu lance (ou 'sair'): ")
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.ToLower(input) == "sair" {
			stream.CloseSend()
			break
		}

		valor, err := strconv.ParseFloat(input, 64)
		if err != nil {
			fmt.Println("Valor inválido. Digite um número.")
			continue
		}

		err = stream.Send(&pb.MensagemLance{
			ClienteId: clienteID,
			Valor:     valor,
		})

		if err != nil {
			log.Printf("Erro ao enviar lance: %v\n", err)
			break
		}
	}
}