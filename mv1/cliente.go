package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "mv1/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SolicitudReserva struct {
	Name        string `json:"name"`
	Phone       string `json:"phone"`
	PartySize   int32  `json:"party_size"`
	Preferences string `json:"preferences"`
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Uso: ./cliente reservas.json")
	}

	archivoReservas := os.Args[1]

	solicitudes, err := leerReservas(archivoReservas)
	if err != nil {
		log.Fatalf("Error al leer archivo de reservas: %v", err)
	}

	fmt.Println("Solicitudes de reserva recibidas\n")

	connReservas, err := grpc.Dial("10.10.31.17:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al servicio de reservas: %v", err)
	}
	defer connReservas.Close()
	clienteReservas := pb.NewReservaServiceClient(connReservas)

	connMonitoreo, err := grpc.Dial("localhost:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al servicio de monitoreo: %v", err)
	}
	defer connMonitoreo.Close()
	clienteMonitoreo := pb.NewMonitoreoServiceClient(connMonitoreo)

	ctx := context.Background()
	stream, err := clienteMonitoreo.RecibirNotificaciones(ctx)
	if err != nil {
		log.Fatalf("Error al crear stream de notificaciones: %v", err)
	}

	done := make(chan struct{})

	go func() {
		recibirNotificaciones(stream)
		close(done)
	}()

	request := &pb.ReservasRequest{
		Solicitudes: solicitudes,
	}

	ctx2, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = clienteReservas.EnviarReservas(ctx2, request)
	if err != nil {
		log.Fatalf("Error al enviar reservas: %v", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	select {
	case <-c:
		fmt.Println("\nCliente finalizando...")
	case <-done:
		fmt.Println("\nNo hay más notificaciones, cerrando cliente...")
	}
}

func leerReservas(archivo string) ([]*pb.Solicitud, error) {
	data, err := os.ReadFile(archivo)
	if err != nil {
		return nil, err
	}

	var solicitudes []SolicitudReserva
	if err := json.Unmarshal(data, &solicitudes); err != nil {
		return nil, err
	}

	var pbSolicitudes []*pb.Solicitud
	for _, s := range solicitudes {
		pbSolicitudes = append(pbSolicitudes, &pb.Solicitud{
			Name:        s.Name,
			Phone:       s.Phone,
			PartySize:   s.PartySize,
			Preferences: s.Preferences,
		})
	}

	return pbSolicitudes, nil
}

func recibirNotificaciones(stream pb.MonitoreoService_RecibirNotificacionesClient) {
	for {
		notif, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error al recibir notificación: %v", err)
			break
		}

		fmt.Println(notif.Mensaje)
	}
}
