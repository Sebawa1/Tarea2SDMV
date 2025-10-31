package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	pb "mv1/proto"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

const (
	RABBITMQ_HOST = "10.10.31.17:5672"
)

type server struct {
	pb.UnimplementedMonitoreoServiceServer
	notificaciones []Notificacion
	rabbitConn     *amqp.Connection
	rabbitCh       *amqp.Channel
}

type Notificacion struct {
	Tipo    string `json:"tipo"`
	Mensaje string `json:"mensaje"`
}

func (s *server) RecibirNotificaciones(stream pb.MonitoreoService_RecibirNotificacionesServer) error {
	log.Println("Cliente conectado al servicio de monitoreo")

	for {
		if len(s.notificaciones) > 0 {
			notif := s.notificaciones[0]
			s.notificaciones = s.notificaciones[1:]

			err := stream.Send(&pb.NotificacionResponse{
				Mensaje: notif.Mensaje,
				Tipo:    notif.Tipo,
			})

			if err != nil {
				log.Printf("Error al enviar notificación: %v", err)
				return err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *server) consumirRabbitMQ() {
	msgs, err := s.rabbitCh.Consume(
		"notificaciones",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Error al consumir de RabbitMQ: %v", err)
	}

	log.Println("Esperando notificaciones de RabbitMQ...")

	for msg := range msgs {
		var notif Notificacion
		err := json.Unmarshal(msg.Body, &notif)
		if err != nil {
			log.Printf("Error al parsear mensaje: %v", err)
			continue
		}

		fmt.Printf("\n%s\n", notif.Mensaje)
		s.notificaciones = append(s.notificaciones, notif)
	}
}

func main() {
	rabbitURI := fmt.Sprintf("amqp://guest:guest@%s/", RABBITMQ_HOST)
	log.Printf("Conectando a RabbitMQ en %s...", RABBITMQ_HOST)

	conn, err := amqp.Dial(rabbitURI)
	if err != nil {
		log.Fatalf("Error al conectar a RabbitMQ: %v", err)
	}
	defer conn.Close()
	log.Println("✓ Conectado a RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error al crear canal: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		"notificaciones",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Error al declarar cola: %v", err)
	}

	srv := &server{
		notificaciones: make([]Notificacion, 0),
		rabbitConn:     conn,
		rabbitCh:       ch,
	}

	go srv.consumirRabbitMQ()

	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMonitoreoServiceServer(s, srv)

	log.Println("✓ Servicio de Monitoreo escuchando en :50053")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
