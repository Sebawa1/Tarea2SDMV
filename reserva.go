package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	pb "github.com/sd2025/restaurante/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedReservaServiceServer
	dbClient   *mongo.Client
	rabbitConn *amqp.Connection
	rabbitCh   *amqp.Channel
}

// Mesa en la base de datos
type Mesa struct {
	TableID  string   `bson:"table_id"`
	Capacity int32    `bson:"capacity"`
	Status   string   `bson:"status"`
	Tipo     []string `bson:"tipo"`
}

// Procesar las solicitudes de reserva
func (s *server) EnviarReservas(ctx context.Context, req *pb.ReservasRequest) (*pb.ReservasResponse, error) {
	log.Println("Recibidas", len(req.Solicitudes), "solicitudes de reserva")

	collection := s.dbClient.Database("restaurante").Collection("mesas")

	for i, solicitud := range req.Solicitudes {
		reservationID := fmt.Sprintf("%02d", i+1)

		// Buscar mesa disponible
		mesa, modificada, err := s.buscarMesa(ctx, collection, solicitud)

		if err != nil {
			// Reserva fallida
			mensaje := fmt.Sprintf("Reserva de %s para %d personas en zona %s fallida.\n%s",
				solicitud.Name, solicitud.PartySize, solicitud.Preferences, err.Error())
			s.enviarNotificacion("fallida", mensaje)
			continue
		}

		// Crear reserva exitosa
		reservaInfo := &pb.ReservaInfo{
			ReservationId: reservationID,
			Name:          solicitud.Name,
			Phone:         solicitud.Phone,
			PartySize:     solicitud.PartySize,
			Preferences:   solicitud.Preferences,
			TableId:       []string{mesa.TableID},
			Status:        "Confirmada",
			MesaCapacidad: fmt.Sprintf("%d", mesa.Capacity),
			MesaTipo:      mesa.Tipo[0],
		}

		// Enviar al servicio de registro
		err = s.registrarReserva(reservaInfo)
		if err != nil {
			log.Printf("Error al registrar reserva: %v", err)
			continue
		}

		// Enviar notificación
		var mensaje string
		if modificada {
			mensaje = fmt.Sprintf("Reserva de %s para %d personas en zona %s exitosa con modificaciones.\nSe ha asignado %s (capacidad %d personas) en zona %s.",
				solicitud.Name, solicitud.PartySize, solicitud.Preferences,
				mesa.TableID, mesa.Capacity, mesa.Tipo[0])
		} else {
			mensaje = fmt.Sprintf("Reserva de %s para %d personas en zona %s exitosa.\nSe ha asignado %s (capacidad %d personas) en zona %s.",
				solicitud.Name, solicitud.PartySize, solicitud.Preferences,
				mesa.TableID, mesa.Capacity, mesa.Tipo[0])
		}

		tipo := "exitosa"
		if modificada {
			tipo = "modificada"
		}

		s.enviarNotificacion(tipo, mensaje)
	}

	return &pb.ReservasResponse{
		Mensaje: "Reservas procesadas",
		Success: true,
	}, nil
}

// Buscar una mesa disponible según los criterios
func (s *server) buscarMesa(ctx context.Context, collection *mongo.Collection, solicitud *pb.Solicitud) (*Mesa, bool, error) {
	// Normalizar preferencias
	prefs := normalizarPreferencias(solicitud.Preferences)

	// Buscar mesa exacta
	filter := bson.M{
		"status":   "Disponible",
		"capacity": bson.M{"$gte": solicitud.PartySize},
		"tipo":     bson.M{"$in": prefs},
	}

	opts := options.Find().SetSort(bson.D{{Key: "capacity", Value: 1}})
	cursor, err := collection.Find(ctx, filter, opts)
	if err == nil {
		defer cursor.Close(ctx)
		if cursor.Next(ctx) {
			var mesa Mesa
			cursor.Decode(&mesa)
			return &mesa, false, nil
		}
	}

	// Buscar mesa alternativa (sin coincidir preferencia)
	filterAlt := bson.M{
		"status":   "Disponible",
		"capacity": bson.M{"$gte": solicitud.PartySize},
		"tipo": bson.M{"$in": alternativasPermitidas(prefs)},
	}

	cursor2, err := collection.Find(ctx, filterAlt, opts)
	if err == nil {
		defer cursor2.Close(ctx)
		if cursor2.Next(ctx) {
			var mesa Mesa
			cursor2.Decode(&mesa)
			return &mesa, true, nil
		}
	}

	return nil, false, fmt.Errorf("No hay mesas disponibles.")
}

// Determina las alternativas permitidas según preferencia original
func alternativasPermitidas(prefs []string) []string {
	var result []string

	for _, p := range prefs {
		switch p {
		case "fumadores", "no fumadores":
			// No cambiar entre fumadores y no fumadores, sí cambiar entre interior/exterior
			result = append(result, "interior", "exterior", "aire libre")
		case "interior", "exterior", "aire libre":
			// No cambiar entre interior/exterior, sí cambiar entre fumadores/no fumadores
			result = append(result, "fumadores", "no fumadores")
		default:
			result = append(result, p)
		}
	}

	// Eliminar duplicados
	unique := make(map[string]bool)
	for _, v := range result {
		unique[v] = true
	}
	out := make([]string, 0, len(unique))
	for k := range unique {
		out = append(out, k)
	}

	return out
}

// Normalizar preferencias para búsqueda
func normalizarPreferencias(pref string) []string {
	switch pref {
	case "fumadores":
		return []string{"fumadores"}
	case "no fumadores":
		return []string{"no fumadores"}
	case "interior":
		return []string{"interior"}
	case "exterior", "aire libre":
		return []string{"exterior", "aire libre"}
	default:
		return []string{pref}
	}
}

// Registrar reserva en el servicio de registro
func (s *server) registrarReserva(reserva *pb.ReservaInfo) error {
	conn, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewRegistroServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.RegistrarReserva(ctx, reserva)
	return err
}

// Enviar notificación a RabbitMQ
func (s *server) enviarNotificacion(tipo, mensaje string) {
	notif := map[string]string{
		"tipo":    tipo,
		"mensaje": mensaje,
	}

	body, _ := json.Marshal(notif)

	err := s.rabbitCh.Publish(
		"",               // exchange
		"notificaciones", // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	if err != nil {
		log.Printf("Error al publicar notificación: %v", err)
	}
}

func main() {
	// Conectar a MongoDB
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	// Conectar a RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	// Declarar cola
	_, err = ch.QueueDeclare(
		"notificaciones",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Iniciar servidor gRPC
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Error al escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterReservaServiceServer(s, &server{
		dbClient:   client,
		rabbitConn: conn,
		rabbitCh:   ch,
	})

	log.Println("Servicio de Reservas escuchando en :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
