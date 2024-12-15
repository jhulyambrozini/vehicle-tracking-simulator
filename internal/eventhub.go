package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
)

type EventHub struct {
	routeService    *RouteService
	mongoClient     *mongo.Client
	chDriverMOved   chan *DriverMovedEvent
	freighWriter    *kafka.Writer
	simulatorWriter *kafka.Writer
}

func NewEventHub(routeService *RouteService, mongoClient *mongo.Client, chDriverMoved chan *DriverMovedEvent, freightWriter, simulatorWriter *kafka.Writer) *EventHub {
	return &EventHub{
		routeService:    routeService,
		mongoClient:     mongoClient,
		chDriverMOved:   chDriverMoved,
		freighWriter:    freightWriter,
		simulatorWriter: simulatorWriter,
	}
}

func (eh *EventHub) HandlerEvent(msg []byte) error {
	var baseEvent struct {
		EventName string `json:"event"`
	}

	err := json.Unmarshal(msg, &baseEvent)
	if err != nil {
		return fmt.Errorf("error unmarshalling event: %w", err)
	}

	switch baseEvent.EventName {
	case "RouteCreated":
		var event RouteCreatedEvent
		err := json.Unmarshal(msg, &event)
		if err != nil {
			return fmt.Errorf("error unmashallimh event: %w", err)
		}
		return eh.handlerRouteCreated(event)
	case "DeliveryStarted":
		var event DeliveryStartedEvent
		err := json.Unmarshal(msg, &event)
		if err != nil {
			return fmt.Errorf("error unmashallimh event: %w", err)
		}
		return eh.handlerDeliveryStarted(event)
	default:
		return errors.New("Unkown event")
	}
}

func (eh *EventHub) handlerRouteCreated(event RouteCreatedEvent) error {
	freightCalculatedEvent, err := RouteCreatedHandler(&event, eh.routeService)
	if err != nil {
		return err
	}
	value, err := json.Marshal(freightCalculatedEvent)
	if err != nil {
		return err
	}

	// publicar no APACHE KAFKA
	err = eh.freighWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(freightCalculatedEvent.RouteID),
		Value: value})
	if err != nil {
		return err
	}
	return nil
}

func (eh *EventHub) handlerDeliveryStarted(event DeliveryStartedEvent) error {
	err := DeliveryStartedHandler(&event, eh.routeService, eh.chDriverMOved)
	if err != nil {
		return err
	}
	// ler canal e publicar no APACHE KAFKA
	go eh.sendDirections() // --> rodando em background
	return nil
}

func (eh *EventHub) sendDirections() {
	for {
		select {
		case movedEvent := <-eh.chDriverMOved:
			value, err := json.Marshal((movedEvent))
			if err != nil {
				return
			}
			err = eh.simulatorWriter.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(movedEvent.RouteID),
				Value: value,
			})
			if err != nil {
				return
			}
		case <-time.After(500 * time.Millisecond):
			return
		}
	}

}
