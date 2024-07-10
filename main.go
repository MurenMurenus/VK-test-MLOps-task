package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Задаем клиента-подписчика для топика vk_test
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "vk_test",
		GroupID:  "processor",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	for {
		// Считывание и десериализация
		m, err := reader.ReadMessage(context.TODO())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var doc TDocument
		err = json.Unmarshal(m.Value, &doc)
		if err != nil {
			log.Printf("Error parsing message: %v\n", err)
			continue
		}

		// Вызов метода-обработчика новой версии документа
		res, err := doc.Process()

		if err != nil {
			log.Printf("Process finished with error: %v\n", err)
			continue
		}

		// Сериализация и запись результата в другую очередь
		if res != nil {
			// to_output, err := json.Marshal(res)
			// if err != nil {
			// 	log.Printf("Serializing finished with error: %v\n", err)
			// 	continue
			// }
			//
			// writer := kafka.NewWriter(kafka.WriterConfig{
			// 	Brokers:  []string{"localhost:9092"},
			// 	Topic:    "another_vk_test",
			// 	Balancer: &kafka.LeastBytes{},
			// })
			// message := kafka.Message{
			// 	Key:   []byte(res.Url),
			// 	Value: []byte(to_output),
			// }
			// err = writer.WriteMessages(context.Background(), message)
			// if err != nil {
			// 	fmt.Printf("Error writing message: %v\n", err)
			// }
			// writer.Close()

			fmt.Printf("Process output: %v\n", res)
		}
	}
}
