package main

import (
	"log"
	"os"

	"pharmacy-service/consumer"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load("../../.env")
	uri := os.Getenv("AMQP_URL")
	if uri == "" {
		log.Fatal("AMQP_URL kosong, cek file .env")
	}
	consumer.StartPrescriptionConsumer(uri)
}