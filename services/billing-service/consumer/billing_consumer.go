package consumer

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BillingEnvelope struct {
	EventID string `json:"event_id"`
	Payload struct {
		AppointmentID  string `json:"appointment_id"`
		PatientID      string `json:"patient_id"`
		BillingSummary struct {
			ConsultationFee float64 `json:"consultation_fee"`
			TriageUrgency   string  `json:"triage_urgency"`
		} `json:"billing_summary"`
	} `json:"payload"`
}

func StartBillingConsumer(amqpURI string) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal koneksi broker: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("medcore.clinical.events", "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("[Billing Consumer] Gagal deklarasi exchange: %v", err)
	}

	q, err := ch.QueueDeclare("billing.invoice.q", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal deklarasi queue: %v", err)
	}

	// Sesuai diagram modul: binding "clinical.*.completed"
	if err := ch.QueueBind(q.Name, "clinical.*.completed", "medcore.clinical.events", false, nil); err != nil {
		log.Fatalf("[Billing Consumer] Gagal binding queue: %v", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("[Billing Consumer] Gagal set QoS: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "billing-worker", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal subscribe: %v", err)
	}

	log.Printf("[Billing Consumer] Menunggu event pada antrean: %s", q.Name)

	for d := range msgs {
		var evt BillingEnvelope
		if err := json.Unmarshal(d.Body, &evt); err != nil {
			log.Printf("[Billing Consumer] Format pesan tidak valid, NACK: %v", err)
			d.Nack(false, false)
			continue
		}

		fee := evt.Payload.BillingSummary.ConsultationFee
		log.Println("---------------------------------------------------------")
		log.Printf("[Billing Consumer] Membuat Draf Tagihan")
		log.Printf("  Event ID       : %s", evt.EventID)
		log.Printf("  Appointment ID : %s", evt.Payload.AppointmentID)
		log.Printf("  Pasien         : %s", evt.Payload.PatientID)
		log.Printf("  Biaya Konsul   : Rp%.0f (Triage: %s)", fee, evt.Payload.BillingSummary.TriageUrgency)
		log.Println("---------------------------------------------------------")

		d.Ack(false)
		log.Println("[Billing Consumer] Pesan berhasil di-ACK.")
	}
}