package new0

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Функция выдачи ошибки
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	//создаём connection
	conn, err := amqp.Dial("ampq://" + os.Getenv("RABBITMQ_USER") + ":" + os.Getenv("RABBITMQ_PASS") + "@" + os.Getenv("RABBITMQ_ADDR"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	//создаём канал
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//создаём Exchange
	err = ch.ExchangeDeclare(
		"main",  // name
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	//создаём очередь Queue
	q, err := ch.QueueDeclare(
		"Notification_Registration", // name
		true,                        // durable
		false,                       // delete when unused
		false,                       // exclusive
		false,                       // no-wait
		nil,                         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//делаем Binding Queue
	err = ch.QueueBind(
		q.Name,           // queue name
		"user.registred", // routing key
		"main",           // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	//Получаемое сообщение
	msgs, err := ch.Consume(
		q.Name,            // queue
		"Notification_DB", // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	//объявляем структуру для принятого сообщения
	type RegistredData struct {
		UUID  string `json:"uuid"`
		Email string `json:"email"`
		Role  string `json:"role"`
	}

	// Устанавливаем соединение с базой данных PostgreSQL
	db, err := sql.Open("postgres", "postgres://user:password@localhost/mydatabase?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Создаем таблицу для сообщений, если она не существует
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS notification (id SERIAL PRIMARY KEY, uuid VARCHAR(36), notification TEXT, read BOOLEAN DEFAULT false, target TEXT, project_id INTEGER)")
	if err != nil {
		log.Fatalf("Failed to create messages table: %v", err)
	}

	var forever chan struct{}

	// Обрабатываем полученные сообщения и сохраняем их в базе данных
	go func() {
		for d := range msgs {
			message := string(d.Body)
			var RegistredData RegistredData
			err := json.Unmarshal([]byte(message), &RegistredData)
			if err != nil {
				log.Fatalf("Failed to unmarshal message: %v", err)
			}
			uuid := RegistredData.UUID
			target := "fill_user"
			// Сохраняем сообщение в базе данных
			_, err = db.Exec("INSERT INTO notification_registration (uuid, notification, type) VALUES ($1, $2, $3)", uuid, message, target)
			if err != nil {
				log.Fatalf("Failed to insert message into database: %v", err)
			}
		}
	}()
	<-forever
}
