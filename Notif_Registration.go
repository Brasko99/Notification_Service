package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

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
		"mail_service", // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
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

	//Получаем сообщение
	msgs, err := ch.Consume(
		q.Name, // queue
		"mail", // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	//объявляем структуру для принятого сообщения
	type RegistredData struct {
		UUID  string `json:"uuid"`
		Email string `json:"email"`
		Role  string `json:"role"`
	}

	//объявляем структуру для отправляемого сообщения
	type APIjson struct {
		Title       string `json:"title"`
		Body        string `json:"body"`
		Target      string `json:"target"`
		Target_data string `json:"target_data"`
	}

	var forever chan struct{}

	//обрабатываем сообщение
	go func() {
		for d := range msgs {
			//принимаем сообщение в переменную
			var ReceivedMessage string = string(d.Body)

			//заполняем отправляемый json
			//SentNotif := APIjson{"Registration", ReceivedMessage, "fill_user", " "}
			//SentNotifMarshal, err := json.Marshal(SentNotif)

			//создаём функцию ответа на запрос
			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

				// Проверяем, что запрос пришел методом POST
				if r.Method == "GET" {

					// Создаем JSON-ответ для фронтэнда
					resp := APIjson{
						Title:       "Registration",
						Body:        ReceivedMessage,
						Target:      "fill_user",
						Target_data: " ",
					}

					// Отправляем JSON-ответ фронтэнду
					w.Header().Set("Content-Type", "application/json")
					err = json.NewEncoder(w).Encode(resp)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
				} else {
					http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
					return
				}
			})
		}
	}()
	http.ListenAndServe(":8080", nil)
	<-forever
}
