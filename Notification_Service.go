package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"net/http"
	"strings"
	"sync"
	jwt "github.com/dgrijalva/jwt-go"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Функция выдачи ошибки
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// Структура для принятия токена

type JWT struct {
	Payload struct {
		UUID string `json:"uuid"`
	} `json:"payload"`
	jwt.StandardClaims
}

// Структура для принятия записи из базы данных

type FromDB struct {
	Notification_id int64  `json:"notification_id"`
	UUID            string `json:"uuid"`
	Notification    string `json:"notification"`
	Is_Read         bool   `json:"is_read"`
	Target          string `json:"target"`
}

//объявляем структуры для Unmarshall`а поля "Notification" из базы данных

type RegistredData struct {
	UUID  string `json:"uuid"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

type ProjectStatusData struct {
	Project_id   uint     `json:"project_id"`
	ProjectTitle string   `json:"project_title"`
	NewStatus    string   `json:"new_status"`
	UUID         []string `json:"uuid"`
}

type ProjectJoinRequest struct {
	Project_id   uint   `json:"project_id"`
	ProjectTitle string `json:"project_title"`
	From         string `json:"from_name"`
	UUID         string `json:"user_uuid"`
}

type NegotiationData struct {
	Project_id   uint   `json:"project_id"`
	ProjectTitle string `json:"project_title"`
	UUID         string `json:"user_uuid"`
}

// Структура для отправляемого уведомления

type SendJson struct {
	Title           string `json:"title"`
	Body            string `json:"body"`
	Target          string `json:"target"`
	Target_data     uint   `json:"target_data"`
	Is_read         bool   `json:"is_read"`
	Notification_id int64  `json:"notification_id"`
}

// Функция сохранения в бд уведомления о регистрации

func Notification_Registration(wg *sync.WaitGroup) {
	//создаём connection
	conn, err := amqp.Dial("amqp://" + os.Getenv("RABBITMQ_USER") + ":" + os.Getenv("RABBITMQ_PASS") + "@" + os.Getenv("RABBITMQ_ADDR"))
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
	db, err := sql.Open(os.Getenv("DB_NAME"), "postgres://"+os.Getenv("DB_USER")+":"+os.Getenv("DB_PASSWORD")+"@"+os.Getenv("NOTIF_DB_ADDR")+"?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Создаем таблицу для сообщений, если она не существует
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS notification (id SERIAL PRIMARY KEY, uuid VARCHAR(36), notification TEXT, read BOOLEAN DEFAULT false, target TEXT)")
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
			_, err = db.Exec("INSERT INTO notification_registration (uuid, notification, target) VALUES ($1, $2, $3)", uuid, message, target)
			if err != nil {
				log.Fatalf("Failed to insert message into database: %v", err)
			}
		}
	}()
	<-forever
}

// Функция сохранения в бд уведомления об изменении статуса проекта

func Notification_ProjectStatus(wg *sync.WaitGroup) {
	//создаём connection
	conn, err := amqp.Dial("amqp://" + os.Getenv("RABBITMQ_USER") + ":" + os.Getenv("RABBITMQ_PASS") + "@" + os.Getenv("RABBITMQ_ADDR"))
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
		"Notification_ProjectStatus", // name
		true,                         // durable
		false,                        // delete when unused
		false,                        // exclusive
		false,                        // no-wait
		nil,                          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//делаем Binding Queue
	err = ch.QueueBind(
		q.Name,                   // queue name
		"project.status_changed", // routing key
		"main",                   // exchange
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
	type ProjectStatusData struct {
		Project_id   uint     `json:"project_id"`
		ProjectTitle string   `json:"project_title"`
		NewStatus    string   `json:"new_status"`
		UUID         []string `json:"uuid"`
	}

	// Устанавливаем соединение с базой данных PostgreSQL
	db, err := sql.Open(os.Getenv("DB_NAME"), "postgres://"+os.Getenv("DB_USER")+":"+os.Getenv("DB_PASSWORD")+"@"+os.Getenv("NOTIF_DB_ADDR")+"?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Создаем таблицу для сообщений, если она не существует
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS notification (id SERIAL PRIMARY KEY, uuid VARCHAR(36), notification TEXT, read BOOLEAN DEFAULT false, target TEXT)")
	if err != nil {
		log.Fatalf("Failed to create messages table: %v", err)
	}

	var forever chan struct{}

	// Обрабатываем полученные сообщения и сохраняем их в базе данных
	go func() {
		for d := range msgs {
			message := string(d.Body)
			var RegistredData ProjectStatusData
			err := json.Unmarshal([]byte(message), &RegistredData)
			if err != nil {
				log.Fatalf("Failed to unmarshal message: %v", err)
			}
			uuid := RegistredData.UUID
			firstUUID, secondUUID := uuid[0], uuid[1]
			target := "status_changed"
			// Сохраняем сообщения в базе данных
			_, err = db.Exec("INSERT INTO notification_registration (uuid, notification, target) VALUES ($1, $2, $3)", firstUUID, message, target)
			_, err = db.Exec("INSERT INTO notification_registration (uuid, notification, target) VALUES ($1, $2, $3)", secondUUID, message, target)
			if err != nil {
				log.Fatalf("Failed to insert message into database: %v", err)
			}
		}
	}()
	<-forever
}

// Функция сохранения в бд уведомления об отклике на проект

func Notification_ProjectJoinRequest(wg *sync.WaitGroup) {
	//создаём connection
	conn, err := amqp.Dial("amqp://" + os.Getenv("RABBITMQ_USER") + ":" + os.Getenv("RABBITMQ_PASS") + "@" + os.Getenv("RABBITMQ_ADDR"))
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
		"Notification_ProjectJoinRequest", // name
		true,                              // durable
		false,                             // delete when unused
		false,                             // exclusive
		false,                             // no-wait
		nil,                               // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//делаем Binding Queue
	err = ch.QueueBind(
		q.Name,                 // queue name
		"project.join_request", // routing key
		"main",                 // exchange
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
	type ProjectJoinRequest struct {
		Project_id   uint   `json:"project_id"`
		ProjectTitle string `json:"project_title"`
		From         string `json:"from_name"`
		UUID         string `json:"user_uuid"`
	}

	// Устанавливаем соединение с базой данных PostgreSQL
	db, err := sql.Open(os.Getenv("DB_NAME"), "postgres://"+os.Getenv("DB_USER")+":"+os.Getenv("DB_PASSWORD")+"@"+os.Getenv("NOTIF_DB_ADDR")+"?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Создаем таблицу для сообщений, если она не существует
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS notification (id SERIAL PRIMARY KEY, uuid VARCHAR(36), notification TEXT, read BOOLEAN DEFAULT false, target TEXT)")
	if err != nil {
		log.Fatalf("Failed to create messages table: %v", err)
	}

	var forever chan struct{}

	// Обрабатываем полученные сообщения и сохраняем их в базе данных
	go func() {
		for d := range msgs {
			message := string(d.Body)
			var RegistredData ProjectJoinRequest
			err := json.Unmarshal([]byte(message), &RegistredData)
			if err != nil {
				log.Fatalf("Failed to unmarshal message: %v", err)
			}
			uuid := RegistredData.UUID
			target := "responce"
			// Сохраняем сообщение в базе данных
			_, err = db.Exec("INSERT INTO notification_registration (uuid, notification, target) VALUES ($1, $2, $3)", uuid, message, target)
			if err != nil {
				log.Fatalf("Failed to insert message into database: %v", err)
			}
		}
	}()
	<-forever
}

// Функция сохранения в бд уведомления о новом согласовании в проекте

func Notification_Negotaition(wg *sync.WaitGroup) {
	//создаём connection
	conn, err := amqp.Dial("amqp://" + os.Getenv("RABBITMQ_USER") + ":" + os.Getenv("RABBITMQ_PASS") + "@" + os.Getenv("RABBITMQ_ADDR"))
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
		"Notification_Negotaition", // name
		true,                       // durable
		false,                      // delete when unused
		false,                      // exclusive
		false,                      // no-wait
		nil,                        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//делаем Binding Queue
	err = ch.QueueBind(
		q.Name,                        // queue name
		"project.negotiation.created", // routing key
		"main",                        // exchange
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
	type NegotiationData struct {
		Project_id   uint   `json:"project_id"`
		ProjectTitle string `json:"project_title"`
		UUID         string `json:"user_uuid"`
	}

	// Устанавливаем соединение с базой данных PostgreSQL
	db, err := sql.Open(os.Getenv("DB_NAME"), "postgres://"+os.Getenv("DB_USER")+":"+os.Getenv("DB_PASSWORD")+"@"+os.Getenv("NOTIF_DB_ADDR")+"?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Создаем таблицу для сообщений, если она не существует
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS notification (id SERIAL PRIMARY KEY, uuid VARCHAR(36), notification TEXT, read BOOLEAN DEFAULT false, target TEXT)")
	if err != nil {
		log.Fatalf("Failed to create messages table: %v", err)
	}

	var forever chan struct{}

	// Обрабатываем полученные сообщения и сохраняем их в базе данных
	go func() {
		for d := range msgs {
			message := string(d.Body)
			var RegistredData NegotiationData
			err := json.Unmarshal([]byte(message), &RegistredData)
			if err != nil {
				log.Fatalf("Failed to unmarshal message: %v", err)
			}
			uuid := RegistredData.UUID
			target := "negotiation"
			// Сохраняем сообщение в базе данных
			_, err = db.Exec("INSERT INTO notification_registration (uuid, notification, target) VALUES ($1, $2, $3)", uuid, message, target)
			if err != nil {
				log.Fatalf("Failed to insert message into database: %v", err)
			}
		}
	}()
	<-forever
}

// Функция выдачи ответа на GET-запрос

func Requestions(wg *sync.WaitGroup) {
	// Устанавливаем соединение с базой данных PostgreSQL
	db, err := sql.Open(os.Getenv("DB_NAME"), "postgres://"+os.Getenv("DB_USER")+":"+os.Getenv("DB_PASSWORD")+"@"+os.Getenv("NOTIF_DB_ADDR")+"?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	var forever chan struct{}

	// Принимаем запрос с токеном

	claims := &JWT{}

	http.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token == "" {
			http.Error(w, "Authorization header is missing", http.StatusUnauthorized)
			return
		}

		// Парсим токен

		splitToken := strings.Split(token, "Bearer ")
		if len(splitToken) != 2 {
			http.Error(w, "Invalid token format", http.StatusUnauthorized)
			return
		}

		tokenString := splitToken[1]
		_, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			return []byte(os.Getenv("JWT_SECRET")), nil // ЗАМЕНИТЬ НА СЕКРЕТНЫЙ КЛЮЧ!!!!
		})

		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		uuid := claims.Payload.UUID

		// Ищем совпадения uuid в строках БД с uuid из токена и сохраняем совпадающие записи в переменную

		rows, err := db.Query("SELECT * FROM notification_registration WHERE uuid=$1", uuid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		//Обрабатываем каждое уведомление

		var registrations string

		for rows.Next() {
			var registration FromDB
			err := rows.Scan(&registration.Notification_id, &registration.UUID, &registration.Notification, &registration.Is_Read, &registration.Target)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Начианаем формировать отправляемый json
			var Sent_Notification SendJson
			Sent_Notification.Notification_id = registration.Notification_id
			Sent_Notification.Is_read = registration.Is_Read
			Sent_Notification.Target = registration.Target

			// В зависимости от "target" формируем оставшиеся поля отправляемого уведомления

			switch registration.Target {

			// Для "Согласования"

			case "negotiation":
				var Unpack_Notific NegotiationData
				err := json.Unmarshal([]byte(registration.Notification), &Unpack_Notific)
				if err != nil {
					log.Fatalf("Failed to unmarshal message: %v", err)
				}
				Sent_Notification.Title = "Согласование"
				Sent_Notification.Body = fmt.Sprintf("В проекте %s необходимо произвести согласование.", Unpack_Notific.ProjectTitle)
				Sent_Notification.Target_data = Unpack_Notific.Project_id
			// Для "Отклика"

			case "responce":
				var Unpack_Notific ProjectJoinRequest
				err := json.Unmarshal([]byte(registration.Notification), &Unpack_Notific)
				if err != nil {
					log.Fatalf("Failed to unmarshal message: %v", err)
				}
				Sent_Notification.Title = "Отклик"
				Sent_Notification.Body = fmt.Sprintf("На проект %s откликнулся исполнитель %s.", Unpack_Notific.ProjectTitle, Unpack_Notific.From)
				Sent_Notification.Target_data = Unpack_Notific.Project_id

			// Для "Изменение статуса"

			case "status_changed":
				var Unpack_Notific ProjectStatusData
				err := json.Unmarshal([]byte(registration.Notification), &Unpack_Notific)
				if err != nil {
					log.Fatalf("Failed to unmarshal message: %v", err)
				}
				Sent_Notification.Title = "Изменение статуса"
				Sent_Notification.Body = fmt.Sprintf("Статус проекта %s изменён на %s.", Unpack_Notific.ProjectTitle, Unpack_Notific.NewStatus)
				Sent_Notification.Target_data = Unpack_Notific.Project_id
			// Для "Заполните профиль"

			case "fill_user":
				var Unpack_Notific RegistredData
				err := json.Unmarshal([]byte(registration.Notification), &Unpack_Notific)
				if err != nil {
					log.Fatalf("Failed to unmarshal message: %v", err)
				}
				Sent_Notification.Title = "Успешная регистрация"
				Sent_Notification.Body = fmt.Sprintf("Добро пожаловать на наш сервис, %s. Вы зарегистрированы в роли %s Для завершени регистрации заполните профиль.", Unpack_Notific.Email, Unpack_Notific.Role)
				Sent_Notification.Target_data = 0
			// Обработка неизвестных значений

			default:
				fmt.Println(`Не удалось определить "Registration.Target" и классифицировать уведомление`)
			}

			// Делаем Marshall отправляемого json и переводим его в string, чтобы накапливать в массиве
			Mrshl_Sent_Notification, err := json.Marshal(Sent_Notification)
			strSentNotification := string(Mrshl_Sent_Notification)

			// Накапливаем Уведомления в одну переменную
			var registrations []string
			registrations = append(registrations, strSentNotification)
		}

		// Проверяем на ошибки после выхода из цикла

		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Отправляем полученный массив на фронтэнд

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(registrations)
		<-forever
	})
}

// Мы создаем пул горутин с помощью канала pool. Размер пула определяется переменной poolSize. 
// Затем мы запускаем пять функций в своих горутинах, каждая из которых будет выполнять свою работу. 
// Функции запускаются с помощью анонимных функций, которые добавляются в пул горутин и выполняются параллельно.
// Мы используем структуру struct{}{} в качестве элемента пула горутин. Это пустая структура, которая не занимает дополнительной памяти и 
// служит только для синхронизации работы горутин в пуле.

func main() {
	for {
		var wg sync.WaitGroup

		poolSize := 5
		pool := make(chan struct{}, poolSize)

		wg.Add(5)
		go func() {
		pool <- struct{}{}
		Notification_Registration(&wg)
		<-pool
		}()
		go func() {
		pool <- struct{}{}
		Notification_ProjectStatus(&wg)
		<-pool
		}()
		go func() {
		pool <- struct{}{}
		Notification_ProjectJoinRequest(&wg)
		<-pool
		}()
		go func() {
		pool <- struct{}{}
		Notification_Negotaition(&wg)
		<-pool
		}()
		go func() {
		pool <- struct{}{}
		Requestions(&wg)
		<-pool
		}()

		wg.Wait()
	}
}