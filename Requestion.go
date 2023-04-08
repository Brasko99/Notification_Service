package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	jwt "github.com/dgrijalva/jwt-go"
)

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

func main() {
	// Устанавливаем соединение с базой данных PostgreSQL
	db, err := sql.Open(os.Getenv("DB_NAME"), "postgres://"+os.Getenv("DB_USER")+":"+os.Getenv("DB_PASSWORD")+"@"+os.Getenv("AUTH_DB_ADDR")+"?sslmode=disable")
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
