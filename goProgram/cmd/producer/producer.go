package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	models "kafka-notify/pkg"
	"log"
	"net/http"
	"strconv"
)

const (
	// Broker is the Kafka broker address
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	kafkaTopic         = "notifications"
)

// =============HELPER FUNCTIONS==============

var ErrUserNotFoundInProducer = errors.New("user not found in producer")

func findUserById(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}
	return models.User{}, ErrUserNotFoundInProducer
}

func getIdFromRequest(formValue string, ctx *gin.Context) (int, error) {
	id, err := strconv.Atoi(ctx.PostForm(formValue)) //Convert string to int ASCII to Int, kiểu string phải là số
	if err != nil {
		return 0, fmt.Errorf("Fail to parse ID from value %s: %w", formValue, err)
	}
	return id, nil
}

// ============== KAFKA RELATED FUNCTIONS ==============
// * sarama.SyncProducer là gửi message đồng bộ, phải chờ xác nhận từ Kafka server thì mới thực hiện tác vụ khác
// đảm bảo dữ liệu đã được ghi thành công, tính nhất quán và an toàn dữ liệu
func sendKafkaMessage(producer sarama.SyncProducer,
	users []models.User, ctx *gin.Context, fromID, toID int) error {
	message := ctx.PostForm("message")
	fromUser, err := findUserById(fromID, users)
	if err != nil {
		return err
	}

	toUser, err := findUserById(toID, users)
	if err != nil {
		return err
	}

	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: message,
	}

	//parse to Json, ngược lại là unMarshal
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("Failed to marshal notification: %w", err) //wrapping error đễ dễ dàng đọc lỗi và kiểm soát
	}

	//Sử dụng &sarama.ProducerMessage là để tạo msg có kiểu là biến con trỏ
	// mục đích sau khi tạo ra nó, thì có thể thao tác thay đổi giá trị trực tiếp của nó, nếu không dùng pointer thì ko thay đổi được
	//EXAMPLE:
	//msg.Topic = "NewTopic"
	//msg.Key = sarama.StringEncoder("NewKey")
	//msg.Value = sarama.StringEncoder("NewValue")
	msg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)), //Convert int to string  int to ASCII
		Value: sarama.StringEncoder(notificationJSON),        //StringEncoder là để parse sang kiểu dữ liệu có thể gửi cho Kafka
	}
	// return 3 value: partition, offset, error
	/*
		partition: số partition của topic mà thông điệp đã được gửi đến. Mỗi topic có thể được chia thành nhiều partition để phân tán dữ liệu.
		offset: vị trí của partition
	*/
	_, _, err = producer.SendMessage(msg)
	return err
}

func sendMessageHandler(producer sarama.SyncProducer, users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		fromID, err := getIdFromRequest("fromID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}

		toID, err := getIdFromRequest("toID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}

		err = sendKafkaMessage(producer, users, ctx, fromID, toID)
		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		ctx.JSON(http.StatusOK, gin.H{
			"message": "Notification sent successfully!",
		})
	}
}

/*
Việc cấu hình Return.Successes là một phần quan trọng trong quá trình xác nhận và đảm bảo tính nhất quán khi gửi thông điệp đến Kafka.
Nếu không bật tùy chọn này, bạn sẽ không biết được thông điệp đã gửi thành công hay không,
và dữ liệu có thể bị mất hoặc không nhất quán trong trường hợp lỗi.
*/
//config.Producer.Flush nếu muốn cấu hình
func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress},
		config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

func main() {
	users := []models.User{
		{ID: 1, Name: "Emma"},
		{ID: 2, Name: "Bruno"},
		{ID: 3, Name: "Rick"},
		{ID: 4, Name: "Lena"},
	}

	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()
	//sử dụng để đảm bảo hàm Close được gọi khi scope này được thực thi xong,
	//và sẽ đóng đúng cách

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/send", sendMessageHandler(producer, users))

	fmt.Printf("Kafka PRODUCER 📨 started at http://localhost%s\n",
		ProducerPort)

	if err := router.Run(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
