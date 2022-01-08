package rabbitMQConnector

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/streadway/amqp"
)

type RabbitConnection struct {
	name     string
	username string
	password string
	hostname string
	port     int
	certPath string
	conn     *amqp.Connection
	channel  *amqp.Channel
	queues   []string
	err      chan error
}

//NewConnection creates and initializes a RabbitMQ TLS connection struct
func NewConnection(name string, queues []string, username string,
	password string, hostname string, port int, certPath string) *RabbitConnection {
	c := &RabbitConnection{
		name:     name,
		username: username,
		password: password,
		hostname: hostname,
		port:     port,
		queues:   queues,
		certPath: certPath,
		err:      make(chan error),
	}

	return c
}

//Connect connects to a remote rabbitmq server
func (c *RabbitConnection) Connect() error {
	log.Printf("[*] Creating channel")

	caCert, err := ioutil.ReadFile(c.certPath)
	failOnError(err, "Failed to read CA certificate")
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: true, //self-signed
	}

	c.conn, err = amqp.DialTLS(
		fmt.Sprintf("amqps://%s:%s@%s:%d", c.username,
			c.password, c.hostname, c.port),
		tlsConfig)
	failOnError(err, "Failed to connect to RabbitMQ server")

	log.Printf("[+] Connected")

	//Listen to NotifyClose events and send error to connection obj channel
	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		c.err <- errors.New("connection Closed")
	}()

	//Create the channel now
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	err = c.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	//Create the queues now
	for _, q := range c.queues {
		if _, err := c.channel.QueueDeclare(
			q,     // name
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			amqp.Table{
				"x-max-priority": uint8(2),
			}, // arguments
		); err != nil {
			return fmt.Errorf("error in declaring the queue %s", err)
		}
		log.Printf("[+] Queue %s created\n", q)
	}

	return nil
}

//publishMessage publishes a message containing the body b to the queue
//indicated by key k on the channel ch. The priority should be either 0 or 1 for
//low or high, respectively
func (c *RabbitConnection) PublishMessage(key string, b []byte, priority uint8) error {

	err := c.channel.Publish(
		"",    // exchange
		key,   // routing key
		false, // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         b,
			Priority:     priority,
		})

	return err
}

//Consume returns messages from a channel
func (c *RabbitConnection) ConsumeMsgs(queuename string) <-chan amqp.Delivery {

	msgs, err := c.channel.Consume(
		queuename, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	return msgs
}

//Reconnect reconnects to the RabbitMQ server configured
func (c *RabbitConnection) Reconnect() error {
	if err := c.Connect(); err != nil {
		return err
	}
	return nil
}

//failOnError logs the error received and a custom error message, then dies
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
