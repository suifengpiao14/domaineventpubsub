package domaineventpubsub

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/pkg/errors"
)

var gochannelPool sync.Map
var MessageLogger = watermill.NewStdLogger(false, false)

func newGoChannel() (pubsub *gochannel.GoChannel) {
	pubsub = gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: false, // 等待订阅者ack消息,防止消息丢失（关闭前一定已经消费完，内部的主要用于数据异构，所以需要确保数据已经处理完）
		},
		MessageLogger,
	)
	return pubsub
}

func Publish(topic string, msg *Message) (err error) {
	publisher := getPublisher(topic)
	err = publisher.Publish(topic, msg) // 发布消息
	if err != nil {
		err = errors.Errorf("Publish message failed: %v", msg)
		return err
	}
	return nil
}

var consumerPool sync.Map

// 注册消费者，如果已存在则不重复创建订阅者

func RegisterConsumer(consumer Consumer) (err error) {
	if _, loaded := consumerPool.LoadOrStore(consumer.Topic, &consumer); loaded { // 已存在，则不重复创建订阅者
		err = errors.Errorf("RegisterConsumer topic:%s already exist", consumer.Topic)
		return err
	}
	return nil
}

// 启动消费者

func StartConsumer() (err error) {
	consumerPool.Range(func(key, value any) bool {
		consumer := value.(*Consumer)
		err = consumer.Consume()
		ok := err == nil
		return ok
	})
	if err != nil {
		return err
	}
	return nil
}

func getPublisher(topic string) (publisher message.Publisher) {
	value, ok := gochannelPool.Load(topic)
	if ok {
		publisher = value.(message.Publisher)
		return publisher
	}
	pubsub := newGoChannel()
	gochannelPool.Store(topic, pubsub)
	publisher = pubsub
	return publisher
}

func getSubscriber(topic string) (subscriber message.Subscriber) {
	value, ok := gochannelPool.Load(topic)
	if ok {
		subscriber = value.(message.Subscriber)
		return subscriber
	}
	pubsub := newGoChannel()
	gochannelPool.Store(topic, pubsub)
	subscriber = pubsub
	return subscriber
}

type Consumer struct {
	Description string                             `json:"description"`
	Topic       string                             `json:"topic"`
	WorkFn      func(message *Message) (err error) `json:"-"`
	Logger      watermill.LoggerAdapter            `json:"-"` // 日志适配器，如果不设置则使用默认日志适配器
}

func (c Consumer) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}

var consumerRunningMap sync.Map

func (s Consumer) Consume() (err error) {
	if _, loaded := consumerRunningMap.LoadOrStore(s.Topic, struct{}{}); loaded { // 已存在，则不重复创建订阅者
		return nil
	}
	logger := s.Logger
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}
	if s.Topic == "" {
		err = errors.Errorf("Subscriber.Consume Topic required, consume:%s", s.String())
		return err
	}
	if s.WorkFn == nil {
		err = errors.Errorf("Subscriber.Consume WorkFn required, consume:%s", s.String())
		return err
	}
	subscriber := getSubscriber(s.Topic)
	go func() {
		msgChan, err := subscriber.Subscribe(context.Background(), s.Topic)
		if err != nil {
			logger.Error("Subscriber.Consumer.Subscribe", err, nil)
			return
		}
		for msg := range msgChan {
			func() { // 使用函数包裹，提供defer 处理 ack 操作，防止消息丢失
				defer msg.Ack()
				err = s.WorkFn(msg)
				if err != nil {
					logger.Error("Subscriber.SubscriberFn", err, nil)
				}
			}()
		}
	}()
	return nil
}

func MakeMessage(event any) (msg *Message, err error) {
	b, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	msg = message.NewMessage(watermill.NewUUID(), b)
	return msg, nil
}

type Message = message.Message

type EventMessage interface {
	ToMessage() (msg *Message, err error)
}

func MakeWorkFn[Event any](doFn func(event Event) (err error)) (fn func(msg *Message) error) {
	return func(msg *Message) error {
		var event Event
		err := json.Unmarshal(msg.Payload, &event)
		if err != nil {
			return err
		}
		err = doFn(event)
		if err != nil {
			return err
		}
		return nil
	}
}
