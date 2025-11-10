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
var subscriberLookPool sync.Map // 用于存储订阅者，防止重复创建
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

func GetPublisher(topic string) (publisher message.Publisher) {
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

func GetSubscriber(topic string) (subscriber message.Subscriber) {
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

// StartSubscriberOnce 防止重复创建订阅者，例如：重复调用订阅者，会导致重复消费消息
func StartSubscriberOnce(consumer Consumer) (err error) {
	topic := consumer.Topic
	if topic == "" {
		err = errors.Errorf("StartSubscriberOnce Topic required, consumer:%s", consumer.String())
		return err
	}
	_, ok := subscriberLookPool.LoadOrStore(topic, true)
	if ok { //已经存在
		return nil
	}
	err = consumer.Consume()
	if err != nil {
		subscriberLookPool.Delete(topic)
	}
	return err
}

type Consumer struct {
	Description string                             `json:"description"`
	Topic       string                             `json:"topic"`
	Subscriber  message.Subscriber                 `json:"-"` //
	WorkFn      func(message *Message) (err error) `json:"-"`
	Logger      watermill.LoggerAdapter            `json:"-"` // 日志适配器，如果不设置则使用默认日志适配器
}

func (c Consumer) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}

func (s Consumer) Consume() (err error) {
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
	if s.Subscriber == nil {
		err = errors.Errorf("Subscriber.Consume Subscriber required, consume:%s", s.String())
		return err
	}
	go func() {
		msgChan, err := s.Subscriber.Subscribe(context.Background(), s.Topic)
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
