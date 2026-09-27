package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

type scramClient struct {
	hash         func() hash.Hash
	client       *scram.Client
	conversation *scram.ClientConversation
}

func newSCRAMSHA256Client() sarama.SCRAMClient { return &scramClient{hash: sha256.New} }
func newSCRAMSHA512Client() sarama.SCRAMClient { return &scramClient{hash: sha512.New} }

func (c *scramClient) Begin(username, password, authzID string) error {
	client, err := scram.HashGeneratorFcn(c.hash).NewClient(username, password, authzID)
	if err != nil {
		return err
	}
	c.client = client
	c.conversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.conversation.Step(challenge)
}

func (c *scramClient) Done() bool { return c.conversation.Done() }
