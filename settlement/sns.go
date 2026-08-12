package settlement

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// SNSMessage is the AWS SNS envelope used by Circle Mint notifications.
type SNSMessage struct {
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	TopicARN         string `json:"TopicArn"`
	Subject          string `json:"Subject"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertURL   string `json:"SigningCertURL"`
	SubscribeURL     string `json:"SubscribeURL"`
	Token            string `json:"Token"`
}

func (m SNSMessage) Verify(ctx context.Context, client *http.Client, topicARN string) error {
	if m.Type != "Notification" && m.Type != "SubscriptionConfirmation" {
		return errors.New("unsupported SNS message type")
	}
	if topicARN != "" && m.TopicARN != topicARN {
		return errors.New("unexpected SNS topic ARN")
	}
	u, err := url.Parse(m.SigningCertURL)
	if err != nil || !trustedSNSURL(u) {
		return errors.New("untrusted SNS signing certificate URL")
	}
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.SigningCertURL, nil)
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return errors.New("invalid SNS certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	key, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return errors.New("SNS certificate is not RSA")
	}
	sig, err := base64.StdEncoding.DecodeString(m.Signature)
	if err != nil {
		return err
	}
	var hash crypto.Hash
	var sum []byte
	switch m.SignatureVersion {
	case "1":
		hash = crypto.SHA1
		s := sha1.Sum([]byte(m.stringToSign()))
		sum = s[:]
	case "2":
		hash = crypto.SHA256
		s := sha256.Sum256([]byte(m.stringToSign()))
		sum = s[:]
	default:
		return errors.New("unsupported SNS signature version")
	}
	return rsa.VerifyPKCS1v15(key, hash, sum, sig)
}
func (m SNSMessage) stringToSign() string {
	fields := []string{"Message", m.Message, "MessageId", m.MessageID}
	if m.Type != "Notification" {
		fields = append(fields, "SubscribeURL", m.SubscribeURL)
	} else if m.Subject != "" {
		fields = append(fields, "Subject", m.Subject)
	}
	fields = append(fields, "Timestamp", m.Timestamp, "TopicArn", m.TopicARN, "Type", m.Type)
	return strings.Join(fields, "\n")
}
func DecodeSNS(raw []byte) (SNSMessage, error) {
	var m SNSMessage
	err := json.NewDecoder(bytes.NewReader(raw)).Decode(&m)
	return m, err
}
func ConfirmSNS(ctx context.Context, client *http.Client, subscribeURL string) error {
	u, err := url.Parse(subscribeURL)
	if err != nil || !trustedSNSURL(u) {
		return errors.New("untrusted SNS subscribe URL")
	}
	if client == nil {
		client = http.DefaultClient
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, subscribeURL, nil)
	if err != nil {
		return err
	}
	res, err := client.Do(r)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("confirm SNS subscription: %s", res.Status)
	}
	return nil
}

func trustedSNSURL(u *url.URL) bool {
	return u.Scheme == "https" && (strings.HasPrefix(u.Host, "sns.") || strings.HasPrefix(u.Host, "sns-fips.")) && strings.HasSuffix(u.Host, ".amazonaws.com")
}
