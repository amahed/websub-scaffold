package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"net/http"
)

type Subscriber struct {
	Callback     string
	Topic        string
	Secret       string
	LeaseSeconds int
}

var (
	subscribersMu sync.RWMutex
	subscribers   = make(map[string]Subscriber, 0)
)

func main() {
	fmt.Println("Hello")

	http.HandleFunc("/subscribe", subscriptionHandler)
	http.HandleFunc("/publish", publishHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func generateChallenge(length int) string {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// handle subscription and verify intent
func subscriptionHandler(w http.ResponseWriter, r *http.Request) {
	// handle subscription
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "application/x-www-form-urlencoded") {
		w.Header().Set("Accept-Post", "application/x-www-form-urlencoded; charset=UTF-8")
		http.Error(w, "Content-Type not valid", http.StatusUnsupportedMediaType)
		return

	}

	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Could not parse form", http.StatusBadRequest)
		return
	}

	mode := r.PostForm.Get("hub.mode")
	topic := r.PostForm.Get("hub.topic")
	callback := r.PostForm.Get("hub.callback")
	secret := r.PostForm.Get("hub.secret")

	if topic == "" || callback == "" || (mode != "subscribe" && mode != "unsubscribe") {
		http.Error(w, "Invalid or missing parameters (topic, callback or mode)", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusAccepted)

	go func(mode, topic, callback, secret string) {

		// verify intent
		challenge := generateChallenge(16)
		leaseSeconds := 700000

		u, err := url.Parse(callback)
		if err != nil {
			log.Println("Invalid callback URL", err)
			return
		}

		query := u.Query()
		query.Set("hub.mode", mode)
		query.Set("hub.topic", topic)
		query.Set("hub.challenge", challenge)
		query.Set("hub.lease_seconds", fmt.Sprintf("%d", leaseSeconds))

		u.RawQuery = query.Encode()
		getURL := u.String()

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Get(getURL)
		if err != nil {
			log.Println("Callback GET failed", err)
			return
		}

		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Println("Failed to read response", err)
			return
		}

		bodyString := string(body)
		if resp.StatusCode > 299 || resp.StatusCode < 200 || bodyString != challenge {
			log.Println("Verification failed for", callback)
			return
		}

		key := topic + "||" + callback

		subscribersMu.Lock()
		defer subscribersMu.Unlock()

		switch mode {

		case "subscribe":

			sub := Subscriber{
				Callback:     callback,
				Topic:        topic,
				Secret:       secret,
				LeaseSeconds: leaseSeconds,
			}

			subscribers[key] = sub
			log.Println("Saved subscriber", sub)

		case "unsubscribe":
			delete(subscribers, key)
			log.Println("Subscriber deleted:", key)

		}
	}(mode, topic, callback, secret)

}

func signMessage(key string, content []byte) string {
	mac := hmac.New(sha1.New, []byte(key))
	mac.Write(content)
	return hex.EncodeToString(mac.Sum(nil))
}

func sendUpdate(callback string, secret string, content []byte) error {

	req, err := http.NewRequest("POST", callback, strings.NewReader(string(content)))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	signature := signMessage(secret, content)
	req.Header.Set("X-Hub-Signature", "sha1="+signature)
	linkHeader := fmt.Sprintf(`<%s>; rel="hub", <%s>; rel="self"`, "http://hub:8080", "http://web-sub-client:8080/a/topic")
	req.Header.Set("Link", linkHeader)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("content distribution failed with status code %d", resp.StatusCode)
	}

	fmt.Println("Data published ", callback)
	return nil
}

func notifySubscribers(topic string, content []byte) {

	subscribersMu.RLock()
	defer subscribersMu.RUnlock()

	var wg sync.WaitGroup

	for _, sub := range subscribers {
		if sub.Topic != topic {
			continue
		}
		wg.Add(1)
		go func(s Subscriber) {
			defer wg.Done()
			err := sendUpdate(s.Callback, s.Secret, content)
			if err != nil {
				fmt.Println("Notification failed")
			}
		}(sub)
	}
	wg.Wait()
}

func randomMessage() string {
	messages := []string{
		"Hello",
		"Goodbye",
		"Today is tuesday",
		"Good morning",
		"New content",
	}
	return messages[mathrand.Intn(len(messages))]
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	topic := "/a/topic"

	data := randomMessage()

	content, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "Failed to create data", http.StatusInternalServerError)
		return
	}

	notifySubscribers(topic, content)

	w.Write([]byte("Data published"))

}
