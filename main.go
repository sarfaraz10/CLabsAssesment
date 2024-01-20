package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type RequestData struct {
	Ev     string      `json:"ev"`
	Et     string      `json:"et"`
	ID     interface{} `json:"id"`
	UID    string      `json:"uid"`
	MID    string      `json:"mid"`
	T      string      `json:"t"`
	P      string      `json:"p"`
	L      string      `json:"l"`
	SC     string      `json:"sc"`
	ATRK1  string      `json:"atrk1"`
	ATRV1  string      `json:"atrv1"`
	ATRT1  string      `json:"atrt1"`
	ATRK2  string      `json:"atrk2"`
	ATRV2  string      `json:"atrv2"`
	ATRT2  string      `json:"atrt2"`
	UATRK1 string      `json:"uatrk1"`
	UATRV1 string      `json:"uatrv1"`
	UATRT1 string      `json:"uatrt1"`
	UATRK2 string      `json:"uatrk2"`
	UATRV2 string      `json:"uatrv2"`
	UATRT2 string      `json:"uatrt2"`
	UATRK3 string      `json:"uatrk3"`
	UATRV3 string      `json:"uatrv3"`
	UATRT3 string      `json:"uatrt3"`
}

type ConvertedData struct {
	Event           string               `json:"event"`
	EventType       string               `json:"event_type"`
	AppID           string               `json:"app_id"`
	UserID          string               `json:"user_id"`
	MessageID       string               `json:"message_id"`
	PageTitle       string               `json:"page_title"`
	PageURL         string               `json:"page_url"`
	BrowserLanguage string               `json:"browser_language"`
	ScreenSize      string               `json:"screen_size"`
	Attributes      map[string]Attribute `json:"attributes"`
	Traits          map[string]Trait     `json:"traits"`
}

type Attribute struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

type Trait struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

func worker(requests <-chan RequestData) {
	for requestData := range requests {
		convertedData := convertData(requestData)
		fmt.Printf("Converted Data: %+v\n", convertedData)
	}
}

func convertData(requestData RequestData) ConvertedData {
	attributes := map[string]Attribute{
		"form_varient": {Value: requestData.ATRV1, Type: requestData.ATRT1},
		"ref":          {Value: requestData.ATRV2, Type: requestData.ATRT2},
	}

	traits := map[string]Trait{
		"name":  {Value: requestData.UATRV1, Type: requestData.UATRT1},
		"email": {Value: requestData.UATRV2, Type: requestData.UATRT2},
		"age":   {Value: requestData.UATRV3, Type: requestData.UATRT3},
	}

	convertedData := ConvertedData{
		Event:           requestData.Ev,
		EventType:       requestData.Et,
		AppID:           requestData.ID.(string),
		UserID:          requestData.UID,
		MessageID:       requestData.MID,
		PageTitle:       requestData.T,
		PageURL:         requestData.P,
		BrowserLanguage: requestData.L,
		ScreenSize:      requestData.SC,
		Attributes:      attributes,
		Traits:          traits,
	}

	return convertedData
}

func handleRequest(w http.ResponseWriter, r *http.Request, requests chan<- RequestData) {
	if r.ContentLength <= 0 {
		http.Error(w, "Empty request body", http.StatusBadRequest)
		return
	}

	var requestData RequestData

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&requestData)
	if err != nil {
		if err.Error() == "EOF" {
			http.Error(w, "Empty or incomplete JSON payload", http.StatusBadRequest)
			return
		}
		http.Error(w, "Error decoding JSON", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	fmt.Printf("Received Request: %+v\n", requestData)

	requests <- requestData

	w.WriteHeader(http.StatusOK)
}

func main() {
	requests := make(chan RequestData)
	go worker(requests)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleRequest(w, r, requests)
	})

	fmt.Println("Server listening on :8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}
