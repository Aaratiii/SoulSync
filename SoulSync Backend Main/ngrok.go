package main

// https://docs.google.com/presentation/d/13SRX_P0BSgWt1DQS1hWX7tE0YbZiM_-OFCXdgDAvkJM/edit#slide=id.g2c012541ba4_0_35

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const LOCALHOST = "localhost"

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

type proxy struct {
	webPort   string
	apiPort   string
	apiKey    string
	publicUrl string
	authToken string
	envFile   string
	address   string
}

type NgrokTunnelResponse struct {
	Tunnel string `json:"tunnel"`
}

func (p *proxy) ServeHTTP(wr http.ResponseWriter, req *http.Request) {
	// client := &http.Client{}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	if req.Method == "OPTIONS" {
		wr.WriteHeader(http.StatusOK)
		return
	}

	req.URL.Scheme = "http"
	if strings.HasPrefix(req.URL.Path, "/api/v1") || strings.Contains(req.URL.Path, "openapi") || strings.HasSuffix(req.URL.Path, "docs") {
		req.URL.Host = LOCALHOST + ":" + p.apiPort
	} else {
		req.URL.Host = LOCALHOST + ":" + p.webPort
	}

	newreq, err := http.NewRequest(req.Method, req.URL.String(), req.Body)
	copyHeader(newreq.Header, req.Header)
	if err != nil {
		http.Error(wr, "Server Error", http.StatusInternalServerError)
		log.Println("client: could not create request:", err, req.Method, req.URL)
		return
	}

	newreq.Header.Set("Accept", "*/*")

	if strings.HasPrefix(req.URL.Path, "/api/v2") {
		newreq.Header.Set("Authorization", req.Header.Get("Authorization"))
		newreq.Header.Set("Referer", req.Header.Get("Referer"))
	}

	resp, err := client.Do(newreq)
	if err != nil {
		http.Error(wr, "Server Error", http.StatusInternalServerError)
		log.Println("ServeHTTP:", err, req.Method, req.URL)
		return
	}
	defer resp.Body.Close()

	resp.Header.Set("Access-Control-Allow-Origin", "*")

	copyHeader(wr.Header(), resp.Header)
	wr.WriteHeader(resp.StatusCode)
	io.Copy(wr, resp.Body)

	log.Println(resp.Status, req.Method, req.URL)
}

type Tunnel struct {
	Id        string `json:"id"`
	PublicURL string `json:"public_url"`
	Proto     string `json:"proto"`
}

type TunnelList struct {
	Tunnels []Tunnel `json:"tunnels"`
}

func (p *proxy) getTunnel() (string, error) {
	client := &http.Client{}

	req, err := http.NewRequest("GET", "https://api.ngrok.com/tunnels", nil)
	if err != nil {
		return "", err
	}

	req.Header.Add("Authorization", "Bearer "+p.apiKey)
	req.Header.Add("ngrok-version", "2")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}

		var tunnelList TunnelList
		if err := json.Unmarshal(body, &tunnelList); err != nil { // Parse []byte to the go struct pointer
			return "", err
		}

		for _, tunnel := range tunnelList.Tunnels {
			if tunnel.Proto == "https" {
				p.publicUrl = tunnel.PublicURL
				return p.publicUrl, nil
			}
		}
	} else {
		log.Println("Error getting tunnel", resp.StatusCode)
	}

	return "", nil
}

func (p *proxy) startNgrok(port string) (*os.Process, error) {
	path, err := exec.LookPath("ngrok")
	if err != nil {
		return nil, err
	}

	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}

	args := []string{"ngrok", "http", port, "--authtoken=" + p.authToken, "--log=./ngrok.log", "--log-format=logfmt"}

	attr := &os.ProcAttr{
		Dir:   "",
		Env:   nil,
		Files: []*os.File{os.Stdin, devnull, os.Stderr}, // process detachted from stdout
		Sys:   &syscall.SysProcAttr{},
	}

	proc, err := os.StartProcess(path, args, attr)
	if err != nil {
		return nil, err
	}

	go func() {
		_, err = proc.Wait()
		if (err != nil) && (err.Error() != "signal: killed") {
			log.Printf("Command finished with error: %v", err)
		}
		devnull.Close()
	}()

	return proc, nil
}

func getEnv(file string, key string) (string, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, key) {
			return strings.Split(line, "=")[1], nil
		}
	}

	return "", errors.New("Key " + key + " not found in " + file + " file")
}

func main() {
	ngrokApiKey := os.Getenv("NGROK_API_KEY")
	if ngrokApiKey == "" {
		value, err := getEnv("./.env", "NGROK_API_KEY")
		if err != nil {
			log.Fatal("NGROK_API_KEY not set or invalid")
		}
		ngrokApiKey = strings.Trim(value, "\"")
	}

	ngrokAuthToken := os.Getenv("NGROK_AUTH_TOKEN")
	if ngrokAuthToken == "" {
		value, err := getEnv("./.env", "NGROK_AUTH_TOKEN")
		if err != nil {
			log.Fatal("NGROK_API_KEY not set or invalid")
		}
		ngrokAuthToken = strings.Trim(value, "\"")
	}

	webPortDefault := os.Getenv("WEB_PORT")
	if webPortDefault == "" {
		value, err := getEnv("./.env", "WEB_PORT")
		if err != nil {
			log.Println("WEB_PORT, using default 3000")
			webPortDefault = "3000"
		} else {
			webPortDefault = strings.Trim(value, "\"")
			log.Println("WEB_PORT, using", webPortDefault)
		}
	}

	apiPortDefault := os.Getenv("API_PORT")
	if apiPortDefault == "" {
		value, err := getEnv("./.env", "API_PORT")
		if err != nil {
			log.Println("API_PORT, using default 8000")
			apiPortDefault = "8000"
		} else {
			apiPortDefault = strings.Trim(value, "\"")
			log.Println("API_PORT, using", apiPortDefault)
		}
	}

	listenPort := flag.String("listen", "2100", "Port to listen on.")
	webPort := flag.String("web", webPortDefault, "Web port to forward to")
	apiPort := flag.String("api", apiPortDefault, "API port to forward to")
	apiKey := flag.String("key", ngrokApiKey, "NGROK API key")
	authToken := flag.String("token", ngrokAuthToken, "NGROK Auth Token")
	// var url = flag.String("url`", publicUrl, "NGROK Auth Token")xs
	envFile := flag.String("env", "./.env", "Environment file")

	flag.Parse()

	handler := &proxy{
		webPort:   *webPort,
		apiPort:   *apiPort,
		apiKey:    *apiKey,
		authToken: *authToken,
		envFile:   *envFile,
	}

	proc, err := handler.startNgrok(*listenPort)
	if (err != nil) || (proc.Pid == -1) {
		log.Fatal("Could not start ngrok", err)
	}
	log.Println("ngrok started", proc.Pid)

	for i := 0; i < 5; i++ {
		tunnel, err := handler.getTunnel()
		if err != nil {
			log.Fatal("Could not get tunnel", err)
		}
		if tunnel == "" {
			time.Sleep(1 * time.Second)
		}
	}

	log.Println("Public URL: " + handler.publicUrl)
	log.Println("forwarding web requests to https://" + LOCALHOST + ":" + *webPort)
	log.Println("forwarding api requests to http://" + LOCALHOST + ":" + *apiPort)

	go func() {
		log.Println("Start listening on http://" + LOCALHOST + ":" + *listenPort)
		err := http.ListenAndServe("0.0.0.0:"+*listenPort, handler)
		if err != nil {
			log.Println("ListenAndServe:", err)
		}
	}()

	// Wait for Ctrl-C
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	log.Println("Shutting down...", proc.Pid)
	proc.Signal(syscall.SIGKILL)
}
