package common

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/rest"
)

// RunAPILoad uses a fixed worker count and a reused transport, rather than one
// kubectl process per request. The parent removes the helper after its measured
// fault window; the helper does not restart itself when a duration expires.
func RunAPILoad(ctx context.Context, config *rest.Config, namespace string, rps int) error {
	if namespace == "" || rps < 1 || rps > 10000 {
		return fmt.Errorf("API load requires a namespace and 1-10000 requests/second per helper")
	}
	transport, err := rest.TransportFor(config)
	if err != nil {
		return err
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	url := config.Host + "/api/v1/namespaces/" + namespace + "/pods?limit=1"
	return runHTTPRequests(ctx, client, url, rps, os.Stdout)
}

func runHTTPRequests(ctx context.Context, client *http.Client, url string, rps int, output io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan struct{})
	var succeeded, failed uint64
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range jobs {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				if err != nil {
					atomic.AddUint64(&failed, 1)
					continue
				}
				response, err := client.Do(request)
				if err != nil {
					atomic.AddUint64(&failed, 1)
					continue
				}
				_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 2<<20))
				response.Body.Close()
				if readErr == nil && response.StatusCode == http.StatusOK {
					atomic.AddUint64(&succeeded, 1)
				} else {
					atomic.AddUint64(&failed, 1)
				}
			}
		}()
	}
	defer func() { cancel(); close(jobs); workers.Wait() }()
	pacing := time.NewTicker(time.Second / time.Duration(rps))
	defer pacing.Stop()
	sampling := time.NewTicker(time.Second)
	defer sampling.Stop()
	encoder := json.NewEncoder(output)
	previous, previousTime := uint64(0), time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pacing.C:
			select {
			case jobs <- struct{}{}:
			default:
				atomic.AddUint64(&failed, 1)
			}
		case now := <-sampling.C:
			total := atomic.LoadUint64(&succeeded)
			sample := struct {
				SuccessfulRate float64 `json:"successfulRate"`
				Successful     uint64  `json:"successful"`
				Failed         uint64  `json:"failed"`
			}{float64(total-previous) / now.Sub(previousTime).Seconds(), total, atomic.LoadUint64(&failed)}
			if err := encoder.Encode(sample); err != nil {
				return err
			}
			previous, previousTime = total, now
		}
	}
}
