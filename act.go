package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

func msdj() {
	n := 30
	url := "https://osiaru.netlify.app/"
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			resp, err := http.Get(url)
			if err != nil {
				fmt.Printf("Request %d failed: %v\n", id, err)
				return
			}
			defer resp.Body.Close()
			_, _ = io.ReadAll(resp.Body)
			fmt.Printf("Request %d done\n", id)
		}(i)
	}
	wg.Wait()
}
