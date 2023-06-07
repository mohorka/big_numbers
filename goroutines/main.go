package main

import (
	"github.com/rs/zerolog/log"
	"flag"
	"os"
	"sync"
	"runtime"
	"encoding/binary"
)


func getPrimeFactorsAmount(number uint32) uint64 {
	amount := uint64(0)
	for number%2 == 0 {
		number = number / 2
		amount++
	}
	for i := uint32(3); i*i <= number; i = i + 2 {
		for number%i == 0 {
			number = number / i
			amount++
		}
	}
	if number > 1 {
		amount++
	}
	return amount
}


func factorizeWorker(sender chan uint32, reciever chan uint64, wg *sync.WaitGroup){
	defer wg.Done()
	for number := range sender {
		reciever <- getPrimeFactorsAmount(number)
	}
}


func sumWorker(reciever chan uint64, summaryCh chan uint64, wg *sync.WaitGroup){
	defer wg.Done()
	var subSummary uint64
	for amountPrimeNumbers := range reciever {
		subSummary += amountPrimeNumbers
	}
	summaryCh <- subSummary
}




func main() {
	var inputFile string 
	flag.StringVar(&inputFile, "input_file", "", "input binary file with big ints")
	flag.Parse()

	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer file.Close()

	var bigInts [5000]uint32
	err = binary.Read(file, binary.BigEndian, &bigInts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	workersAmount := runtime.NumCPU()
	log.Info().Msgf("Goes with %v cpus on board.", workersAmount)

	const bufferSize = 1024
	sender := make(chan uint32, bufferSize)
	reciever := make(chan uint64, bufferSize)
	summaryCh := make(chan uint64, workersAmount)
	
	var factorizeWg sync.WaitGroup
	var summaryWg sync.WaitGroup

	for i := 0; i < workersAmount; i++ {
		factorizeWg.Add(1)
		go factorizeWorker(sender ,reciever, &factorizeWg)
	}

	for i := 0; i < workersAmount; i++ {
		summaryWg.Add(1)
		go sumWorker(reciever, summaryCh, &summaryWg)
	}
	
	for _, bigInt := range bigInts {
		sender <- bigInt
	}
	close(sender)

	go func(){
		factorizeWg.Wait()
		close(reciever)
	}()

	go func(){
		summaryWg.Wait()
		close(summaryCh)
	}()

	var summary uint64
	for partialSum := range summaryCh {
		summary += partialSum
	}
	log.Info().Msgf("Done! Amount of prime numbers for ints in %s: %v", inputFile, summary)
} 