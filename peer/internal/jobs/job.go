package jobs

import (
	"encoding/json"
	"errors"
	"os"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"bufio"
	"log"
	"fmt"
	"io"
	"context"
)

func AddJob(job Job) {
	Manager.Mutex.Lock()
	Manager.Jobs = append(Manager.Jobs, job)
	Manager.Changed = true
	Manager.Mutex.Unlock()
}

func LoadHistory() ([]Job, error) {
	fileData, err := os.ReadFile("./internal/jobs/jobs.json")
	if err != nil {
		return nil, err
	}
	var jobs []Job
	err = json.Unmarshal(fileData, &jobs)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}
func SaveHistory(jobs []Job) error {
	Manager.Changed = false
	jsonData, err := json.Marshal(jobs)
	if err != nil {
		return err
	}
	err = os.WriteFile("./internal/jobs/jobs.json", jsonData, 0644)
	if err != nil {
		return err
	}
	return nil
}

func RemoveFromHistory(jobId string) error {
	Manager.Mutex.Lock()
	for idx, job := range Manager.Jobs {
		if job.JobId == jobId {
			Manager.Jobs = append(Manager.Jobs[:idx], Manager.Jobs[idx+1:]...)
			Manager.Changed = true
			Manager.Mutex.Unlock()
			return nil
		}
	}
	Manager.Mutex.Unlock()
	return errors.New("unable to find job that matches jobID")
}

func ClearHistory() {
	Manager.Mutex.Lock()
	newJobs := make([]Job, 0)
	for _, job := range Manager.Jobs {
		if job.Status != "completed" {
			Manager.Changed = true
			newJobs = append(newJobs, job)
		}
	}
	Manager.Jobs = newJobs
	Manager.Mutex.Unlock()
}

func TerminateJob(jobId string) error {
	Manager.Mutex.Lock()
	for idx, job := range Manager.Jobs {
		if job.JobId == jobId {
			Manager.Jobs[idx].Status = "terminated"
			Manager.Changed = true
			Manager.Mutex.Unlock()
			return nil
		}
	}
	Manager.Mutex.Unlock()
	return errors.New("Unable to find jobId: " + jobId)
}

func PauseJob(jobId string) error {
	Manager.Mutex.Lock()
	for idx, job := range Manager.Jobs {
		if job.JobId == jobId {
			Manager.Jobs[idx].Status = "paused"
			Manager.Changed = true
			Manager.Mutex.Unlock()
			return nil
		}
	}
	Manager.Mutex.Unlock()
	return errors.New("Unable to find jobId: " + jobId)
}

//TODO in any error situtation stop/delete job?
func StartJob(jobId string) error {
	Manager.Mutex.Lock()
	for idx, job := range Manager.Jobs {
		if job.JobId == jobId {
			Manager.Jobs[idx].Status = "active"
			Manager.Changed = true
			host := Manager.Host
			
			peerMA, err := multiaddr.NewMultiaddr(job.PeerId)
			if err != nil {
				log.Println(err)
				Manager.Mutex.Unlock()
				return err
			}

			peer, err := peer.AddrInfoFromP2pAddr(peerMA)
			if err != nil {
				log.Println(err)
				Manager.Mutex.Unlock()
				return err
			}

			host.Peerstore().AddAddrs(peer.ID, peer.Addrs, peerstore.AddressTTL)

			host.SetStreamHandler(protocol.ID("orcanet-fileshare/1.0/" + job.FileHash), handleStream)
			s, err := host.NewStream(context.Background(), peer.ID, protocol.ID("orcanet-fileshare/1.0/" + job.FileHash))
			if err != nil {
				log.Println(err)
				Manager.Mutex.Unlock()
				return err
			}

			rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
			Manager.Mutex.Unlock()
			job, err := FindJob(jobId)
			Manager.Mutex.Lock()
			if err != nil {
				fmt.Println("Error:", err)
				return err
			}

			fmt.Println("found job and read writer")

			fileChunkReq := FileChunkRequest{
				FileHash: job.FileHash,
				ChunkIndex: 0,
				JobId: job.JobId,
			}

			nextChunkReqBytes, err := json.Marshal(fileChunkReq)
			if err != nil {
				fmt.Println("Error:", err)
				return err
			}

			nextChunkReqBytes = append(nextChunkReqBytes, 0xff)

			fmt.Println("Writing marshal bytes")
			rw.Write(nextChunkReqBytes)
			rw.Flush()

			Manager.Mutex.Unlock()
			return nil
		}
	}
	Manager.Mutex.Unlock()
	return errors.New("Unable to find jobId: " + jobId)
}

func handleStream(s network.Stream) {
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	go readData(rw) //TODO set up channel to close stream
}

//TODO send transaction 
func readData(rw *bufio.ReadWriter){
	for {
		data, err := rw.ReadSlice(0xff)
		fmt.Printf("Read %d bytes\n", len(data))
		if err != nil {
			if err != io.EOF {
				fmt.Printf("err %s\n", err)
				break
			}
		}
	
		fileChunk := FileChunk{}
		err = json.Unmarshal(data[:len(data) - 1], &fileChunk)
		if err != nil {
			fmt.Printf("Error unmarshaling json data %s\n", err)
			return 
		}
	
		_, err = FindJob(fileChunk.JobId)
		if err != nil {
			log.Fatal(err)
		}
		hash := fileChunk.FileHash
	
		file, err := os.OpenFile("./files/requested/" + hash, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		defer file.Close()
	
		_, err = file.Write(fileChunk.Data)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Chunk %d for %s received and written\n", hash, fileChunk.ChunkIndex)

		if fileChunk.ChunkIndex == fileChunk.MaxChunk {
			break
		}
	
		fileChunkReq := FileChunkRequest{
			FileHash: hash,
			ChunkIndex: fileChunk.ChunkIndex + 1,
			JobId: fileChunk.JobId,
		}
	
		nextChunkReqBytes, err := json.Marshal(fileChunkReq)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		nextChunkReqBytes = append(nextChunkReqBytes, 0xff)
	
		rw.Write(nextChunkReqBytes)
		rw.Flush()
	}	 
}

func FindJob(jobId string) (Job, error) {
	Manager.Mutex.Lock()
	for _, job := range Manager.Jobs {
		if job.JobId == jobId {
			Manager.Mutex.Unlock()
			return job, nil
		}
	}
	Manager.Mutex.Unlock()
	return Job{}, errors.New("unable to find job with specified jobId")
}

func FindJobByHash(file_hash string) (Job, error) {
	Manager.Mutex.Lock()
	for _, job := range Manager.Jobs {
		if job.FileHash == file_hash {
			Manager.Mutex.Unlock()
			return job, nil
		}
	}
	Manager.Mutex.Unlock()
	return Job{}, errors.New("unable to find job with specified file hash")
}
