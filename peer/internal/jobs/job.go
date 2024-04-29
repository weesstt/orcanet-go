package jobs

import (
	"encoding/json"
	"errors"
	"os"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"bufio"
	"log"
	"fmt"
	"io"
	"context"
	"encoding/binary"
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

			err = host.Connect(context.Background(), *peer)
			if err != nil {
				log.Println(err)
				Manager.Mutex.Unlock()
				return err
			}

			s, err := host.NewStream(context.Background(), peer.ID, protocol.ID("orcanet-fileshare/1.0/" + job.FileHash))
			if err != nil {
				log.Println(err)
				Manager.Mutex.Unlock()
				return err
			}
			defer s.Close()

			Manager.Mutex.Unlock()
			job, err := FindJob(jobId)
			Manager.Mutex.Lock()
			if err != nil {
				fmt.Println("Error:", err)
				Manager.Mutex.Unlock()
				return err
			}

			fileChunkReq := FileChunkRequest{
				FileHash: job.FileHash,
				ChunkIndex: 0,
				JobId: job.JobId,
			}

			nextChunkReqBytes, err := json.Marshal(fileChunkReq)
			if err != nil {
				fmt.Println("Error:", err)
				Manager.Mutex.Unlock()
				return err
			}

			lengthBytes := make([]byte, 4)
    		binary.LittleEndian.PutUint32(lengthBytes, uint32(len(nextChunkReqBytes)))
			_, err = s.Write(lengthBytes)
			if err != nil {
				fmt.Println(err)
				Manager.Mutex.Unlock()
				return nil
			}
			
			_, err = s.Write(nextChunkReqBytes)
			if err != nil {
				fmt.Println(err)
				Manager.Mutex.Unlock()
				return nil
			}

			for {
				buf := bufio.NewReader(s)
				lengthBytes := make([]byte, 0)
				for i := 0; i < 4; i++ {
					b, err := buf.ReadByte()
					if err != nil {
						fmt.Println(err)
						Manager.Mutex.Unlock()
						return err
					}	
					lengthBytes = append(lengthBytes, b)
				}
		
				length := binary.LittleEndian.Uint32(lengthBytes)
				payload := make([]byte, length)
				_, err := io.ReadFull(buf, payload)
				if err != nil {
					fmt.Println(err)
					Manager.Mutex.Unlock()
					return err
				}
				
				fileChunk := FileChunk{}
				err = json.Unmarshal(payload, &fileChunk)
				if err != nil {
					fmt.Println("Error unmarshaling JSON:", err)
					Manager.Mutex.Unlock()
					return err
				}
		
				Manager.Mutex.Unlock()
				_, err = FindJob(fileChunk.JobId)
				Manager.Mutex.Lock()
				if err != nil {
					log.Fatal(err)
					Manager.Mutex.Unlock()
					return err
				}
				hash := fileChunk.FileHash
			
				file, err := os.OpenFile("./files/requested/" + hash, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
				defer file.Close()
			
				_, err = file.Write(fileChunk.Data)
				if err != nil {
					log.Fatal(err)
					Manager.Mutex.Unlock()
					return err
				}
		
				fmt.Printf("Chunk %d for %s received and written\n", hash, fileChunk.ChunkIndex)
		
				if fileChunk.ChunkIndex == fileChunk.MaxChunk - 1 {
					fmt.Println("All chunks received and written")
					Manager.Mutex.Unlock()
					return nil
				}
			
				fileChunkReq := FileChunkRequest{
					FileHash: hash,
					ChunkIndex: fileChunk.ChunkIndex + 1,
					JobId: fileChunk.JobId,
				}
			
				nextChunkReqBytes, err := json.Marshal(fileChunkReq)
				if err != nil {
					fmt.Println("Error:", err)
					Manager.Mutex.Unlock()
					return err
				}
		
				reqLengthHeader := make([]byte, 4)
				binary.LittleEndian.PutUint32(reqLengthHeader, uint32(len(nextChunkReqBytes)))
				_, err = s.Write(reqLengthHeader)
				if err != nil {
					fmt.Println(err)
					Manager.Mutex.Unlock()
					return err
				}

				_, err = s.Write(nextChunkReqBytes)
				if err != nil {
					fmt.Println(err)
					Manager.Mutex.Unlock()
					return err
				}
			}

			Manager.Mutex.Unlock()
			return nil
		}
	}
	Manager.Mutex.Unlock()
	return errors.New("Unable to find jobId: " + jobId)
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
