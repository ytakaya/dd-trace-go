package pipelines

import (
	"encoding/binary"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/globalconfig"
	"hash/fnv"
	"math/rand"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

type Pipeline struct {
	hash     uint64
	callTime time.Time
	service  string
	edgeName string
}

// func dataPipelineFromBaggage(data []byte, service string) (Pipeline, error) {
// 	pipeline := &Pipeline{service: service}
// 	if len(data) < 8 {
// 		return nil, errors.New("pipeline hash smaller than 8 bytes")
// 	}
// 	pipeline.hash = binary.LittleEndian.Uint64(data)
// 	data = data[8:]
// 	t, err := encoding.DecodeVarint64(&data)
// 	if err != nil {
// 		return nil, err
// 	}
// 	pipeline.callTime = time.Unix(0, t*int64(time.Millisecond))
// 	return pipeline, nil
// }
//
// func (p *Pipeline) ToBaggage() ([]byte, error) {
// 	data := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(data, p.hash)
// 	encoding.EncodeVarint64(&data, p.callTime.UnixNano()/int64(time.Millisecond))
// 	return data, nil
// }

// Merge merges multiple pipelines
func Merge(pipelines []Pipeline) Pipeline {
	// for now, randomly select a pipeline.
	n := rand.Intn(len(pipelines))
	return pipelines[n]
}

func nodeHash(service, edgeName string) uint64 {
	b := make([]byte, 0, len(service) + len(edgeName))
	b = append(b, service...)
	b = append(b, edgeName...)
	h := fnv.New64()
	h.Write(b)
	return h.Sum64()
}

func pipelineHash(nodeHash, parentHash uint64) uint64 {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, nodeHash)
	binary.LittleEndian.PutUint64(b[8:], parentHash)
	h := fnv.New64()
	h.Write(b)
	return h.Sum64()
}

func New() Pipeline {
	now := time.Now()
	p := Pipeline{
		hash:     0,
		callTime: now,
		service:  globalconfig.ServiceName(),
	}
	return p.SetCheckpoint("", now)
}

func (p Pipeline) SetCheckpoint(edgeName string, t time.Time) Pipeline {
	child := Pipeline{
		hash:     pipelineHash(nodeHash(p.service, edgeName), p.hash),
		callTime: p.callTime,
		service:  p.service,
		edgeName: edgeName,
	}
	if processor := getGlobalProcessor(); processor != nil {
		select {
		case processor.in <- statsPoint{
			service:               p.service,
			receivingPipelineName: edgeName,
			parentHash:            p.hash,
			hash:                  child.hash,
			timestamp:             t.UnixNano(),
			latency:               t.Sub(p.callTime).Nanoseconds(),
		}:
		default:
			log.Error("Processor input channel full, disregarding stats point.")
		}
	}
	return child
}