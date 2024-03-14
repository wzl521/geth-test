package track

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	communicate "github.com/ethereum/go-ethereum/track/proto"

	"github.com/google/uuid"
)

type clientSubContext struct {
	closed int
	msgCh  chan message
	ctx    context.Context
}

type Server struct {
	communicate.UnimplementedTrackPuberServer

	subs map[string]*clientSubContext
	m    sync.Mutex
}

func (s *Server) register(ctx context.Context) chan message {
	s.m.Lock()
	defer s.m.Unlock()
	ch := make(chan message, 1024)
	requestID := uuid.New().String()
	s.subs[requestID] = &clientSubContext{
		msgCh: ch,
		ctx:   ctx,
	}
	log.Info("[TrackServer] register", "id", requestID)
	go func() {
		<-ctx.Done()
		s.m.Lock()
		log.Info("[TrackServer] unregister", "id", requestID)
		//close(ch)
		delete(s.subs, requestID)
		s.m.Unlock()
	}()
	return ch
}

func (s *Server) GetTrack(request *communicate.GetTrackRequest, stream communicate.TrackPuber_GetTrackServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgChan := s.register(ctx)

	count := 0
	ticker := time.NewTicker(time.Minute)

loop:
	for {
		select {
		case <-stream.Context().Done():
			cancel()
			break loop
		case <-ctx.Done():
			break loop
		case message := <-msgChan:
			dataBytes, err := json.Marshal(message.data)
			if err != nil {
				log.Error("[TrackServer] Error marshalling message", "error", err)
				continue
			}
			if err := stream.Send(&communicate.Track{
				Topic:     uint32(message.topic),
				Timestamp: message.timestamp,
				Data:      dataBytes,
			}); err != nil {
				log.Error("[TrackServer] Error send stream message", "error", err)
				cancel()
				break loop
			}
			count++
		case <-ticker.C:
			log.Info("[TrackServer] stream push per minute", "count", count)
			count = 0
		}
	}
	log.Info("[TrackServer] GetTrack done")
	return nil
}

func (s *Server) GetTrackList(request *communicate.GetTrackRequest, stream communicate.TrackPuber_GetTrackListServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgChan := s.register(ctx)

	const listBatch = 500

	count := 0
	ticker := time.NewTicker(time.Minute)
	trackList := make([]*communicate.Track, 0, listBatch)

loop:
	for {
		select {
		case <-stream.Context().Done():
			cancel()
			break loop
		case <-ctx.Done():
			break loop
		case message := <-msgChan:
			dataBytes, err := json.Marshal(message.data)
			if err != nil {
				log.Error("[TrackServer] Error marshalling message", "error", err)
				continue
			}
			trackList = append(trackList, &communicate.Track{
				Topic:     uint32(message.topic),
				Timestamp: message.timestamp,
				Data:      dataBytes,
			})
			count++
		case <-ticker.C:
			if err := stream.Send(&communicate.TrackList{List: trackList}); err != nil {
				log.Error("[TrackServer] Error send stream message", "error", err)
				cancel()
				break loop
			}
			trackList = trackList[:0]
			log.Info("[TrackServer] stream push per minute", "count", count)
			count = 0
		default:
			if len(trackList) >= listBatch {
				if err := stream.Send(&communicate.TrackList{List: trackList}); err != nil {
					log.Error("[TrackServer] Error send stream message", "error", err)
					cancel()
					break loop
				}
				trackList = trackList[:0]
			}
		}
	}
	log.Info("[TrackServer] GetTrack done")
	return nil
}
