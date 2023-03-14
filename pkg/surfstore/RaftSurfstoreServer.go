package surfstore

import (
	context "context"
	"fmt"
	"log"
	math "math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore      *MetaStore
	matchIndex     []int64
	nextIndex      []int64
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if !s.MajorityCheck(ctx) {
		return nil, ERR_NO_MAJORITY
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
	//return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	//panic("todo")
	//return nil, nil
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if !s.MajorityCheck(ctx) {
		return nil, ERR_NO_MAJORITY
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	//panic("todo")
	//return nil, nil
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if !s.MajorityCheck(ctx) {
		return nil, ERR_NO_MAJORITY
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	fmt.Println("Check if we are setting s.log properly", s.log)
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	//get the index of the current log
	//idx := len(s.log) - 1
	// send entry to all followers in parallel
	//go s.sendToAllFollowersInParallel(ctx, int64(idx))
	output, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	// keep trying indefinitely (even after responding) ** rely on sendheartbeat
	if(err!=nil){
		log.Println("Issue with sendheartbeat", err)
		return nil, err
	}
	// commit the entry once majority of followers have it in their log
	//commit := <-commitChan

	// once committed, apply to the state machine
	if output.Flag {
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.isLeader {
			return nil, ERR_NOT_LEADER
		}
		fmt.Println("Entered this section")
		version, err := s.metaStore.UpdateFile(ctx, filemeta)
		fmt.Println("My version and err of update file call", version, err)
		if err != nil {
			log.Println("error in updating file", err)
			return nil, err
		}
		//Update commit index once it is successful only
		s.commitIndex++
		//Let us do second phase of commit here
		s.MajorityCheck(ctx)
		return version, err

	}

	return nil, nil
}

// func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context, idx int64) {
// 	// send entry to all my followers and count the replies

// 	responses := make(chan bool, len(s.peers)-1)
// 	// contact all the follower, send some AppendEntries call
// 	for idx, addr := range s.peers {
// 		if int64(idx) == s.id {
// 			continue
// 		}

// 		go s.sendToFollower(ctx, addr, responses)
// 	}

// 	totalResponses := 1
// 	totalAppends := 1

// 	// wait in loop for responses -> these responses need not be successful, it just waits until it gets all the responses
// 	for {
// 		result := <-responses
// 		totalResponses++
// 		if result {
// 			totalAppends++
// 		}
// 		if totalResponses == len(s.peers) {
// 			break
// 		}
// 	}

// 	if totalAppends > len(s.peers)/2 {
// 		// TODO put on correct channel
// 		*s.pendingCommits[idx] <- true
// 		// TODO update commit Index correctly
// 		s.commitIndex++
// 	}
// }

func (s *RaftSurfstore) sendToFollower(dummyAppendEntriesInput *AppendEntryInput, addr string, ctx context.Context, idx int) {
	if s.isCrashed {
		return
	}
	if !s.isLeader {
		return 
	}
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)
	for {
		output, err := client.AppendEntries(ctx, dummyAppendEntriesInput)
		if(err!=nil){
			log.Println("error is trying to append the entries to server", err)
			return
		}
		fmt.Println("Retry send to follower", output, err)
		if err == nil && output.Success == true {
			if len(s.log) > 0 {
				s.nextIndex[idx] = output.MatchedIndex + 1
				s.matchIndex[idx] = output.MatchedIndex
			}
			return
		} else {
			if dummyAppendEntriesInput.PrevLogIndex >= int64(1) {
				dummyAppendEntriesInput.PrevLogIndex--
			}
		}

	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	var output AppendEntryOutput
	// Case 1
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
		output.Success = false
		output.ServerId = s.id
		return &output, nil
	}
	// Case 2
	if input.PrevLogIndex >= 0 && input.PrevLogIndex < int64(len(s.log)) {
		if input.PrevLogTerm != s.log[input.PrevLogIndex].Term {
			output.Success = false
			output.Term = input.Term
			output.ServerId = s.id
			return &output, nil
		}
	}

	//case 3 : Delete everything after matched index
	if input.PrevLogIndex < int64(len(s.log)-1) {
		s.log = s.log[:input.PrevLogIndex+1]
	}
	//case 4
	fmt.Println("check input entries in append entries before appending ", s.id, input.Entries, s.log)
	if(len(input.Entries)>0){
		s.log = append(s.log, input.Entries...)
	}
	
	output.Success = true
	output.Term = input.Term
	output.MatchedIndex = int64(len(s.log) - 1)
	output.ServerId = s.id
	//fmt.Println("check input entries in append entries", input.Entries, s.log)
	//return &output, nil

	//case 5
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	//fmt.Println("Value of last applied and log", s.lastApplied, s.log, input.LeaderCommit)
	for s.lastApplied < input.LeaderCommit && len(s.log) > 0 {
		//fmt.Println("s.log and applied inside", s.log[0], s.lastApplied)
		fmt.Println("Committing entry to follower")
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	return &output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++
	matchIndex := make([]int64, len(s.peers))
	nextIndex := make([]int64, len(s.peers))
	for i := range matchIndex {
		matchIndex[i] = -1 // set all values in the array to the specified value
	}
	for i := range nextIndex {
		nextIndex[i] = int64(len(s.log)) // set all values in the array to the specified value
	}

	s.matchIndex = matchIndex
	s.nextIndex = nextIndex

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	
	// contact all the follower, send some AppendEntries call
	for {
		responses := 1
		for idx, addr := range s.peers {
			if s.isCrashed {
				return nil, ERR_SERVER_CRASHED
			}
			if !s.isLeader {
				return nil, ERR_NOT_LEADER
			}
			if int64(idx) == s.id {
				continue
			}

			// TODO check all errors
			conn, _ := grpc.Dial(addr, grpc.WithInsecure())
			client := NewRaftSurfstoreClient(conn)
			//fmt.Println("check valueof match index", s.matchIndex, idx)
			var entry int64 = s.matchIndex[idx]
			var prevlogTerm int64 
			var entries []*UpdateOperation
			if entry >= 0 {
				prevlogTerm = s.log[s.matchIndex[idx]].Term
			}
			entries = s.log[s.matchIndex[idx]+1:]
			fmt.Println("Doing heartbeat for server", idx)
			//fmt.Println("Check entries in heartbeat", entries, s.log)
			dummyAppendEntriesInput := AppendEntryInput{
				Term: s.term,
				// TODO put the right values
				PrevLogTerm:  prevlogTerm,
				PrevLogIndex: s.matchIndex[idx],
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			//fmt.Println("Check entry", dummyAppendEntriesInput)
			output, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
			if err == nil && output.Success {
				//update count on only successful return value
				responses++
				if len(s.log) > 0 {
					s.nextIndex[idx] = output.MatchedIndex + 1
					s.matchIndex[idx] = output.MatchedIndex
				}
			} else {
				//Append entries did not work coz of some mismatch, update the nextIndex of that server
				s.sendToFollower(&dummyAppendEntriesInput, addr, ctx, idx)

			}
		}
		if responses > len(s.peers)/2 {
			//Checking for majority in hearbeat
			//*s.pendingCommits[len(s.log)-1] <- true
			fmt.Println("Successful majority")
			return &Success{Flag: true}, nil
		}

		//Wait until next iteration of heartbeats
		time.Sleep(10 * time.Millisecond)
	}

	return nil, nil
}

func (s *RaftSurfstore) MajorityCheck(ctx context.Context) bool {
	output, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
	return output.Flag
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
