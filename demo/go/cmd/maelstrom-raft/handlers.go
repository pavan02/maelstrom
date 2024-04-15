package main

import (
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/pavan/maelstrom/demo/go/cmd/maelstrom-raft/structs"
	"log"
)

// When a node requests our vote...
func (raft *RaftNode) requestVote(msg maelstrom.Message) error {
	raft.requestVoteHandlerMu.Lock()
	defer raft.requestVoteHandlerMu.Unlock()
	var requestVoteMsgBody structs.RequestVoteMsgBody
	if err := json.Unmarshal(msg.Body, &requestVoteMsgBody); err != nil {
		return err
	}

	raft.maybeStepDown(requestVoteMsgBody.Term)
	grant := false

	if requestVoteMsgBody.Term < raft.currentTerm {
		log.Printf("candidate Term %d lower than %d not granting vote \n", requestVoteMsgBody.Term, raft.currentTerm)
	} else if raft.votedFor != "" {
		log.Printf("already voted for %s not granting vote \n", raft.votedFor)
	} else if requestVoteMsgBody.LastLogTerm < raft.log.lastTerm() {
		log.Printf("have log Entries From Term %d which is newer than remote Term %d not granting vote\n", raft.log.lastTerm(), requestVoteMsgBody.LastLogTerm)
	} else if requestVoteMsgBody.LastLogTerm == raft.log.lastTerm() && requestVoteMsgBody.LastLogIndex < raft.log.size() {
		log.Printf("our logs are both at Term %d but our log is %d and theirs is only %d \n", raft.log.lastTerm(), raft.log.size(), requestVoteMsgBody.LastLogIndex)
	} else {
		log.Printf("before raft.votedFor %s\n", raft.votedFor)
		log.Printf("CandidateId: %s\n", requestVoteMsgBody.CandidateId)
		log.Printf("Granting vote To %s\n", msg.Src)
		grant = true
		raft.votedFor = requestVoteMsgBody.CandidateId
		raft.resetElectionDeadline()
		log.Printf("after raft.votedFor %s\n", raft.votedFor)
	}

	err := raft.node.Reply(msg, map[string]interface{}{
		"type":         structs.MsgTypeRequestVoteResult,
		"term":         raft.currentTerm,
		"vote_granted": grant,
	})
	if err != nil {
		return err
	}

	return nil
}

func (raft *RaftNode) appendEntries(msg maelstrom.Message) error {
	raft.appendEntriesHandlerMu.Lock()
	defer raft.appendEntriesHandlerMu.Unlock()

	var appendEntriesMsgBody structs.AppendEntriesMsgBody
	err := json.Unmarshal(msg.Body, &appendEntriesMsgBody)
	if err != nil {
		panic(err)
	}

	raft.maybeStepDown(appendEntriesMsgBody.Term)

	result := map[string]interface{}{
		"type":    structs.MsgTypeAppendEntriesResult,
		"term":    raft.currentTerm,
		"success": false,
	}

	if appendEntriesMsgBody.Term < raft.currentTerm {
		// leader is behind us
		log.Println("leader is behind us func appendEntriesappendEntries", appendEntriesMsgBody.Term, raft.currentTerm)
		raft.node.Reply(msg, result)
		return nil
	}

	// This leader is valid; remember them and don't try to run our own election for a bit
	//raft.leaderId = appendEntriesMsgBody.LeaderId
	raft.leaderId = appendEntriesMsgBody.LeaderId
	raft.resetElectionDeadline()

	// Check previous entry To see if it matches
	if appendEntriesMsgBody.PrevLogIndex <= 0 {
		panic(fmt.Errorf("out of bounds previous log index %d \n", appendEntriesMsgBody.PrevLogIndex))
	}

	if appendEntriesMsgBody.PrevLogIndex > len(raft.log.Entries) || (appendEntriesMsgBody.PrevLogTerm != raft.log.get(appendEntriesMsgBody.PrevLogIndex).Term) {
		// We disagree on the previous term
		log.Println("We disagree on the previous Term func appendEntries", appendEntriesMsgBody.Term, raft.currentTerm)
		raft.node.Reply(msg, result)
		return nil
	}

	// We agree on the previous log Term; truncate and append
	raft.log.truncate(appendEntriesMsgBody.PrevLogIndex)
	raft.log.append(appendEntriesMsgBody.Entries)

	// Advance commit pointer
	if raft.commitIndex < appendEntriesMsgBody.LeaderCommit {
		raft.commitIndex = min(appendEntriesMsgBody.LeaderCommit, raft.log.size())
		raft.advanceCommitIndex()
	}

	// Acknowledge
	result["success"] = true
	raft.node.Reply(msg, result)
	return nil
}

func (raft *RaftNode) setupHandlers() error {
	// Handle Client KV requests
	kvRequests := func(msg maelstrom.Message, op structs.Operation) error {
		raft.leaderStateMu.Lock()
		defer raft.leaderStateMu.Unlock()

		if raft.state == StateLeader {
			raft.log.append([]structs.Entry{{
				Term: raft.currentTerm,
				Op:   &op,
				Msg:  msg,
			}})
			//res := raft.stateMachine.apply(op)
			//if err := raft.node.Reply(msg, res); err != nil {
			//	panic(err)
			//}
		} else if raft.leaderId != "" {
			// We're not the leader, but we can proxy To one
			msg.Dest = raft.leaderId
			raft.node.Send(raft.leaderId, msg.Body)
		} else {
			return raft.node.Reply(msg, &structs.ErrorMsgBody{
				Type: structs.MsgTypeError,
				Code: 11,
				Text: "not a leader",
			})
		}

		return nil
	}

	kvReadRequest := func(msg maelstrom.Message) error {
		var readMsgBody structs.ReadMsgBody
		err := json.Unmarshal(msg.Body, &readMsgBody)
		if err != nil {
			panic(err)
		}

		return kvRequests(msg, structs.Operation{
			Type:   readMsgBody.Type,
			MsgId:  readMsgBody.MsgId,
			Key:    readMsgBody.Key,
			Client: readMsgBody.Client,
		})
	}

	kvWriteRequest := func(msg maelstrom.Message) error {
		var writeMsgBody structs.WriteMsgBody
		err := json.Unmarshal(msg.Body, &writeMsgBody)
		if err != nil {
			panic(err)
		}

		return kvRequests(msg, structs.Operation{
			Type:   writeMsgBody.Type,
			MsgId:  int(writeMsgBody.MsgId),
			Key:    writeMsgBody.Key,
			Client: writeMsgBody.Client,
			Value:  writeMsgBody.Value,
		})
	}

	kvCasRequest := func(msg maelstrom.Message) error {
		var casMsgBody structs.CasMsgBody
		err := json.Unmarshal(msg.Body, &casMsgBody)
		if err != nil {
			panic(err)
		}

		return kvRequests(msg, structs.Operation{
			Type:   casMsgBody.Type,
			MsgId:  casMsgBody.MsgId,
			Key:    casMsgBody.Key,
			Client: casMsgBody.Client,
			From:   casMsgBody.From,
			To:     casMsgBody.To,
		})
	}

	raft.node.Handle(string(structs.MsgTypeRead), kvReadRequest)
	raft.node.Handle(string(structs.MsgTypeWrite), kvWriteRequest)
	raft.node.Handle(string(structs.MsgTypeCas), kvCasRequest)
	raft.node.Handle(string(structs.MsgTypeRequestVote), raft.requestVote)
	raft.node.Handle(string(structs.MsgTypeAppendEntries), raft.appendEntries)
	return nil
}
