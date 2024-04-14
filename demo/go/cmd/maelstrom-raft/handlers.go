package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/pavan/maelstrom/demo/go/cmd/maelstrom-raft/structs"
)

// Handle initialization message
//func (raft *RaftNode) raftInit(msg Msg) error {
//	if raft.state != StateNascent {
//		return fmt.Errorf("Can't init twice")
//	}
//
//	raft.setNodeId(msg.Body.NodeId)
//	raft.nodeIds = msg.Body.NodeIds
//	//raft.becomeFollower()
//
//	log.Println("I am: ", raft.nodeId)
//	raft.net.reply(msg, map[string]interface{}{"type": initOkMsgBodyType})
//	return nil
//}

//// When a node requests our vote...
//func (raft *RaftNode) requestVote(msg Msg) error {
//	if err := raft.maybeStepDown(msg.Body.Term); err != nil {
//		return err
//	}
//	grant := false
//
//	if msg.Body.Term < raft.currentTerm {
//		log.Printf("candidate Term %d lower than %d not granting vote \n", msg.Body.Term, raft.currentTerm)
//	} else if raft.votedFor != "" {
//		log.Printf("already voted for %s not granting vote \n", raft.votedFor)
//	} else if msg.Body.LastLogTerm < raft.log.lastTerm() {
//		log.Printf("have log Entries From Term %d which is newer than remote Term %d not granting vote\n", raft.log.lastTerm(), msg.Body.LastLogTerm)
//	} else if msg.Body.LastLogTerm == raft.log.lastTerm() && msg.Body.LastLogIndex < raft.log.size() {
//		log.Printf("our logs are both at Term %d but our log is %d and theirs is only %d \n", raft.log.lastTerm(), raft.log.size(), msg.Body.LastLogIndex)
//	} else {
//		log.Printf("Granting vote To %s\n", msg.Src)
//		grant = true
//		raft.votedFor = msg.Body.CandidateId
//		raft.resetElectionDeadline()
//	}
//
//	raft.net.reply(msg, map[string]interface{}{
//		"type":         requestVoteResultMsgType,
//		"term":         raft.currentTerm,
//		"vote_granted": grant,
//	})
//
//	return nil
//}

//func (raft *RaftNode) appendEntries(msg Msg) error {
//	log.Println("begin func appendEntries", msg)
//	if err := raft.maybeStepDown(msg.Body.Term); err != nil {
//		return err
//	}
//
//	result := map[string]interface{}{
//		"type":    appendEntriesResultMsgType,
//		"term":    raft.currentTerm,
//		"success": false,
//	}
//
//	if msg.Body.Term < raft.currentTerm {
//		// leader is behind us
//		log.Println("leader is behind us func appendEntries", msg.Body.Term, raft.currentTerm)
//		raft.net.reply(msg, result)
//		return nil
//	}
//
//	// This leader is valid; remember them and don't try to run our own election for a bit
//	raft.leaderId = msg.Body.LeaderId
//	raft.resetElectionDeadline()
//
//	// Check previous entry To see if it matches
//	if msg.Body.PrevLogIndex <= 0 {
//		return fmt.Errorf("out of bounds previous log index %d \n", msg.Body.PrevLogIndex)
//	}
//
//	if msg.Body.PrevLogIndex < len(raft.log.Entries) && (msg.Body.PrevLogTerm != raft.log.get(msg.Body.PrevLogIndex).Term) {
//		// We disagree on the previous Term
//		log.Println("We disagree on the previous Term func appendEntries", msg.Body.Term, raft.currentTerm)
//		raft.net.reply(msg, result)
//		return nil
//	}
//
//	// We agree on the previous log Term; truncate and append
//	raft.log.truncate(msg.Body.PrevLogIndex)
//	raft.log.append(msg.Body.Entries)
//
//	// Advance commit pointer
//	if raft.commitIndex < msg.Body.LeaderCommit {
//		raft.commitIndex = min(msg.Body.LeaderCommit, raft.log.size())
//	}
//
//	log.Println("end func appendEntries", msg)
//	// Acknowledge
//	result["success"] = true
//	raft.net.reply(msg, result)
//	return nil
//}

func (raft *RaftNode) setupHandlers() error {
	// Handle Client KV requests
	kvRequests := func(msg maelstrom.Message, op structs.Operation) error {
		res := raft.stateMachine.apply(op)
		if err := raft.node.Reply(msg, res); err != nil {
			panic(err)
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
	return nil
}
