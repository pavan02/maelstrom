package structs

type ResponseBody interface {
	SetInReplyTo(int)
}

type InitOkMsgBody struct {
	Type      MsgType `mapstructure:"type" json:"type"`
	InReplyTo int     `mapstructure:"in_reply_to" json:"in_reply_to"`
}

func (msgBody *InitOkMsgBody) SetInReplyTo(inReplyTo int) {
	msgBody.InReplyTo = inReplyTo
}

type ReadOkMsgBody struct {
	Type      MsgType `mapstructure:"type" json:"type"`
	Value     int     `mapstructure:"value" json:"value"`
	InReplyTo int     `mapstructure:"in_reply_to" json:"in_reply_to"`
}

func (msgBody *ReadOkMsgBody) SetInReplyTo(inReplyTo int) {
	msgBody.InReplyTo = inReplyTo
}

type WriteOkMsgBody struct {
	Type      MsgType `mapstructure:"type" json:"type"`
	InReplyTo int     `mapstructure:"in_reply_to" json:"in_reply_to"`
}

func (msgBody *WriteOkMsgBody) SetInReplyTo(inReplyTo int) {
	msgBody.InReplyTo = inReplyTo
}

type CasOkMsgBody struct {
	Type      MsgType `mapstructure:"type" json:"type"`
	InReplyTo int     `mapstructure:"in_reply_to" json:"in_reply_to"`
}

func (msgBody *CasOkMsgBody) SetInReplyTo(inReplyTo int) {
	msgBody.InReplyTo = inReplyTo
}

type ErrorMsgBody struct {
	Type      MsgType `mapstructure:"type" json:"type"`
	Code      ErrCode `mapstructure:"code" json:"code"`
	Text      string  `mapstructure:"text" json:"text"`
	InReplyTo int     `mapstructure:"in_reply_to" json:"in_reply_to"`
}

func (msgBody *ErrorMsgBody) SetInReplyTo(inReplyTo int) {
	msgBody.InReplyTo = inReplyTo
}

type RequestVoteResMsgBody struct {
	Type         MsgType `mapstructure:"type" json:"type"`
	Term         int     `mapstructure:"term" json:"term"`
	VotedGranted bool    `mapstructure:"vote_granted" json:"vote_granted"`
	InReplyTo    int     `mapstructure:"in_reply_to" json:"in_reply_to"`
}

func (msgBody *RequestVoteResMsgBody) SetInReplyTo(inReplyTo int) {
	msgBody.InReplyTo = inReplyTo
}

type AppendEntriesResMsgBody struct {
	Type      MsgType `mapstructure:"type" json:"type"`
	Term      int     `mapstructure:"term" json:"term"`
	Success   bool    `mapstructure:"success" json:"success"`
	InReplyTo int     `mapstructure:"in_reply_to" json:"in_reply_to"`
}

func (msgBody *AppendEntriesResMsgBody) SetInReplyTo(inReplyTo int) {
	msgBody.InReplyTo = inReplyTo
}
