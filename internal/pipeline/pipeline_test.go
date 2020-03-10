package pipeline_test

import (
	"testing"

	"github.com/alxarch/red/internal/pipeline"
)

func TestQueue(t *testing.T) {
	q := new(pipeline.State)
	q.ReplySkip()
	if q.Dirty() {
		t.Errorf("ReplySkip dirty")
	}
	q.Command()
	if q.Dirty() {
		t.Errorf("ReplySkip dirty")
	}
	q.Command()
	if !q.Dirty() {
		t.Errorf("Queue not dirty")
	}

}

func TestState_ClientReplySkipInMulti(t *testing.T) {
	state := new(pipeline.State)
	if ok := state.Multi(); !ok {
		t.Errorf("Invalid non ok MULTI")

	}
	if ok := state.Multi(); ok {
		t.Errorf("Invalid ok MULTI")
	}
	if state.ReplySkip(); state.IsReplySkip() {
		t.Errorf("ReplySkip in MULTI")
	}

}
