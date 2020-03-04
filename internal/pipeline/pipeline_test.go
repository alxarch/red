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
