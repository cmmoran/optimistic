package optimistic

import (
	"encoding/json"

	"github.com/google/go-cmp/cmp"
)

type Change struct {
	From any `json:"from" mapstructure:"from"`
	To   any `json:"to" mapstructure:"to"`
}

func (c Change) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"from": c.From,
		"to":   c.To,
	}
	return json.MarshalIndent(m, "", "  ")
}

func (c *Change) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, c)
}

func (c Change) String() string {
	b, _ := c.MarshalJSON()
	return string(b)
}

type diffReporter struct {
	path  cmp.Path
	diffs map[string]Change
}

func newDiffReporter() *diffReporter {
	return &diffReporter{
		diffs: make(map[string]Change),
	}
}

// PushStep adds the current path step to the stack.
func (r *diffReporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

// PopStep removes the last path step from the stack.
func (r *diffReporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

// Report is called for each comparison result.
// If the values differ, it logs the difference.
func (r *diffReporter) Report(rs cmp.Result) {
	if rs.Equal() {
		return
	}

	// Build a human-readable key
	key := r.path.GoString()

	// Grab the last step and extract values
	step := r.path.Last()
	vx, vy := step.Values() // assuming Values() returns (left, right) reflect.Value

	var from, to any
	if vx.IsValid() && vx.CanInterface() {
		from = vx.Interface()
	}
	if vy.IsValid() && vy.CanInterface() {
		to = vy.Interface()
	}

	// Store it in a typed struct for clarity
	r.diffs[key] = Change{From: from, To: to}
}

// Diff returns the collected differences as a map.
func (r *diffReporter) Diff() map[string]Change {
	return r.diffs
}

func (r *diffReporter) Path() cmp.Path {
	return r.path
}

func (r *diffReporter) Reset() *diffReporter {
	r.path = make(cmp.Path, 0)
	r.diffs = make(map[string]Change)
	return r
}
