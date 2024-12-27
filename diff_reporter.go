package optimistic

import "github.com/google/go-cmp/cmp"

type diffReporter struct {
	path  cmp.Path
	diffs map[string]map[string]any
}

func newDiffReporter() *diffReporter {
	return &diffReporter{
		diffs: make(map[string]map[string]any),
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

	// Build a readable path as a string
	pathKey := r.path.GoString()

	// Extract left-hand (from) and right-hand (to) values
	var x, y any
	if len(r.path) > 0 {
		lastStep := r.path.Last()
		rx, ry := lastStep.Values() // Right-hand value
		if rx.IsValid() {
			if rx.CanAddr() {
				x = rx.Elem().Interface()
			} else {
				x = rx.Interface()
			}
		}
		if ry.IsValid() {
			if ry.CanAddr() {
				y = ry.Elem().Interface()
			} else {
				y = ry.Interface()
			}
		}
	}

	// Store the difference
	r.diffs[pathKey] = map[string]any{
		"from": x,
		"to":   y,
	}
}

// Diff returns the collected differences as a map.
func (r *diffReporter) Diff() map[string]map[string]any {
	return r.diffs
}
