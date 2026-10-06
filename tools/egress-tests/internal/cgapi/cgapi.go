// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package cgapi is the HTTP contract between the egress-tests driver and the
// cgroup reader DaemonSet: the samples route and the rows it returns.
package cgapi

// SamplesRoute returns the rows after ?since=<seq>, at most MaxRows of
// them. ?since=latest returns no rows, only where the ring is. Each
// ?target=<path>, a cgroup path relative to the cgroup root, is sampled from
// then on, until it goes TargetTTL without a request.
const SamplesRoute = "/samples"

// SinceLatest is the since value that skips every row already held.
const SinceLatest = "latest"

// MaxRows bounds one response; More says to ask again from the last row.
const MaxRows = 5000

// Port is where the reader serves.
const Port = 8080

// Row is one cgroup read once. Leaf is empty for the target itself, or the
// name of a child cgroup of it.
type Row struct {
	Seq uint64 `json:"seq"`
	// WallNanos is the node's wall clock; UptimeSeconds is /proc/uptime, a
	// monotonic clock for intervals.
	WallNanos     int64   `json:"wallNanos"`
	UptimeSeconds float64 `json:"uptimeSeconds"`
	Target        string  `json:"target"`
	Leaf          string  `json:"leaf,omitempty"`
	// Gone is set on the first read after the cgroup disappeared; the
	// counters are then those of the last read.
	Gone            bool  `json:"gone,omitempty"`
	UsageUsec       int64 `json:"usageUsec"`
	NrPeriods       int64 `json:"nrPeriods"`
	NrThrottled     int64 `json:"nrThrottled"`
	ThrottledUsec   int64 `json:"throttledUsec"`
	CPUSomeUsec     int64 `json:"cpuSomeUsec"`
	MemoryCurrent   int64 `json:"memoryCurrent"`
	WorkingSetBytes int64 `json:"workingSetBytes"`
}

// Response is the answer to the samples route.
type Response struct {
	// WallNanos and UptimeSeconds are the node's clocks when it answered.
	WallNanos     int64   `json:"wallNanos"`
	UptimeSeconds float64 `json:"uptimeSeconds"`
	// Oldest is the oldest seq still held; a since below it lost rows.
	Oldest uint64 `json:"oldest"`
	// Latest is the newest seq written. A since above it, or a changed
	// Boot, means the reader restarted.
	Latest uint64 `json:"latest"`
	// Boot is when this reader process started, in Unix nanoseconds.
	Boot int64 `json:"boot"`
	Rows []Row `json:"rows"`
	More bool  `json:"more,omitempty"`
}
