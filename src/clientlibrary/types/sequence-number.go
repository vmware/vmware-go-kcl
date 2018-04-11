package types

// ExtendedSequenceNumber represents a two-part sequence number for records aggregated by the Kinesis Producer Library.
//
// The KPL combines multiple user records into a single Kinesis record. Each user record therefore has an integer
// sub-sequence number, in addition to the regular sequence number of the Kinesis record. The sub-sequence number
// is used to checkpoint within an aggregated record.
type ExtendedSequenceNumber struct {
	sequenceNumber    string
	subSequenceNumber int64
}
