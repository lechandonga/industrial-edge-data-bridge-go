package protocol

import "bytes"

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
