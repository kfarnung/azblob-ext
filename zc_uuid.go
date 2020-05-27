package azblobext

import (
	"crypto/rand"
)

// The UUID reserved variants.
const reservedRFC4122 byte = 0x40

// A UUID representation compliant with specification in RFC 4122 document.
type uuid [16]byte

// NewUUID returns a new uuid using RFC 4122 algorithm.
func newUUID() (u uuid) {
	u = uuid{}
	// Set all bits to randomly (or pseudo-randomly) chosen values.
	rand.Read(u[:])
	u[8] = (u[8] | reservedRFC4122) & 0x7F // u.setVariant(ReservedRFC4122)

	var version byte = 4
	u[6] = (u[6] & 0xF) | (version << 4) // u.setVersion(4)
	return
}

func (u uuid) bytes() []byte {
	return u[:]
}
