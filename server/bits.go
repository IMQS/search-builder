package server

// Utilities for packing/unpacking values that are less than 8 bits.

func load2Bit(storage []uint8, item int) uint8 {
	u32 := uint32(item)
	bucket := u32 / 4
	bit := (u32 & 3) * 2
	return uint8((storage[bucket] >> bit) & 3)
}

func store2bit(storage []uint8, item int, value uint8) {
	u32 := uint32(item)
	bucket := u32 / 4
	bit := (u32 & 3) * 2
	storage[bucket] = (storage[bucket] &^ (3 << bit)) | (value << bit)
}

func getBit(storage []uint32, item int) bool {
	u32 := uint32(item)
	bucket := u32 / 32
	bit := u32 & 31
	return (storage[bucket]>>bit)&1 == 1
}

func setBit(storage []uint32, item int, value bool) {
	u32 := uint32(item)
	bucket := u32 / 32
	bit := u32 & 31
	v := uint32(0)
	if value {
		v = 1
	}
	storage[bucket] = (storage[bucket] &^ (uint32(1) << bit)) | (v << bit)
}
