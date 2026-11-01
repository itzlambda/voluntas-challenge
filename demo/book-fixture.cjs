// Test records built independently from the documented SBE wire layout.
function record(type, token = '123', data = {}) {
  const block = type === 1 ? 16 : 33;
  const bids = data.bids || [[450001n, 12345678901234567n]], asks = data.asks || [[550000n, 2500000n]];
  const size = 8 + block + (type === 1 ? 8 + (bids.length + asks.length) * 16 : 0) + 4 + token.length;
  const buf = Buffer.alloc(size); let at = 0;
  buf.writeUInt16LE(block, 0); buf.writeUInt16LE(type, 2); buf.writeUInt16LE(1, 4); buf.writeUInt16LE(2, 6); at = 8 + block;
  if (type === 1) {
    for (const levels of [bids, asks]) {
      buf.writeUInt16LE(16, at); buf.writeUInt16LE(levels.length, at + 2); at += 4;
      for (const [price, qty] of levels) { buf.writeBigInt64LE(price, at); buf.writeBigInt64LE(qty, at + 8); at += 16; }
    }
  } else { buf.writeBigInt64LE(data.price ?? 450001n, 24); buf.writeBigInt64LE(data.quantity ?? 0n, 32); buf[40] = data.side ?? 0; }
  buf.writeUInt32LE(token.length, at); at += 4; buf.write(token, at); return buf;
}
const frame = (...records) => { const b = Buffer.concat(records); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };

module.exports = {record, frame};
