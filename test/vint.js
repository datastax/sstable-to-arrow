/**
 * JavaScript varint definition for debugging using the Kaitai Web IDE.
 */
class Vint {
    constructor(_io) {
        this._io = _io;
    }

    _read() {
        this.val = this._io.readU1();

        // negative value
        if ((this.val & 0x80) > 0) {
            let size = 0;
            // count number of leading ones
            for (let track = 0x80; (this.val & track) > 0; track >>= 1)
                size++;
            // get val after the first zero
            this.val = this.val & (0xff >> size);
            for (let i = 0; i < size; ++i) {
                const b = this._io.readU1();
                this.val <<= 8;
                this.val |= b & 0xff;
            }
        }
    }
}
this.Vint = Vint;

class DeserializationHelper {
    constructor(_io) {}
}
this.DeserializationHelper = DeserializationHelper;

module.exports = function read_vint(byte_array) {
    let k = 0;
    let val = byte_array[k++];

    if ((val & 0x80) > 0) {
        let size = 0;
        // count number of leading ones
        for (let track = 0x80; (val & track) > 0; track >>= 1)
            size++;
        // get val after the first zero
        val = val & (0xff >> size);
        for (let i = 0; i < size; ++i) {
            const b = byte_array[k++];
            val <<= 8;
            val |= b & 0xff;
        }
    }
    return [k, val];
}
